# Copyright 2025 The android_world Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Controller for Android that adds UI tree information to the observation."""

import contextlib
import enum
import os
import subprocess
import threading
import time
from typing import Any
from typing import cast
from typing import Optional
from absl import logging
from android_env import env_interface
from android_env import loader
from android_env.components import config_classes
from android_env.proto.a11y import android_accessibility_forest_pb2
from android_env.wrappers import a11y_grpc_wrapper
from android_env.wrappers import base_wrapper
from android_world.env import adb_utils
from android_world.env import representation_utils
from android_world.utils import file_utils
import dm_env


# Lock for ADB reconnection to prevent concurrent reconnection attempts
_adb_reconnect_lock = threading.Lock()

# Throttle check_airplane_mode: at most once per interval to reduce ADB load
_last_airplane_check: dict[int, float] = {}
_AIRPLANE_CHECK_INTERVAL = 30.0


def _has_wrapper(
    env: env_interface.AndroidEnvInterface,
    target_wrapper: Any,
) -> bool:
  """Checks recursively if an environment object has a certain wrapper.

  Args:
    env: The environment object potentially wrapped.
    target_wrapper: The wrapper type to search for.

  Returns:
    True if the target_wrapper is found, otherwise False.
  """
  if isinstance(env, target_wrapper):
    return True
  elif hasattr(env, '_env'):
    return _has_wrapper(env._env, target_wrapper)  # pylint: disable=protected-access
  else:
    return False


def get_a11y_tree(
    env: env_interface.AndroidEnvInterface,
    max_retries: int = 10,
    sleep_duration: float = 2.0,
) -> android_accessibility_forest_pb2.AndroidAccessibilityForest:
  """Gets a11y tree.

  Args:
    env: AndroidEnv.
    max_retries: Maximum number of retries to get a11y tree.
    sleep_duration: Time to sleep between each retry in seconds.

  Returns:
    A11y tree.

  Raises:
    RuntimeError: If the a11y tree was not able to be retrieved.
  """
  if not _has_wrapper(env, a11y_grpc_wrapper.A11yGrpcWrapper):
    raise ValueError(
        'Must use a11y_grpc_wrapper.A11yGrpcWrapper to get the a11y tree.'
    )
  env = cast(a11y_grpc_wrapper.A11yGrpcWrapper, env)
  # Throttle airplane mode check to reduce ADB load under concurrency
  env_id = id(env)
  now = time.time()
  if now - _last_airplane_check.get(env_id, 0) > _AIRPLANE_CHECK_INTERVAL:
    _last_airplane_check[env_id] = now
    if adb_utils.retry(1)(adb_utils.check_airplane_mode)(env):
      logging.warning(
          'Airplane mode is on -- cannot retrieve a11y tree via gRPC. Turning'
          ' it off...'
      )
      logging.info('Enabling networking...')
      env.attempt_enable_networking()
      time.sleep(1.0)

  forest: Optional[
      android_accessibility_forest_pb2.AndroidAccessibilityForest
  ] = None
  for _ in range(max_retries):
    try:
      forest = env.accumulate_new_extras()['accessibility_tree'][-1]  # pytype:disable=attribute-error
      return forest
    except KeyError:
      logging.warning('Could not get a11y tree, retrying.')
    time.sleep(sleep_duration)

  if forest is None:
    raise RuntimeError('Could not get a11y tree.')
  return forest


_TASK_PATH = file_utils.convert_to_posix_path(
    file_utils.get_local_tmp_directory(), 'default.textproto'
)
DEFAULT_ADB_PATH = '~/Android/Sdk/platform-tools/adb'


# UI tree-specific keys that are added to observations:

# The forest is essentially a comprehensive snapshot of all user interface
# elements currently displayed on an Android device's screen. Each 'tree' in
# this 'forest' represents the accessibility details of a different window or
# screen section, providing structured information. The tree's origin is from
# the AccessibilityService. Please see the following for more detail:
# https://developer.android.com/reference/android/accessibilityservice/AccessibilityService

OBSERVATION_KEY_FOREST = 'forest'
# UI elements are specific nodes extracted from forest. See
# representation_utils.forest_to_ui_elements for details.
OBSERVATION_KEY_UI_ELEMENTS = 'ui_elements'


class A11yMethod(enum.Enum):
  """Method to get a11y tree."""

  # Custom gRPC wrapper that uses a11y forwarder app.
  A11Y_FORWARDER_APP = 'a11y_forwarder_app'

  # From `uiautomator dump``.
  UIAUTOMATOR = 'uiautomator'

  # No A11y tree retrieval
  NONE = 'none'


def _is_a11y_forwarder_installed(env: env_interface.AndroidEnvInterface) -> bool:
  """Check if accessibility forwarder app is already installed."""
  from android_env.proto import adb_pb2

  check_request = adb_pb2.AdbRequest(
      generic=adb_pb2.AdbRequest.GenericRequest(
          args=['shell', 'pm', 'list', 'packages',
                'com.google.androidenv.accessibilityforwarder']
      )
  )
  response = env.execute_adb_call(check_request)
  if response.status == adb_pb2.AdbResponse.Status.OK:
    output = response.generic.output.decode('utf-8', errors='ignore')
    if 'com.google.androidenv.accessibilityforwarder' in output:
      logging.info('Accessibility forwarder app is already installed.')
      return True
  return False


def apply_a11y_forwarder_app_wrapper(
    env: env_interface.AndroidEnvInterface, install_a11y_forwarding_app: bool
) -> env_interface.AndroidEnvInterface:
  # Check if already installed to avoid redundant download
  should_install = install_a11y_forwarding_app and not _is_a11y_forwarder_installed(env)
  if install_a11y_forwarding_app and not should_install:
    logging.info('Skipping accessibility forwarder installation (already installed).')

  wrapper = a11y_grpc_wrapper.A11yGrpcWrapper(
      env,
      install_a11y_forwarding=should_install,
      start_a11y_service=True,
      enable_a11y_tree_info=True,
      latest_a11y_info_only=True,
  )

  # In remote/Docker mode, the emulator runs inside a container.
  # The Forwarder app connects to 10.0.2.2:<port> which resolves to
  # the Docker container, not the host. Use "adb reverse" so that
  # device:<port> is forwarded back to the host's gRPC server.
  # Use "-s <device>" to target the correct device when multiple
  # Docker instances are running.
  if _is_remote_mode():
    a11y_port = wrapper.get_port()
    adb_path = os.path.expanduser(
        os.environ.get('ANDROID_SDK_ROOT', '~/Android/Sdk') + '/platform-tools/adb'
    )
    device_name = _get_remote_device_name()
    adb_server_port = int(os.getenv("ANDROID_ADB_SERVER_PORT", "5037"))
    logging.info(
        'Remote mode: setting up adb reverse for a11y gRPC port %s on device %s (adb server port: %s)',
        a11y_port, device_name, adb_server_port,
    )
    _setup_adb_reverse(adb_path, device_name, a11y_port, adb_server_port)

  return wrapper


class AndroidWorldController(base_wrapper.BaseWrapper):
  """Controller for an Android instance that adds accessibility tree data.

  The Accessibility Tree in Android is a tree-based structure, originally for
  for assisting accessibility services. It provides information about UI
  elements (like text, buttons, and images) in a hierarchical format. The tree
  includes details such as the properties and actions available for each
  element.
  """

  def __init__(
      self,
      env: env_interface.AndroidEnvInterface,
      a11y_method: A11yMethod = A11yMethod.A11Y_FORWARDER_APP,
      install_a11y_forwarding_app: bool = True,
  ):
    self._original_env = env
    self._a11y_port = None
    self._is_remote = _is_remote_mode()
    self._adb_path = os.path.expanduser(
        os.environ.get('ANDROID_SDK_ROOT', '~/Android/Sdk') + '/platform-tools/adb'
    )
    self._device_name = _get_remote_device_name() if self._is_remote else None
    self._adb_server_port = int(os.getenv("ANDROID_ADB_SERVER_PORT", "5037"))

    if a11y_method == A11yMethod.A11Y_FORWARDER_APP:
      self._env = apply_a11y_forwarder_app_wrapper(
          env, install_a11y_forwarding_app
      )
      self._env.reset()  # Initializes required server services in a11y wrapper.
      # Store the a11y port for reconnection
      if _has_wrapper(self._env, a11y_grpc_wrapper.A11yGrpcWrapper):
        wrapper = cast(a11y_grpc_wrapper.A11yGrpcWrapper, self._env)
        self._a11y_port = wrapper.get_port()
    else:
      self._env = env
    self._a11y_method = a11y_method

  @property
  def device_screen_size(self) -> tuple[int, int]:
    """Returns the physical screen size of the device: (width, height)."""
    return adb_utils.get_screen_size(self._env)

  @property
  def logical_screen_size(self) -> tuple[int, int]:
    """Returns the logical screen size of the device.

    This will be different with the physical size if orientation or resolution
    is changed.
    """
    return adb_utils.get_logical_screen_size(self._env)

  @property
  def env(self) -> env_interface.AndroidEnvInterface:
    return self._env

  def check_adb_connection(self) -> bool:
    """Check if ADB connection is alive.

    Returns:
      True if connected, False otherwise.
    """
    if not self._is_remote:
      # For local emulator, assume always connected
      return True
    return _check_adb_connection(
        self._adb_path, self._device_name, self._adb_server_port
    )

  def ensure_adb_connection(self) -> bool:
    """Ensure ADB connection is alive, reconnect if necessary.

    Returns:
      True if connection is established, False if reconnection failed.
    """
    if not self._is_remote:
      return True

    with _adb_reconnect_lock:
      if self.check_adb_connection():
        return True

      logging.warning(
          'ADB connection lost to device %s, attempting to reconnect...',
          self._device_name
      )

      # Reconnect to ADB
      if not _adb_connect_remote(
          self._adb_path, self._device_name, self._adb_server_port
      ):
        logging.error('Failed to reconnect to ADB device %s', self._device_name)
        return False

      # Re-establish adb reverse if we have an a11y port
      if self._a11y_port:
        if not _setup_adb_reverse(
            self._adb_path, self._device_name, self._a11y_port, self._adb_server_port
        ):
          logging.warning(
              'Failed to re-establish adb reverse for port %s',
              self._a11y_port
          )
          # Continue anyway, the a11y service might still work

      logging.info('Successfully reconnected to ADB device %s', self._device_name)
      return True

  def restore_adb_reverse(self) -> bool:
    """Restore adb reverse mapping if in remote mode.

    This is useful when ADB server was restarted but connection is still alive.

    Returns:
      True if successful or not in remote mode, False otherwise.
    """
    if not self._is_remote or not self._a11y_port:
      return True

    return _setup_adb_reverse(
        self._adb_path, self._device_name, self._a11y_port, self._adb_server_port
    )

  def execute_adb_with_retry(
      self,
      adb_func,
      *args,
      max_retries: int = 2,
      **kwargs
  ):
    """Execute an ADB function with automatic reconnection on failure.

    Args:
      adb_func: The ADB function to execute.
      *args: Positional arguments to pass to the function.
      max_retries: Maximum number of retry attempts.
      **kwargs: Keyword arguments to pass to the function.

    Returns:
      The result of the ADB function.

    Raises:
      The last exception if all retries fail.
    """
    last_exception = None
    for attempt in range(max_retries + 1):
      try:
        return adb_func(*args, **kwargs)
      except Exception as e:
        last_exception = e
        if attempt < max_retries:
          logging.warning(
              'ADB operation failed (attempt %d/%d): %s. Attempting to reconnect...',
              attempt + 1, max_retries + 1, e
          )
          if self.ensure_adb_connection():
            time.sleep(0.5)  # Brief pause before retry
            continue
        raise last_exception

  def refresh_env(self):
    # pylint: disable=protected-access
    # pytype: disable=attribute-error
    # Reconnect to emulator and reload a11y wrapper in case we lose connection.

    # First ensure ADB connection is alive
    if self._is_remote:
      logging.info('Ensuring ADB connection before refreshing environment...')
      _adb_connect_remote(
          self._adb_path, self._device_name, self._adb_server_port
      )

    new_controller = get_controller(
        console_port=self.env._coordinator._simulator._config.emulator_launcher.emulator_console_port,
        adb_path=self.env._coordinator._simulator._config.adb_controller.adb_path,
        grpc_port=self.env._coordinator._simulator._config.emulator_launcher.grpc_port,
    )
    self._env = new_controller.env
    self._a11y_port = new_controller._a11y_port
    # pylint: enable=protected-access
    # pytype: enable=attribute-error

  def _get_a11y_forest(
      self,
      max_retries: int = 5,
      sleep_duration: float = 1.0,
  ) -> android_accessibility_forest_pb2.AndroidAccessibilityForest:
    return get_a11y_tree(self._env, max_retries=max_retries, sleep_duration=sleep_duration)

  def _wait_for_a11y_service_ready(
      self,
      max_wait_time: float = 60.0,
      check_interval: float = 2.0,
  ) -> bool:
    """Wait for a11y service to be ready after refresh_env.

    Instead of a fixed sleep, actively probe the service until it responds.

    Args:
      max_wait_time: Maximum time to wait in seconds.
      check_interval: Time between checks in seconds.

    Returns:
      True if service is ready, False if timeout.
    """
    start_time = time.time()
    attempt = 0
    while time.time() - start_time < max_wait_time:
      attempt += 1
      try:
        # Try a quick probe with minimal retries
        self._get_a11y_forest(max_retries=1, sleep_duration=0.5)
        logging.info(
            f'A11y service ready after {attempt} attempts '
            f'({time.time() - start_time:.1f}s)'
        )
        return True
      except (RuntimeError, KeyError):
        logging.debug(
            f'A11y service not ready yet (attempt {attempt}), waiting...'
        )
        time.sleep(check_interval)

    logging.warning(
        f'A11y service not ready after {max_wait_time}s, proceeding anyway'
    )
    return False

  def _restart_a11y_forwarder(self) -> bool:
    """Re-enable the AccessibilityForwarder service on the device.

    This is a lightweight recovery for cases where the accessibility service
    was disrupted (e.g. by uiautomator dump), without doing a full refresh_env.

    Returns:
      True if the restart commands succeeded, False otherwise.
    """
    if not self._a11y_port:
      return False

    try:
      adb_path = self._adb_path
      server_port = str(self._adb_server_port)
      device_args = ['-s', self._device_name] if self._device_name else []

      # Step 1: Re-enable the accessibility service
      cmd = [adb_path, '-P', server_port] + device_args + [
          'shell', 'settings', 'put', 'secure',
          'enabled_accessibility_services',
          'com.google.androidenv.accessibilityforwarder/'
          'com.google.androidenv.accessibilityforwarder.AccessibilityForwarder',
      ]
      result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
      if result.returncode != 0:
        logging.warning('Failed to re-enable a11y service: %s', result.stderr)
        return False

      logging.info('Re-enabled AccessibilityForwarder service')
      time.sleep(2.0)  # Give the service time to start

      # Step 2: For remote mode, restore adb reverse + gRPC port broadcast
      if self._is_remote:
        _setup_adb_reverse(
            adb_path, self._device_name, self._a11y_port, self._adb_server_port
        )
      else:
        # For local mode, send the gRPC port broadcast via adb
        cmd = [adb_path, '-P', server_port] + device_args + [
            'shell', 'am', 'broadcast',
            '-a', 'accessibility_forwarder.intent.action.SET_GRPC',
            '--ei', 'port', str(self._a11y_port),
            '-n', 'com.google.androidenv.accessibilityforwarder/'
                  'com.google.androidenv.accessibilityforwarder.FlagsBroadcastReceiver',
        ]
        subprocess.run(cmd, capture_output=True, text=True, timeout=30)

      logging.info('Sent gRPC port %s to AccessibilityForwarder', self._a11y_port)
      return True
    except Exception as e:
      logging.warning('Failed to restart a11y forwarder: %s', e)
      return False

  def get_a11y_forest(
      self,
  ) -> android_accessibility_forest_pb2.AndroidAccessibilityForest:
    """Returns the most recent a11y forest from the device.

    Recovery strategy (from lightest to heaviest):
      1. Direct fetch with retries
      2. Restart AccessibilityForwarder service (handles uiautomator disruption)
      3. Full environment refresh (handles ADB disconnection / deep failures)
    """
    try:
      return self._get_a11y_forest()
    except RuntimeError:
      pass

    # Step 2: Lightweight recovery — re-enable AccessibilityForwarder.
    # Handles cases where the service was disrupted by external tools
    # (e.g. uiautomator dump) without needing a full env refresh.
    # For remote mode this also restores adb reverse + gRPC port broadcast.
    logging.info('A11y tree fetch failed, attempting to restart AccessibilityForwarder...')
    if self._restart_a11y_forwarder():
      if self._wait_for_a11y_service_ready(max_wait_time=15.0, check_interval=1.0):
        try:
          return self._get_a11y_forest(max_retries=3, sleep_duration=1.0)
        except RuntimeError:
          pass

    # Step 3: Full refresh — rebuild the entire android_env + a11y wrapper.
    print(
        'Could not get a11y tree. Reconnecting to Android, reinitializing'
        ' AndroidEnv, and restarting a11y forwarding.'
    )
    self.refresh_env()
    if self._wait_for_a11y_service_ready(max_wait_time=60.0, check_interval=2.0):
      return self._get_a11y_forest(max_retries=3, sleep_duration=1.0)
    else:
      return self._get_a11y_forest(max_retries=10, sleep_duration=2.0)

  def get_ui_elements(self) -> list[representation_utils.UIElement]:
    """Returns the most recent UI elements from the device."""
    # Ensure ADB connection before getting UI elements
    self.ensure_adb_connection()

    if self._a11y_method == A11yMethod.A11Y_FORWARDER_APP:
      return representation_utils.forest_to_ui_elements(
          self.get_a11y_forest(),
          exclude_invisible_elements=True,
      )
    elif self._a11y_method == A11yMethod.UIAUTOMATOR:
      return representation_utils.xml_dump_to_ui_elements(
          adb_utils.uiautomator_dump(self._env)
      )
    else:
      return []

  def _process_timestep(self, timestep: dm_env.TimeStep) -> dm_env.TimeStep:
    """Adds a11y tree info to the observation."""
    if self._a11y_method == A11yMethod.A11Y_FORWARDER_APP:
      forest = self.get_a11y_forest()
      ui_elements = representation_utils.forest_to_ui_elements(
          forest,
          exclude_invisible_elements=True,
      )
    else:
      forest = None
      ui_elements = self.get_ui_elements()
    timestep.observation[OBSERVATION_KEY_FOREST] = forest
    timestep.observation[OBSERVATION_KEY_UI_ELEMENTS] = ui_elements
    return timestep

  def pull_file(
      self, remote_db_file_path: str, timeout_sec: Optional[float] = None
  ) -> contextlib._GeneratorContextManager[str]:
    """Pulls a file from the device to a temporary directory.

    The directory will be deleted when the context manager exits.
    Args:
      remote_db_file_path: The path to the file on the device.
      timeout_sec: Timeout in seconds for the adb calls.

    Returns:
      The path to the temporary directory containing the file.
    """
    # Ensure ADB connection before file operation
    self.ensure_adb_connection()

    remote_db_directory = os.path.dirname(remote_db_file_path)
    return file_utils.tmp_directory_from_device(
        remote_db_directory, self.env, timeout_sec
    )

  def push_file(
      self,
      local_db_file_path: str,
      remote_db_file_path: str,
      timeout_sec: Optional[float] = None,
  ) -> None:
    """Pushes a local file to the device."""
    # Ensure ADB connection before file operation
    self.ensure_adb_connection()

    remote_db_directory = os.path.dirname(remote_db_file_path)

    # First delete old .db, .db-wal, and .db-shm files.
    file_utils.clear_directory(remote_db_directory, self)
    file_utils.copy_data_to_device(
        local_db_file_path,
        remote_db_file_path,
        self.env,
        timeout_sec,
    )

    # Restore SELinux security context so the app can read the pushed file.
    adb_utils.issue_generic_request(
        ["shell", "restorecon", "-R", remote_db_directory], self.env
    )


def _write_default_task_proto() -> str:
  with open(_TASK_PATH, 'w') as f:
    f.write("""\
id: "default"

name: "Default task for device control."
description: "Empty task"

max_episode_sec: 7200  # Prevent infinite episodes.
  """)
  return _TASK_PATH


def _is_remote_mode() -> bool:
  """Check if running in remote/Docker mode."""
  return os.getenv("ANDROID_CONNECTION_TYPE") == "Remote"


def _get_remote_device_name() -> str:
  """Get device name for remote mode (host:port format)."""
  host = os.getenv("ANDROID_REMOTE_HOST", "localhost")
  port = os.getenv("ANDROID_ADB_PORT", "5555")
  return f"{host}:{port}"


def _check_adb_connection(adb_path: str, device_name: str, adb_server_port: int = 5037) -> bool:
  """Check if ADB connection to device is alive.

  Args:
    adb_path: Path to adb binary.
    device_name: Device name in host:port format.
    adb_server_port: ADB server port to use.

  Returns:
    True if device is connected and responsive, False otherwise.
  """
  try:
    adb_path = os.path.expanduser(adb_path)
    cmd = [adb_path, "-P", str(adb_server_port), "-s", device_name, "shell", "echo", "ping"]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=15
    )
    return result.returncode == 0 and "ping" in result.stdout
  except Exception as e:
    logging.warning("ADB connection check failed: %s", e)
    return False


def _setup_adb_reverse(adb_path: str, device_name: str, a11y_port: int, adb_server_port: int = 5037) -> bool:
  """Set up adb reverse for a11y gRPC port with retry.

  Args:
    adb_path: Path to adb binary.
    device_name: Device name in host:port format.
    a11y_port: The a11y gRPC port to forward.
    adb_server_port: ADB server port to use.

  Returns:
    True if setup was successful, False otherwise.
  """
  import random as _random

  adb_path = os.path.expanduser(adb_path)
  max_retries = 3

  for attempt in range(1, max_retries + 1):
    try:
      logging.info(
          'Setting up adb reverse for a11y gRPC port %s on device %s (attempt %d/%d)',
          a11y_port, device_name, attempt, max_retries,
      )

      # Set up adb reverse
      result = subprocess.run(
          [adb_path, '-P', str(adb_server_port), '-s', device_name,
           'reverse', f'tcp:{a11y_port}', f'tcp:{a11y_port}'],
          capture_output=True, text=True, timeout=30,
      )
      if result.returncode != 0:
        logging.warning('adb reverse failed: %s', result.stderr)
        if attempt < max_retries:
          time.sleep(2 + _random.uniform(0, 1))
          continue
        return False

      # Tell the Forwarder App to connect to localhost
      result = subprocess.run(
          [adb_path, '-P', str(adb_server_port), '-s', device_name,
           'shell', 'am', 'broadcast',
           '-a', 'accessibility_forwarder.intent.action.SET_GRPC',
           '--es', 'host', 'localhost',
           '--ei', 'port', str(a11y_port),
           '-n', 'com.google.androidenv.accessibilityforwarder/com.google.androidenv.accessibilityforwarder.FlagsBroadcastReceiver'],
          capture_output=True, text=True, timeout=30,
      )
      if result.returncode != 0:
        logging.warning('Failed to set Forwarder App gRPC target: %s', result.stderr)
        if attempt < max_retries:
          time.sleep(2 + _random.uniform(0, 1))
          continue
        return False

      logging.info(
          'Successfully set up adb reverse and Forwarder App gRPC target to localhost:%s',
          a11y_port,
      )
      return True
    except Exception as e:
      logging.warning(
          'adb reverse attempt %d/%d failed for port %s on device %s: %s',
          attempt, max_retries, a11y_port, device_name, e,
      )
      if attempt < max_retries:
        time.sleep(2 + _random.uniform(0, 1))

  return False


def _adb_connect_remote(adb_path: str, device_name: str, adb_server_port: int = 5037) -> bool:
  """Connect to remote ADB device with retry.

  Args:
    adb_path: Path to adb binary.
    device_name: Device name in host:port format.
    adb_server_port: ADB server port to use (default 5037).

  Returns:
    True if connection successful, False otherwise.
  """
  import random as _random

  adb_path = os.path.expanduser(adb_path)
  max_retries = 3

  for attempt in range(1, max_retries + 1):
    try:
      cmd = [adb_path, "-P", str(adb_server_port), "connect", device_name]
      logging.info("Executing (attempt %d/%d): %s", attempt, max_retries, " ".join(cmd))
      result = subprocess.run(
          cmd,
          capture_output=True,
          text=True,
          timeout=30
      )

      output = result.stdout + result.stderr
      logging.info("adb connect output: %s", output)

      if "connected" in output.lower() or "already connected" in output.lower():
        logging.info("Successfully connected to remote device: %s", device_name)
        return True
      else:
        logging.warning("Failed to connect to remote device: %s, output: %s",
                        device_name, output)
    except Exception as e:
      logging.warning("adb connect attempt %d/%d failed: %s", attempt, max_retries, e)

    if attempt < max_retries:
      delay = 2 * attempt + _random.uniform(0, 1)
      logging.info("Retrying adb connect in %.1fs...", delay)
      time.sleep(delay)

  logging.error("Failed to connect to remote device %s after %d attempts", device_name, max_retries)
  return False


def _load_android_env(config: config_classes.AndroidEnvConfig):
  """Custom loader that supports remote mode.

  This replaces loader.load() to support remote ADB connections.
  """
  from android_env import environment
  from android_env.components import coordinator as coordinator_lib
  from android_env.components import device_settings as device_settings_lib
  from android_env.components import task_manager as task_manager_lib
  from android_env.proto import task_pb2
  from android_world.env.android_world_emulator_simulator import AndroidWorldEmulatorSimulator
  from google.protobuf import text_format

  # Load task
  task = task_pb2.Task()
  if isinstance(config.task, config_classes.FilesystemTaskConfig):
    with open(config.task.path, 'r') as proto_file:
      text_format.Parse(proto_file.read(), task)

  task_manager = task_manager_lib.TaskManager(task)

  # Process emulator config (expand paths)
  if isinstance(config.simulator, config_classes.EmulatorConfig):
    launcher_config = config.simulator.emulator_launcher
    launcher_config.android_avd_home = os.path.expanduser(
        launcher_config.android_avd_home)
    launcher_config.android_sdk_root = os.path.expanduser(
        launcher_config.android_sdk_root)
    launcher_config.emulator_path = os.path.expanduser(
        launcher_config.emulator_path)
    config.simulator.adb_controller.adb_path = os.path.expanduser(
        config.simulator.adb_controller.adb_path)

    simulator = AndroidWorldEmulatorSimulator(config=config.simulator)
  else:
    raise ValueError(f'Unsupported simulator config: {config.simulator}')

  device_settings = device_settings_lib.DeviceSettings(simulator)
  coordinator = coordinator_lib.Coordinator(
      simulator, task_manager, device_settings)

  return environment.AndroidEnv(
      simulator=simulator, coordinator=coordinator, task_manager=task_manager)


def get_controller(
    console_port: int = 5554,
    adb_path: str = DEFAULT_ADB_PATH,
    grpc_port: int = int(os.getenv("ANDROID_GRPC_PORT", "8554")),
) -> AndroidWorldController:
  """Creates a controller by connecting to an existing Android environment."""

  # Check if running in remote mode
  is_remote = _is_remote_mode()
  remote_device_name = _get_remote_device_name() if is_remote else None

  # Get ADB server port from environment variable (default 5037)
  # Each parallel execution group should use a different ADB server port
  # to avoid conflicts when one process restarts the ADB server
  adb_server_port = int(os.getenv("ANDROID_ADB_SERVER_PORT", "5037"))
  logging.info("Using ADB server port: %d", adb_server_port)

  if is_remote:
    logging.info("Running in remote mode, device: %s", remote_device_name)
    # Connect to remote device first
    _adb_connect_remote(adb_path, remote_device_name, adb_server_port)

  config = config_classes.AndroidEnvConfig(
      task=config_classes.FilesystemTaskConfig(
          path=_write_default_task_proto()
      ),
      simulator=config_classes.EmulatorConfig(
          emulator_launcher=config_classes.EmulatorLauncherConfig(
              emulator_console_port=console_port,
              adb_port=console_port + 1,
              grpc_port=grpc_port,
          ),
          adb_controller=config_classes.AdbControllerConfig(
              adb_path=adb_path,
              adb_server_port=adb_server_port,
          ),
      ),
  )

  # Use custom loader for remote mode, standard loader otherwise
  if is_remote:
    android_env_instance = _load_android_env(config)
  else:
    android_env_instance = loader.load(config)

  logging.info('Setting up AndroidWorldController.')
  return AndroidWorldController(
      android_env_instance
  )
