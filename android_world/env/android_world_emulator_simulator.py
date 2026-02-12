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

"""Extended EmulatorSimulator for android_world project."""

import os
import subprocess
import time

from absl import logging
from android_env.components import adb_controller
from android_env.components import config_classes
from android_env.components.simulators.emulator import emulator_simulator


def _is_remote_mode() -> bool:
  """Check if running in remote/Docker mode."""
  return os.getenv("ANDROID_CONNECTION_TYPE") == "Remote"


def _get_remote_device_name() -> str:
  """Get device name for remote mode (host:port format)."""
  host = os.getenv("ANDROID_REMOTE_HOST", "localhost")
  port = os.getenv("ANDROID_ADB_PORT", "5555")
  return f"{host}:{port}"


class RemoteAdbController(adb_controller.AdbController):
  """Extended AdbController that reconnects TCP devices after server restart.

  When running in remote/Docker mode, ADB connects to devices via TCP
  (e.g., localhost:5581). Unlike local emulators which are automatically
  detected after ADB server restart, TCP-connected devices need to be
  explicitly reconnected using 'adb connect'.

  This class overrides _restart_server() to automatically reconnect to
  the remote device after restarting the ADB server.
  """

  def __init__(self, config: config_classes.AdbControllerConfig):
    # Disable ADB mDNS discovery to prevent cross-contamination between concurrent instances
    os.environ['ADB_MDNS_OPENSCREEN'] = '0'
    super().__init__(config)
    self._remote_device_name = _get_remote_device_name() if _is_remote_mode() else None

  def _restart_server(self, timeout: float | None = None):
    """Kills and restarts the adb server, then reconnects remote devices.

    Args:
      timeout: A timeout to use for this operation. If not set the default
        timeout set on the constructor will be used.
    """
    restart_timeout = 10.0  # Fixed short timeout to avoid inheriting caller's long timeout

    logging.info('Restarting adb server.')
    # kill-server may fail if the server is already dead — that's fine
    try:
      self.execute_command(
          ['kill-server'], timeout=restart_timeout, device_specific=False)
    except Exception:
      logging.warning('kill-server failed, proceeding with start-server anyway')
    time.sleep(0.2)

    cmd_output = self.execute_command(
        ['start-server'], timeout=restart_timeout, device_specific=False)
    logging.info('start-server output: %r', cmd_output.decode('utf-8'))
    time.sleep(2.0)

    if self._remote_device_name:
      # Remote mode: reconnect first, then devices verification happens inside
      self._reconnect_remote_device(restart_timeout)
    else:
      # Local mode: just check devices list
      try:
        self.execute_command(
            ['devices'], timeout=restart_timeout, device_specific=False)
      except Exception:
        logging.warning('devices check failed after restart')
      time.sleep(0.2)

  def _reconnect_remote_device(self, timeout: float | None = None):
    """Reconnects to the remote ADB device after server restart.

    Args:
      timeout: A timeout to use for this operation.
    """
    logging.info('Reconnecting to remote device: %s', self._remote_device_name)
    try:
      # Use subprocess directly to avoid recursion through execute_command
      # which might trigger another _restart_server if it fails
      adb_path = self._config.adb_path
      server_port = self._config.adb_server_port
      cmd = [adb_path, '-P', str(server_port), 'connect', self._remote_device_name]

      result = subprocess.run(
          cmd,
          capture_output=True,
          text=True,
          timeout=timeout or 30
      )

      output = result.stdout + result.stderr
      logging.info('adb connect output: %s', output.strip())

      if 'connected' in output.lower() or 'already connected' in output.lower():
        logging.info('Successfully reconnected to remote device: %s', self._remote_device_name)
        # Wait a bit for the connection to stabilize
        time.sleep(1.0)
        # Best-effort devices verification — connect already succeeded, don't block on this
        try:
          devices_output = self.execute_command(['devices'], timeout=10, device_specific=False)
          logging.info('Devices after reconnect: %s', devices_output.decode('utf-8').strip())
        except Exception:
          logging.warning('devices check after reconnect timed out, but connect succeeded')
      else:
        logging.warning('Failed to reconnect to remote device: %s, output: %s',
                        self._remote_device_name, output)
    except Exception as e:
      logging.error('Error reconnecting to remote device %s: %s',
                    self._remote_device_name, e)


class AndroidWorldEmulatorSimulator(emulator_simulator.EmulatorSimulator):
  """Extended EmulatorSimulator for android_world project.

  Supports both local and remote ADB connections:
  - Local mode: uses default "emulator-<port>" format
  - Remote mode: uses "host:port" format (e.g., "localhost:5555")
  """

  def adb_device_name(self) -> str:
    """Returns the ADB device name based on connection mode."""
    if _is_remote_mode():
      return _get_remote_device_name()
    return super().adb_device_name()

  def create_adb_controller(self):
    """Returns an ADB controller which can communicate with this simulator.

    In remote mode, returns a RemoteAdbController that automatically
    reconnects to TCP devices after ADB server restart.
    """
    if _is_remote_mode():
      logging.info('Using RemoteAdbController for remote mode')
      return RemoteAdbController(self._config.adb_controller)
    return adb_controller.AdbController(self._config.adb_controller)
