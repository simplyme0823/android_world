
""" Midscene Agent"""


from android_world.agents import base_agent
from android_world.env import interface
from android_world.env import representation_utils

import requests
import os
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler


class MidsceneAgent(base_agent.EnvironmentInteractingAgent):
  def __init__(
      self,
      env: interface.AsyncEnv,
  ):
    """Initializes a Midscene Agent.
    name: The agent name.
    """
    super().__init__(env, "MidsceneAgent")
    self.history = []
    self.run_log = []
    self._init_json_rpc();
    self.step_count = 0
    self.task_status = {}
    self.failed_step_reason = ''
    self._dom_server = None
    self._dom_server_url = None
    self._start_dom_server()

  def reset(self, go_home: bool = False) -> None:
    super().reset(go_home)
    # Hide pointer-location traces that can confuse vision-based drawing tasks.
    self.env.hide_automation_ui()
    self.step_count = 0

  def start_new_task(self, task_name: str, task_id: str) -> None:
    """Starts a new task."""
    self._formatted_console("Starting new task, name:  " + task_name + " id: " + task_id)
    self.current_task_name = "Task-" + task_id + "-" +  str(task_name)

    device = { "type": "Android" }

    # Get common ports
    console_port = os.environ.get("ANDROID_CONSOLE_PORT", "5554")
    adb_port = os.environ.get("ANDROID_ADB_PORT", "5555")

    if os.environ.get("ANDROID_CONNECTION_TYPE") == "Local":
      # Local mode: use explicit device ID if provided, otherwise derive from console port
      device_id = os.environ.get("ANDROID_ADB_DEVICE_ID")
      if device_id:
        device["deviceId"] = device_id
      else:
        device["deviceId"] = f"emulator-{console_port}"
    else:
      # Remote mode: use host and adb port
      device["host"] = os.environ.get("ANDROID_REMOTE_HOST", "localhost")
      device["port"] = adb_port

    rpc_params = {"type": "Android", "device": device, "id": self.current_task_name}
    if self._dom_server_url:
      rpc_params["domProviderUrl"] = self._dom_server_url
    self._send_rpc_request("new-agent", rpc_params)

    self.step_count = 0


  # Set max steps of all tasks to be 1
  def set_max_steps(self, max_steps: int) -> None:
    self._max_steps = 1


  def step(self, goal: str, ) -> base_agent.AgentInteractionResult:
    """Performs a step of the agent on the environment.
    goal: The goal.
    """
    self.step_count += 1

    self._formatted_console("Step: " + str(self.step_count)  + "; Goal: " + goal  )

    midscene_res = self._send_rpc_request("run-ai-method", {"id": self.current_task_name, "task": goal})


    self.run_log.append(midscene_res)

    self.failed_step_reason = '';

    if midscene_res['result']['code'] == 1:
       
      action_raw_res = midscene_res['result'].get('data', '')

      self.env.interaction_cache = str(action_raw_res)

      return base_agent.AgentInteractionResult(
        done=True,
        data={ 
          "midscene_action_response": action_raw_res
        },
      )
    else:
      self.failed_step_reason = midscene_res['result']['data'].get('reason', '')
      return base_agent.AgentInteractionResult(
        done=False,
        data={},
      )

  def update_task_status(self, status: str = 'Failed') -> None:
    self.task_status[self.current_task_name] = status
    self._send_rpc_request("terminate-agent", {"id": self.current_task_name, "userTaskStatus": self.task_status.get(self.current_task_name, 'Failed'),  'agentStepError':  self.failed_step_reason })

  def _init_json_rpc(self):
    """Initializes the JSON-RPC connection to the Midscene server."""
    self.rpc_url = os.environ.get("MIDSCENE_BENCH_RPC_URL")
    if not self.rpc_url:
      raise RuntimeError("MIDSCENE_BENCH_RPC_URL environment variable not set.")

  def _send_rpc_request(self, method: str, params: dict) -> dict:
    """Sends a JSON-RPC request to the Midscene server."""
    headers = {'Content-Type': 'application/json'}
    payload = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": time.time()
    }

    request_cnt = 0;
    response = None

    while request_cnt < 3:
      request_cnt += 1
      try:
        response = requests.post(self.rpc_url, headers=headers, json=payload)
        break
      except Exception as e:
        self._formatted_console("RPC Request Failed: " + str(e) + "; Retry: " + str(request_cnt))


    if response is None:
      raise RuntimeError(self._formatted_console("Failed to send RPC request"))

    response.raise_for_status()
    result = response.json()
    self._formatted_console("RPC Response: " + str(result))

    return result

  def _start_dom_server(self):
    """Starts a background HTTP server that serves the current a11y tree as raw XML."""
    agent_ref = self

    class DomHandler(BaseHTTPRequestHandler):
      def do_GET(self):
        try:
          state = agent_ref.env.get_state()
          if state.forest is not None:
            raw_xml = representation_utils.forest_to_raw_xml(state.forest)
          else:
            raw_xml = ''
          self.send_response(200)
          self.send_header('Content-Type', 'text/xml; charset=utf-8')
          self.end_headers()
          self.wfile.write(raw_xml.encode('utf-8'))
        except Exception as e:
          self.send_response(500)
          self.send_header('Content-Type', 'text/plain')
          self.end_headers()
          self.wfile.write(str(e).encode('utf-8'))

      def log_message(self, format, *args):
        pass  # Suppress default access logs

    server = HTTPServer(('127.0.0.1', 0), DomHandler)
    port = server.server_address[1]
    self._dom_server = server
    self._dom_server_url = f'http://127.0.0.1:{port}/dom'
    self._formatted_console(f"DOM provider server started at {self._dom_server_url}")

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

  def _formatted_console(self, content: str) -> None:
    """Formats the console output."""
    print("[MidsceneAgent] " + content)
