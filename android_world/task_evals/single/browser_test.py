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

import ast
import pathlib
from unittest import mock

from absl.testing import absltest
import numpy as np

from android_world.env import interface
from android_world.env import representation_utils
from android_world.task_evals.single import browser


def _browser_draw_html():
  browser_path = pathlib.Path(__file__).with_name("browser.py")
  module = ast.parse(browser_path.read_text())
  for node in module.body:
    if isinstance(node, ast.ClassDef) and node.name == "BrowserDraw":
      for class_node in node.body:
        if (
            isinstance(class_node, ast.Assign)
            and len(class_node.targets) == 1
            and isinstance(class_node.targets[0], ast.Name)
            and class_node.targets[0].id == "HTML"
        ):
          return ast.literal_eval(class_node.value)
  raise AssertionError("BrowserDraw.HTML not found.")


def _browser_draw_success_from_final_pixels(task_colors, pixels):
  color_counts = {color: 0 for color in task_colors}
  for color in pixels:
    if color in color_counts:
      color_counts[color] += 1
  return all(color_counts[color] > 0 for color in task_colors)


def _state_with_texts(*texts):
  return interface.State(
      pixels=np.zeros((1, 1, 3), dtype=np.uint8),
      forest=None,
      ui_elements=[
          representation_utils.UIElement(text=text) for text in texts
      ],
  )


class _FakeEnv:

  def __init__(self, states):
    self.controller = object()
    self._states = list(states)
    self.get_state_call_count = 0

  def get_state(self, wait_to_stabilize: bool = False):
    del wait_to_stabilize
    self.get_state_call_count += 1
    if len(self._states) > 1:
      return self._states.pop(0)
    return self._states[0]


class BrowserTaskTest(absltest.TestCase):

  def test_success_check_retries_stale_accessibility_tree(self):
    env = _FakeEnv([
        _state_with_texts('Memory Task', 'Enter the product', 'Submit'),
        _state_with_texts('Memory Task', '20250', 'Submit', 'Success!'),
    ])
    task = browser.BrowserMultiply({'browser_task_seed': 1})

    with mock.patch.object(
        browser.adb_utils,
        'get_current_activity',
        return_value=('com.android.chrome/org.chromium.ChromeActivity', None),
    ), mock.patch.object(browser.time, 'sleep') as mock_sleep:
      self.assertEqual(task.is_successful(env), 1.0)

    self.assertEqual(env.get_state_call_count, 2)
    mock_sleep.assert_called_once_with(5.0)


class BrowserDrawTest(absltest.TestCase):

  def test_canvas_draws_thick_strokes(self):
    html = _browser_draw_html()

    self.assertIn("ctx.lineWidth = 6;", html)
    self.assertIn("ctx.lineCap = 'round';", html)
    self.assertIn("ctx.lineJoin = 'round';", html)

  def test_evaluator_counts_exact_target_pixels(self):
    html = _browser_draw_html()

    self.assertIn("const colorCounts = Object.fromEntries", html)
    self.assertIn("taskColors.map(color => [color, 0])", html)
    self.assertIn("colorCounts[color]++", html)
    self.assertIn(
        "taskColors.every(color => colorCounts[color] > 0)", html
    )
    self.assertNotIn("const usedColors = new Set();", html)
    self.assertNotIn("usedColors.has(color)", html)

  def test_visible_overlap_with_all_target_colors_passes(self):
    task_colors = ["#ff0000", "#008080", "#00ff00"]
    final_pixels = [
        "#ff0000",
        "#ff0000",
        "#008080",
        "#00ff00",
        "#000000",
    ]

    self.assertTrue(
        _browser_draw_success_from_final_pixels(task_colors, final_pixels)
    )

  def test_fully_covered_target_color_fails(self):
    task_colors = ["#ff0000", "#008080", "#00ff00"]
    final_pixels = [
        "#ff0000",
        "#ff0000",
        "#00ff00",
        "#00ff00",
        "#000000",
    ]

    self.assertFalse(
        _browser_draw_success_from_final_pixels(task_colors, final_pixels)
    )


if __name__ == "__main__":
  absltest.main()
