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

from absl.testing import absltest


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
