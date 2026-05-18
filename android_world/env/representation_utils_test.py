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

import dataclasses
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
from android_world.env import representation_utils


@dataclasses.dataclass(frozen=True)
class BoundsInScreen:
  left: int
  right: int
  top: int
  bottom: int


@dataclasses.dataclass
class FakeNode:
  text: str = ''
  view_id_resource_name: str = ''
  class_name: str = ''
  bounds_in_screen: BoundsInScreen = dataclasses.field(
      default_factory=lambda: BoundsInScreen(1, 3, 2, 4)
  )
  is_focused: bool = False
  is_editable: bool = False
  text_selection_start: int = 0
  text_selection_end: int = 0


def _fake_forest(nodes: list[FakeNode]):
  tree = mock.MagicMock()
  tree.nodes = nodes
  window = mock.MagicMock()
  window.tree = tree
  forest = mock.MagicMock()
  forest.windows = [window]
  return forest


class TestAccessibilityNodeToUIElement(parameterized.TestCase):

  @parameterized.named_parameters(
      dict(
          testcase_name='rectangle_to_rectangle_normalization',
          node_bounds=BoundsInScreen(0, 150, 0, 100),
          screen_size=(500, 500),
          expected_normalized_bbox=representation_utils.BoundingBox(
              0.0, 0.3, 0.0, 0.2
          ),
      ),
      dict(
          testcase_name='square_to_square_normalization',
          node_bounds=BoundsInScreen(100, 200, 100, 200),
          screen_size=(1000, 1000),
          expected_normalized_bbox=representation_utils.BoundingBox(
              0.1, 0.2, 0.1, 0.2
          ),
      ),
      dict(
          testcase_name='square_to_rectangle_normalization',
          node_bounds=BoundsInScreen(0, 100, 0, 100),
          screen_size=(1000, 500),
          expected_normalized_bbox=representation_utils.BoundingBox(
              0.0, 0.1, 0.0, 0.2
          ),
      ),
      dict(
          testcase_name='no_change_square_normalization',
          node_bounds=BoundsInScreen(0, 100, 0, 100),
          screen_size=(100, 100),
          expected_normalized_bbox=representation_utils.BoundingBox(
              0.0, 1.0, 0.0, 1.0
          ),
      ),
      dict(
          testcase_name='no_change_rectangle_normalization',
          node_bounds=BoundsInScreen(0, 200, 0, 100),
          screen_size=(200, 100),
          expected_normalized_bbox=representation_utils.BoundingBox(
              0.0, 1.0, 0.0, 1.0
          ),
      ),
      dict(
          testcase_name='normalization_causing_dimensions_to_grow',
          node_bounds=BoundsInScreen(0, 50, 0, 50),
          screen_size=(200, 200),
          expected_normalized_bbox=representation_utils.BoundingBox(
              0.0, 0.25, 0.0, 0.25
          ),
      ),
      dict(
          testcase_name='zero_size_bbox_normalization',
          node_bounds=BoundsInScreen(0, 0, 0, 0),
          screen_size=(100, 100),
          expected_normalized_bbox=representation_utils.BoundingBox(
              0.0, 0.0, 0.0, 0.0
          ),
      ),
      dict(
          testcase_name='no_normalization',
          node_bounds=BoundsInScreen(10, 20, 11, 13),
          screen_size=None,
          expected_normalized_bbox=None,
      ),
  )
  def test_normalize_bboxes(
      self, node_bounds, screen_size, expected_normalized_bbox
  ):
    node = mock.MagicMock()
    node.bounds_in_screen = node_bounds

    ui_element = representation_utils.accessibility_node_to_ui_element(
        node, screen_size
    )
    self.assertEqual(ui_element.bbox_pixels.x_min, node_bounds.left)
    self.assertEqual(ui_element.bbox_pixels.x_max, node_bounds.right)
    self.assertEqual(ui_element.bbox_pixels.y_min, node_bounds.top)
    self.assertEqual(ui_element.bbox_pixels.y_max, node_bounds.bottom)

    if screen_size is not None:
      ui_element.bbox = representation_utils._normalize_bounding_box(
          ui_element.bbox_pixels, screen_size
      )
    self.assertEqual(ui_element.bbox, expected_normalized_bbox)


class TestForestToCursorInfoXml(absltest.TestCase):

  def test_focused_editable_zero_cursor_is_reported(self):
    forest = _fake_forest([
        FakeNode(
            text='hello & bye',
            view_id_resource_name='pkg:id/editor',
            class_name='android.widget.EditText',
            is_focused=True,
            is_editable=True,
            text_selection_start=0,
            text_selection_end=0,
        )
    ])

    self.assertEqual(
        representation_utils.forest_to_cursor_info_xml(forest),
        '<CursorInfo>\n'
        '  <cursor text="hello &amp; bye" resource-id="pkg:id/editor" '
        'class="android.widget.EditText" bounds="[1,2][3,4]" '
        'focused="true" editable="true" text-selection-start="0" '
        'text-selection-end="0" cursor-position="0" />\n'
        '</CursorInfo>',
    )

  def test_unfocused_default_selection_is_ignored(self):
    forest = _fake_forest([
        FakeNode(
            text='hello',
            class_name='android.widget.TextView',
            text_selection_start=0,
            text_selection_end=0,
        )
    ])

    self.assertEqual(representation_utils.forest_to_cursor_info_xml(forest), '')

  def test_selection_range_without_focus_is_reported(self):
    forest = _fake_forest([
        FakeNode(
            text='abcdef',
            class_name='android.widget.TextView',
            text_selection_start=1,
            text_selection_end=4,
        )
    ])

    self.assertEqual(
        representation_utils.forest_to_cursor_info_xml(forest),
        '<CursorInfo>\n'
        '  <cursor text="abcdef" resource-id="" '
        'class="android.widget.TextView" bounds="[1,2][3,4]" '
        'focused="false" editable="false" text-selection-start="1" '
        'text-selection-end="4" />\n'
        '</CursorInfo>',
    )


if __name__ == '__main__':
  absltest.main()
