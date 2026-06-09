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
  content_description: str = ''
  bounds_in_screen: BoundsInScreen = dataclasses.field(
      default_factory=lambda: BoundsInScreen(1, 3, 2, 4)
  )
  unique_id: int = 1
  child_ids: list[int] = dataclasses.field(default_factory=list)
  is_clickable: bool = False
  is_scrollable: bool = False
  is_selected: bool = False
  is_checked: bool = False
  is_enabled: bool = True
  is_focusable: bool = False
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


class TestForestToRawXml(absltest.TestCase):

  def test_focused_editable_zero_cursor_is_embedded_on_node(self):
    forest = _fake_forest([
        FakeNode(
            text='hello & bye',
            view_id_resource_name='pkg:id/editor',
            class_name='android.widget.EditText',
            is_focusable=True,
            is_focused=True,
            is_editable=True,
            text_selection_start=0,
            text_selection_end=0,
        )
    ])

    self.assertEqual(
        representation_utils.forest_to_raw_xml(forest),
        '<hierarchy rotation="0">\n'
        '  <node text="hello &amp; bye" resource-id="pkg:id/editor" '
        'class="android.widget.EditText" content-desc="" clickable="false" '
        'scrollable="false" selected="false" checked="false" enabled="true" '
        'focusable="true" focused="true" editable="true" bounds="[1,2][3,4]" '
        'text-selection-start="0" text-selection-end="0" cursor-position="0" />\n'
        '</hierarchy>',
    )

  def test_unfocused_default_selection_is_not_embedded(self):
    forest = _fake_forest([
        FakeNode(
            text='hello',
            class_name='android.widget.TextView',
            text_selection_start=0,
            text_selection_end=0,
        )
    ])

    xml = representation_utils.forest_to_raw_xml(forest)

    self.assertIn('bounds="[1,2][3,4]"', xml)
    self.assertNotIn('text-selection-start', xml)
    self.assertNotIn('cursor-position', xml)

  def test_negative_selection_is_not_embedded(self):
    forest = _fake_forest([
        FakeNode(
            text='hello',
            class_name='android.widget.TextView',
            text_selection_start=-1,
            text_selection_end=-1,
        )
    ])

    xml = representation_utils.forest_to_raw_xml(forest)

    self.assertIn('bounds="[1,2][3,4]"', xml)
    self.assertNotIn('text-selection-start', xml)
    self.assertNotIn('cursor-position', xml)


if __name__ == '__main__':
  absltest.main()
