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

from unittest import mock

from absl.testing import absltest
from android_env import env_interface
from android_env.proto import adb_pb2
from android_world.env import actuation
from android_world.env import adb_utils
from android_world.utils import contacts_utils


def _adb_response(output: str = ""):
  response = adb_pb2.AdbResponse()
  response.generic.output = output.encode("utf-8")
  return response


_EMPTY_RESPONSE = _adb_response()


@mock.patch.object(adb_utils, "issue_generic_request")
@mock.patch.object(actuation, "find_and_click_element")
class TestContactsUtils(absltest.TestCase):

  def test_add_contact(self, mock_click_element, mock_generic_request):
    """Test adding a contact."""
    mock_env = mock.create_autospec(env_interface.AndroidEnvInterface)

    phone_number = "+123456789"
    name = "Emma Watson"
    contacts_utils.add_contact(name, phone_number, mock_env)

    # Construct the expected adb command
    expected_adb_command = [
        "shell",
        (
            "am start -a android.intent.action.INSERT -t"
            f' vnd.android.cursor.dir/contact -e name "{name}" -e phone'
            f" {phone_number}"
        ),
    ]

    # Assert that the correct adb command was issued
    mock_generic_request.assert_called_once_with(expected_adb_command, mock_env)

    # Assert that the _click_element method was called with the correct argument
    mock_click_element.assert_called_once_with("SAVE", mock_env)

  def test_list_contacts(self, unused_mock_click_element, mock_generic_request):
    """Test listing all contacts."""
    mock_env = mock.create_autospec(env_interface.AndroidEnvInterface)
    adb_response = adb_pb2.AdbResponse()
    adb_response.generic.output = """
      Row: 0 display_name=Jane Doe, number=1 (234) 567-89
      Row: 0 display_name=Chen, number=98765
    """.encode("utf-8")
    mock_generic_request.return_value = adb_response

    contacts = contacts_utils.list_contacts(mock_env)

    self.assertEqual(
        contacts,
        [
            contacts_utils.Contact("Jane Doe", "123456789"),
            contacts_utils.Contact("Chen", "98765"),
        ],
    )

    # Construct the expected adb command
    expected_adb_command = [
        "shell",
        (
            "content query --uri content://contacts/phones/ --projection"
            " display_name:number"
        ),
    ]

    # Assert that the correct adb command was issued
    mock_generic_request.assert_called_once_with(expected_adb_command, mock_env)

  def test_clear_contacts(
      self, unused_mock_click_element, mock_generic_request
  ):
    """Test clearing all contacts."""
    mock_env = mock.create_autospec(env_interface.AndroidEnvInterface)

    contacts_utils.clear_contacts(mock_env)

    # Construct the expected adb command
    expected_adb_command = [
        "shell",
        "pm",
        "clear",
        "com.android.providers.contacts",
    ]

    # Assert that the correct adb command was issued
    mock_generic_request.assert_called_once_with(expected_adb_command, mock_env)

  def test_has_contact_exact_number(
      self, unused_mock_click_element, mock_generic_request
  ):
    mock_env = mock.create_autospec(env_interface.AndroidEnvInterface)
    mock_generic_request.return_value = _adb_response(
        "Row: 0 display_name=Gabriel Ibrahim, number=+19696338338"
    )

    self.assertTrue(
        contacts_utils.has_contact("Gabriel Ibrahim", "+19696338338", mock_env)
    )

  def test_has_contact_matches_formatted_us_number(
      self, unused_mock_click_element, mock_generic_request
  ):
    mock_env = mock.create_autospec(env_interface.AndroidEnvInterface)
    mock_generic_request.return_value = _adb_response(
        "Row: 0 display_name=Gabriel Ibrahim, number=(969) 633-8338"
    )

    self.assertTrue(
        contacts_utils.has_contact("Gabriel Ibrahim", "+19696338338", mock_env)
    )

  def test_add_contact_verified_success_first_attempt(
      self, mock_click_element, mock_generic_request
  ):
    mock_env = mock.create_autospec(env_interface.AndroidEnvInterface)
    mock_generic_request.side_effect = [
        _EMPTY_RESPONSE,
        _adb_response("Row: 0 display_name=Gabriel Ibrahim, number=+19696338338"),
    ]

    contacts_utils.add_contact_verified(
        "Gabriel Ibrahim",
        "+19696338338",
        mock_env,
        ui_delay_sec=0,
        retry_delay_sec=0,
    )

    mock_click_element.assert_called_once_with("SAVE", mock_env)

  def test_add_contact_verified_retries_until_present(
      self, mock_click_element, mock_generic_request
  ):
    mock_env = mock.create_autospec(env_interface.AndroidEnvInterface)
    mock_generic_request.side_effect = [
        _EMPTY_RESPONSE,
        _adb_response("Row: 0 display_name=Oscar Mohamed, number=+14379969633"),
        _EMPTY_RESPONSE,
        _adb_response("Row: 0 display_name=Gabriel Ibrahim, number=+19696338338"),
    ]

    contacts_utils.add_contact_verified(
        "Gabriel Ibrahim",
        "+19696338338",
        mock_env,
        ui_delay_sec=0,
        retry_delay_sec=0,
    )

    self.assertEqual(mock_click_element.call_count, 2)

  def test_add_contact_verified_retries_after_add_contact_exception(
      self, mock_click_element, mock_generic_request
  ):
    mock_env = mock.create_autospec(env_interface.AndroidEnvInterface)
    mock_click_element.side_effect = [ValueError("SAVE not found"), None]
    mock_generic_request.side_effect = [
        _EMPTY_RESPONSE,
        _EMPTY_RESPONSE,
        _adb_response("Row: 0 display_name=Gabriel Ibrahim, number=+19696338338"),
    ]

    contacts_utils.add_contact_verified(
        "Gabriel Ibrahim",
        "+19696338338",
        mock_env,
        ui_delay_sec=0,
        retry_delay_sec=0,
    )

    self.assertEqual(mock_click_element.call_count, 2)

  def test_add_contact_verified_raises_after_retries(
      self, mock_click_element, mock_generic_request
  ):
    mock_env = mock.create_autospec(env_interface.AndroidEnvInterface)
    mock_generic_request.side_effect = [
        _EMPTY_RESPONSE,
        _adb_response("Row: 0 display_name=Oscar Mohamed, number=+14379969633"),
        _EMPTY_RESPONSE,
        _adb_response("Row: 0 display_name=Oscar Mohamed, number=+14379969633"),
        _EMPTY_RESPONSE,
        _adb_response("Row: 0 display_name=Oscar Mohamed, number=+14379969633"),
    ]

    with self.assertRaisesRegex(RuntimeError, "Failed to create contact"):
      contacts_utils.add_contact_verified(
          "Gabriel Ibrahim",
          "+19696338338",
          mock_env,
          ui_delay_sec=0,
          retry_delay_sec=0,
      )

    self.assertEqual(mock_click_element.call_count, 3)


if __name__ == "__main__":
  absltest.main()
