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

"""Utils for contacts operations using adb."""

import dataclasses
import re
import time
from typing import Iterator

from absl import logging
from android_world.env import actuation
from android_world.env import adb_utils
from android_world.env import android_world_controller


def clean_phone_number(phone_number: str) -> str:
  """Removes all non-numeric characters from a phone number.

  Args:
    phone_number: The phone number to clean.

  Returns:
    The phone number with all non-numeric characters removed.
  """
  return re.sub(r"\D", "", phone_number)


def add_contact(
    name: str,
    phone_number: str,
    env: android_world_controller.AndroidWorldController,
    ui_delay_sec: float = 1.0,
):
  """Adds a contact with the specified name and phone number.

  This function sends an intent to the Android system to add a contact with
  the information pre-filled, clicks the "Save" button to create it, and then
  returns from the activity.

  Args:
    name: The name of the new contact
    phone_number: The phone number belonging to that contact.
    env: The android environment to add the contact to.
    ui_delay_sec: Delay between UI interactions. If this value is too low, the
      "save" button may be mis-clicked.
  """
  intent_command = (
      "am start -a android.intent.action.INSERT -t"
      f' vnd.android.cursor.dir/contact -e name "{name}" -e phone'
      f" {phone_number}"
  )

  adb_command = ["shell", intent_command]
  adb_utils.issue_generic_request(adb_command, env)
  time.sleep(ui_delay_sec)
  actuation.find_and_click_element("SAVE", env)
  time.sleep(ui_delay_sec)
  adb_utils.press_back_button(env)
  time.sleep(ui_delay_sec)


@dataclasses.dataclass(frozen=True)
class Contact:
  """Basic contact information."""
  name: str
  number: str


def list_contacts(
    env: android_world_controller.AndroidWorldController,
) -> list[Contact]:
  """Lists all contacts available in the Android environment.

  Args:
    env: Android environment to search for contacts.

  Returns:
    A list of all contact names and numbers present on the device.
  """
  intent_command = (
      "content query --uri content://contacts/phones/ --projection"
      " display_name:number"
  )
  adb_command = ["shell", intent_command]

  def parse(adb_output: str) -> Iterator[Contact]:
    for match in re.finditer(r"display_name=(.*), number=(.*)", adb_output):
      yield Contact(match.group(1), clean_phone_number(match.group(2)))

  return list(
      parse(
          adb_utils.issue_generic_request(
              adb_command, env
          ).generic.output.decode("utf-8")
      )
  )


def _canonicalize_us_phone_number(phone_number: str) -> str:
  """Returns the 10-digit national number for an AndroidWorld US phone number."""
  phone_number = clean_phone_number(phone_number)

  if len(phone_number) == 11 and phone_number.startswith("1"):
    return phone_number[1:]

  if len(phone_number) >= 10:
    return phone_number[-10:]

  return phone_number


def _phone_numbers_match(actual_number: str, expected_number: str) -> bool:
  """Returns whether two AndroidWorld US phone number strings match."""
  return _canonicalize_us_phone_number(actual_number) == (
      _canonicalize_us_phone_number(expected_number)
  )


def has_contact(
    name: str,
    phone_number: str,
    env: android_world_controller.AndroidWorldController,
) -> bool:
  """Returns whether the contact exists on the device."""
  return any(
      contact.name == name
      and _phone_numbers_match(contact.number, phone_number)
      for contact in list_contacts(env)
  )


def add_contact_verified(
    name: str,
    phone_number: str,
    env: android_world_controller.AndroidWorldController,
    max_attempts: int = 3,
    ui_delay_sec: float = 1.0,
    retry_delay_sec: float = 2.0,
):
  """Adds a contact and verifies it exists in the Contacts provider.

  The Contacts app UI can occasionally expose stale or off-screen SAVE buttons.
  Retrying against the device state keeps task setup failures from leaking into
  agent execution as missing-contact tasks.
  """
  if max_attempts < 1:
    raise ValueError("max_attempts must be at least 1.")

  last_error = None
  for attempt in range(1, max_attempts + 1):
    try:
      add_contact(name, phone_number, env, ui_delay_sec=ui_delay_sec)
      if has_contact(name, phone_number, env):
        return
    except Exception as err:
      last_error = err
      logging.warning(
          "Contact creation attempt failed: %s %s, attempt %d/%d",
          name,
          phone_number,
          attempt,
          max_attempts,
          exc_info=True,
      )
    else:
      last_error = None
      logging.warning(
          "Contact creation not verified: %s %s, attempt %d/%d",
          name,
          phone_number,
          attempt,
          max_attempts,
      )
    if attempt < max_attempts:
      time.sleep(retry_delay_sec)

  error_message = f"Failed to create contact: {name} {phone_number}"
  if last_error is not None:
    raise RuntimeError(error_message) from last_error
  raise RuntimeError(error_message)


def clear_contacts(env: android_world_controller.AndroidWorldController):
  """Clears all contacts on the device."""
  adb_utils.clear_app_data("com.android.providers.contacts", env)
