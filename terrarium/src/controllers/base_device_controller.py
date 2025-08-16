# vivarium/terrarium/src/controllers/base_device_controller.py

import os
import sys
import gpiod
from typing import Optional
from datetime import datetime
from gpiod.line import Direction, Value

# Assuming this file is in vivarium/terrarium/src/controllers
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations
from database.device_data_ops.device_status_queries import DeviceStatusQueries
from database.device_data_ops.device_queries import DeviceQueries

logger = LogHelper.get_logger(__name__)

class BaseDeviceController:
    """
    A base class for controlling devices via GPIO and managing their status
    in the database.
    """
    def __init__(self, device_id: int, relay_pin: Optional[int], consumer_name: str, db_operations: DBOperations):
        """
        Initializes the base device controller.

        :param device_id: The ID of the device as stored in the database.
        :type device_id: int
        :param relay_pin: The BCM GPIO pin number connected to the relay.
        :type relay_pin: int
        :param consumer_name: A descriptive name for the GPIO consumer.
        :type consumer_name: str
        :param db_operations: A database operations object for interacting with the database.
        :type db_operations: DBOperations
        """
        self.device_id = device_id
        self.relay_pin = relay_pin
        self.consumer_name = consumer_name
        self.db_ops = db_operations

        self.line_request = None
        self._setup_gpio()

        self._devicequeries = DeviceQueries(db_operations=self.db_ops)
        self._devicestatus = DeviceStatusQueries(db_operations=self.db_ops)

    def close(self):
        """Ensures the GPIO line is explicitly released."""
        if self.line_request:
            try:
                self.line_request.release()
                self.line_request = None  # Ensure the object is cleaned up
                logger.info(f"GPIO line {self.relay_pin} ({self.consumer_name}) released.")
            except Exception as e:
                logger.error(f"Error releasing GPIO line {self.relay_pin} for {self.consumer_name}: {e}")

    def __del__(self):
        """
        Ensures the GPIO line is released when the object is destroyed.
        """
        if self.line_request:
            self.close()

    def _setup_gpio(self):
        """
        Sets up the GPIO pin for controlling the device.
        """
        if self.relay_pin is None or self.relay_pin < 0:
            logger.info(f"GPIO setup skipped for {self.consumer_name}. Checking for Network based operation.")
            return

        try:
            self.line_request = gpiod.request_lines(
                f'/dev/gpiochip0',
                { self.relay_pin: gpiod.LineSettings(direction=Direction.OUTPUT) },
                consumer=self.consumer_name,
            )
            logger.info(f"GPIO line {self.relay_pin} ({self.consumer_name}) configured as output.")
        except FileNotFoundError as e:
            logger.critical(f"GPIO device 'gpiochip0' not found. "
                            f"Ensure your program is running on a Raspberry Pi or similar device with GPIO. Error: {e}")
            sys.exit(1)
        except Exception as e:
            logger.critical(f"An unexpected error occurred during GPIO setup for {self.consumer_name}: {e}")
            sys.exit(1)

    def _get_status(self) -> dict:
        """
        Fetches the current status of the device from the database.

        :return: A dictionary containing the latest device status, or None if not found.
        :rtype: dict
        :raises: Exception if an error occurs during the database query.
        """
        try:
            return self._devicestatus.get_latest_status_by_device_id(device_id=self.device_id)
        except Exception as e:
            error_message = f"Failed to get status for {self.device_id} ({self.consumer_name}): {e}"
            logger.error(error_message)
            raise

    def _update_status(self, status: bool):
        """
        Updates the status of the device in the database.

        :param status: The new status of the device (True for ON, False for OFF).
        :type status: bool
        :raises: Exception if an error occurs during the database update.
        """
        try:
            self.db_ops.begin_transaction()
            self._devicestatus.insert_device_status(
                device_id=self.device_id,
                timestamp=(datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),
                is_on=status
            )
            self.db_ops.commit_transaction()
            logger.info(f"Status for {self.device_id} ({self.consumer_name}) updated.")
        except Exception as e:
            self.db_ops.rollback_transaction()
            error_message = f"Failed to update status for {self.device_id} ({self.consumer_name}): {e}"
            logger.error(error_message)
            raise

    def _set_gpio_state(self, state: bool):
        """
        Sets the GPIO pin high (on) or low (off).

        :param state: True for ON (1), False for OFF (0).
        :type state: bool
        :raises: Exception if an error occurs while setting the GPIO value.
        """
        if self.relay_pin is None or self.relay_pin < 0:
            logger.info(f"GPIO control manipulation skipped for {self.consumer_name} as no valid pin was provided.")
            return

        if not self.line_request:
            logger.error(f"GPIO line for {self.consumer_name} not initialized. Cannot control device.")
            return

        gpio_value = Value.ACTIVE if state else Value.INACTIVE
        try:
            self.line_request.set_value(self.relay_pin, gpio_value)
            logger.info(f"GPIO {self.consumer_name} pin {self.relay_pin} set to {gpio_value} ( {'ON' if state else 'OFF'} ).")
        except Exception as e:
            logger.error(f"Error setting GPIO value for {self.consumer_name} pin {self.relay_pin}: {e}")
            raise

    def toggle_device(self, action: str, start_tm=None, stop_tm=None):
        """
        Controls the device based on the provided action ('on' or 'off').
        
        If no action is provided, the device state is determined by a schedule.

        :param action: The desired action: 'on', 'off', or None for schedule-based control.
        :type action: str
        :param start_tm: The start time for the scheduled operation.
        :type start_tm: datetime.time
        :param stop_tm: The stop time for the scheduled operation.
        :type stop_tm: datetime.time
        """
        if not self.line_request:
            logger.error(f"GPIO line for {self.consumer_name} not initialized. Cannot control device.")
            return

        current_status = self._get_status()
        current_state = current_status['is_on'] if current_status else None
        
        if action == "on":
            desired_state = True
        elif action == "off":
            desired_state = False
        else:
            logger.info(f"1: Controlling {self.consumer_name} based on schedule: Current time {datetime.now().strftime('%H:%M')}, ON {start_tm.strftime('%H:%M')}, OFF {stop_tm.strftime('%H:%M')}. Desired state: {desired_state}.")
            if start_tm and stop_tm:
                now_time = datetime.now().time()
                is_on_time = start_tm <= now_time < stop_tm
                desired_state = is_on_time
            else:
                logger.warning(f"No valid schedule times provided for {self.consumer_name}. Cannot determine desired state.")
                return

        if desired_state is not None and desired_state == current_state:
            logger.info(f"{self.consumer_name} is already {'ON' if current_state else 'OFF'}. No change needed.")
            return

        try:
            self._set_gpio_state(desired_state)
            self._update_status(desired_state)
            logger.info(f"{self.consumer_name} successfully turned {'ON' if desired_state else 'OFF'}.")
        except Exception as e:
            logger.error(f"An error occurred while controlling {self.consumer_name}: {e}")