# vivarium/terrarium/src/controllers/base_device_controller.py

import os
import sys
import gpiod
from datetime import datetime

# Adjust path as needed to import your utilities
# Assuming this file is in vivarium/terrarium/src/controllers
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.database_operations import DatabaseOperations
from terrarium.src.database.device_status_queries import DeviceStatusQueries
from terrarium.src.database.device_queries import DeviceQueries # Assuming DeviceQueries is used by status queries

logger = LogHelper.get_logger(__name__)

class BaseDeviceController:
    """
    A base class for controlling devices via GPIO and managing their status
    in the database.
    """
    def __init__(self, device_id: str, relay_pin: int, consumer_name: str, db_operations: DatabaseOperations):
        """
        Initializes the base device controller.

        Args:
            device_id (str): The unique identifier for the equipment (e.g., 'l' for light, 'm' for mister).
            relay_pin (int): The GPIO pin number connected to the device's relay.
            consumer_name (str): A descriptive name for the GPIO consumer (e.g., 'light_control', 'mister_control').
            db_operations (DatabaseOperations): An instance of DatabaseOperations for DB interaction.
        """
        self.device_id = device_id
        self.relay_pin = relay_pin
        self.consumer_name = consumer_name
        self.db_ops = db_operations

        self.chip = None
        self.line = None
        self._setup_gpio()

        self._devicequeries = DeviceQueries(db_operations=self.db_ops)
        self._devicestatus = DeviceStatusQueries(db_operations=self.db_ops)

    def __del__(self):
        """
        Ensures the GPIO line is released and chip closed when the object is destroyed.
        """
        if self.line:
            try:
                self.line.release()
                logger.info(f"GPIO line {self.relay_pin} ({self.consumer_name}) released.")
            except Exception as e:
                logger.error(f"Error releasing GPIO line {self.relay_pin} for {self.consumer_name}: {e}")

        if self.chip:
            try:
                self.chip.close()
                logger.info(f"GPIO chip closed for {self.consumer_name}.")
            except Exception as e:
                logger.error(f"Error closing GPIO chip for {self.consumer_name}: {e}")

    def _setup_gpio(self):
        """Sets up the GPIO pin for controlling the device."""
        try:
            self.chip = gpiod.Chip('gpiochip0')
            self.line = self.chip.get_line(self.relay_pin)
            self.line.request(consumer=self.consumer_name, type=gpiod.LINE_REQ_DIR_OUT)
            logger.info(f"GPIO line {self.relay_pin} ({self.consumer_name}) configured as output.")
        except gpiod.ChipError as e:
            logger.error(f"Failed to open GPIO chip or get line for {self.consumer_name}: {e}")
            sys.exit(1) # Critical failure, cannot control device
        except Exception as e:
            logger.error(f"An unexpected error occurred during GPIO setup for {self.consumer_name}: {e}")
            sys.exit(1) # Critical failure

    def _get_status(self) -> dict:
        """
        Fetches the current status of the device from the database.

        Returns:
            dict: A dictionary containing the device status, or None if not found/error.
        """
        try:
            return self._devicestatus.get_latest_status_by_device_id(device_id=self.device_id)
        except Exception as e:
            error_message = f"Failed to get status for {self.device_id} ({self.consumer_name}): {e}"
            logger.error(error_message)
            raise # Re-raise for calling method to handle

    def _update_status(self, status: bool):
        """
        Updates the status of the device in the database.

        Args:
            status (bool): The new status to set (True for on, False for off).
        """
        try:
            self._devicestatus.insert_device_status(
                device_id=self.device_id,
                timestamp=(datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),
                is_on=status
            )
        except Exception as e:
            error_message = f"Failed to update status for {self.device_id} ({self.consumer_name}): {e}"
            logger.error(error_message)
            raise # Re-raise for calling method to handle

    def _set_gpio_state(self, state: bool):
        """
        Sets the GPIO pin high (on) or low (off).
        Args:
            state (bool): True for ON (1), False for OFF (0).
        """
        if not self.line:
            logger.error(f"GPIO line for {self.consumer_name} not initialized. Cannot control device.")
            return

        gpio_value = 1 if state else 0
        try:
            self.line.set_value(gpio_value)
            logger.info(f"GPIO {self.consumer_name} pin {self.relay_pin} set to {gpio_value} ( {'ON' if state else 'OFF'} ).")
        except Exception as e:
            logger.error(f"Error setting GPIO value for {self.consumer_name} pin {self.relay_pin}: {e}")
            raise # Re-raise for calling method to handle

    # A common control method for simple ON/OFF devices
    def toggle_device(self, action: str):
        """
        Controls the device based on the provided action ('on' or 'off'),
        only performing action if the state needs to change.
        """
        if not self.line:
            logger.error(f"GPIO line for {self.consumer_name} not initialized. Cannot control device.")
            return

        target_state = None
        if action.lower() == 'on':
            target_state = True
        elif action.lower() == 'off':
            target_state = False
        else:
            logger.warning(f"Invalid action: '{action}' for {self.consumer_name}. Must be 'on' or 'off'.")
            return

        try:
            current_status_dict = self._get_status()
            current_is_on = None
            if current_status_dict is not None and 'is_on' in current_status_dict:
                current_is_on = current_status_dict['is_on']
            else:
                logger.warning(f"Could not retrieve current status for {self.consumer_name} from database. Assuming unknown state and proceeding with action.")

            if current_is_on is not None and current_is_on == target_state:
                logger.info(f"{self.consumer_name} is already {'ON' if target_state else 'OFF'}. No action taken.")
            else:
                self._set_gpio_state(target_state)
                self._update_status(target_state)
                logger.info(f"{self.consumer_name} successfully {'turned ON' if target_state else 'turned OFF'}.")

        except Exception as e:
            logger.error(f'An error occurred while controlling {self.consumer_name}: {e}')