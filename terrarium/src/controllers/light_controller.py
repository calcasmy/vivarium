# vivarium/terrarium/src/controllers/light_controller.py

import os
import sys
import argparse
from datetime import time

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.config import LightConfig, DatabaseConfig
from utilities.src.db_operations import DBOperations, ConnectionDetails
from terrarium.src.controllers.base_device_controller import BaseDeviceController
from gpiod.line import Value

logger = LogHelper.get_logger(__name__)
light_config = LightConfig()

class LightController(BaseDeviceController):
    """
    Controls the vivarium lights using GPIO and database interaction.

    This class inherits common logic from :class:`BaseDeviceController` and
    specializes it for light control, including scheduling and status management.
    """
    def __init__(self, db_operations: DBOperations):
        """
        Initializes the LightController object.

        :param db_operations: An instance of the DBOperations class for all database
                              interactions. This connection is shared and managed
                              by a higher-level orchestrator.
        :type db_operations: DBOperations
        """
        self.device_id = light_config.device_id
        self.relay_pin = int(light_config.lights_control_pin)
        self.consumer_name = 'light_control'  # Unique consumer name for GPIO
        self.on_time: time = None
        self.off_time: time = None

        # Call the base class constructor
        super().__init__(self.device_id, self.relay_pin, self.consumer_name, db_operations)
        logger.info("LightController initialized.")

    def update_schedule_time(self, on_time: time, off_time: time):
        """
        Updates the internal on_time and off_time for the light.

        These times are used when :meth:`control_light` is called with no action,
        allowing the controller to operate based on a pre-defined schedule.

        :param on_time: The time of day to turn the light on.
        :type on_time: datetime.time
        :param off_time: The time of day to turn the light off.
        :type off_time: datetime.time
        """
        self.on_time = on_time
        self.off_time = off_time
        logger.info(f"Light schedule times updated: ON at {self.on_time}, OFF at {self.off_time}")

    def control_light(self, action: str = None):
        """
        Controls the light based on the provided action.

        If `action` is 'on' or 'off', the light is immediately toggled.
        If `action` is None, the light's state is determined by the
        previously set schedule (`self.on_time` and `self.off_time`).

        :param action: The desired action: 'on', 'off', 'status', or None for
                       schedule-based control.
        :type action: str, optional
        :raises Exception: Propagates any exceptions from the base class methods.
        """
        try:
            if action == 'status':
                current_status_dict = self._get_status()
                if current_status_dict is not None and 'is_on' in current_status_dict:
                    logger.info(f"Current LIGHT status: {'ON' if current_status_dict['is_on'] else 'OFF'}")

            elif action == 'on':
                self.toggle_device(action)

            elif action == 'off':
                self.toggle_device(action)

            elif action is None:
                if self.on_time and self.off_time:
                    # Pass the stored on_time and off_time to the base class's toggle_device
                    self.toggle_device(action=None, start_tm=self.on_time, stop_tm=self.off_time)
                else:
                    logger.warning("No schedule times are set. Cannot perform schedule-based control.")
            else:
                logger.warning(f"LightController: Invalid action '{action}' provided. Ignoring.")

        except Exception as e:
            logger.exception(f"An unexpected error occurred while controlling the light: {e}")

def main():
    """
    Parses command-line arguments and controls the light manually.
    """

    parser = argparse.ArgumentParser(description="Manually control the vivarium light.")
    parser.add_argument("action", choices=["on", "off"], help="The desired action: 'on' or 'off'.")
    args = parser.parse_args()

    try:
        db_config = DatabaseConfig()
        db_operations: DBOperations = DBOperations()
        db_connectiondetails = ConnectionDetails(
                host= db_config.postgres_remote_connection.host,
                port= db_config.postgres_remote_connection.port,
                user= db_config.postgres_remote_connection.user,
                password= db_config.postgres_remote_connection.password,
                dbname= db_config.postgres_remote_connection.dbname,
                sslmode=None
        )
        db_operations.connect(db_connectiondetails)
        light_controller = LightController(db_operations)
        
        logger.info(f"Manual control activated: Turning light {args.action}.")
        light_controller.control_light(action=args.action)
        logger.info(f"Manual control for light completed.")

    except Exception as e:
        logger.error(f"Failed to perform manual light control: {e}")

if __name__ == "__main__":
    main()