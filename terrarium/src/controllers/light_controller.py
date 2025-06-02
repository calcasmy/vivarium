# vivarium/terrarium/src/controllers/light_controller.py (adjust path)

import os
import sys
from datetime import time

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.config import LightConfig
from utilities.src.database_operations import DatabaseOperations
from terrarium.src.controllers.base_device_controller import BaseDeviceController # Import the base class

logger = LogHelper.get_logger(__name__)
light_config = LightConfig()

class LightController(BaseDeviceController):
    """
    Controls the vivarium lights using GPIO and database interaction.
    Inherits common logic from BaseDeviceController.
    """
    def __init__(self, db_operations: DatabaseOperations):
        """
        Initializes the LightControler object.
        """
        self.device_id = 1
        self.relay_pin = int(light_config.lights_control_pin)
        self.consumer_name = 'light_control' # Unique consumer name for GPIO
        self.on_time: time = None
        self.off_time: time = None

        # Call the base class constructor
        super().__init__(self.device_id, self.relay_pin, self.consumer_name, db_operations)
        logger.info("LightControler initialized.")

    def update_schedule_time(self, on_time: time, off_time: time):
        """
        Updates the internal on_time and off_time for the light.
        These are used when control_light(action=None) is called.
        """
        self.on_time = on_time
        self.off_time = off_time
        logger.info(f"Light schedule times updated: ON at {self.on_time}, OFF at {self.off_time}")

    def control_light(self, action: str):
        """
        Controls the light based on the provided action ('on' or 'off').
        Uses the common toggle_device method from BaseDeviceController.
        """
        if action == 'status':
            current_status_dict = self._get_status()
            if current_status_dict is not None and 'is_on' in current_status_dict:
                logger.info(f"Current LIGHT status: {current_status_dict['is_on']}")
        elif action in ['on', 'off']:
            self.toggle_device(action)
        elif action is None:
            if self.on_time is None or self.off_time is None:
                self.on_time = time(light_config.lights_on)
                self.off_time = time(light_config.lights_off)
                return
            # Pass the stored on_time and off_time to the base class's toggle_device
            self.toggle_device(action=None, start_tm=self.on_time, stop_tm=self.off_time)
        else:
            logger.warning(f"LightControllerV2: Invalid action '{action}' provided for control_light. Ignoring.")


def main(action: str):
    """
    Main function to create and run the LightControler.
    """
    db_operations = DatabaseOperations()
    db_operations.connect()

    try:
        light_controller = LightController(db_operations = db_operations)
        light_controller.control_light(action)
    except Exception as e:
        logger.exception(f"An unexpected error occurred in LightController main: {e}")
    finally:
        db_operations.close()

if __name__ == "__main__":
    # ... (existing __main__ block for command-line execution) ...
    if len(sys.argv) > 1:
        action = sys.argv[1]
        main(action)
    else:
        logger.warning("No action provided. Please specify 'on' or 'off' as a command-line argument.")