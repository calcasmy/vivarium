# vivarium/terrarium/src/controllers/light_controller.py (adjust path)

import os
import sys

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.config import LightConfig
from utilities.src.database_operations import DatabaseOperations
from terrarium.src.controllers.base_device_controller import BaseDeviceController # Import the base class

logger = LogHelper.get_logger(__name__)
light_config = LightConfig()

class LightControllerV2(BaseDeviceController):
    """
    Controls the vivarium lights using GPIO and database interaction.
    Inherits common logic from BaseDeviceController.
    """
    def __init__(self, db_operations: DatabaseOperations):
        """
        Initializes the LightControler object.
        """
        equipment_id = 'l' # Consistent string ID for lights
        relay_pin = int(light_config.lights_control_pin)
        consumer_name = 'light_control' # Unique consumer name for GPIO

        # Call the base class constructor
        super().__init__(equipment_id, relay_pin, consumer_name, db_operations)
        logger.info("LightControler initialized.")

    def control_light(self, action: str):
        """
        Controls the light based on the provided action ('on' or 'off').
        Uses the common toggle_device method from BaseDeviceController.
        """
        self.toggle_device(action) # Just call the base class method

def main(action: str):
    """
    Main function to create and run the LightControler.
    """
    db_operations = DatabaseOperations()
    db_operations.connect()

    light_controller = LightControllerV2(db_operations=db_operations)
    light_controller.control_light(action)

    db_operations.close() # Close DB connection in main

if __name__ == "__main__":
    # ... (your existing __main__ block for command-line execution) ...
    import sys
    if len(sys.argv) > 1:
        action = sys.argv[1]
        main(action)
    else:
        logger.warning("No action provided. Please specify 'on' or 'off' as a command-line argument.")