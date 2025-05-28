# vivarium/terrarium/src/controllers/mister_controller.py (adjust path)

import os
import sys
import time # For time.sleep
from datetime import datetime

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.config import MisterConfig
from utilities.src.database_operations import DatabaseOperations
from terrarium.src.controllers.base_device_controller import BaseDeviceController

logger = LogHelper.get_logger(__name__)
mister_config = MisterConfig()

class MisterControllerV2(BaseDeviceController):
    """
    Controls the vivarium mister using GPIO and database interaction.
    Inherits common logic from BaseDeviceController.
    """
    def __init__(self, device_id = 2, db_operations: DatabaseOperations = None):
        relay_pin = int(mister_config.mister_control_pin)
        self.device_id = device_id
        consumer_name = 'mister_control' # Unique consumer name for GPIO

        super().__init__(device_id, relay_pin, consumer_name, db_operations)
        logger.info("MisterController initialized.")

        self.humidity_threshold = mister_config.humidity_threshold
        self.mister_duration = mister_config.mister_duration
        self.mister_interval = mister_config.mister_interval

    def control_mister(self, action:bool = False):
        """
        Activates the mister for a specified duration.
        """
        if action == "status":
            current_status_dict = self._get_status()
            if current_status_dict is not None and 'is_on' in current_status_dict:
                logger.info(f"Current MISTER status: {current_status_dict['is_on']}")
        elif action == 'on' or action == 'off':
            self.toggle_device(action) # Just call the base class method

    def control_mister_auto(self):
        """
        Controls the mister automatically based on humidity and interval.
        This would be triggered by your scheduler.
        """
        try:
            # You would likely fetch current humidity here from a sensor reading
            # For now, let's assume you'd have a sensor_queries.get_latest_humidity() method
            # current_humidity = some_sensor_reading_method()
            # if current_humidity < self.humidity_threshold:

            mister_status = self.get_status()
            last_runtime = None
            if mister_status and 'timestamp' in mister_status and mister_status['is_on'] == False: # Only consider if it's currently OFF
                 last_runtime = mister_status['timestamp']
            else:
                 logger.info("Mister is currently ON or status not found, not checking for auto run.")
                 return # Exit if mister is already on or status unknown

            run_delta = float('inf') # Assume infinite delta if no last run
            if last_runtime:
                # Ensure last_runtime is a datetime object
                if isinstance(last_runtime, str):
                    # Try to parse different formats if necessary, or ensure consistent DB format
                    try:
                        last_runtime = datetime.strptime(last_runtime, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        logger.error(f"Could not parse last_runtime timestamp: {last_runtime}")
                        return # Cannot proceed with interval check
                
                # Check for timezone awareness if mixing datetime.now() with pytz.timezone
                # It's safest to make both naive or both aware and convert to common TZ
                # For simplicity, ensure they are both naive (no timezone info) or handle conversion.
                # Assuming datetime.now() without tzinfo, and last_runtime from DB is also naive.
                run_delta = (datetime.now() - last_runtime).total_seconds() / 60 # In minutes

            if run_delta >= self.mister_interval:
                logger.info(f"Mister interval of {self.mister_interval} minutes met. Running mister.")
                self.run_mister(self.mister_duration)
            else:
                logger.info(f"Mister minimum interval duration '{self.mister_interval} minutes' not yet met (last run {round(run_delta, 1)} mins ago). Mister not activated.")

        except Exception as e:
            logger.error(f"Error in automatic mister control: {e}")

def main(action: str, duration: int):
    """
    Main function to create and run the MisterController.
    """
    db_operations = DatabaseOperations()
    db_operations.connect()

    try: 
        mister_controller = MisterControllerV2(db_operations=db_operations)
        
        if action == 'run': # For manual run of mister
            mister_controller.run_mister(duration)
        elif action == 'auto': # For automatic check
            mister_controller.control_mister_auto()
        else:
            logger.warning(f"Invalid mister action: {action}. Use 'run' or 'auto'.")
    except Exception as e:
        logger.error(f'Error occurred while trying to control mister: {e}')


    db_operations.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Control the mister duration.")
    parser.add_argument("action", type=str, help="Action to perform: 'run' (manual run) or 'auto' (automatic check).")
    parser.add_argument("--duration", type=int, default=30, help="Duration (in seconds) to run the mister (default: 30 seconds). Only applies to 'run' action.")
    args = parser.parse_args()

    main(args.action, args.duration)