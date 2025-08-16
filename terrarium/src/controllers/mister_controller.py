# vivarium/terrarium/src/controllers/mister_controller.py

import os
import sys
import time
from datetime import datetime, time as dt_time

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.config import MisterConfig, DatabaseConfig
from utilities.src.db_operations import DBOperations, ConnectionDetails
from terrarium.src.controllers.base_device_controller import BaseDeviceController

logger = LogHelper.get_logger(__name__)
mister_config = MisterConfig()

class MisterController(BaseDeviceController):
    """
    Controls the vivarium mister using GPIO and database interaction.

    Inherits common logic from :class:`~terrarium.src.controllers.base_device_controller.BaseDeviceController`.
    """
    def __init__(self, db_operations: DBOperations):
        """
        Initializes the MisterController object.

        :param db_operations: An instance of DBOperations for database interaction.
        :type db_operations: DBOperations
        """
        self.device_id = mister_config.device_id
        self.relay_pin = int(mister_config.mister_control_pin)
        self.consumer_name = 'mister_control'
        
        super().__init__(self.device_id, self.relay_pin, self.consumer_name, db_operations)
        logger.info("MisterController initialized.")

        self.mister_duration = mister_config.duration
        self.mister_interval = mister_config.mister_interval
        self.humidity_threshold = mister_config.humidity_threshold

    def run_mister(self, duration: int):
        """
        Activates the mister for a specified duration.

        :param duration: The duration in seconds to run the mister.
        :type duration: int
        """
        try:
            logger.info(f"Activating mister for {duration} seconds.")
            self.toggle_device(action='on')
            time.sleep(duration)
            self.toggle_device(action='off')
            logger.info("Mister deactivated.")
        except Exception as e:
            logger.error(f"Error during mister run: {e}")

    def control_mister(self, action: str):
        """
        Controls the mister based on the provided action ('on', 'off', 'run', or 'status').

        This method uses the common :meth:`~.toggle_device` method from the
        base class to execute the command. If 'run' is provided, it runs
        the mister for the configured duration.

        :param action: The desired action: 'on', 'off', 'run', or 'status'.
        :type action: str
        """
        if action == "status":
            current_status_dict = self._get_status()
            if current_status_dict is not None and 'is_on' in current_status_dict:
                logger.info(f"Current MISTER status: {current_status_dict['is_on']}")
        elif action in ['on', 'off']:
            self.toggle_device(action)
        elif action == 'run':
            self.run_mister(duration=self.mister_duration)
        else:
            logger.warning(f"Invalid action '{action}' provided for control_mister. Ignoring.")

    def control_mister_auto(self, current_humidity: float):
        """
        Controls the mister automatically based on humidity and interval.

        This method is intended to be triggered by a scheduler. It checks if the
        current humidity is below the configured threshold and if the last run
        was more than the configured interval ago.

        :param current_humidity: The current humidity reading from a sensor.
        :type current_humidity: float
        """
        try:
            if current_humidity >= self.humidity_threshold:
                logger.info(f"Current humidity ({current_humidity}%) is above threshold ({self.humidity_threshold}%). Mister not activated.")
                return

            mister_status = self._get_status()
            
            if mister_status and mister_status.get('is_on', False):
                 logger.info("Mister is currently ON. Not checking for auto run.")
                 return
            
            last_runtime_str = mister_status.get('timestamp') if mister_status else None
            
            run_delta = float('inf')
            if last_runtime_str:
                try:
                    last_runtime = datetime.strptime(last_runtime_str, "%Y-%m-%d %H:%M:%S")
                    run_delta = (datetime.now() - last_runtime).total_seconds() / 60
                except (ValueError, TypeError):
                    logger.error(f"Could not parse last_runtime timestamp: {last_runtime_str}. Assuming infinite delta.")

            if run_delta >= self.mister_interval:
                logger.info(f"Humidity is below threshold and interval of {self.mister_interval} minutes met. Running mister.")
                self.run_mister(self.mister_duration)
            else:
                logger.info(f"Mister minimum interval duration '{self.mister_interval} minutes' not yet met (last run {round(run_delta, 1)} mins ago). Mister not activated.")

        except Exception as e:
            logger.error(f"Error in automatic mister control: {e}")

def main(action: str, duration: int, humidity: float):
    """
    Main function to create and run the MisterController.

    :param action: The action to perform: 'run' (manual), 'auto' (automatic), 'on', 'off', or 'status'.
    :type action: str
    :param duration: The duration (in seconds) to run the mister.
    :type duration: int
    :param humidity: The current humidity reading for 'auto' mode.
    :type humidity: float
    """
    db_config = DatabaseConfig()
    db_operations = DBOperations()
    db_operations.connect(ConnectionDetails(
        host=db_config.postgres_local_connection.host,
        port=db_config.postgres_local_connection.port,
        user=db_config.postgres_local_connection.user,
        password=db_config.postgres_local_connection.password,
        dbname=db_config.postgres_local_connection.dbname,
        sslmode=None
    ))

    try:
        mister_controller = MisterController(db_operations=db_operations)
        
        if action == 'run':
            mister_controller.run_mister(duration)
        elif action == 'auto':
            mister_controller.control_mister_auto(humidity)
        elif action in ['on', 'off', 'status']:
            mister_controller.control_mister(action)
        else:
            logger.warning(f"Invalid mister action: {action}. Use 'run', 'auto', 'on', 'off', or 'status'.")

    except Exception as e:
        logger.exception(f"An unexpected error occurred in MisterController main: {e}")
    finally:
        db_operations.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Control the vivarium mister.")
    parser.add_argument("action", type=str, help="Action to perform: 'run' (manual), 'auto' (automatic), 'on', 'off', or 'status'.")
    parser.add_argument("--duration", type=int, default=mister_config.mister_duration, help=f"Duration (in seconds) to run the mister (default: {mister_config.mister_duration}s).")
    parser.add_argument("--humidity", type=float, default=100.0, help="Current humidity reading for 'auto' action (default: 100.0).")
    args = parser.parse_args()

    main(args.action, args.duration, args.humidity)