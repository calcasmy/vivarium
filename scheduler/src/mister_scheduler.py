# vivarium/scheduler/src/mister_scheduler.py

import os
import sys
import json
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler

# Adjust path as needed to import your utilities and controllers
# Assuming this file is in vivarium/scheduler/src/
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.config import MisterConfig, SensorConfig # Assuming TempConfig holds sensor thresholds
from utilities.src.database_operations import DatabaseOperations # For type hinting
from terrarium.src.controllers.mister_controller import MisterControllerV2
from scheduler.src.device_scheduler_base import DeviceSchedulerBase # Import the base scheduler
from terrarium.src.database.sensor_data_queries import SensorDataQueries
from terrarium.src.database.device_status_queries import DeviceStatusQueries

logger = LogHelper.get_logger(__name__)
mister_config = MisterConfig()
sensor_config = SensorConfig()

class MisterScheduler(DeviceSchedulerBase):
    """
    Manages the scheduling and automatic control of the vivarium mister.
    """
    def __init__(self, scheduler: BlockingScheduler, db_operations: DatabaseOperations, mister_controller: MisterControllerV2):
        """
        Initializes the MisterScheduler.

        Args:
            scheduler (BlockingScheduler): The main APScheduler instance.
            db_operations (DatabaseOperations): The shared database operations instance.
            mister_controller (MisterController): An instance of the MisterController to operate the mister.
        """
        super().__init__(scheduler, db_operations)
        self.mister_controller = mister_controller
        self.sensor_data_queries = SensorDataQueries(self.db_operations)
        self.device_status_queries = DeviceStatusQueries(self.db_operations)
        logger.info("MisterScheduler initialized.")

    def check_and_run_mister(self):
        """
        Fetches latest environmental data (humidity) and makes decisions
        for mister activation based on thresholds and intervals.
        This method should be called periodically (e.g., every 5 minutes).
        """
        # logger.info("Checking environmental data for automatic mister control.")
        try:
            # 1. Fetch latest humidity from DB
            sensor_readings = self.sensor_data_queries.get_latest_readings_by_sensor_id(sensor_id = sensor_config.HTU21D)
            
            if not sensor_readings or 'raw_data' not in sensor_readings:
                logger.warning("Could not retrieve latest sensor reading with raw_data. Cannot perform mister check.")
                return

            # Assuming humidity is stored in raw_data as a dictionary
            raw_data = sensor_readings['raw_data']
            if isinstance(raw_data, str): # If raw_data is stored as JSON string
                try:
                    raw_data = json.loads(raw_data) # Need to import json
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode raw_data JSON: {raw_data}")
                    return

            current_humidity = raw_data.get('humidity_percentage')

            if current_humidity is None:
                logger.warning("Humidity data not found in latest sensor reading. Cannot perform mister check.")
                return

            # logger.info(f"Current humidity: {current_humidity}%")

            # 2. Mister Control Logic
            self._handle_mister_activation(current_humidity)

        except Exception as e:
            logger.error(f"Error during automatic mister check: {e}")

    def _handle_mister_activation(self, current_humidity: float):
        """
        Handles the logic for controlling the mister based on humidity and interval.
        """
        humidity_threshold = mister_config.humidity_threshold
        mister_interval_hours = mister_config.mister_interval / 60 # Convert minutes to hours
        mister_duration_seconds = mister_config.mister_duration

        logger.info(f"Mister activation check: Current humidity {current_humidity}%, Threshold {humidity_threshold}%, Interval {mister_interval_hours} hours, Duration {mister_duration_seconds} seconds.")

        if current_humidity < humidity_threshold:
            logger.info("Humidity is below threshold. Checking mister interval.")

            last_mister_run_status = self.device_status_queries.get_latest_status_by_device_id(device_id = self.device_id)
            last_run_timestamp_str = None
            if last_mister_run_status and 'timestamp' in last_mister_run_status:
                last_run_timestamp_str = last_mister_run_status['timestamp']

            if last_run_timestamp_str:
                try:
                    # Convert DB timestamp string to datetime object
                    last_run_datetime = datetime.strptime(last_run_timestamp_str, "%Y-%m-%d %H:%M:%S")
                    time_since_last_run = (datetime.now() - last_run_datetime).total_seconds() / 3600 # In hours

                    if time_since_last_run >= mister_interval_hours:
                        logger.info(f"Mister interval (last run {round(time_since_last_run, 2)} hours ago) fulfilled. Activating mister.")
                        # Schedule mister to run for duration and then stop (Non-blocking for scheduler)
                        self._schedule_date_job(
                            self.mister_controller.control_mister,
                            run_date=datetime.now() + timedelta(seconds=1), # Run almost immediately
                            args=[True],
                            job_id='run_mister_now'
                        )
                        self._schedule_date_job(
                            self.mister_controller.turn_off_mister,  # Call the non-blocking OFF method
                            run_date=datetime.now() + timedelta(seconds=1 + mister_duration_seconds),
                            # Turn off after duration
                            args=[False],  # No arguments needed for turn_off_mister
                            job_id='mister_off_job'
                        )
                        logger.info(
                            f"Mister scheduled to turn ON for {mister_duration_seconds} seconds (non-blocking).")
                    else:
                        logger.info(f"Mister interval not fulfilled. Last run was {round(time_since_last_run, 2)} hours ago (required {mister_interval_hours} hours). Mister not activated.")
                except ValueError:
                    logger.error(f"Failed to parse last mister run timestamp: {last_run_timestamp_str}. Mister interval check skipped.")
                except Exception as e:
                    logger.error(f"Error checking mister interval: {e}")
            else:
                logger.info("No previous mister run timestamp found. Activating mister (first run).")
                # Schedule mister to run for duration and then stop (Non-blocking for scheduler)
                self._schedule_date_job(
                    self.mister_controller.run_mister,
                    run_date=datetime.now() + timedelta(seconds=1), # Run almost immediately
                    args=[mister_duration_seconds],
                    job_id='run_mister_now'
                )
        else:
            logger.info("Humidity is above threshold. Misting not required.")

