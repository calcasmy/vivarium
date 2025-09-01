# vivarium/scheduler/src/humidifier_scheduler.py

import os
import sys
import json
from datetime import datetime, timedelta

# Adjust path as needed
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if str(vivarium_path) not in sys.path:
    sys.path.insert(0, str(vivarium_path))

from apscheduler.schedulers.blocking import BlockingScheduler
from utilities.src.logger import LogHelper
from utilities.src.config import HumidifierConfig, SensorConfig
from utilities.src.db_operations import DBOperations
from terrarium.src.controllers.humidifier_controller import HumidifierController
from terrarium.src.controllers.aeration_controller import AerationController
from scheduler.src.device_scheduler_base import DeviceSchedulerBase
from database.device_data_ops.device_status_queries import DeviceStatusQueries
from database.sensor_data_ops.sensor_data_queries import SensorDataQueries

logger = LogHelper.get_logger(__name__)
humid_config = HumidifierConfig()
sensor_config = SensorConfig()


class HumidifierScheduler(DeviceSchedulerBase):
    """
    Manages the scheduling and automatic control of the vivarium humidifier.
    """

    def __init__(self, scheduler: BlockingScheduler, 
                 db_operations: DBOperations, 
                 humidifier_controller: HumidifierController, 
                 aeration_controller: AerationController):
        """
        Initializes the HumidifierScheduler.

        :param scheduler: The main APScheduler instance.
        :type scheduler: BlockingScheduler
        :param db_operations: The shared database operations instance.
        :type db_operations: DBOperations
        :param humidifier_controller: An instance of the HumidifierController.
        :type humidifier_controller: HumidifierController
        """
        super().__init__(scheduler, db_operations)
        self.humidifier_controller = humidifier_controller
        self.aeration_controller = aeration_controller
        self.device_id = humid_config.device_id
        
        self.sensor_data_queries = SensorDataQueries(self.db_operations)
        self.device_status_queries = DeviceStatusQueries(self.db_operations)
        
        self.target_humidity = humid_config.target_humidity
        self.hysteresis = humid_config.hysteresis
        self.humidity_sensor_id = sensor_config.THsensorID

        self._humidifier_off_time = None
        
        logger.info("HumidifierScheduler initialized.")

    def run(self):
        """
        Implements the abstract method from the base class.

        The main logic for this scheduler is handled by the
        :func:`~HumidifierScheduler.check_and_run_humidifier` method,
        which is typically called by the primary vivarium scheduler.
        """
        pass

    def check_and_run_humidifier(self):
        """
        Fetches the latest humidity data and controls the humidifier based on
        pre-defined thresholds.

        This method is the main entry point for the humidifier's automatic control.
        """
        logger.info("Checking environmental data for humidifier control.")

        # 1. Check if a fixed-duration run is in progress
        if self._humidifier_off_time is not None:
            if datetime.now() < self._humidifier_off_time:
                logger.info("Fixed humidifier run in progress. Skipping humidity check.")
                return
            else:
                logger.info("Fixed humidifier run has expired. Resetting state.")
                self._humidifier_off_time = None
        
        try:
            sensor_readings = self.sensor_data_queries.get_latest_reading_by_sensor_id(
                sensor_id=self.humidity_sensor_id
            )

            if not sensor_readings or 'raw_data' not in sensor_readings:
                logger.warning("No latest sensor reading found. Humidifier check aborted.")
                return

            current_humidity = json.loads(sensor_readings['raw_data']).get('humidity_percentage')

            if current_humidity is None:
                logger.warning("Humidity data not in sensor reading. Humidifier check aborted.")
                return

            logger.info(f"Current vivarium humidity: {current_humidity}%.")

            if current_humidity < (self.target_humidity - self.hysteresis):
                if not self.humidifier_controller.is_on():
                    logger.info("Humidity is below target. Activating humidifier.")
                    
                    self.humidifier_controller.control_humidifier(action='on')
                    self.aeration_controller.set_fans_to_max_speed()
                    
                    self._humidifier_off_time = datetime.now() + timedelta(minutes=humid_config.runtime)
                    self._schedule_date_job(
                        self.humidifier_controller.control_humidifier,
                        run_date=self._humidifier_off_time,
                        args=['off'],
                        job_id='run_humidifier_off'
                    )
                    self._schedule_date_job(
                        self.aeration_controller.set_fans_to_default_speed,
                        run_date=self._humidifier_off_time,
                        job_id='aeration_default_speed_from_humidifier'
                    )
                    logger.info(f"Scheduled humidifier to turn OFF at {self._humidifier_off_time.strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    logger.info("Humidity is low, but humidifier is already running. No action taken.")
            else:
                if self.humidifier_controller.is_on() and self._humidifier_off_time is None:
                    logger.info("Humidity is above target. Humidifier not required. Turning OFF.")
                    self.humidifier_controller.control_humidifier(action='off')
                    self.aeration_controller.set_fans_to_default_speed()

        except Exception as e:
            logger.error(f"Error during automatic humidifier check: {e}")