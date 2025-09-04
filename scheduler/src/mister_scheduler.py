# vivarium/scheduler/src/mister_scheduler.py

import os
import sys
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

# Adjust path as needed
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.config import MisterConfig
from utilities.src.db_operations import DBOperations
from terrarium.src.controllers.mister_controller import MisterController
from terrarium.src.controllers.aeration_controller import AerationController
from scheduler.src.device_scheduler_base import DeviceSchedulerBase

logger = LogHelper.get_logger(__name__)
mister_config = MisterConfig()

class MisterScheduler(DeviceSchedulerBase):
    """
    Manages the scheduling and automatic control of the vivarium mister.
    """
    def __init__(self, scheduler: BlockingScheduler, db_operations: DBOperations, mister_controller: MisterController, aeration_controller: AerationController):
        """
        Initializes the MisterScheduler.

        :param scheduler: The main APScheduler instance.
        :type scheduler: BlockingScheduler
        :param db_operations: The shared database operations instance.
        :type db_operations: DBOperations
        :param mister_controller: An instance of the MisterController.
        :type mister_controller: MisterController
        """
        super().__init__(scheduler, db_operations)
        self.mister_controller = mister_controller
        self.at_hour = mister_config.at_hour
        self.at_minute = mister_config.at_minute
        self.duration = mister_config.duration
        self.aeration_controller = aeration_controller
        logger.info("MisterScheduler initialized.")

    def schedule_misting_job(self, duration: int = None) -> None:
        """
        Schedules job to run the mister at a specific time daily.

        :param duration: The duration in seconds to run the mister. Overrides the default
                         duration from the configuration if provided.
        :type duration: int, optional
        """
        if duration is not None:
            self.duration = duration

        job_id_on = 'mister_on'
        if self.scheduler.get_job(job_id_on):
            logger.info(f"Misting job '{job_id_on}' already exists. Skipping.")
            return

        def run_mister_cycle():
            """
            A helper method to execute the mister cycle.
            """
            # 1. -- ACTUAL - Schedule Mister --
            logger.info("Mister job activated. Turning ON.")
            # Turn fans to max speed
            self.aeration_controller.set_fans_to_max_speed()
            # Turn on the mister
            self.mister_controller.control_mister(action='on')
            
            try:
                off_time = datetime.now() + timedelta(seconds=self.duration)
                self.scheduler.add_job(
                    self.mister_controller.control_mister,
                    trigger=DateTrigger(run_date=off_time),
                    id='mister_off',
                    name='Morning Mister OFF',
                    args=['off'],
                    replace_existing=True
                )
                # After turning off the mister, set fans back to default speed
                self.scheduler.add_job(
                    self.aeration_controller.set_fans_to_default_speed,
                    trigger=DateTrigger(run_date=off_time),
                    id='aeration_default_speed_from_mister',
                    name='Aeration Default Speed (Mister)',
                    replace_existing=True
                )
                logger.info(f"Mister scheduled to turn OFF at {off_time.strftime('%Y-%m-%d %H:%M:%S')}")
            except Exception as e:
                logger.error(f"Error scheduling mister off job: {e}")

        self.scheduler.add_job(
            run_mister_cycle,
            trigger=CronTrigger(hour=self.at_hour, minute=self.at_minute),
            id=job_id_on,
            name='Morning Mister ON'
        )
        logger.info(f"Scheduled daily mister run at {self.at_hour}:{self.at_minute} for {self.duration} seconds. 💧")

        # 2. -- TESTING: SCHEDULE MISTER TO RUN NOW --
        # logger.info("TESTING: Starting mister run immediately and scheduling OFF.")
        
        # # Turn on the mister immediately
        # self.mister_controller.control_mister(action='on')
        
        # # Schedule the mister to turn off 5 seconds from now
        # off_time = datetime.now() + timedelta(seconds=5)
        # self.scheduler.add_job(
        #     self.mister_controller.control_mister,
        #     'date',
        #     run_date=off_time,
        #     args=['off'],
        #     id='mister_off_test_job',
        #     name='Test Mister OFF Job'
        # )
        # logger.info(f"TESTING: Scheduled mister to turn OFF at {off_time.strftime('%Y-%m-%d %H:%M:%S')}")


# import os
# import sys
# import json
# from datetime import datetime, timedelta
# from apscheduler.schedulers.blocking import BlockingScheduler
# from apscheduler.triggers.cron import CronTrigger
# from apscheduler.triggers.date import DateTrigger

# # Adjust path as needed to import your utilities and controllers
# # Assuming this file is in vivarium/scheduler/src/
# vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
# if vivarium_path not in sys.path:
#     sys.path.insert(0, vivarium_path)

# from utilities.src.logger import LogHelper
# from utilities.src.config import MisterConfig, SensorConfig
# from utilities.src.db_operations import DBOperations
# from terrarium.src.controllers.mister_controller import MisterController
# from scheduler.src.device_scheduler_base import DeviceSchedulerBase
# from terrarium.src.database.sensor_data_queries import SensorDataQueries
# from terrarium.src.database.device_status_queries import DeviceStatusQueries

# logger = LogHelper.get_logger(__name__)
# mister_config = MisterConfig()
# # sensor_config = SensorConfig()

# class MisterScheduler(DeviceSchedulerBase):
#     """
#     Manages the scheduling and automatic control of the vivarium mister.
#     """
#     def __init__(self, scheduler: BlockingScheduler, db_operations: DBOperations, mister_controller: MisterController):
#         """
#         Initializes the MisterScheduler.

#         Args:
#             scheduler (BlockingScheduler): The main APScheduler instance.
#             db_operations (DatabaseOperations): The shared database operations instance.
#             mister_controller (MisterController): An instance of the MisterController to operate the mister.
#         """
#         super().__init__(scheduler, db_operations)
#         self.mister_controller = mister_controller
#         self.sensor_data_queries = SensorDataQueries(self.db_operations)
#         self.device_status_queries = DeviceStatusQueries(self.db_operations)
#         self.at_hour = mister_config.at_hour
#         self.at_minute = mister_config.at_minute
#         self.duration = mister_config.duration
#         logger.info("MisterScheduler initialized.")

#     # -- Commented for future use. --
#     # def check_and_run_mister(self):
#     #     """
#     #     Fetches latest environmental data (humidity) and makes decisions
#     #     for mister activation based on thresholds and intervals.
#     #     This method should be called periodically (e.g., every 5 minutes).
#     #     """
#     #     # logger.info("Checking environmental data for automatic mister control.")
#     #     try:
#     #         # 1. Fetch latest humidity from DB
#     #         sensor_readings = self.sensor_data_queries.get_latest_readings_by_sensor_id(sensor_id = sensor_config.HTU21D)
            
#     #         if not sensor_readings or 'raw_data' not in sensor_readings:
#     #             logger.warning("Could not retrieve latest sensor reading with raw_data. Cannot perform mister check.")
#     #             return

#     #         raw_data = sensor_readings['raw_data']
#     #         if isinstance(raw_data, str): # If raw_data is stored as JSON string
#     #             try:
#     #                 raw_data = json.loads(raw_data)
#     #             except json.JSONDecodeError:
#     #                 logger.error(f"Failed to decode raw_data JSON: {raw_data}")
#     #                 return

#     #         current_humidity = raw_data.get('humidity_percentage')

#     #         if current_humidity is None:
#     #             logger.warning("Humidity data not found in latest sensor reading. Cannot perform mister check.")
#     #             return

#     #         # logger.info(f"Current humidity: {current_humidity}%")

#     #         # 2. Mister Control Logic
#     #         self._handle_mister_activation(current_humidity)

#     #     except Exception as e:
#     #         logger.error(f"Error during automatic mister check: {e}")

#     # def _handle_mister_activation(self, current_humidity: float):
#     #     """
#     #     Handles the logic for controlling the mister based on humidity and interval.
#     #     """
#     #     humidity_threshold = mister_config.humidity_threshold
#     #     mister_interval_hours = mister_config.mister_interval / 60 # Convert minutes to hours
#     #     mister_duration_seconds = mister_config.mister_duration

#     #     logger.info(f"Mister activation check: Current humidity {current_humidity}%, Threshold {humidity_threshold}%, Interval {mister_interval_hours} hours, Duration {mister_duration_seconds} seconds.")

#     #     if current_humidity < humidity_threshold:
#     #         logger.info("Humidity is below threshold. Checking mister interval.")

#     #         last_mister_run_status = self.device_status_queries.get_latest_status_by_device_id(device_id = self.device_id)
#     #         last_run_timestamp_str = None
#     #         if last_mister_run_status and 'timestamp' in last_mister_run_status:
#     #             last_run_timestamp_str = last_mister_run_status['timestamp']

#     #         if last_run_timestamp_str:
#     #             try:
#     #                 # Convert DB timestamp string to datetime object
#     #                 last_run_datetime = datetime.strptime(last_run_timestamp_str, "%Y-%m-%d %H:%M:%S")
#     #                 time_since_last_run = (datetime.now() - last_run_datetime).total_seconds() / 3600 # In hours

#     #                 if time_since_last_run >= mister_interval_hours:
#     #                     logger.info(f"Mister interval (last run {round(time_since_last_run, 2)} hours ago) fulfilled. Activating mister.")
#     #                     if time_since_last_run >= mister_interval_hours:
                            
#     #                         # Schedule mister to turn ON
#     #                         self._schedule_date_job(
#     #                             self.mister_controller.control_mister,
#     #                             run_date=datetime.now() + timedelta(seconds=1),
#     #                             args=['on'],
#     #                             job_id='run_mister_on'
#     #                         )
#     #                         # Schedule mister to turn OFF after the duration
#     #                         self._schedule_date_job(
#     #                             self.mister_controller.control_mister,
#     #                             run_date=datetime.now() + timedelta(seconds=1 + mister_duration_seconds),
#     #                             args=['off'],
#     #                             job_id='run_mister_off'
#     #                         )
#     #                     # # Schedule mister to run for duration and then stop (Non-blocking for scheduler)
#     #                     # self._schedule_date_job(
#     #                     #     self.mister_controller.control_mister,
#     #                     #     run_date=datetime.now() + timedelta(seconds=1), # Run almost immediately
#     #                     #     args=[True],
#     #                     #     job_id='run_mister_now'
#     #                     # )
#     #                     # self._schedule_date_job(
#     #                     #     self.mister_controller.turn_off_mister,  # Call the non-blocking OFF method
#     #                     #     run_date=datetime.now() + timedelta(seconds=1 + mister_duration_seconds),
#     #                     #     # Turn off after duration
#     #                     #     args=[False],  # No arguments needed for turn_off_mister
#     #                     #     job_id='mister_off_job'
#     #                     # )
#     #                     # logger.info(
#     #                     #     f"Mister scheduled to turn ON for {mister_duration_seconds} seconds (non-blocking).")
#     #                 else:
#     #                     logger.info(f"Mister interval not fulfilled. Last run was {round(time_since_last_run, 2)} hours ago (required {mister_interval_hours} hours). Mister not activated.")
#     #             except ValueError:
#     #                 logger.error(f"Failed to parse last mister run timestamp: {last_run_timestamp_str}. Mister interval check skipped.")
#     #             except Exception as e:
#     #                 logger.error(f"Error checking mister interval: {e}")
#     #         else:
#     #             logger.info("No previous mister run timestamp found. Activating mister (first run).")
#     #             # Schedule mister to run for duration and then stop (Non-blocking for scheduler)
#     #             self._schedule_date_job(
#     #                 self.mister_controller.run_mister,
#     #                 run_date=datetime.now() + timedelta(seconds=1), # Run almost immediately
#     #                 args=[mister_duration_seconds],
#     #                 job_id='run_mister_now'
#     #             )
#     #     else:
#     #         logger.info("Humidity is above threshold. Misting not required.")

#     def schedule_fixed_misting_job(self, duration: int = None) -> None:
#         """
#         Schedules a job to run the mister at a specific time daily.
#         """
#         if duration is not None:
#             self.duration = duration

#         job_id_on = 'mister_on'
#         # job_id_off = 'mister_off'
        
#         # Check if jobs already exist to prevent duplicates
#         if self.scheduler.get_job(job_id_on):
#             logger.info(f"Misting job '{job_id_on}' already exists. Skipping.")
#             return

#         # 1. -- ACTUAL - Schedule Mister --
#         # Schedule the "ON" action at 7:00 AM every day
#         # self.scheduler.add_job(
#         #     self.mister_controller.control_mister,
#         #     trigger=CronTrigger(hour = self.at_hour, minute = self.at_minute),
#         #     id=job_id_on,
#         #     name='Morning Mister ON',
#         #     args=['on']
#         # )

#         # Schedule the "ON" action using configurable hour and minute
#         self.scheduler.add_job(
#             self._run_mister_job,
#             trigger=CronTrigger(hour=self.at_hour, minute=self.at_minute),
#             id=job_id_on,
#             name='Morning Mister ON'
#         )
#         logger.info(f"Scheduled fixed daily mister run at {self.at_hour}:{self.at_minute} for {self.duration} seconds. 💧")
        
#         # # Schedule the "OFF" action after the "ON" action based on duration from config.
#         # # run_time = datetime.now().replace(hour = self.at_hour, minute = self.at_minute, second = self.duration, microsecond = 0)
#         # off_time = datetime.now() + timedelta(seconds=self.duration)
#         # self.scheduler.add_job(
#         #     self.mister_controller.control_mister,
#         #     trigger=DateTrigger(run_date = off_time), #CronTrigger(hour = self.at_hour, minute = self.at_minute, second = self.duration),
#         #     id=job_id_off,
#         #     name='Morning Mister OFF',
#         #     args=['off']
#         # )
#         # logger.info("Scheduled daily mister run at 7:00 AM for 15 seconds.")

#         # # 2. -- TESTING: SCHEDULE MISTER TO RUN NOW --
#         # logger.info("TESTING: Starting mister run immediately and scheduling OFF.")
        
#         # # 1. Turn on the mister immediately
#         # self.mister_controller.control_mister(action='on')
        
#         # # 2. Schedule the mister to turn off 15 seconds from now
#         # off_time = datetime.now() + timedelta(seconds=15)
#         # self.scheduler.add_job(
#         #     self.mister_controller.control_mister,
#         #     'date',
#         #     run_date=off_time,
#         #     args=['off'],
#         #     id='mister_off_test_job',
#         #     name='Test Mister OFF Job'
#         # )
#         # logger.info(f"TESTING: Scheduled mister to turn OFF at {off_time.strftime('%Y-%m-%d %H:%M:%S')}")

#     def _run_mister_job(self):
#         """
#         Helper method to run the mister job.
#         This is used to ensure the job runs correctly with the scheduler.
#         """
#         try:
#             self.mister_controller.control_mister('on')
#             logger.info("Mister turned ON successfully.")

#             # Schedule the "OFF" action based on the configured duration
#             off_time = datetime.now() + timedelta(seconds=self.duration)
#             self.scheduler.add_job(
#                 self.mister_controller.control_mister,
#                 trigger=DateTrigger(run_date=off_time),
#                 id='mister_off',
#                 name='Morning Mister OFF',
#                 args=['off'],
#                 replace_existing=True
#             )
#             logger.info(f"Mister scheduled to turn OFF at {off_time.strftime('%Y-%m-%d %H:%M:%S')}")

#         except Exception as e:
#             logger.error(f"Error turning off mister: {e}")
