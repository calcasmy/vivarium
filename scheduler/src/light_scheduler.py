# vivarium/scheduler/src/light_scheduler.py

import os
import sys
from datetime import date, datetime, time,  timedelta
from apscheduler.schedulers.blocking import BlockingScheduler

# Assuming this file is in vivarium/scheduler/src/
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.config import LightConfig
from utilities.src.db_operations import DBOperations, ConnectionDetails
from database.climate_data_ops.astro_queries import AstroQueries
from terrarium.src.controllers.light_controller import LightController
from scheduler.src.device_scheduler_base import DeviceSchedulerBase # Import the base scheduler

logger = LogHelper.get_logger(__name__)


class LightScheduler(DeviceSchedulerBase):
    """
    Manages the scheduling of vivarium lights based on astro data or defaults.
    """
    def __init__(self, scheduler: BlockingScheduler, db_operations: DBOperations, light_controller: LightController):
        """
        Initializes the LightScheduler.

        Args:
            scheduler (BlockingScheduler): The main APScheduler instance.
            db_operations (DatabaseOperations): The shared database operations instance.
            light_controller (LightControler): An instance of the LightControler to operate the lights.
        """
        super().__init__(scheduler, db_operations)
        self.light_config = LightConfig()
        self.light_controller = light_controller
        self.astro_queries = AstroQueries(self.db_operations)
        logger.info("LightScheduler initialized.")

    def _fetch_sunrise_sunset(self) -> dict:
        yesterday = (datetime.now().date() - timedelta(days=1)).strftime('%Y-%m-%d')
        location_id = 1 # Consider making this configurable or dynamic

        sunrise_time_to_schedule = None
        sunset_time_to_schedule = None

        try:
            # Attempt to fetch astro data from DB
            astro_data = self.astro_queries.get_sunrise_sunset(location_id, yesterday)

            if not astro_data:
                # 2. If no data for yesterday, try to get the very last record.
                logger.warning(f"No astro data found for {yesterday}. Attempting to fetch the latest record instead.")
                astro_data = self.astro_queries.get_latest_sunrise_sunset(location_id)
            
            if astro_data and astro_data.get('sunrise') and astro_data.get('sunset'):
                db_sunrise_str = astro_data['sunrise']
                db_sunset_str = astro_data['sunset']
                logger.info(f"Using fetched sunrise/sunset for {yesterday}: Sunrise: {db_sunrise_str}, Sunset: {db_sunset_str}")

                sunrise_time_to_schedule = datetime.strptime(db_sunrise_str.split('\t')[0].strip(), '%I:%M %p').time()
                sunset_time_to_schedule = datetime.strptime(db_sunset_str.split('\t')[0].strip(), '%I:%M %p').time()
            else:
                logger.warning(f"Could not retrieve complete sunrise/sunset data from database for {yesterday}. Using default times from config.")
                # Fallback to defaults from LightConfig
                sunrise_time_to_schedule = datetime.strptime(self.light_config.lights_on.split('\t')[0].strip(), '%I:%M %p').time()
                sunset_time_to_schedule = datetime.strptime(self.light_config.lights_off.split('\t')[0].strip(), '%I:%M %p').time()
        except Exception as e:
            logger.error(f"Error fetching/parsing astro data or config defaults: {e}. Using hardcoded fallback times.")
            # Fallback to hardcoded times if config parsing also fails as a last resort
            sunrise_time_to_schedule = datetime.strptime("06:00 AM", '%I:%M %p').time()
            sunset_time_to_schedule = datetime.strptime("06:00 PM", '%I:%M %p').time()

        return {
            'sunrise_time_to_schedule': sunrise_time_to_schedule, 
            'sunset_time_to_schedule': sunset_time_to_schedule
        }

    def schedule_daily_lights(self):
        """
        Updates the daily light schedule based on fetched astro data or configured defaults.
        This method should be called periodically (e.g., once a day after weather fetch).
        """

        # -- TESTING: Schedule a job to turn on lights immediately and then off after 30 seconds
        # 1. Turn on the lights immediately
        # logger.info("TESTING: Turning lights ON immediately.")
        # self.light_controller.control_light(action='on')

        # # 2. Schedule the lights to turn off after 30 seconds
        # off_time = datetime.now() + timedelta(seconds=30)
        # self.scheduler.add_job(
        #     self.light_controller.control_light,
        #     'date',
        #     run_date=off_time,
        #     args=['off'],
        #     id='lights_off_test_job',
        #     name='Test Lights OFF Job'
        # )
        # logger.info(f"TESTING: Scheduled lights to turn OFF at {off_time.strftime('%Y-%m-%d %H:%M:%S')}")

        logger.info("Updating terrarium lights schedule.")
        
        sun_schedule = self._fetch_sunrise_sunset()

        sunrise_time_to_schedule = sun_schedule['sunrise_time_to_schedule']
        sunset_time_to_schedule = sun_schedule['sunset_time_to_schedule']

        if sunrise_time_to_schedule and sunset_time_to_schedule:
            # Convert the time object to a full datetime object
            sunset_datetime = datetime.combine(date.today(), sunset_time_to_schedule)
            sunset_datetime_with_offset = sunset_datetime + timedelta(hours=2)
            final_sunset_time = sunset_datetime_with_offset.time()

            # 1. IMMEDIATE STATE CHECK: Turn on or off the light based on the adjusted time.
            current_time = datetime.now().time()
            if sunrise_time_to_schedule <= current_time <= final_sunset_time: # Use final_sunset_time here
                logger.info("Current time is within the scheduled ON period. Turning lights ON.")
                self.light_controller.control_light(action='on')
            else:
                logger.info("Current time is outside the scheduled ON period. Ensuring lights are OFF.")
                self.light_controller.control_light(action='off')

            # 2. SCHEDULE CRON JOBS: Now schedule the jobs for future events.
            self._schedule_cron_job(
                self.light_controller.control_light,
                hour=sunrise_time_to_schedule.hour,
                minute=sunrise_time_to_schedule.minute,
                second=sunrise_time_to_schedule.second,
                args=['on'],
                job_id='lights_on_daily'
            )
            self._schedule_cron_job(
                self.light_controller.control_light,
                hour=final_sunset_time.hour,
                minute=final_sunset_time.minute,
                second=final_sunset_time.second,
                args=['off'],
                job_id='lights_off_daily'
            )
            logger.info(f"Daily light schedule set: ON at {sunrise_time_to_schedule.strftime('%H:%M:%S')}, OFF at {final_sunset_time.strftime('%H:%M:%S')}")
        else:
            logger.critical("Failed to determine valid sunrise/sunset times. Light schedule not set.")




        # logger.info("Updating terrarium lights schedule.")
        
        # sun_schedule = self._fetch_sunrise_sunset()

        # sunrise_time_to_schedule = sun_schedule['sunrise_time_to_schedule']
        # sunset_time_to_schedule = sun_schedule['sunset_time_to_schedule']

        # # -- ACTUAL SCHEDULING LOGIC
        # if sunrise_time_to_schedule and sunset_time_to_schedule:
        #     # 1. IMMEDIATE STATE CHECK: Turn on or off the light based on the current time.
        #     current_time = datetime.now().time()
        #     if sunrise_time_to_schedule <= current_time <= sunset_time_to_schedule:
        #         logger.info("Current time is within the scheduled ON period. Turning lights ON.")
        #         self.light_controller.control_light(action='on')
        #     else:
        #         logger.info("Current time is outside the scheduled ON period. Ensuring lights are OFF.")
        #         self.light_controller.control_light(action='off')

        #     # Convert the time object to a full datetime object
        #     sunset_datetime = datetime.combine(date.today(), sunset_time_to_schedule)
        #     sunset_datetime_with_offset = sunset_datetime + timedelta(hours=2)
        #     final_sunset_time = sunset_datetime_with_offset.time()

        #     # 2. SCHEDULE CRON JOBS: Now schedule the jobs for future events.
        #     self._schedule_cron_job(
        #         self.light_controller.control_light,
        #         hour=sunrise_time_to_schedule.hour,
        #         minute=sunrise_time_to_schedule.minute,
        #         second=sunrise_time_to_schedule.second,
        #         args=['on'],
        #         job_id='lights_on_daily'
        #     )
        #     self._schedule_cron_job(
        #         self.light_controller.control_light,
        #         hour=final_sunset_time.hour,
        #         minute=final_sunset_time.minute,
        #         second=final_sunset_time.second,
        #         args=['off'],
        #         job_id='lights_off_daily'
        #     )
        #     logger.info(f"Daily light schedule set: ON at {sunrise_time_to_schedule.strftime('%H:%M:%S')}, OFF at {final_sunset_time.strftime('%H:%M:%S')}")
        # else:
        #     logger.critical("Failed to determine valid sunrise/sunset times. Light schedule not set.")
