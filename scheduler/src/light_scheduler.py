# vivarium/scheduler/src/light_scheduler.py

import os
import sys
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler

# Assuming this file is in vivarium/scheduler/src/
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.config import LightConfig
from utilities.src.database_operations import DatabaseOperations # For type hinting
from weather.src.database.astro_queries import AstroQueries
from terrarium.src.controllers.light_controller import LightControler
from scheduler.src.device_scheduler_base import DeviceSchedulerBase # Import the base scheduler

logger = LogHelper.get_logger(__name__)
light_config = LightConfig()

class LightScheduler(DeviceSchedulerBase):
    """
    Manages the scheduling of vivarium lights based on astro data or defaults.
    """
    def __init__(self, scheduler: BlockingScheduler, db_operations: DatabaseOperations, light_controller: LightControler):
        """
        Initializes the LightScheduler.

        Args:
            scheduler (BlockingScheduler): The main APScheduler instance.
            db_operations (DatabaseOperations): The shared database operations instance.
            light_controller (LightControler): An instance of the LightControler to operate the lights.
        """
        super().__init__(scheduler, db_operations)
        self.light_controller = light_controller
        self.astro_queries = AstroQueries(self.db_operations) # AstroQueries needs db_operations
        logger.info("LightScheduler initialized.")

    def schedule_daily_lights(self):
        """
        Updates the daily light schedule based on fetched astro data or configured defaults.
        This method should be called periodically (e.g., once a day after weather fetch).
        """
        logger.info("Updating terrarium lights schedule.")
        
        today_str = datetime.now().date().strftime('%Y-%m-%d')
        location_id = 1 # Consider making this configurable or dynamic

        sunrise_time_to_schedule = None
        sunset_time_to_schedule = None

        try:
            # Attempt to fetch astro data from DB
            astro_data = self.astro_queries.get_sunrise_sunset(location_id, today_str)

            if astro_data and astro_data.get('sunrise') and astro_data.get('sunset'):
                db_sunrise_str = astro_data['sunrise']
                db_sunset_str = astro_data['sunset']
                logger.info(f"Using fetched sunrise/sunset for {today_str}: Sunrise: {db_sunrise_str}, Sunset: {db_sunset_str}")

                sunrise_dt_obj = datetime.strptime(db_sunrise_str.split('\t')[0].strip(), '%I:%M %p')
                sunset_dt_obj = datetime.strptime(db_sunset_str.split('\t')[0].strip(), '%I:%M %p')
                
                sunrise_time_to_schedule = sunrise_dt_obj.time()
                sunset_time_to_schedule = sunset_dt_obj.time()

            else:
                logger.warning(f"Could not retrieve complete sunrise/sunset data from database for {today_str}. Using default times from config.")
                # Fallback to defaults from LightConfig
                sunrise_dt_obj = datetime.strptime(light_config.lights_on.split('\t')[0].strip(), '%I:%M %p')
                sunset_dt_obj = datetime.strptime(light_config.lights_off.split('\t')[0].strip(), '%I:%M %p')
                
                sunrise_time_to_schedule = sunrise_dt_obj.time()
                sunset_time_to_schedule = sunset_dt_obj.time()

        except Exception as e:
            logger.error(f"Error fetching/parsing astro data or config defaults: {e}. Using hardcoded fallback times.")
            # Fallback to hardcoded times if config parsing also fails as a last resort
            sunrise_time_to_schedule = datetime.strptime("06:00 AM", '%I:%M %p').time()
            sunset_time_to_schedule = datetime.strptime("06:00 PM", '%I:%M %p').time()
        
        # Now, schedule the jobs using the determined times
        if sunrise_time_to_schedule and sunset_time_to_schedule:
            # Schedule light ON at sunrise
            self._schedule_cron_job(
                self.light_controller.control_light, # Call the controller method
                hour=sunrise_time_to_schedule.hour,
                minute=sunrise_time_to_schedule.minute,
                second=sunrise_time_to_schedule.second,
                args=['on'],
                job_id='lights_on_daily'
            )

            # Schedule light OFF at sunset
            self._schedule_cron_job(
                self.light_controller.control_light, # Call the controller method
                hour=sunset_time_to_schedule.hour,
                minute=sunset_time_to_schedule.minute,
                second=sunset_time_to_schedule.second,
                args=['off'],
                job_id='lights_off_daily'
            )
        else:
            logger.critical("Failed to determine valid sunrise/sunset times. Light schedule not set.")

