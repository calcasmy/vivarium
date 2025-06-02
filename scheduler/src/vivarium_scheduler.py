# vivarium/scheduler/vivarium_scheduler.py
''' Primary Scheduler for all vivarium related activities'''

import os
import sys
import traceback
import subprocess # Keep this if fetch_daily_weather.py is still an external script

from datetime import time as datetime_time,date, datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler.triggers.interval import IntervalTrigger

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Add 'vivarium' to the Python path if it's not already there
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# Importing utilities package
from utilities.src.logger import LogHelper
from utilities.src.config import WeatherAPIConfig
from utilities.src.database_operations import DatabaseOperations

# PRIMARY CHANGE: Import the NEW TerrariumSensorReader
from terrarium.src.sensors.terrarium_sensor_reader import TerrariumSensorReader

# Import device controller classes
from terrarium.src.controllers.light_controller import LightController
from terrarium.src.controllers.mister_controller import MisterControllerV2
# from terrarium.src.controllers.humidifier_control import HumidiferController # Uncomment when ready

# New: Import scheduler classes
from scheduler.src.light_scheduler import LightScheduler
from scheduler.src.mister_scheduler import MisterScheduler

# New: Import Sensor Queries for temperature/humidity data (likely not directly needed in scheduler's main file, but ok for now)
from weather.fetch_daily_weather import FetchDailyWeather

logger = LogHelper.get_logger(__name__)

class VivariumScheduler:
    """
        Scheduler class for all vivarium [Aquarium, Terrarium etc.] related jobs
    """
    def __init__(self):
        self.scheduler = BlockingScheduler()
        self.scheduler.add_listener(self.job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

        self.weather_config = WeatherAPIConfig()

        # Centralized Database Operations
        self.db_operations = DatabaseOperations()
        self.db_operations.connect()
        logger.info("Database connection established for VivariumScheduler.")

        # Initialize Device Controllers (These are instantiated ONCE)
        self.light_controller = LightController(db_operations=self.db_operations)
        self.mister_controller = MisterControllerV2(db_operations=self.db_operations)
        # self.humidifier_controller = HumidiferController(db_operations=self.db_operations) # Uncomment when ready

        # NEW: Initialize the Sensor Reader (Instantiated ONCE)
        self.terrarium_sensor_reader = TerrariumSensorReader(db_operations=self.db_operations)
        logger.info("Terrarium Sensor Reader initialized.")


        # Initialize specific device schedulers
        self.light_scheduler = LightScheduler(
            scheduler=self.scheduler,
            db_operations=self.db_operations,
            light_controller=self.light_controller
        )
        self.mister_scheduler = MisterScheduler(
            scheduler=self.scheduler,
            db_operations=self.db_operations,
            mister_controller=self.mister_controller
        )
        logger.info("VivariumScheduler and sub-schedulers initialized.")

        self._perform_system_boot_check()
        

    def __del__(self):
        """
        Ensures database connection is closed when scheduler object is destroyed.
        """
        if self.db_operations:
            self.db_operations.close()
            logger.info("Database connection closed by VivariumScheduler (from __del__).")

    def job_listener(self, event):
        if event.exception:
            logger.error(f"Job '{event.job_id}' raised {event.exception.__class__.__name__}: {event.exception}")
            logger.error(f"Traceback for job '{event.job_id}':\n{traceback.format_exc()}")

            # -- Retry logic for 'fetch daily weather
            if event.job_id == 'fetch_weather_daily':
                retry_interval_hours = self.weather_config.get('weather_fetch_interval', default=2, type = int)
                retry_time = datetime.now() + timedelta(hours = retry_interval_hours)
                self.scheduler.add_job(
                    self._run_external_script,
                    'date',
                    run_date=retry_time,
                    args=[FetchDailyWeather.script_path()],
                    id='fetch_weather_daily_retry',
                    replace_existing=True # If previous retry failed, replace it
                )
                logger.warning(f"Job 'fetch_weather_daily' failed. Retrying in 2 hours at {retry_time.strftime('%Y-%m-%d %H:%M:%S')}.")

        elif event.job_id == 'fetch_weather_daily' or event.job_id == 'fetch_weather_daily_retry':
            logger.info(f"Job '{event.job_id}' successfully finished. Triggering light schedule update.")
            self.light_scheduler.schedule_daily_lights() # Call the light scheduler's method
        elif event.job_id == 'run_current_status':
            logger.info(f"Job '{event.job_id}' successfully finished. Triggering environmental checks.")
            self.mister_scheduler.check_and_run_mister()

    def _perform_system_boot_check(self):
        """
        Performs a comprehensive system boot check for all connected components.
        This runs only once when the scheduler is initialized.
        It checks equipment availability, sets initial states, and performs
        any necessary initial logic (like light schedule setup).
        """
        logger.info("--- Starting System Boot Check ---")

        # -- Light System Check --
        try:
            logger.info("Checking Light System...")
            
            # 1. Ensure light is OFF initially (Safe State)
            self.light_controller.control_light(action = "off")

             # 2. Set up today's schedule and perform immediate check
            # This call will:
            #   a) Fetch today's sunrise/sunset.
            #   b) Update light_controller's internal schedule.
            #   c) Perform the immediate check (control_light(action=None))
            #      to set light to ON/OFF based on current time and new schedule.
            #   d) Schedule today's specific ON/OFF cron jobs.
            # This is your one-time light state check on boot.
            self.light_scheduler.schedule_daily_lights()
            logger.info("Light system: Daily schedule set up and immediate state adjusted based on current time.")
            
        except Exception as e:
            logger.error(f"Error during Light System boot check: {e}", exc_info=True)

        # -- Mister System Check --
        try:
            logger.info("Checking Mister System...")
            # 1. Ensure mister is OFF initially (safe state)
            self.mister_controller.control_mister(action="off")
            logger.info("Mister system: Initial state set to OFF.")

            self.mister_scheduler.check_and_run_mister() # Added this call
            logger.info("Mister system: Current state adjusted based on environmental conditions.")
            
        except Exception as e:
            logger.error(f"Error during Mister System boot check: {e}", exc_info=True)
            # Add error handling (e.g., send notification, disable mister functionality).

        # --- Sensor System Check ---
        try:
            logger.info("Checking Sensor Systems...")
            # Attempt to read data to confirm sensors are operational.
            self.terrarium_sensor_reader.read_and_store_data()
            logger.info("Sensor systems: Performed initial reading. Check logs for sensor status.")
        except Exception as e:
            logger.error(f"Error during Sensor System boot check: {e}", exc_info=True)
        
        logger.info("--- System Boot Check Complete ---")

    def schedule_jobs(self):
        logger.info("Scheduling core Vivarium jobs.")
        # Schedule fetch_daily_weather.py to run once a day at 1:00 AM
        fetch_weather_script = FetchDailyWeather.script_path()
        self.scheduler.add_job(
            self._run_external_script,
            'cron',
            hour=1,
            minute=0,
            args=[fetch_weather_script],
            id='fetch_weather_daily'
        )
        logger.info(f"Scheduled {os.path.basename(fetch_weather_script)} to run daily at 01:00.")

        # # NEW: Schedule the method of the instantiated TerrariumSensorReader
        self.scheduler.add_job(
            self.terrarium_sensor_reader.read_and_store_data, # <--- THIS IS THE KEY CHANGE
            'interval',
            minutes=5, # You can set this lower for testing (e.g., 0.5 or 1) then back to 5
            id='run_current_status')
        logger.info(f"Scheduled Terrarium sensor reading to run every 5 minutes.")

        # -- Testing purposes --
        # ** Schedule fetch_daily_weather.py to run RIGHT NOW
        # fetch_weather_script = FetchDailyWeather.script_path()
        # self.scheduler.add_job(
        #     self._run_external_script,
        #     'date',
        #     run_date=datetime.now(), # Set the run date to the current time
        #     args=[fetch_weather_script],
        #     id='fetch_weather_daily')
        # logger.info(f"Scheduled {os.path.basename(fetch_weather_script)} to run immediately.")

        # self.scheduler.add_job(
        #     self.terrarium_sensor_reader.read_and_store_data, # <--- THIS IS THE KEY CHANGE
        #     'interval',
        #     seconds=30,
        #     id='run_current_status')
        # logger.info(f"Scheduled Terrarium sensor reading to run every 5 minutes.")
        
    def _run_external_script(self, script_path):
        """
        Helper method to run external Python scripts using subprocess.
        This is kept in VivariumScheduler as it's a general utility.
        """
        script_name = os.path.basename(script_path)
        logger.info(f"Running external script: {script_name}")
        try:
            process = subprocess.Popen(['python3', script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate(timeout=60) # Increased timeout to 60s
            if process.returncode == 0:
                logger.info(f"Script {script_name} executed successfully.")
                if stdout:
                    logger.debug(f"Stdout for {script_name}:\n{stdout.decode().strip()}")
            else:
                logger.error(f"Script {script_name} failed with error (Return Code: {process.returncode}).")
                if stderr:
                    logger.error(f"Stderr for {script_name}:\n{stderr.decode().strip()}")
        except subprocess.TimeoutExpired:
            logger.error(f"Script {script_name} timed out after {60} seconds.")
            process.kill()
        except FileNotFoundError:
            logger.error(f"Script not found: {script_name} at path {script_path}")
        except Exception as e:
            logger.error(f"Unexpected error running script {script_name}: {e}")

    def run(self):
        logger.info("Vivarium Scheduler starting main loop.")
        self.schedule_jobs()

        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Vivarium Scheduler stopping gracefully...")
        finally:
            # Ensure DB connection is closed even if an unexpected error occurs
            if self.db_operations:
                self.db_operations.close()
                logger.info("Database connection closed")
            logger.info("Vivarium Scheduler stopped.")

if __name__ == "__main__":
    scheduler = VivariumScheduler()
    scheduler.run()