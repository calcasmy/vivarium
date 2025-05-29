# # scheduler/vivariumscheduler.py
# ''' Primary Scheduler for all vivarium related activities'''

# import os
# import sys
# import time
# import json # Needed for parsing raw_data if it's JSON string
# import traceback
# import subprocess

# from datetime import time as datetime_time, date, datetime, timedelta
# from apscheduler.schedulers.blocking import BlockingScheduler
# from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

# # Get the absolute path to the 'vivarium' directory
# vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# # Add 'vivarium' to the Python path if it's not already there
# if vivarium_path not in sys.path:
#     sys.path.insert(0, vivarium_path)

# # Importing utilities package
# from utilities.src.logger import LogHelper
# from utilities.src.database_operations import DatabaseOperations

# # Primary terrariu status
# # from terrarium.src.controllers.terrarium_status import TerrariumStatus
# from terrarium.src.sensors.terrarium_sensor_reader import TerrariumSensorReader

# # Import device controller classes
# from terrarium.src.controllers.light_controller import LightController
# from terrarium.src.controllers.mister_controller_v2 import MisterControllerV2
# # from terrarium.src.controllers.humidifier_control import HumidiferController # Uncomment when ready

# # New: Import scheduler classes
# from scheduler.src.light_scheduler import LightScheduler
# from scheduler.src.mister_scheduler import MisterScheduler

# # New: Import Sensor Queries for temperature/humidity data
# from terrarium.src.database.sensor_queries import SensorQueries
# from weather.fetch_daily_weather import FetchDailyWeather
# from weather.src.database.astro_queries import AstroQueries # Still needed for LightScheduler

# logger = LogHelper.get_logger(__name__)

# class VivariumSchedulerV2:
#     """
#         Scheduler class for all vivarium [Aquarium, Terrarium etc.] related jobs
#     """
#     def __init__(self):
#         self.scheduler = BlockingScheduler()
#         self.scheduler.add_listener(self.job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

#         # Centralized Database Operations
#         self.db_operations = DatabaseOperations()
#         self.db_operations.connect()
#         logger.info("Database connection established for VivariumScheduler.")

#         # Initialize Device Controllers
#         self.light_controller = LightController(db_operations=self.db_operations)
#         self.mister_controller = MisterControllerV2(db_operations=self.db_operations)
#         # self.humidifier_controller = HumidiferController(db_operations=self.db_operations) # Uncomment when ready

#         # Initialize specific device schedulers
#         self.light_scheduler = LightScheduler(
#             scheduler=self.scheduler,
#             db_operations=self.db_operations,
#             light_controller=self.light_controller
#         )

#         self.mister_scheduler = MisterScheduler(
#             scheduler=self.scheduler,
#             db_operations=self.db_operations,
#             mister_controller=self.mister_controller
#         )

#         logger.info("VivariumScheduler and sub-schedulers initialized.")

#         logger.info("Setting initial device states for system startup.")
#         try:
#             # self.light_controller.control_light(action="off") # Ensures light is off on scheduler startup
#             # self.mister_controller.control_mister(action="off") # Ensures mister is off on scheduler startup
#             logger.info("All devices set to initial OFF state by VivariumScheduler.")
#         except Exception as e:
#             logger.error(f"Failed to set initial device states: {e}", exc_info=True)


#     def __del__(self):
#         """
#         Ensures database connection is closed when scheduler object is destroyed.
#         """
#         if self.db_operations:
#             self.db_operations.close()
#             logger.info("Database connection closed by VivariumScheduler (from __del__).")

#     def job_listener(self, event):
#         if event.exception:
#             logger.error(f"Job '{event.job_id}' raised {event.exception.__class__.__name__}: {event.exception}")
#             logger.error(f"Traceback for job '{event.job_id}':\n{traceback.format_exc()}")
#         elif event.job_id == 'fetch_weather_daily':
#             logger.info(f"Job '{event.job_id}' successfully finished. Triggering light schedule update.")
#             self.light_scheduler.schedule_daily_lights() # Call the light scheduler's method
#         elif event.job_id == 'run_current_status':
#             logger.info(f"Job '{event.job_id}' successfully finished. Triggering environmental checks.")
#             self.mister_scheduler.check_and_run_mister() # Call the mister scheduler's method
            
#             # Add other device checks here (e.g., self.humidifier_scheduler.check_and_run_humidifier())

#     def schedule_jobs(self):
#         logger.info("Scheduling core Vivarium jobs.")
#         # Schedule fetch_daily_weather.py to run once a day at 1:00 AM
#         # :: Temporarly commented
#         # fetch_weather_script = FetchDailyWeather.script_path()
#         # self.scheduler.add_job(
#         #     self._run_external_script,
#         #     'cron',
#         #     hour=1,
#         #     minute=0,
#         #     args=[fetch_weather_script],
#         #     id='fetch_weather_daily')
#         # logger.info(f"Scheduled {os.path.basename(fetch_weather_script)} to run daily at 01:00.")

#         # Schedule terrarium_status.py to run every 5 minutes
#         # :: Temporarly commented
#         terrarium_status_script = terrarium_status_script.script_path()
#         self.scheduler.add_job(
#             self._run_external_script,
#             'interval',
#             minutes=5,
#             args=[terrarium_status_script],
#             id='run_current_status')

#         # self.scheduler.add_job(
#         #     self._run_external_script, 
#         #     'interval', 
#         #     minutes=0.5,
#         #     args=[terrarium_status_script], 
#         #     id='run_current_status')
#         logger.info(f"Scheduled {os.path.basename(terrarium_status_script)} to run every 1 minutes.")

#         # logger.info(f"Scheduled {os.path.basename(terrarium_status_script)} to run every 5 minutes.")

#     def _run_external_script(self, script_path):
#         """
#         Helper method to run external Python scripts using subprocess.
#         This is kept in VivariumScheduler as it's a general utility.
#         """
#         script_name = os.path.basename(script_path)
#         logger.info(f"Running external script: {script_name}")
#         try:
#             process = subprocess.Popen(['python3', script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#             stdout, stderr = process.communicate(timeout=60) # Increased timeout to 60s
#             if process.returncode == 0:
#                 logger.info(f"Script {script_name} executed successfully.")
#                 if stdout:
#                     logger.debug(f"Stdout for {script_name}:\n{stdout.decode().strip()}")
#             else:
#                 logger.error(f"Script {script_name} failed with error (Return Code: {process.returncode}).")
#                 if stderr:
#                     logger.error(f"Stderr for {script_name}:\n{stderr.decode().strip()}")
#         except subprocess.TimeoutExpired:
#             logger.error(f"Script {script_name} timed out after {60} seconds.")
#             process.kill()
#         except FileNotFoundError:
#             logger.error(f"Script not found: {script_name} at path {script_path}")
#         except Exception as e:
#             logger.error(f"Unexpected error running script {script_name}: {e}")

#     def run(self):
#         logger.info("Vivarium Scheduler starting main loop.")
#         self.schedule_jobs()

#         try:
#             self.scheduler.start() # BlockingScheduler will block here
#         except (KeyboardInterrupt, SystemExit):
#             logger.info("Vivarium Scheduler stopping gracefully...")
#         finally:
#             # Ensure DB connection is closed even if an unexpected error occurs
#             if self.db_operations:
#                 self.db_operations.close()
#                 logger.info("Database connection closed")
#             logger.info("Vivarium Scheduler stopped.")

# if __name__ == "__main__":
#     scheduler = VivariumSchedulerV2()
#     scheduler.run()


# vivarium/scheduler/vivariumscheduler.py
''' Primary Scheduler for all vivarium related activities'''

import os
import sys
import time
import json
import traceback
import subprocess # Keep this if fetch_daily_weather.py is still an external script

from datetime import time as datetime_time, date, datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Add 'vivarium' to the Python path if it's not already there
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# Importing utilities package
from utilities.src.logger import LogHelper
from utilities.src.database_operations import DatabaseOperations

# PRIMARY CHANGE: Import the NEW TerrariumSensorReader
from terrarium.src.sensors.terrarium_sensor_reader import TerrariumSensorReader # <--- UPDATED THIS LINE

# Import device controller classes
from terrarium.src.controllers.light_controller import LightController
from terrarium.src.controllers.mister_controller_v2 import MisterControllerV2
# from terrarium.src.controllers.humidifier_control import HumidiferController # Uncomment when ready

# New: Import scheduler classes
from scheduler.src.light_scheduler import LightScheduler
from scheduler.src.mister_scheduler import MisterScheduler

# New: Import Sensor Queries for temperature/humidity data (likely not directly needed in scheduler's main file, but ok for now)
from terrarium.src.database.sensor_queries import SensorQueries
from weather.fetch_daily_weather import FetchDailyWeather
from weather.src.database.astro_queries import AstroQueries # Still needed for LightScheduler

logger = LogHelper.get_logger(__name__)

class VivariumSchedulerV2:
    """
        Scheduler class for all vivarium [Aquarium, Terrarium etc.] related jobs
    """
    def __init__(self):
        self.scheduler = BlockingScheduler()
        self.scheduler.add_listener(self.job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

        # Centralized Database Operations
        self.db_operations = DatabaseOperations()
        self.db_operations.connect()
        logger.info("Database connection established for VivariumScheduler.")

        # Initialize Device Controllers (These are instantiated ONCE)
        self.light_controller = LightController(db_operations=self.db_operations)
        self.mister_controller = MisterControllerV2(db_operations=self.db_operations)
        # self.humidifier_controller = HumidiferController(db_operations=self.db_operations) # Uncomment when ready

        # NEW: Initialize the Sensor Reader (Instantiated ONCE)
        self.terrarium_sensor_reader = TerrariumSensorReader(db_operations=self.db_operations) # <--- ADDED THIS LINE
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

        logger.info("Setting initial device states for system startup.")
        try:
            # UNCOMMENT THESE LINES IF YOU WANT DEVICES TO GO OFF WHEN SCHEDULER STARTS
            self.light_controller.control_light(action="off") # Ensures light is off on scheduler startup
            self.mister_controller.control_mister(action="off") # Ensures mister is off on scheduler startup
            logger.info("All devices set to initial OFF state by VivariumScheduler.")
        except Exception as e:
            logger.error(f"Failed to set initial device states: {e}", exc_info=True)


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
        elif event.job_id == 'fetch_weather_daily':
            logger.info(f"Job '{event.job_id}' successfully finished. Triggering light schedule update.")
            self.light_scheduler.schedule_daily_lights() # Call the light scheduler's method
        elif event.job_id == 'run_current_status':
            logger.info(f"Job '{event.job_id}' successfully finished. Triggering environmental checks.")
            # The terrarium sensor data has already been read by the time this listener runs
            # as it was scheduled directly by `self.terrarium_sensor_reader.read_and_store_data`
            self.mister_scheduler.check_and_run_mister()


    def schedule_jobs(self):
        logger.info("Scheduling core Vivarium jobs.")
        # Schedule fetch_daily_weather.py to run once a day at 1:00 AM
        # If fetch_daily_weather is still an external script, keep this.
        # This still uses subprocess, if you refactor fetch_daily_weather to a class, change this.
        fetch_weather_script = FetchDailyWeather.script_path()
        self.scheduler.add_job(
            self._run_external_script,
            'cron',
            hour=1,
            minute=0,
            args=[fetch_weather_script],
            id='fetch_weather_daily')
        logger.info(f"Scheduled {os.path.basename(fetch_weather_script)} to run daily at 01:00.")

        # REMOVE THE OLD SUBPROCESS CALL FOR TERRARIUM STATUS:
        # terrarium_status_script = TerrariumStatus.script_path()
        # self.scheduler.add_job(
        #     self._run_external_script,
        #     'interval',
        #     minutes=5,
        #     args=[terrarium_status_script],
        #     id='run_current_status')

        # NEW: Schedule the method of the instantiated TerrariumSensorReader
        self.scheduler.add_job(
            self.terrarium_sensor_reader.read_and_store_data, # <--- THIS IS THE KEY CHANGE
            'interval',
            minutes=5, # You can set this lower for testing (e.g., 0.5 or 1) then back to 5
            id='run_current_status')
        logger.info(f"Scheduled Terrarium sensor reading to run every 5 minutes.")


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
    scheduler = VivariumSchedulerV2()
    scheduler.run()