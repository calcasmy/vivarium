# vivarium/scheduler/vivarium_scheduler.py
''' Primary Scheduler for all vivarium related activities'''

import os
import sys
import traceback
import subprocess
from datetime import time as datetime_time, date, datetime, timedelta

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler.triggers.interval import IntervalTrigger

# --- Path Configuration ---
# Get the absolute path to the 'vivarium' directory
# This ensures that imports from 'utilities' and 'terrarium' work correctly
# regardless of where vivarium_scheduler.py is executed from.
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Add 'vivarium' (the root of the project) to the Python path if it's not already there
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# --- Project Imports ---
# Importing core utilities
from utilities.src.logger import LogHelper
from utilities.src.config import WeatherAPIConfig
from utilities.src.db_operations import DBOperations, ConnectionDetails
# from utilities.src.database_operations import DatabaseOperations

# Importing terrarium-specific components
from terrarium.src.sensors.terrarium_sensor_reader import TerrariumSensorReader
from terrarium.src.controllers.light_controller import LightController
from terrarium.src.controllers.mister_controller import MisterControllerV2
# from terrarium.src.controllers.humidifier_control import HumidiferController # Uncomment when ready

# Importing scheduler-specific components
from scheduler.src.light_scheduler import LightScheduler
from scheduler.src.mister_scheduler import MisterScheduler

# Importing external scripts/modules
from weather.fetch_daily_weather import FetchDailyWeather

# --- Global Logger Instance ---
logger = LogHelper.get_logger(__name__)

# --- Constants ---
WEATHER_FETCH_HOUR: int = 1
WEATHER_FETCH_MINUTE: int = 0
SENSOR_READ_INTERVAL_MINUTES: int = 5
EXTERNAL_SCRIPT_TIMEOUT_SECONDS: int = 60 # Timeout for external subprocess execution


class VivariumScheduler:
    """
    Primary scheduler class responsible for orchestrating all vivarium-related jobs.

    This class initializes the core components, sets up job listeners for error handling
    and success notifications, performs a comprehensive system boot check, and schedules
    periodic tasks for various vivarium functionalities (e.g., sensor reading,
    light control, misting, weather data fetching).
    """

    def __init__(self):
        """
        Initializes the VivariumScheduler, its components, and sets up job listeners.

        This involves:
        - Setting up the APScheduler's BlockingScheduler.
        - Establishing a database connection.
        - Instantiating all necessary device controllers (Light, Mister, Humidifier, etc.).
        - Instantiating the primary sensor reader.
        - Initializing sub-schedulers for specific device management (LightScheduler, MisterScheduler).
        - Performing a critical system boot check to ensure initial safe states and configurations.
        """
        self.scheduler: BlockingScheduler = BlockingScheduler()
        self.scheduler.add_listener(self._job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        logger.info("APScheduler initialized and listener added.")

        self.weather_config: WeatherAPIConfig = WeatherAPIConfig()

        # Centralized Database Operations
        self.db_operations: DBOperations = DBOperations()
        self.db_operations.connect()
        logger.info("Database connection established for VivariumScheduler.")

        # Initialize Device Controllers (Instantiated ONCE, passed to sub-schedulers)
        self.light_controller: LightController = LightController(db_operations=self.db_operations)
        self.mister_controller: MisterControllerV2 = MisterControllerV2(db_operations=self.db_operations)
        # self.humidifier_controller: HumidiferController = HumidiferController(db_operations=self.db_operations) # Uncomment when ready
        logger.info("Device controllers initialized.")

        # Initialize the Sensor Reader (Instantiated ONCE, method scheduled directly)
        self.terrarium_sensor_reader: TerrariumSensorReader = TerrariumSensorReader(db_operations=self.db_operations)
        logger.info("Terrarium Sensor Reader initialized.")

        # Initialize specific device schedulers (responsible for managing their own job scheduling)
        self.light_scheduler: LightScheduler = LightScheduler(
            scheduler=self.scheduler,
            db_operations=self.db_operations,
            light_controller=self.light_controller
        )
        self.mister_scheduler: MisterScheduler = MisterScheduler(
            scheduler=self.scheduler,
            db_operations=self.db_operations,
            mister_controller=self.mister_controller
        )
        logger.info("VivariumScheduler and sub-schedulers initialized.")

        self._perform_system_boot_check()
        logger.info("System boot check completed.")

    def __del__(self):
        """
        Ensures the database connection is gracefully closed when the VivariumScheduler
        object is about to be destroyed.

        This helps prevent resource leaks and ensures data integrity.
        """
        if self.db_operations and self.db_operations.is_connected():
            self.db_operations.close()
            logger.info("Database connection closed by VivariumScheduler (from __del__).")

    def _job_listener(self, event):
        """
        Listener for APScheduler job events (execution and error).

        This method handles logging for job success/failure and implements
        specific logic, such as retrying failed weather fetches or triggering
        dependent actions upon job completion.

        :param event: The APScheduler event object containing job details.
        :type event: apscheduler.events.JobEvent
        """
        if event.exception:
            logger.error(f"Job '{event.job_id}' raised an exception: {event.exception.__class__.__name__}: {event.exception}")
            logger.error(f"Traceback for job '{event.job_id}':\n{traceback.format_exc()}")

            # -- Retry logic for 'fetch_weather_daily' --
            if event.job_id == 'fetch_weather_daily':
                retry_interval_hours: int = self.weather_config.get('weather_fetch_interval', default=2, type=int)
                retry_time: datetime = datetime.now() + timedelta(hours=retry_interval_hours)
                self.scheduler.add_job(
                    self._run_external_script,
                    'date',
                    run_date=retry_time,
                    args=[FetchDailyWeather.script_path()],
                    id='fetch_weather_daily_retry',
                    replace_existing=True  # Replace if a previous retry job failed
                )
                logger.warning(f"Job 'fetch_weather_daily' failed. Retrying in {retry_interval_hours} hours at {retry_time.strftime('%Y-%m-%d %H:%M:%S')}.")

        elif event.job_id == 'fetch_weather_daily' or event.job_id == 'fetch_weather_daily_retry':
            logger.info(f"Job '{event.job_id}' successfully finished. Triggering light schedule update.")
            # Trigger the light scheduler to update its daily light schedule
        #     self.light_scheduler.schedule_daily_lights()
        # elif event.job_id == 'run_current_status':
        #     logger.info(f"Job '{event.job_id}' successfully finished. Triggering environmental checks for mister.")
        #     # Trigger the mister scheduler to check conditions and act
        #     self.mister_scheduler.check_and_run_mister()

    def _perform_system_boot_check(self) -> None:
        """
        Performs a comprehensive system boot check for all connected vivarium components.

        This method runs only once when the VivariumScheduler is initialized.
        It's responsible for:
        - Ensuring devices are in a safe initial state (e.g., lights/mister OFF).
        - Setting up initial schedules (e.g., today's light schedule).
        - Performing initial sensor readings to confirm functionality.
        - Catching and logging errors for each component during boot.
        """
        logger.info("--- Starting System Boot Check ---")

        # --- Light System Check ---
        try:
            logger.info("Checking Light System...")
            # 1. Ensure light is OFF initially (Safe State)
            self.light_controller.control_light(action="off")
            logger.info("Light system: Ensured initial state is OFF.")

            # 2. Set up today's schedule and perform immediate check.
            # This call will:
            #   a) Fetch today's sunrise/sunset data (if available/needed).
            #   b) Update light_controller's internal schedule based on this.
            #   c) Perform an immediate state adjustment (control_light(action=None))
            #      to set light to ON/OFF based on current time and the new schedule.
            #   d) Schedule today's specific ON/OFF cron jobs within APScheduler.
            self.light_scheduler.schedule_daily_lights()
            logger.info("Light system: Daily schedule set up and immediate state adjusted based on current time.")

        except Exception as e:
            logger.error(f"Error during Light System boot check: {e}", exc_info=True)
            # Future: Add more robust error handling (e.g., send critical notification, disable light functionality).

        # --- Mister System Check ---
        try:
            logger.info("Checking Mister System...")
            # 1. Ensure mister is OFF initially (safe state)
            self.mister_controller.control_mister(action="off")
            logger.info("Mister system: Initial state set to OFF.")

            # 2. Perform an immediate check to set mister state based on current conditions
            self.mister_scheduler.check_and_run_mister()
            logger.info("Mister system: Current state adjusted based on environmental conditions.")

        except Exception as e:
            logger.error(f"Error during Mister System boot check: {e}", exc_info=True)
            # Future: Add error handling (e.g., send notification, disable mister functionality).

        # --- Sensor System Check ---
        try:
            logger.info("Checking Sensor Systems...")
            # Attempt to read data to confirm sensors are operational and store initial values.
            self.terrarium_sensor_reader.read_and_store_data()
            logger.info("Sensor systems: Performed initial reading. Check logs for sensor status.")
        except Exception as e:
            logger.error(f"Error during Sensor System boot check: {e}", exc_info=True)
            # Future: Add error handling (e.g., send notification, disable sensor functionality).

        logger.info("--- System Boot Check Complete ---")

    def schedule_jobs(self) -> None:
        """
        Schedules the core periodic and cron jobs for the vivarium system.

        This includes:

        - Regular reading and storing of terrarium sensor data.
        """
        logger.info("Scheduling core Vivarium jobs.")

        # # Schedule fetch_daily_weather.py to run once a day at a specific time (e.g., 01:00 AM)
        # fetch_weather_script: str = FetchDailyWeather.script_path()
        # self.scheduler.add_job(
        #     self._run_external_script,
        #     'cron',
        #     hour=WEATHER_FETCH_HOUR,
        #     minute=WEATHER_FETCH_MINUTE,
        #     args=[fetch_weather_script],
        #     id='fetch_weather_daily'
        # )
        # logger.info(f"Scheduled {os.path.basename(fetch_weather_script)} to run daily at {WEATHER_FETCH_HOUR:02d}:{WEATHER_FETCH_MINUTE:02d}.")

        # # Schedule the method of the instantiated TerrariumSensorReader to run at intervals
        # self.scheduler.add_job(
        #     self.terrarium_sensor_reader.read_and_store_data,
        #     'interval',
        #     minutes=SENSOR_READ_INTERVAL_MINUTES,
        #     id='run_current_status'
        # )
        # logger.info(f"Scheduled Terrarium sensor reading to run every {SENSOR_READ_INTERVAL_MINUTES} minutes.")

        # --- Testing purposes  ---
        # To enable for testing, uncomment and ensure `id` values are unique or `replace_existing=True`
        fetch_weather_script = FetchDailyWeather.script_path()
        self.scheduler.add_job(
            self._run_external_script,
            'date',
            run_date=datetime.now(), # Set the run date to the current time for immediate execution
            args=[fetch_weather_script],
            id='fetch_weather_daily_immediate'
        )
        logger.info(f"Scheduled {os.path.basename(fetch_weather_script)} to run immediately (TESTING).")

        # self.scheduler.add_job(
        #     self.terrarium_sensor_reader.read_and_store_data,
        #     'interval',
        #     seconds=30, # For testing, run more frequently
        #     id='run_current_status_fast'
        # )
        # logger.info(f"Scheduled Terrarium sensor reading to run every 30 seconds (TESTING).")

    # def _run_external_script(self, script_path: str) -> None:
    #     """
    #     Helper method to execute external Python scripts using subprocess.

    #     This method is designed to run standalone scripts like the weather data fetcher.
    #     It captures standard output and error, and handles timeouts and common exceptions.

    #     :param script_path: The absolute path to the Python script to be executed.
    #     :type script_path: str
    #     :raises subprocess.TimeoutExpired: If the script execution exceeds the defined timeout.
    #     :raises FileNotFoundError: If the specified script file does not exist.
    #     :raises Exception: For any other unexpected errors during script execution.
    #     """
    #     script_name: str = os.path.basename(script_path)
    #     logger.info(f"Running external script: {script_name}")
    #     try:
    #         process: subprocess.Popen = subprocess.Popen(
    #             ['python3', script_path],
    #             stdout=subprocess.PIPE,
    #             stderr=subprocess.PIPE,
    #             text=True, # Decode stdout/stderr as text
    #             bufsize=1 # Line-buffered output
    #         )
    #         stdout, stderr = process.communicate(timeout=EXTERNAL_SCRIPT_TIMEOUT_SECONDS)
    #         if process.returncode == 0:
    #             logger.info(f"Script {script_name} executed successfully.")
    #             if stdout:
    #                 logger.debug(f"Stdout for {script_name}:\n{stdout.strip()}")
    #         else:
    #             logger.error(f"Script {script_name} failed with error (Return Code: {process.returncode}).")
    #             if stderr:
    #                 logger.error(f"Stderr for {script_name}:\n{stderr.strip()}")
    #     except subprocess.TimeoutExpired:
    #         logger.error(f"Script {script_name} timed out after {EXTERNAL_SCRIPT_TIMEOUT_SECONDS} seconds. Killing process.")
    #         process.kill()
    #         stdout, stderr = process.communicate() # Collect any remaining output
    #         if stdout: logger.debug(f"Timeout Stdout for {script_name}:\n{stdout.strip()}")
    #         if stderr: logger.error(f"Timeout Stderr for {script_name}:\n{stderr.strip()}")
    #     except FileNotFoundError:
    #         logger.error(f"Script not found: {script_name} at path {script_path}")
    #     except Exception as e:
    #         logger.error(f"Unexpected error running script {script_name}: {e}", exc_info=True)

    def run(self) -> None:
        """
        Starts the main loop of the Vivarium Scheduler.

        This method first schedules all defined jobs and then starts the
        APScheduler's blocking execution. It includes graceful shutdown
        handling for common interruptions.
        """
        logger.info("Vivarium Scheduler starting main loop.")
        self.schedule_jobs()

        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit) as e:
            logger.info(f"Vivarium Scheduler stopping gracefully due to {e.__class__.__name__}...")
        except Exception as e:
            logger.critical(f"Vivarium Scheduler encountered a critical error and will stop: {e}", exc_info=True)
        finally:
            # Ensure DB connection is closed even if an unexpected error occurs or on shutdown
            if self.db_operations and self.db_operations.is_connected():
                self.db_operations.close()
                logger.info("Database connection closed.")
            logger.info("Vivarium Scheduler stopped.")


if __name__ == "__main__":
    # Ensure correct working directory if running directly
    # This might be redundant if sys.path.insert is reliable for your setup
    # but provides an extra layer of robustness for direct execution.
    # If running as a systemd service, the CWD can be configured there.
    # os.chdir(os.path.dirname(os.path.abspath(__file__))) 
    
    scheduler = VivariumScheduler()
    scheduler.run()