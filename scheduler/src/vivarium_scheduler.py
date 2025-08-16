# vivarium/scheduler/vivarium_scheduler.py
''' Primary Scheduler for all vivarium related activities'''

import os
import sys
import traceback
from datetime import time as datetime_time, date, datetime, timedelta

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler.triggers.interval import IntervalTrigger

# --- Path Configuration ---
# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# --- Project Imports ---
from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations, ConnectionDetails
from utilities.src.config import SchedulerConfig, TimeConfig, DatabaseConfig
from terrarium.src.sensors.terrarium_sensor_reader import TerrariumSensorReader

# Device Controllers
from terrarium.src.controllers.light_controller import LightController
from terrarium.src.controllers.mister_controller import MisterController
from terrarium.src.controllers.humidifier_controller import HumidifierController

# Schedulers for specific devices
from scheduler.src.light_scheduler import LightScheduler
from scheduler.src.mister_scheduler import MisterScheduler
from scheduler.src.humidifier_scheduler import HumidifierScheduler

logger = LogHelper.get_logger(__name__)

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

        self.scheduler_config: SchedulerConfig = SchedulerConfig()
        self.db_config: DatabaseConfig = DatabaseConfig()
        self.db_operations: DBOperations = DBOperations()
        self.db_operations.connect(self._db_connectiondetails())
        logger.info("Database connection established for VivariumScheduler.")

        self.light_controller: LightController = LightController(db_operations=self.db_operations)
        self.mister_controller: MisterController = MisterController(db_operations=self.db_operations)
        self.humidifier_controller: HumidifierController = HumidifierController(db_operations=self.db_operations)
        logger.info("Device controllers initialized.")

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
        self.humidifier_scheduler = HumidifierScheduler(
            scheduler=self.scheduler,
            db_operations=self.db_operations,
            humidifier_controller=self.humidifier_controller
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
        if self.db_operations: #and self.db_operations.is_connected():
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
        else:
            logger.info(f"Job '{event.job_id}' completed successfully.")
            if event.job_id == 'read_sensor_data':
                logger.info(f"Job '{event.job_id}' finished. Triggering environmental checks for mister.")
                # self.mister_scheduler.check_and_run_mister()
                self.humidifier_scheduler.check_and_run_humidifier()

    def _update_lights_job(self) -> None:
        """
        Job method to fetch the latest sunrise/sunset data and update the light schedule.
        """
        logger.info("Executing light schedule update job.")
        self.light_scheduler.schedule_daily_lights()

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
            self.light_controller.control_light(action="off")
            self.light_controller._update_status(False)
            logger.info("Light system: Ensured initial state is OFF.")
            # self.light_scheduler.schedule_daily_lights()
            logger.info("Light system: Daily schedule set up and immediate state adjusted based on current time.")
        except Exception as e:
            logger.error(f"Error during Light System boot check: {e}", exc_info=True)

        # --- Mister System Check ---
        try:
            logger.info("Checking Mister System...")
            self.mister_controller.control_mister(action="off")
            self.mister_controller._update_status(False)
            logger.info("Mister system: Initial state set to OFF.")
            # self.mister_scheduler.check_and_run_mister()
            logger.info("Mister system: Current state adjusted based on environmental conditions.")
        except Exception as e:
            logger.error(f"Error during Mister System boot check: {e}", exc_info=True)

        # --- Humidifier System Check ---
        try:
            logger.info("Checking Humidifier System...")
            self.humidifier_controller.control_humidifier(action="off")
            self.humidifier_controller._update_status(False)
            logger.info("Humidifier system: Initial state set to OFF.")
            # self.humidifier_scheduler.check_and_run_humidifier()
            logger.info("Humidifier system: Current state adjusted based on environmental conditions.")
        except Exception as e:
            logger.error(f"Error during Humidifier System boot check: {e}", exc_info=True)

        # # --- Sensor System Check ---
        try:
            logger.info("Checking Sensor Systems...")
            self.terrarium_sensor_reader.read_and_store_data()
            logger.info("Sensor systems: Performed initial reading. Check logs for sensor status.")
        except Exception as e:
            logger.error(f"Error during Sensor System boot check: {e}", exc_info=True)
        logger.info("--- System Boot Check Complete ---")

    def _db_connectiondetails(self) -> ConnectionDetails:
        """
        Creates a ConnectionDetails object for the database connection.

        :returns: A ConnectionDetails object with the database connection details.
        :rtype: ConnectionDetails
        """
        if self.scheduler_config.application_db_type == 'remote':
            return ConnectionDetails(
                host=self.db_config.postgres_remote_connection.host,
                port=self.db_config.postgres_remote_connection.port,
                user=self.db_config.postgres_remote_connection.user,
                password=self.db_config.postgres_remote_connection.password,
                dbname=self.db_config.postgres_remote_connection.dbname,
                sslmode=None
            )
        else:
            return ConnectionDetails(
                host=self.db_config.postgres_local_connection.host,
                port=self.db_config.postgres_local_connection.port,
                user=self.db_config.postgres_local_connection.user,
                password=self.db_config.postgres_local_connection.password,
                dbname=self.db_config.postgres_local_connection.dbname,
                sslmode=None
            )

    def schedule_jobs(self) -> None:
        """
        Schedules the core periodic and cron jobs for the vivarium system.

        This includes:
        - Daily fetching of weather data (e.g., sunrise/sunset times).
        - Regular reading and storing of terrarium sensor data.
        """
        logger.info("Scheduling core Vivarium jobs.")

        # 1. -- At 4:00 AM everyday, fetch last record to adjust and schedule lights --
        self.scheduler.add_job(
            self._update_lights_job,
            'cron',
            hour = self.scheduler_config.schedule_light_hour,
            minute = self.scheduler_config.schedule_light_minute,
            id='update_lights_daily'
        )
        logger.info(f"Scheduled light schedule update to run daily at {self.scheduler_config.schedule_light_hour:02d}:{self.scheduler_config.schedule_light_minute:02d}.")

        # 1a. For testing, run light update more frequently
        # self.scheduler.add_job(
        #     self._update_lights_job,
        #     'interval',
        #     seconds=30, # For testing, run more frequently
        #     id='update_lights_daily_fast'
        # )
        # logger.info("Scheduled light schedule update to run every 30 seconds (TESTING).")

        # 1.b For testing, run light update immediately
        # logger.info("TESTING: Scheduling immediate light update job.")
        # self.scheduler.add_job(
        #     self._update_lights_job,
        #     'date',
        #     run_date=datetime.now(), # Set the run date to the current time for immediate execution
        #     id='update_lights_immediate'
        # )

        # 2. -- Scheduler Mister --
        self.mister_scheduler.schedule_misting_job()

        # 3. Read sensor status every 5 min, turn on mister if needed
        self.scheduler.add_job(
            self.terrarium_sensor_reader.read_and_store_data,
            'interval',
            minutes = self.scheduler_config.scheule_sensor_read,
            id='read_sensor_data'
        )
        logger.info(f"Scheduled Terrarium sensor reading to run every {self.scheduler_config.scheule_sensor_read} minutes.")

        # 3a. For testing, run sensor reading more frequently
        # self.scheduler.add_job(
        #     self.terrarium_sensor_reader.read_and_store_data,
        #     'interval',
        #     seconds=30, # For testing, run more frequently
        #     id='read_sensor_data_fast'
        # )
        # logger.info(f"Scheduled Terrarium sensor reading to run every 30 seconds (TESTING).")

        # 3b. For testing, run sensor reading immediately
        # logger.info("TESTING: Scheduling immediate sensor reading job.")
        # self.scheduler.add_job(
        #     self.terrarium_sensor_reader.read_and_store_data,
        #     'date',
        #     run_date=datetime.now(), # Set the run date to the current time for immediate execution
        #     id='read_sensor_data'
        # )

    def run(self) -> None:
        """
        Starts the main loop of the Vivarium Scheduler.
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
            if hasattr(self, 'light_controller') and self.light_controller:
                self.light_controller.close()
            if hasattr(self, 'mister_controller') and self.mister_controller:
                self.mister_controller.close()
            if hasattr(self, 'humidifier_controller') and self.humidifier_controller:
                self.humidifier_controller.close()

            # Ensure DB connection is closed even if an unexpected error occurs or on shutdown
            if hasattr(self, 'db_operations') and self.db_operations and not self.db_operations.conn.closed:
                self.db_operations.close()
                logger.info("Database connection closed.")
            logger.info("Vivarium Scheduler stopped.")


if __name__ == "__main__":
    scheduler = VivariumScheduler()
    scheduler.run()