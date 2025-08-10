''' A dedicated scheduler for all climate data-related tasks. '''

import os
import sys
import traceback
from datetime import datetime, timedelta
from typing import Optional

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

# --- Path Configuration ---
# Get the absolute path to the 'vivarium' directory
vivarium_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vivarium_root_path not in sys.path:
    sys.path.insert(0, vivarium_root_path)

# --- Project Imports ---
from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations, ConnectionDetails
from utilities.src.config import WeatherAPIConfig, FileConfig, DatabaseConfig, SchedulerConfig
from weather.weatherfetch_orchestrator import WeatherFetchOrchestrator

# --- Global Logger Instance ---
logger = LogHelper.get_logger(__name__)

# --- Constants ---
WEATHER_FETCH_HOUR: int = 1
WEATHER_FETCH_MINUTE: int = 0


class WeatherScheduler:
    """
    Dedicated scheduler for orchestrating weather data fetching and processing jobs.

    This scheduler is designed to be run as a standalone service, independent of
    the main VivariumScheduler.
    """
    def __init__(self):
        """
        Initializes the ClimateScheduler and its components.
        """

        self.db_config = DatabaseConfig()

        self.weather_api_config = WeatherAPIConfig()
        self.file_config = FileConfig()
        self.db_operations = DBOperations()
        self.scheduler_config = SchedulerConfig()

        self.db_conn_details: ConnectionDetails = self._db_connectiondetails()

        self.db_operations.connect(self.db_conn_details)

        self.scheduler: BlockingScheduler = BlockingScheduler()
        self.orchestrator: WeatherFetchOrchestrator = WeatherFetchOrchestrator(db_operations = self.db_operations,
                                                                                 weather_api_config = self.weather_api_config,
                                                                                 file_config = self.file_config)
        self.retry_count: int = 0
        self.max_retries: int = self.scheduler_config.max_retry_attempts
        self.retry_interval: int = self.scheduler_config.retry_interval_minutes
        self._initialize_scheduler()
        logger.info("ClimateScheduler initialized.")
    
    def _initialize_scheduler(self):
        """
        Initializes the APScheduler instance and sets up listeners.
        
        :returns: None
        :rtype: None
        """
        self.scheduler.add_listener(self._job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        logger.info("APScheduler initialized and listener added for ClimateScheduler.")

    def _job_listener(self, event):
        """
        Listener for APScheduler job events (execution and error).

        :param event: The APScheduler event object.
        :type event: apscheduler.events.JobEvent
        :returns: None
        :rtype: None
        """
        if event.exception:
            logger.error(f"Climate job '{event.job_id}' raised an exception: {event.exception.__class__.__name__}: {event.exception}")
            logger.error(f"Traceback for job '{event.job_id}':\n{traceback.format_exc()}")

            if self.retry_count < self.max_retries:
                self.retry_count += 1
                retry_time = datetime.now() + timedelta(minutes=self.retry_interval)
                logger.warning(
                    f"Job '{event.job_id}' failed. Retrying (Attempt {self.retry_count}/{self.max_retries}) "
                    f"in {self.retry_interval} minutes at {retry_time.strftime('%Y-%m-%d %H:%M:%S')}."
                )
                self.scheduler.add_job(
                    self.orchestrator.fetch_and_store_weather_data,
                    'date',
                    run_date=retry_time,
                    id='weather_fetch_retry',
                    replace_existing=True
                )
            else:
                logger.critical(
                    f"Job '{event.job_id}' failed after {self.max_retries} retries. "
                    "Skipping further attempts for this run."
                )
                self.retry_count = 0 
        else:
            logger.info(f"Climate job '{event.job_id}' executed successfully.")

    def _fetch_and_store_weather_data_job(self) -> None:
        """
        Job method to fetch and store weather data using the orchestrator.
        
        :returns: None
        :rtype: None
        """
        logger.info("Executing weather data fetch and store job.")
        self.orchestrator.fetch_and_store_weather_data()

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
        Schedules the core jobs for the climate scheduler.

        :returns: None
        :rtype: None
        """
        logger.info("Scheduling climate jobs.")
        # self.scheduler.add_job(
        #     self._fetch_and_store_weather_data_job,
        #     'cron',
        #     hour=WEATHER_FETCH_HOUR,
        #     minute=WEATHER_FETCH_MINUTE,
        #     id='fetch_weather_daily'
        # )
        # logger.info(f"Scheduled weather data fetch to run daily at {WEATHER_FETCH_HOUR:02d}:{WEATHER_FETCH_MINUTE:02d}.")

        self.scheduler.add_job(
            self._fetch_and_store_weather_data_job,
            'date',
            run_date=datetime.now(), # Set the run date to the current time for immediate execution
            id='fetch_weather_daily_immediate'
        )
        logger.info(f"Scheduled Weather fetch scheduler to run immediately (TESTING).")

    def run(self) -> None:
        """
        Starts the main loop of the Climate Scheduler.
        
        :returns: None
        :rtype: None
        """
        logger.info("Climate Scheduler starting main loop.")
        self.schedule_jobs()
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Climate Scheduler stopping gracefully.")
        except Exception as e:
            logger.critical(f"Climate Scheduler encountered a critical error: {e}", exc_info=True)
        finally:
            if self.db_operations and self.db_operations.is_connected():
                self.db_operations.close()
                logger.info("Database connection closed gracefully.")

if __name__ == "__main__":
    climate_scheduler = WeatherScheduler()
    climate_scheduler.run()