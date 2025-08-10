# vivarium/scheduler/src/device_scheduler_base.py

import os
import sys
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler

# Assuming this file is in vivarium/scheduler/src/
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations

logger = LogHelper.get_logger(__name__)

class DeviceSchedulerBase:
    """
    A base class for scheduling device-related jobs.
    Provides common methods for interacting with APScheduler and shared resources.
    """
    def __init__(self, scheduler: BlockingScheduler, db_operations: DBOperations):
        """
        Initializes the DeviceSchedulerBase.

        Args:
            scheduler (BlockingScheduler): The main APScheduler instance.
            db_operations (DatabaseOperations): The shared database operations instance.
        """
        self.scheduler = scheduler
        self.db_operations = db_operations
        logger.info(f"Initialized DeviceSchedulerBase for {self.__class__.__name__}.")

    def _schedule_cron_job(self, func, hour: int, minute: int, second: int, args: list, job_id: str, replace_existing: bool = True):
        """
        Helper method to schedule a cron job.
        """
        self.scheduler.add_job(
            func,
            'cron',
            hour=hour,
            minute=minute,
            second=second,
            args=args,
            id=job_id,
            replace_existing=replace_existing
        )
        logger.info(f"Scheduled cron job '{job_id}' for {func.__name__} at {hour:02d}:{minute:02d}:{second:02d}.")

    def _schedule_date_job(self, func, run_date: datetime, args: list, job_id: str, replace_existing: bool = True):
        """
        Helper method to schedule a date job (run once at a specific datetime).
        """
        self.scheduler.add_job(
            func,
            'date',
            run_date=run_date,
            args=args,
            id=job_id,
            replace_existing=replace_existing
        )
        logger.info(f"Scheduled date job '{job_id}' for {func.__name__} at {run_date.strftime('%Y-%m-%d %H:%M:%S')}.")

    def _schedule_interval_job(self, func, minutes: int, args: list, job_id: str, replace_existing: bool = True):
        """
        Helper method to schedule an interval job.
        """
        self.scheduler.add_job(
            func,
            'interval',
            minutes=minutes,
            args=args,
            id=job_id,
            replace_existing=replace_existing
        )
        logger.info(f"Scheduled interval job '{job_id}' for {func.__name__} every {minutes} minutes.")

    # Future common methods can go here, e.g., _log_device_action, _get_device_config
