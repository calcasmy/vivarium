# scheduler.vivariumscheduler.py
''' Primary Scheduler for all vivarium related activities'''

import os
import sys
import time
import subprocess
from datetime import time, date, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Add 'vivarium' to the Python path if it's not already there
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# importing utilties package
from utilities.src.logger import LogHelper
from utilities.src.config import Config
from utilities.src.path_utils import PathUtils

from weather.fetch_daily_weather import FetchDailyWeather
from terrarium.src.controllers.terrarium_status import TerrariumStatus
# from terrarium.src

logger = LogHelper.get_logger(__name__)

class VivariumScheduler:
    '''
        Scheduler class for all vivarium [Aquarium, Terrarium etc.] related jobs

    Parameters:
        None
    Attributes:
        scheduler: Instance of BlockingScheduler
    Methods:
        schedule_jobs   : Schedules jobs and defines when each job will run.
        run_script      :
        run             :

    '''
    def __init__(self):
        self.scheduler = BlockingScheduler()

    def schedule_jobs(self):
        # Schedule fetch_daily_weather.py to run once a day at 1:00 AM
        fetch_weather_script = FetchDailyWeather.script_path()
        self.scheduler.add_job(self.run_script, 'cron', hour=1, minute=0, args=[fetch_weather_script], id='fetch_weather_daily')
        logger.info(f"Scheduled {fetch_weather_script} to run daily at 01:00.")

        # Schedule currentstatus.py to run every 5 minutes
        terrarium_status_script = TerrariumStatus.script_path()
        self.scheduler.add_job(self.run_script, 'interval', minutes=5, args=[terrarium_status_script], id='run_current_status')
        logger.info(f"Scheduled {terrarium_status_script} to run every 5 minutes.")

        # # Schedule devices.py update based on sunrise/sunset (run after fetching weather data)
        # self.scheduler.add_job(self.update_devices_based_on_astro, 'cron', hour=1, minute=5, id='update_devices_astro')
        # logger.info("Scheduled device update based on astro data shortly after weather fetch.")

    def run_script(self, script_path):
        logger.info(f"Running script: {script_path}")
        try:
            process = subprocess.Popen(['python3', script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate(timeout=30)
            if process.returncode == 0:
                logger.info(f"Script {script_path} executed successfully.")
                if stdout:
                    logger.debug(f"Stdout: {stdout.decode()}")
            else:
                logger.error(f"Script {script_path} failed with error: {stderr.decode()}")
        except subprocess.TimeoutExpired:
            logger.error(f"Script {script_path} timed out.")
            process.kill()
        except FileNotFoundError:
            logger.error(f"Script not found: {script_path}")
        except Exception as e:
            logger.error(f"Error running script {script_path}: {e}")

    def run(self):
        logger.info("Viva Scheduler started.")
        self.schedule_jobs()
        try:
            while True:
                self.scheduler.run_pending()
                time.sleep(60 * 5)
        except (KeyboardInterrupt, SystemExit):
            logger.info("Viva Scheduler stopped.")

if __name__ == "__main__":
    scheduler = VivariumScheduler()
    scheduler.run()

        # # Schedule currentstatus.py to run every 5 minutes
        # self.scheduler.add_job(self.run_script, 'interval', minutes=5, args=[VIVARIUM_STATUS_SCRIPT], id='run_current_status')
        # logger.info(f"Scheduled {VIVARIUM_STATUS_SCRIPT} to run every 5 minutes.")

        # # Schedule devices.py update based on sunrise/sunset (run after fetching weather data)
        # self.scheduler.add_job(self.update_devices_based_on_astro, 'cron', hour=1, minute=5, id='update_devices_astro')
        # logger.info("Scheduled device update based on astro data shortly after weather fetch.")



