# scheduler/vivariumscheduler.py
''' Primary Scheduler for all vivarium related activities'''

import os
import sys
import time
import json
import threading
import subprocess

from datetime import time as datetime_time, date, datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

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
from weather.src.database.astro_queries import AstroQueries
from weather.src.database.database_operations import DatabaseOperations

from terrarium.src.controllers.light_controller import LightControler
from terrarium.src.controllers.terrarium_status import TerrariumStatus
from terrarium.src.controllers.mister_controller import MisterController
from terrarium.src.controllers.humidifier_control import HumidiferController

# logger = LogHelper.get_logger(__name__)
logger = LogHelper.get_logger("Vivarium_Scheduler")


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
        self.scheduler.add_listener(self.job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    def job_listener(self, event):
        if event.exception:
            logger.error(f"Job {event.job_id} raised {event.exception}")
        elif event.job_id == 'fetch_weather_daily_now':
            logger.info(f"Job {event.job_id} successfully finished.")
            # Immediately run the schedule_lights job
            self.schedule_lights()

    def schedule_jobs(self):

        # Weather API related Jobs

        # Schedule fetch_daily_weather.py to run RIGHT NOW
        fetch_weather_script = FetchDailyWeather.script_path()
        self.scheduler.add_job(
            self.run_script,
            'date',  # Use the 'date' trigger
            run_date=datetime.now(), # Set the run date to the current time
            args=[fetch_weather_script],
            id='fetch_weather_daily_now')
        logger.info(f"Scheduled {os.path.basename(fetch_weather_script)} to run immediately.")

        # Schedule fetch_daily_weather.py to run once a day at 1:00 AM
        # fetch_weather_script = FetchDailyWeather.script_path()
        # self.scheduler.add_job(
        #     self.run_script, 
        #     'cron', 
        #     hour=1, 
        #     minute=0, 
        #     args=[fetch_weather_script], 
        #     id='fetch_weather_daily')
        # logger.info(f"Scheduled {os.path.basename(fetch_weather_script)} to run daily at 01:00.")

        # Not necessary as this job gets scheduled immediately after the weather fetch event is completed.
        # Schedule devices.py update based on sunrise/sunset (run after fetching weather data)
        # self.scheduler.add_job(
        #     self.schedule_lights, 
        #     'cron', 
        #     hour=1, 
        #     minute=5, 
        #     id='update_devices_astro')
        # logger.info("Scheduled device update based on astro data shortly after weather fetch.")

        # Schedule currentstatus.py to run every 5 minutes
        terrarium_status_script = TerrariumStatus.script_path()
        self.scheduler.add_job(
            self.run_script, 
            'interval', 
            seconds=30, 
            args=[terrarium_status_script], 
            id='run_current_status')
        logger.info(f"Scheduled {os.path.basename(terrarium_status_script)} to run every 5 minutes.")

        '''.................................................'''


    def run_script(self, script_path):
        script_name = os.path.basename(script_path)
        logger.info(f"Running script: {script_name}")
        try:
            process = subprocess.Popen(['python3', script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate(timeout=30)
            if process.returncode == 0:
                logger.info(f"Script {script_name} executed successfully.")
                if stdout:
                    logger.debug(f"Stdout: {stdout.decode()}")
            else:
                logger.error(f"Script {script_name} failed with error: {stderr.decode()}")
        except subprocess.TimeoutExpired:
            logger.error(f"Script {script_name} timed out.")
            process.kill()
        except FileNotFoundError:
            logger.error(f"Script not found: {script_name}")
        except Exception as e:
            logger.error(f"Error running script {script_name}: {e}")

    def schedule_lights(self, sunrise: str = "06:00 AM", sunset: str = "06:00 PM"):
        logger.info("Updating terrarium lights based on sunrise/sunset.")
        try:
            # config = Config()
            # db_operations = DatabaseOperations(config)
            db_operations = DatabaseOperations()
            db_operations.connect()
            astro_queries = AstroQueries(db_operations)

            yesterday = datetime.now().date() - timedelta(days=1)
            yesterday_str = yesterday.strftime('%Y-%m-%d')
            location_id = 1  # ** Need to find a way to fetch this.

            astro_data = astro_queries.get_sunrise_sunset(location_id, yesterday_str)
            if astro_data:
                sunrise = astro_data.get('sunrise')
                sunset = astro_data.get('sunset')
                logger.info(f"Sunrise for {yesterday_str}: {sunrise}, Sunset: {sunset}")
            else:
                logger.warning(f"Could not retrieve sunrise/sunset data for {yesterday_str}, using defaults.")
                
            if sunrise and sunset:
                try:
                    sunrise_dt_obj = datetime.strptime(sunrise.split('\t')[0].strip(), '%I:%M %p')
                    sunrise_time = sunrise_dt_obj.time()

                    # Parse the sunset time
                    sunset_dt_obj = datetime.strptime(sunset.split('\t')[0].strip(), '%I:%M %p')
                    sunset_time = sunset_dt_obj.time()

                    # Schedule light ON at sunrise

                    self.scheduler.add_job(
                        self.run_light_control,
                        'date',  # Use the 'date' trigger
                        run_date=datetime.now(), # Set the run date to the current time
                        args=['on'],
                        id='lights_on_sunrise',
                    )
                    logger.info(f"Scheduled lights ON at {sunrise_time.strftime('%H:%M:%S')}.")


                    # self.scheduler.add_job(
                    #     self.run_light_control,
                    #     'cron',
                    #     hour=sunrise_time.hour,
                    #     minute=sunrise_time.minute,
                    #     second=sunrise_time.second,
                    #     args=['on'],
                    #     id='lights_on_sunrise',
                    #     replace_existing=True
                    # )
                    # logger.info(f"Scheduled lights ON at {sunrise_time.strftime('%H:%M:%S')}.")

                    # Schedule light OFF at sunset
                    self.scheduler.add_job(
                        self.run_light_control,
                        'cron',
                        hour=sunset_time.hour,
                        minute=sunset_time.minute,
                        second=sunset_time.second,
                        args=['off'],
                        id='lights_off_sunset',
                        replace_existing=True
                    )
                    logger.info(f"Scheduled lights OFF at {sunset_time.strftime('%H:%M:%S')}.")

                except ValueError as e:
                    logger.error(f"Error parsing sunrise/sunset time: {e} - Sunrise: '{sunrise}', Sunset: '{sunset}'")
        
            db_operations.close()
        except ImportError as e:
            logger.error(f"Error importing database modules: {e}")
        except Exception as e:
            logger.error(f"Error updating devices: {e}")

    def run_light_control(self, action):
        light_control_script = LightControler.script_path()
        logger.info(f"Running light control: {action}")
        try:
            process = subprocess.Popen(['python3', light_control_script, action], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate(timeout=30)
            if process.returncode == 0:
                logger.info(f"Light control '{action}' executed successfully.")
                if stdout:
                    logger.debug(f"Stdout: {stdout.decode()}")
            else:
                logger.error(f"Light control '{action}' failed with error: {stderr.decode()}")
                if stderr:
                    logger.error(f"Stderr: {stderr.decode()}")
        except subprocess.TimeoutExpired:
            logger.error(f"Light control '{action}' timed out.")
            process.kill()
        except FileNotFoundError:
            logger.error(f"Light control script not found: {light_control_script}")
        except Exception as e:
            logger.error(f"Error running light control '{action}': {e}")

    def run(self):
        logger.info("Vivarium Scheduler started.")
        self.schedule_jobs()

        def start_scheduler():
            self.scheduler.start()

        scheduler_thread = threading.Thread(target=start_scheduler)
        scheduler_thread.daemon = True  # Allow the main thread to exit even if the scheduler thread is still running
        scheduler_thread.start()

        try:
            while True:
                time.sleep(60 * 5)  # Check for shutdown every 5 minutes
        except (KeyboardInterrupt, SystemExit):
            logger.info("Vivarium Scheduler stopping...")
            self.scheduler.shutdown()
            scheduler_thread.join()  # Wait for the scheduler thread to finish
            logger.info("Vivarium Scheduler stopped.")

if __name__ == "__main__":
    scheduler = VivariumScheduler()
    scheduler.run()

        # # Schedule currentstatus.py to run every 5 minutes
        # self.scheduler.add_job(self.run_script, 'interval', minutes=5, args=[VIVARIUM_STATUS_SCRIPT], id='run_current_status')
        # logger.info(f"Scheduled {VIVARIUM_STATUS_SCRIPT} to run every 5 minutes.")

        # # Schedule devices.py update based on sunrise/sunset (run after fetching weather data)
        # self.scheduler.add_job(self.update_devices_based_on_astro, 'cron', hour=1, minute=5, id='update_devices_astro')
        # logger.info("Scheduled device update based on astro data shortly after weather fetch.")