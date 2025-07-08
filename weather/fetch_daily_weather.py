# vivarium/src/fetch_daily_weather.py
"""
A script to fetch daily historical weather data from an external API
and store it into the Vivarium database.

This script demonstrates how to integrate with the WeatherAPIClient to
retrieve weather data and then use the ClimateDataProcessor to parse,
validate, and persist this data into the database.
"""
import os
import sys
import json
import time
import re
from typing import Optional, Dict, Any
from datetime import datetime, timedelta

# Ensure vivarium root is in sys.path to resolve imports correctly.
# This block is at the very top.
if __name__ == "__main__":
    # Go up one level from 'src' to 'vivarium' root
    vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if vivarium_path not in sys.path:
        sys.path.insert(0, vivarium_path)

from weather.src.atmosphere.weather_api_client import WeatherAPIClient
from weather.src.database.raw_data_queries import RawDataQueries

from weather.src.database.json_processor import JSONProcessor

from utilities.src.logger import LogHelper
from utilities.src.database_operations import DatabaseOperations
from utilities.src.config import DatabaseConfig, FileConfig, WeatherAPIConfig

# Initialize core configurations and logger
db_config = DatabaseConfig()
file_config = FileConfig()
weather_api_config = WeatherAPIConfig()

logger = LogHelper.get_logger(__name__)


class FetchDailyWeather:
    """
    A class to fetch and store daily weather data into the Vivarium database.

    This class integrates with an external weather API and uses a dedicated
    processor to handle the parsing, validation, and storage of the fetched data.
    """

    def __init__(self, db_operations: DatabaseOperations):
        """
        Initializes the FetchDailyWeather with database operations and API client.

        :param db_operations: An initialized instance of :class:`DatabaseOperations`
                              for all database interactions.
        :type db_operations: DatabaseOperations
        """
        self.db_ops = db_operations
        self.weather_client = WeatherAPIClient()
        self.raw_data_db = RawDataQueries(db_operations)

        # Initialize the ClimateDataProcessor with the shared DatabaseOperations instance
        self.json_processor = JSONProcessor(db_operations)
        logger.info("FetchDailyWeather initialized.")

    @staticmethod
    def _get_raw_files_directory() -> str:
        """
        Determines and creates the directory for storing raw JSON weather files.

        The directory is set to 'vivarium/weather/rawfiles' relative to the project root.

        :returns: The absolute path to the raw JSON files directory.
        :rtype: str
        """
        # Start from the current script's directory (vivarium/src),
        # go up one level (to vivarium), then into 'weather' and 'rawfiles'.
        files_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            '..',
            'weather',
            'rawfiles'
        )
        os.makedirs(files_dir, exist_ok=True)
        return files_dir

    def get_raw_weather_data(self, date: str, location_lat_long: Optional[str] = None) -> Optional[str]:
        """
        Retrieves raw weather data, prioritizing a local file cache, then the database,
        and finally the external API. The retrieved data is always saved to a JSON
        file, and the file's path is returned.

        :param date: The date (YYYY-MM-DD) for which to retrieve the data.
        :type date: str
        :param location_lat_long: Optional latitude,longitude string for the API call.
                                  Defaults to configured value if None.
        :type location_lat_long: Optional[str]
        :returns: The absolute file path of the saved JSON data, or `None` if data could
                  not be retrieved or saved.
        :rtype: Optional[str]
        """
        # Define the local file path for caching.
        files_dir = self._get_raw_files_directory()
        _file_path = os.path.join(files_dir, f"{date}.json")

        LogHelper.log_newline(logger)  # Add a newline for better log readability.

        # 1. Check local file cache first.
        if os.path.exists(_file_path) and os.path.getsize(_file_path) > 0:
            logger.info(f"Raw climate file found for {date} at {_file_path}. Processing from file.")
            # We return the file path, so the processor can handle reading it.
            return _file_path

        # 2. Check database.
        # This check for existing raw data ensures we don't refetch from the API
        # if it's already in the DB. If found, we create a local file from it.
        existing_raw_data = self.raw_data_db.get_raw_data_by_date(date)
        if existing_raw_data:
            logger.info(f"Raw climate data already exists in the database for {date}. Creating local file cache.")
            try:
                # Write the data from the database to a local file.
                with open(_file_path, 'w', encoding='utf-8') as outfile:
                    json.dump(existing_raw_data, outfile, indent=4)
                logger.info(f"Saved DB data to local file: {_file_path}.")
                return _file_path
            except Exception as e:
                logger.error(f"Failed to save data from DB to local file {_file_path}: {e}", exc_info=True)
                # Fall through to API fetch if file creation from DB fails.

        # 3. Fetch from API if not found locally or in DB.
        logger.info(f"Raw climate data not found locally or in DB for {date}. Fetching from API.")
        final_location = location_lat_long if location_lat_long else weather_api_config.lat_long
        weather_data_from_api = self.weather_client.get_historical_data(date, final_location)

        if weather_data_from_api:
            logger.debug(f"Successfully fetched weather_data from API for {date}.")
            # Cache the fetched data to a local file.
            try:
                with open(_file_path, 'w', encoding='utf-8') as outfile:
                    json.dump(weather_data_from_api, outfile, indent=4)
                logger.info(f"Saved fetched weather data to local file: {_file_path}.")
                return _file_path
            except Exception as e:
                logger.error(f"Failed to save fetched weather data to local file {_file_path}: {e}", exc_info=True)
                return None  # Return None if saving to file fails.
        else:
            logger.error(f"Failed to fetch weather data from API for {date}.")
            return None

    def fetch_and_store_weather_data(self, target_date: Optional[str] = None, location_latlong: Optional[str] = None) -> bool:
        """
        Fetches daily weather data and stores it in the database by processing a file.

        :param target_date: The specific date (YYYY-MM-DD) for which to fetch data.
                            If `None`, defaults to yesterday's date.
        :type target_date: Optional[str]
        :param location_latlong: Optional latitude,longitude string to override the default
                                 API location.
        :type location_latlong: Optional[str]
        :returns: `True` if data fetching and storing is successful, `False` otherwise.
        :rtype: bool
        """
        process_date = target_date if target_date else (datetime.now().date() - timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f"Attempting to fetch and store weather data for date: {process_date}.")

        try:
            # This call now returns the file path, not the data dictionary.
            file_path = self.get_raw_weather_data(process_date, location_latlong)

            if not file_path:
                logger.error(f"No weather data file could be retrieved or created for {process_date}. Cannot proceed with storing.")
                return False

            # Delegate the processing and storing of the *file* to the ClimateDataProcessor.
            if self.json_processor.process_json_file(file_path):
                logger.info(f"Successfully processed and stored weather data from file: {os.path.basename(file_path)}.")
                return True
            else:
                logger.error(f"Failed to process and store weather data from file {os.path.basename(file_path)} using ClimateDataProcessor.")
                return False

        except Exception as e:
            logger.exception(f"An unexpected error occurred during fetch and store operation: {e}")
            return False

def main():
    """
    Main function to fetch and store daily weather data.

    Connects to the database, initializes the data fetcher, and executes
    the data fetching and storage process.
    """
    db_operations = DatabaseOperations(db_config)
    try:
        # Establish a single connection for the duration of the main process.
        db_operations.connect()
        logger.info("Database connection established for main weather fetching process.")

        weather_fetcher = FetchDailyWeather(db_operations)

        # Example method calls:
        # 1. Fetch yesterday's data (default behavior if no date is provided)
        weather_fetcher.fetch_and_store_weather_data()
        
        # 2. Fetch data for a specific date (e.g., 2025-06-24)
        # weather_fetcher.fetch_and_store_weather_data('2025-06-27') 
        
        # 3. Fetch data for a specific date and location (e.g., 2025-05-18, 5.98,116.07)
        # weather_fetcher.fetch_and_store_weather_data('2025-05-18', '5.98,116.07') 
        
    except Exception as e:
        logger.exception(f"An unexpected error occurred in main execution: {e}")
    finally:
        # Ensure the database connection is closed at the end of the script.
        db_operations.close()
        logger.info("Database connection closed after main weather fetching process.")

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    logger.info(f"Total script execution time: {end_time - start_time:.2f} seconds")