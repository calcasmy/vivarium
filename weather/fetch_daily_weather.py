# vivarium/weather/fetch_daily_weather.py
"""
A script to fetch daily historical weather data from an external API,
cache it locally, and store it into the Vivarium database.

This script integrates with a WeatherAPIClient to retrieve weather data
and then uses a JSONProcessor to parse, validate, and persist this data
into the database. It is designed to run as a standalone process,
managing its own database connection.
"""
import os
import re
import sys
import json
import time
import argparse
from typing import Optional, Dict, Any, Union
from datetime import datetime, timedelta

# Ensure vivarium root is in sys.path to resolve imports correctly.
# This allows imports like 'weather.src...' and 'utilities.src...' to work.
vivarium_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if vivarium_root_path not in sys.path:
    sys.path.insert(0, vivarium_root_path)

# --- Project-Specific Imports ---
from weather.src.atmosphere.weather_api_client import WeatherAPIClient
from weather.src.database.raw_data_queries import RawDataQueries
# from weather.src.database.json_processor import JSONProcessor

# --- Utility Imports ---
from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations, ConnectionDetails
from utilities.src.config import DatabaseConfig, FileConfig, WeatherAPIConfig, SupabaseConfig  # Import ConnectionDetails here

logger = LogHelper.get_logger(__name__)

class FetchDailyWeather:
    """
    Manages the fetching, caching, and storage of daily historical weather data.

    This class orchestrates the interaction between the weather API client,
    local file caching, and database operations. It ensures data is retrieved
    efficiently, prioritizing local cache, then database, then external API.
    """

    def __init__(self, db_operations: DBOperations, file_config: FileConfig, weather_api_config: WeatherAPIConfig, supabase_config: SupabaseConfig):
        """
        Initializes the FetchDailyWeather class.

        This constructor sets up the necessary components for weather data
        retrieval and processing, including database interaction instances.

        :param db_operations: An initialized instance of :class:`~utilities.src.db_operations.DBOperations`.
                              This instance should already have an active database connection.
        :type db_operations: utilities.src.db_operations.DBOperations
        :param file_config: An initialized instance of :class:`~utilities.src.new_config.FileConfig`.
        :type file_config: utilities.src.new_config.FileConfig
        :param weather_api_config: An initialized instance of :class:`~utilities.src.new_config.WeatherAPIConfig`.
        :type weather_api_config: utilities.src.new_config.WeatherAPIConfig
        :param supabase_config: An initialized instance of :class:`~utilities.src.new_config.SupabaseConfig`.
        :type supabase_config: utilities.src.new_config.SupabaseConfig
        """
        self.db_ops: DBOperations = db_operations
        self.file_config: FileConfig = file_config
        self.weather_api_config: WeatherAPIConfig = weather_api_config
        
        self.weather_client: WeatherAPIClient = WeatherAPIClient() # WeatherAPIClient implicitly uses weather_api_config. Should probably be passed too
        self.raw_data_db: RawDataQueries = RawDataQueries(db_operations)
        # Pass db_operations AND supabase_config to JSONProcessor
        logger.info("FetchDailyWeather instance initialized.")

    @staticmethod
    def script_path() -> str:
        """
        Provides the absolute path to this script for external execution (e.g., by APScheduler).
        """
        return os.path.abspath(__file__)

    def _get_raw_files_directory(self) -> str:
        """
        Determines and ensures the existence of the directory for storing raw JSON weather files.
        It now uses `self.file_config.json_folder`.
        """
        # Use the configured JSON folder from FileConfig
        files_dir = os.path.join(vivarium_root_path, self.file_config.json_folder)
        os.makedirs(files_dir, exist_ok=True)
        return files_dir

    def get_raw_weather_data(self, date_str: str, location_lat_long: Optional[str] = None) -> Optional[str]:
        """
        Retrieves raw weather data, prioritizing a local file cache, then the database,
        and finally the external API. The retrieved data is always saved to a JSON
        file, and the file's path is returned.
        """
        files_dir: str = self._get_raw_files_directory()
        file_path: str = os.path.join(files_dir, f"{date_str}.json")

        logger.info(f"Attempting to retrieve raw weather data for {date_str}. Cache file: {file_path}")

        # 1. Check local file cache first.
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            logger.info(f"Raw climate file found for {date_str} at {file_path}. Processing from file.")
            return file_path

        # 2. Check database for existing raw data.
        existing_raw_data: Optional[Dict[str, Any]] = self.raw_data_db.get_raw_data_by_date(date_str)
        if existing_raw_data:
            logger.info(f"Raw climate data already exists in database for {date_str}. Creating local file cache.")
            try:
                with open(file_path, 'w', encoding='utf-8') as outfile:
                    json.dump(existing_raw_data, outfile, indent=4)
                logger.info(f"Saved database data to local file: {file_path}.")
                return file_path
            except Exception as e:
                logger.error(f"Failed to save data from DB to local file {file_path}: {e}", exc_info=True)
                # Fall through to API fetch if file creation from DB fails.

        # 3. Fetch from API if not found locally or in DB.
        logger.info(f"Raw climate data not found locally or in DB for {date_str}. Fetching from API.")
        # Use self.weather_api_config for default lat_long
        final_location: str = location_lat_long if location_lat_long else self.weather_api_config.lat_long
        weather_data_from_api: Optional[Dict[str, Any]] = self.weather_client.get_historical_data(date_str, final_location)

        if weather_data_from_api:
            logger.debug(f"Successfully fetched weather data from API for {date_str}.")
            try:
                with open(file_path, 'w', encoding='utf-8') as outfile:
                    json.dump(weather_data_from_api, outfile, indent=4)
                logger.info(f"Saved fetched weather data to local file: {file_path}.")
                return file_path
            except Exception as e:
                logger.error(f"Failed to save fetched weather data to local file {file_path}: {e}", exc_info=True)
                return None
        else:
            logger.error(f"Failed to fetch weather data from API for {date_str}.")
            return None

    def fetch_and_store_weather_data(self, target_date_str: Optional[str] = None, location_latlong: Optional[str] = None) -> bool:
        """
        Orchestrates the fetching of daily weather data and its subsequent storage
        in the database by processing a local JSON file.
        """
        process_date: str = target_date_str if target_date_str else (datetime.now().date() - timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f"Initiating fetch and store operation for weather data on date: {process_date}.")

        try:
            file_path: Optional[str] = self.get_raw_weather_data(process_date, location_latlong)

            if not file_path:
                logger.error(f"No weather data file could be retrieved or created for {process_date}. Cannot proceed with processing.")
                return False

            if self.json_processor.process_json_file(file_path):
                logger.info(f"Successfully processed and stored weather data from file: {os.path.basename(file_path)}.")
                return True
            else:
                logger.error(f"Failed to process and store weather data from file {os.path.basename(file_path)} using JSONProcessor.")
                return False

        except Exception as e:
            logger.exception(f"An unexpected error occurred during the fetch and store operation for {process_date}: {e}")
            return False


def main(target_date_str: Optional[str], location_latlong: Optional[str], is_remote: bool) -> None:
    """
    Main entry point for fetching and storing daily weather data as a standalone script.
    """
    db_operations: Optional[DBOperations] = None
    try:
        # Instantiate config classes within main or pass them from a higher level.
        # This makes dependencies explicit.
        database_config = DatabaseConfig()
        file_config = FileConfig()
        weather_api_config = WeatherAPIConfig()
        supabase_config = SupabaseConfig()

        # Determine which database connection details to use
        connection_details: ConnectionDetails
        if is_remote:
            logger.info("Attempting to connect to the remote database.")
            connection_details = database_config.postgres_remote_connection
        else:
            logger.info("Attempting to connect to the local database (default).")
            connection_details = database_config.postgres_local_connection

        # Establish an independent database connection for this subprocess.
        db_operations = DBOperations()
        db_operations.connect(connection_details=connection_details)
        logger.info(f"Database connection established for standalone weather fetching process: {connection_details.host}")

        # Pass all necessary config objects to FetchDailyWeather
        weather_fetcher = FetchDailyWeather(db_operations, file_config, weather_api_config, supabase_config)

        success: bool = weather_fetcher.fetch_and_store_weather_data(target_date_str, location_latlong)
        if success:
            logger.info("Weather data fetching and storing completed successfully.")
        else:
            logger.error("Weather data fetching and storing encountered errors.")

    except Exception as e:
        logger.exception(f"A critical error occurred in main execution of FetchDailyWeather: {e}")
    finally:
        # Ensure the database connection is closed at the end of the script's execution.
        if db_operations:
            db_operations.close()
            logger.info("Database connection closed after standalone weather fetching process.")


if __name__ == "__main__":
    start_time: float = time.time()

    # -- PARSE COMMAND LINE ARGUMENTS --
    parser = argparse.ArgumentParser(
        prog='fetch_daily_weather.py',
        description="Fetch and store daily weather data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        '-d', '--date',
        type=str,
        help="Date to fetch weather data for in YYYY-MM-DD format.\n"
             "Defaults to yesterday if not specified."
    )
    parser.add_argument(
        '-l', '--location',
        type=str,
        help="Latitude,longitude string (e.g., '40.71,-74.00') to override\n"
             "the default configured API location."
    )
    parser.add_argument(
        '-r', '--remote',
        action='store_true',
        help="Connect to the remote database instead of the local one.\n"
             "By default, connects to the local database."
    )

    args = parser.parse_args()

    # Validating date and location arguments if passed.
    if args.date and not re.fullmatch(r'\d{4}-\d{2}-\d{2}', args.date):
        logger.warning(f"Invalid date format for --date: {args.date}. Expected YYYY-MM-DD. Ignoring provided date.")
        args.date = None

    if args.location and not re.fullmatch(r'^-?\d+\.?\d*,-?\d+\.?\d*$', args.location):
        logger.warning(f"Invalid location format for --location: {args.location}. Expected latitude,longitude. Ignoring provided location.")
        args.location = None

    main(target_date_str=args.date, location_latlong=args.location, is_remote=args.remote)

    end_time: float = time.time()
    logger.info(f"Total script execution time: {end_time - start_time:.2f} seconds")