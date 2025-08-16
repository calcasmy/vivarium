# vivarium/weather/weatherfetch_orchestrator.py
import os
import sys
import time
import argparse
from typing import Optional
from datetime import datetime, timedelta
from pathlib import Path

vivarium_root_path = Path(__file__).resolve().parents[1]
if str(vivarium_root_path) not in sys.path:
    sys.path.insert(0, str(vivarium_root_path))

from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations, ConnectionDetails
from utilities.src.config import FileConfig, WeatherAPIConfig, DatabaseConfig, SupabaseConfig

from utilities.src.enums.global_enums import ErrorCodes
from utilities.src.enums.database_enums import DatabaseType, ConnectionType

from weather.src.weather_data_retriever import WeatherDataRetriever
from database.data_loader_ops.json_data_loader import JSONDataLoader

logger = LogHelper.get_logger(__name__)

class WeatherFetchOrchestrator:
    """
    Orchestrates the fetching, existence checking, processing, and storage
    of daily historical weather data by delegating to specialized components.
    """
    def __init__(self, db_operations: DBOperations, file_config: FileConfig, weather_api_config: WeatherAPIConfig):
        """
        Initializes the WeatherFetchOrchestrator with necessary dependencies.

        :param db_operations: An instance of :class:`utilities.src.database_operations.DatabaseOperations` for database interaction.
        :type db_operations: :class:`utilities.src.database_operations.DatabaseOperations`
        :param file_config: An instance of :class:`utilities.src.config.FileConfig` for file path configurations.
        :type file_config: :class:`utilities.src.config.FileConfig`
        :param weather_api_config: An instance of :class:`utilities.src.config.WeatherAPIConfig` for weather API configurations.
        :type weather_api_config: :class:`utilities.src.config.WeatherAPIConfig`
        """
        self.db_operations = db_operations
        self.file_config = file_config
        self.weather_api_config = weather_api_config

        self.data_retriever = WeatherDataRetriever(
            weather_api_config=self.weather_api_config,
            file_config=self.file_config
        )
        self.json_data_loader = JSONDataLoader(
            file_config=self.file_config,
            db_operations=self.db_operations
        )
        logger.info("WeatherFetchOrchestrator instance initialized with delegated components.")

    def fetch_and_store_weather_data(self, target_date_str: Optional[str] = None, location_latlong: Optional[str] = None) -> bool:
        """
        Orchestrates the high-level workflow for fetching and storing weather data.

        :param target_date_str: The specific date to fetch weather data for in 'YYYY-MM-DD' format.
                                If ``None``, defaults to yesterday's date.
        :type target_date_str: Optional[str]
        :param location_latlong: Optional latitude,longitude string to override the default configured API location.
        :type location_latlong: Optional[str]
        :returns: ``True`` if the operation was successful, ``False`` otherwise.
        :rtype: bool
        """
        process_date: str = target_date_str if target_date_str else (datetime.now().date() - timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f"Orchestrating fetch and store operation for weather data on date: {process_date}.")

        try:
            raw_json_file_path = self.data_retriever.retrieve_raw_data(date = process_date, location_lat_long = location_latlong)

            if not raw_json_file_path:
                logger.error(f"Failed to retrieve or locate raw JSON file for {process_date}. Cannot proceed with ingestion.")
                return False

            if self.json_data_loader.process_raw_data_file(raw_file_path = raw_json_file_path):
                logger.info(f"Successfully processed and stored structured weather data for {process_date} from {raw_json_file_path}.")
                return True
            else:
                logger.error(f"Failed to process and store structured weather data for {process_date} from {raw_json_file_path}.")
                return False

        except Exception as e:
            logger.exception(f"An unexpected error occurred during weather data orchestration for {process_date}: {e}")
            return False
    

logger = LogHelper.get_logger(__name__)

def _is_valid_date(date_str: str) -> bool:
    """
    Validates if a string is in 'YYYY-MM-DD' format.

    :param date_str: The date string to validate.
    :type date_str: str
    :returns: ``True`` if the date string is valid, ``False`` otherwise.
    :rtype: bool
    """
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def is_valid_lat_long(lat_long_str: str) -> bool:
    """
    Validates if a string represents a valid latitude,longitude pair.

    :param lat_long_str: The latitude,longitude string to validate (e.g., '40.71,-74.00').
    :type lat_long_str: str
    :returns: ``True`` if the string is a valid lat/long pair, ``False`` otherwise.
    :rtype: bool
    """
    try:
        lat_str, lon_str = lat_long_str.split(',')
        lat = float(lat_str)
        lon = float(lon_str)
        return -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0
    except (ValueError, IndexError):
        return False
    
def _get_db_operations(args: argparse.Namespace, db_config: DatabaseConfig) -> DBOperations:
    """
    Initializes and returns a DBOperations instance based on the specified database type and connection type.

    :param database_type: The type of database to connect to (e.g., PostgreSQL, Supabase).
    :type database_type: DatabaseType
    :param connection_type: The type of connection to use (local or remote).
    :type connection_type: ConnectionType
    :returns: An instance of DBOperations configured for the specified database.
    :rtype: DBOperations
    """
    database_type: DatabaseType
    if args.postgres:
        database_type = DatabaseType.POSTGRES
    elif args.supabase:
        database_type = DatabaseType.SUPABASE
    else:
        # Default to PostgreSQL if neither -P nor -S is provided
        database_type = DatabaseType.POSTGRES
        logger.info("No database type specified via flags. Defaulting to PostgreSQL.")
    
    if args.remote:
        connection_type: ConnectionType = ConnectionType.REMOTE
    elif args.local:
        connection_type: ConnectionType = ConnectionType.LOCAL
    else:
        connection_type: ConnectionType = ConnectionType.LOCAL if database_type == DatabaseType.POSTGRES else ConnectionType.REMOTE

    conn_config = (db_config.postgres_local_connection if connection_type == ConnectionType.LOCAL
                           else db_config.postgres_remote_connection)
                            
    app_conn_details = ConnectionDetails(
        host=conn_config.host,
        port=int(conn_config.port),
        user=conn_config.user,
        password=conn_config.password,
        dbname=conn_config.dbname,
        sslmode=None
    )

    db_operations = DBOperations()
    db_operations.connect(app_conn_details)
    return db_operations

def _parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments for the orchestrator script.
    
    :returns: An argparse.Namespace object containing the parsed arguments.
    """
    parser = argparse.ArgumentParser(
        prog='weatherfetch_orchestrator.py',
        description="Fetch and store daily weather data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    db_type_group = parser.add_mutually_exclusive_group()
    db_type_group.add_argument(
        "-P", "--postgres",
        action="store_true",
        help="Use PostgreSQL database for setup and data loading."
    )
    db_type_group.add_argument(
        "-S", "--supabase",
        action="store_true",
        help="Use Supabase database for setup and data loading."
    )

    conn_type_group = parser.add_mutually_exclusive_group()
    conn_type_group.add_argument(
        "-l", "--local",
        action="store_true",
        help="Use a local database connection. (Primarily for PostgreSQL)."
    )
    conn_type_group.add_argument(
        "-r", "--remote",
        action="store_true",
        help="Use a remote database connection. (Applies to both PostgreSQL and Supabase)."
    )

    parser.add_argument(
        '-d', '--date', type=str,
        help="Date to fetch weather data for in YYYY-MM-DD format. If not provided, defaults to yesterday."
    )
    parser.add_argument(
        '-la', '--location', type=str,
        help="Latitude,longitude string (e.g., '40.71,-74.00') to override the default configured API location."
    )
    
    return parser.parse_args()

def main() -> None:
    """
    Main entry point for fetching and storing daily weather data.

    Handles command-line arguments, user prompts for date/location,
    database connection, and orchestrator instantiation.
    """
    start_time_main = time.time()
    db_operations: Optional[DBOperations] = None

    # -- STEP 1: Parse command-line arguments --
    args = _parse_args()

    # -- STEP 2: Validate and set up date and location parameters --
    target_date_str = args.date
    if target_date_str and not _is_valid_date(target_date_str):
        logger.warning(f"Invalid date format for --date: {target_date_str}. Expected YYYY-MM-DD. Ignoring provided date.")
        target_date_str = None
    if not target_date_str:
        while True:
            user_input = input("Enter date for weather data (YYYY-MM-DD) or press Enter for yesterday: ").strip()
            if not user_input:
                target_date_str = (datetime.now().date() - timedelta(days=1)).strftime('%Y-%m-%d')
                logger.info(f"No date provided. Defaulting to yesterday: {target_date_str}")
                break
            elif _is_valid_date(user_input):
                target_date_str = user_input
                logger.info(f"Using provided date: {target_date_str}")
                break
            else:
                print("Invalid date format. Please use YYYY-MM-DD.")
                logger.warning(f"Invalid date format entered by user: {user_input}")

    location_latlong = args.location
    if location_latlong and not is_valid_lat_long(location_latlong):
        logger.warning(f"Invalid location format or range for --location: {location_latlong}. Ignoring provided location.")
        location_latlong = None

    try:
        database_config = DatabaseConfig()
        file_config = FileConfig()
        weather_api_config = WeatherAPIConfig()

        # -- STEP 3: Determine database type and connection type and Initialize DB Operations--
        db_operations = _get_db_operations(args = args, db_config = database_config)
        if not db_operations:
            logger.error("Failed to initialize database operations. Exiting.")
            return
        else:
            logger.info(f"Database operations initialized successfully")

        # -- STEP 4: Initialize WeatherFetchOrchestrator with dependencies --
        weather_orchestrator = WeatherFetchOrchestrator(
            db_operations=db_operations,
            file_config=file_config,
            weather_api_config=weather_api_config
        )

        # -- STEP 5: Fetch and store weather data for the specified date and location --
        success = weather_orchestrator.fetch_and_store_weather_data(target_date_str, location_latlong)
        if success:
            logger.info("Weather data fetching and storing completed successfully.")
        else:
            logger.error("Weather data fetching and storing encountered errors.")

    except Exception as e:
        logger.exception(f"A critical error occurred in main execution: {e}")
    finally:
        if db_operations:
            db_operations.close()
            logger.info("Database connection closed after weather fetching process.")

    end_time_main = time.time()
    logger.info(f"Total script execution time: {end_time_main - start_time_main:.2f} seconds")

if __name__ == "__main__":
    main()