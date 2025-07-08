# vivarium/deploy/src/data_loader/json_data_loader.py
"""
A concrete data loading strategy for ingesting raw climate data from local JSON files
into the database. This class implements the DataLoaderStrategy interface.
"""

import json
import re
import logging
from typing import Optional, Dict, List, Any
from pathlib import Path

from utilities.src.logger import LogHelper
from utilities.src.path_utils import PathUtils
from utilities.src.db_operations import DBOperations
from utilities.src.new_config import DatabaseConfig, FileConfig

from deploy.src.data_loader.data_loader_strategy import DataLoaderStrategy

from weather.src.database.location_queries import LocationQueries
from database.climate_data_ops.raw_data_queries import RawDataQueries
from weather.src.database.forecast_queries import ForecastQueries
from weather.src.database.day_queries import DayQueries
from weather.src.database.astro_queries import AstroQueries
from weather.src.database.condition_queries import ConditionQueries
from weather.src.database.hour_queries import HourQueries

logger = LogHelper.get_logger(__name__)


class JSONDataLoader(DataLoaderStrategy):
    """
    A concrete data loading strategy for ingesting raw climate data from local JSON files
    into the database. This class implements the DataLoaderStrategy interface.
    """

    def __init__(self, folder_path: Optional[str] = None,
                 db_config: Optional[DatabaseConfig] = None):
        """
        Initializes the JSONDataLoader with paths and database configuration.

        Note: The DBOperations instance for database interaction will be passed
        during the `execute_full_data_load` call from the orchestrator.

        :param folder_path: Path to the directory containing raw weather JSON files.
                            If :obj:`None`, defaults to `FileConfig().json_folder`.
                            This path will be resolved to an absolute path.
        :type folder_path: Optional[str]
        :param db_config: Database configuration object. If :obj:`None`, a new
                          :class:`DatabaseConfig` instance will be created.
                          This config is used by query classes if they need it,
                          but the active connection (`DBOperations`) is passed later.
        :type db_config: Optional[DatabaseConfig]
        """
        super().__init__()

        # Resolve the raw JSON folder path to an absolute path using PathUtils
        self.raw_json_folder_path: Path = PathUtils.get_resource_path(
            folder_path if folder_path is not None else FileConfig().json_folder,
            must_exist=True
        )
        # Resolve the processed JSON folder path to an absolute path
        self.processed_json_folder_path: Path = PathUtils.get_resource_path(
            FileConfig().processed_json_folder,
            must_exist=False
        )
        # Ensure the processed folder exists
        self.processed_json_folder_path.mkdir(parents=True, exist_ok=True)

        self.db_config = db_config if db_config is not None else DatabaseConfig()

        self.database_ops: Optional[DBOperations] = None

        # Query handlers are initialized here, but will be assigned the actual DBOperations instance later.
        self.raw_data_queries: Optional[RawDataQueries] = None
        self.location_db: Optional[LocationQueries] = None
        self.forecast_db: Optional[ForecastQueries] = None
        self.day_db: Optional[DayQueries] = None
        self.astro_db: Optional[AstroQueries] = None
        self.condition_db: Optional[ConditionQueries] = None
        self.hour_db: Optional[HourQueries] = None

        logger.info(f"JSONDataLoader initialized. Raw JSON files folder: {self.raw_json_folder_path}")
        logger.info(f"Processed JSON files will be stored in: {self.processed_json_folder_path}")

    def load_from_dump(self) -> bool:
        """
        This method is not applicable for JSONDataLoader. It logs a warning and returns False.

        :returns: :obj:`False`, as loading from a dump is not supported by this strategy.
        :rtype: bool
        """
        logger.warning(
            "JSONDataLoader does not support loading from database dumps. "
            "Ignoring dump_file_path parameter and proceeding with JSON data."
        )
        return False

    def load_json_data(self) -> bool:
        """
        Loads local JSON files from `self.raw_json_folder_path` into the database.
        Processes each JSON file, extracts relevant data, and inserts/updates it
        into the various database tables via the query handlers.

        :returns: :obj:`True` if all files are processed successfully, :obj:`False` otherwise.
        :rtype: bool
        """
        if self.database_ops is None:
            logger.error("DatabaseOperations instance not set. Cannot load JSON data.")
            return False

        logger.info(f"Starting JSON data loading process from folder: {self.raw_json_folder_path}.")

        overall_success = True
        processed_count = 0

        try:
            if not self.raw_json_folder_path.exists():
                logger.error(f"JSON data folder does not exist: {self.raw_json_folder_path}")
                return False
            if not self.raw_json_folder_path.is_dir():
                logger.error(f"Provided path is not a directory: {self.raw_json_folder_path}")
                return False

            # List only JSON files and sort them
            json_files = [f for f in self.raw_json_folder_path.iterdir() if f.suffix == '.json' and f.is_file()]
            if not json_files:
                logger.info(f"No JSON files found in raw data folder: {self.raw_json_folder_path}. Nothing to load.")
                return True

            for original_file_path in sorted(json_files):
                logger.debug(f"Processing data from file: {original_file_path.name}...")

                # Call the new method to copy, process, and save the JSON
                processed_data_dict = self._copy_and_process_json_file(original_file_path)

                if processed_data_dict is None:
                    overall_success = False
                    logger.error(f"Failed to copy or process data from {original_file_path.name}. Skipping database insertion.")
                    continue  # Move to next file

                # Now use the processed_data_dict for database insertion
                # The filename format is already validated within _copy_and_process_json_file
                match = re.match(r'(\d{4}-\d{2}-\d{2})\.json', original_file_path.name)
                weather_date = match.group(1) # This match is guaranteed due to _validate_json_schema

                raw_data_json_str = json.dumps(processed_data_dict)
                raw_data_inserted = self.raw_data_queries.insert(date=weather_date, raw_data=raw_data_json_str)

                if not raw_data_inserted:
                    logger.error(f"Failed to insert/update raw climate data for {weather_date} from {original_file_path.name}.")
                    overall_success = False
                    continue  # Move to next file

                logger.info(f"Successfully inserted/updated raw climate data for {weather_date}.")

                location_processed = self._handle_location_data(processed_data_dict, weather_date)
                forecast_processed = self._handle_forecast_data(processed_data_dict, weather_date)

                if location_processed and forecast_processed:
                    processed_count += 1
                    logger.info(f"Successfully processed and persisted all data from {original_file_path.name}.")
                else:
                    overall_success = False
                    logger.error(f"Failed to fully process and persist data from {original_file_path.name}. See previous logs for details.")

        except Exception as e:
            logger.error(f"Error accessing JSON data directory {self.raw_json_folder_path}: {e}", exc_info=True)
            overall_success = False

        logger.info(f"JSON data loading completed. Successfully processed {processed_count} files. Overall success: {overall_success}")
        return overall_success

    def execute_full_data_load(self, db_operations: DBOperations) -> bool:
        """
        Orchestrates the full data loading process for raw climate data.

        For JSONDataLoader, this primarily means loading from raw JSON files
        located in the `self.raw_json_folder_path`.

        :param db_operations: An initialized and connected :class:`DBOperations` instance.
        :type db_operations: DBOperations
        :returns: :obj:`True` if the raw climate data loading is successful, :obj:`False` otherwise.
        :rtype: bool
        """
        logger.info("Executing full data load using JSONDataLoader strategy.")

        self.database_ops = db_operations  # Assign the passed DBOperations instance

        # Initialize all database query handlers with the received DatabaseOperations instance
        self.raw_data_queries = RawDataQueries(self.database_ops)
        self.location_db = LocationQueries(self.database_ops)
        self.forecast_db = ForecastQueries(self.database_ops)
        self.day_db = DayQueries(self.database_ops)
        self.astro_db = AstroQueries(self.database_ops)
        self.condition_db = ConditionQueries(self.database_ops)
        self.hour_db = HourQueries(self.database_ops)

        load_success = self.load_json_data()

        if load_success:
            logger.info("Full data load (raw JSON climate data) completed successfully.")
        else:
            logger.error("Full data load (raw JSON climate data) failed.")

        # Connection closing is handled by the orchestrator (main() function)
        return load_success

    # --- PRIVATE HELPER FUNCTIONS ---

    def _copy_and_process_json_file(self, original_file_path: Path) -> Optional[Dict[str, Any]]:
        """
        Reads an original JSON file, performs in-memory coordinate rounding,
        saves the modified content to a new file in the processed folder,
        and returns the modified dictionary. The original file is not altered.

        :param original_file_path: The absolute path to the original JSON file.
        :type original_file_path: Path
        :returns: The modified dictionary if successful, :obj:`None` otherwise.
        :rtype: Optional[Dict[str, Any]]
        """
        file_name = original_file_path.name
        processed_file_path = self.processed_json_folder_path / file_name

        try:
            # Validate filename format first
            if not re.match(r'\d{4}-\d{2}-\d{2}\.json', file_name):
                logger.warning(f"Skipping '{file_name}': Filename does not match 'YYYY-MM-DD.json' format.")
                return None

            # Read the original file (read-only mode is implicit with 'r')
            with open(original_file_path, 'r', encoding='utf-8') as f:
                raw_data_dict = json.load(f)

            # Validate schema before processing
            if not self._validate_json_schema(data=raw_data_dict, file_name=file_name):
                logger.error(f"Schema validation failed for original file {file_name}. Skipping processing.")
                return None

            # Perform in-memory rounding
            modified_data_dict = self._round_location_coordinates_in_memory(raw_data_dict, file_name)

            # Save the modified data to the new processed file
            with open(processed_file_path, 'w', encoding='utf-8') as f:
                json.dump(modified_data_dict, f, indent=4)
            logger.info(f"Processed JSON file saved to: {processed_file_path}")

            return modified_data_dict

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from original file {original_file_path}: {e}", exc_info=True)
            return None
        except FileNotFoundError:
            logger.error(f"Original file not found: {original_file_path}.", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred during copy and processing of {original_file_path}: {e}", exc_info=True)
            return None

    def _round_location_coordinates_in_memory(self, data_dict: Dict[str, Any], file_name: str) -> Dict[str, Any]:
        """
        Rounds 'lat' and 'lon' coordinates within the provided dictionary in memory.
        This method modifies the dictionary in place and returns it.

        :param data_dict: The dictionary containing location data to be rounded.
                          This dictionary will be modified.
        :type data_dict: Dict[str, Any]
        :param file_name: The name of the file being processed, for logging.
        :type file_name: str
        :returns: The modified dictionary with rounded coordinates.
        :rtype: Dict[str, Any]
        """
        location_data = data_dict.get('location', {})
        lat = location_data.get('lat')
        lon = location_data.get('lon')

        if lat is not None and isinstance(lat, (int, float)):
            rounded_lat = round(lat, 2)
            # Compare formatted strings for precision to avoid floating-point inaccuracies
            if f'{lat:.10f}' != f'{rounded_lat:.10f}':
                location_data['lat'] = rounded_lat
                logger.warning(f"Latitude {lat} rounded to {rounded_lat} in data from {file_name}.")

        if lon is not None and isinstance(lon, (int, float)):
            rounded_lon = round(lon, 2)
            if f'{lon:.10f}' != f'{rounded_lon:.10f}':
                location_data['lon'] = rounded_lon
                logger.warning(f"Longitude {lon} rounded to {rounded_lon} in data from {file_name}.")

        return data_dict  # Return the (potentially modified) dictionary

    def _handle_location_data(self, raw_data: Dict[str, Any], weather_date: str) -> bool:
        """
        Handles the retrieval or insertion of location data from the raw weather dictionary.

        It checks if a location with the same latitude and longitude already exists in the
        `locations` table. If not, it inserts the new location. The method stores the
        retrieved or new location ID in `self._current_location_id` for use by subsequent
        data handlers.

        :param raw_data: The dictionary containing the full raw weather API response.
        :type raw_data: Dict[str, Any]
        :param weather_date: The date string (YYYY-MM-DD) associated with the data.
        :type weather_date: str
        :returns: :obj:`True` if location data is successfully handled, :obj:`False` otherwise.
        :rtype: bool
        """
        if not raw_data:
            logger.error(f"No raw data dictionary provided for location processing on {weather_date}.")
            return False

        try:
            location_data = raw_data.get('location', {})
            latitude = location_data.get('lat')
            longitude = location_data.get('lon')

            if latitude is None or longitude is None:
                logger.error(f"Latitude or Longitude is missing in raw data for {weather_date}. Cannot process location data.")
                return False

            location_id = self.location_db.get_location_id(latitude, longitude)
            if not location_id:
                location_id = self.location_db.insert_location_data(location_data)
                if location_id is None:
                    logger.error(f"Failed to insert location data for {weather_date}.")
                    return False
                logger.info(f"Inserted new location with ID: {location_id} for {weather_date}.")
            else:
                logger.info(f"Location already exists with ID: {location_id} for {weather_date}.")

            self._current_location_id = location_id
            return True
        except Exception as e:
            logger.exception(f"An unexpected error occurred during location data handling for {weather_date}: {e}")
            return False

    def _handle_forecast_data(self, raw_data: Dict[str, Any], weather_date: str) -> bool:
        """
        Handles the processing of forecast data for each forecast day.

        The method iterates through each forecast day in the raw data, checks if
        a forecast entry already exists for the given location and date, and inserts
        it if it doesn't. It then calls nested handlers to process the daily,
        astronomical, and hourly data for each forecast day.

        :param raw_data: The dictionary containing the full raw weather API response.
        :type raw_data: Dict[str, Any]
        :param weather_date: The date string (YYYY-MM-DD) associated with the main data.
        :type weather_date: str
        :returns: :obj:`True` if all forecast days are processed successfully, :obj:`False` otherwise.
        :rtype: bool
        """
        if not hasattr(self, '_current_location_id') or self._current_location_id is None:
            logger.error(f"Location ID not set. Skipping forecast data processing for {weather_date}.")
            return False

        location_id = self._current_location_id
        forecast_data = raw_data.get('forecast', {}).get('forecastday', [])

        if not forecast_data:
            logger.warning(f"No forecast data found in raw JSON for {weather_date}.")
            return True

        all_forecast_days_processed_successfully = True
        for forecast_day_dict in forecast_data:
            forecast_date = forecast_day_dict.get('date')
            if not forecast_date:
                logger.warning("Forecast day entry without a 'date' key. Skipping this entry.")
                all_forecast_days_processed_successfully = False
                continue

            try:
                existing_forecast_id = self.forecast_db.get_forecast_by_location_and_date(location_id, forecast_date)
                if not existing_forecast_id:
                    forecast_id = self.forecast_db.insert_forecast_data(location_id, forecast_day_dict)
                    if not forecast_id:
                        logger.error(f"Failed to insert forecast data for {forecast_date} (Location ID: {location_id}).")
                        all_forecast_days_processed_successfully = False
                        continue
                    logger.info(f"Inserted new forecast with ID: {forecast_id} for {forecast_date}.")
                else:
                    forecast_id = existing_forecast_id
                    logger.info(f"Forecast data already exists for location ID {location_id} and date {forecast_date}.")
                if forecast_id:
                    nested_data_processed_successfully = True

                    try:
                        day_processed = self._handle_day_data(location_id, forecast_date, forecast_day_dict.get('day', {}))
                        astro_processed = self._handle_astro_data(location_id, forecast_date, forecast_day_dict.get('astro', {}))
                        hour_processed = self._handle_hour_data(location_id, forecast_date, forecast_day_dict.get('hour', []))

                        if not (day_processed and astro_processed and hour_processed):
                            nested_data_processed_successfully = False
                            logger.warning(f"Partial success for forecast date {forecast_date}. Some nested data (day/astro/hour) failed to insert correctly.")

                    except Exception as e:
                        nested_data_processed_successfully = False
                        logger.exception(f"An unexpected error occurred while processing nested data for forecast date {forecast_date}: {e}")

                    if not nested_data_processed_successfully:
                        all_forecast_days_processed_successfully = False
                else:
                    all_forecast_days_processed_successfully = False

            except Exception as e:
                logger.exception(f"An unexpected error occurred processing forecast day {forecast_date} for location {location_id}: {e}")
                all_forecast_days_processed_successfully = False

        return all_forecast_days_processed_successfully

    def _handle_day_data(self, location_id: int, date: str, day_data: Dict[str, Any]) -> bool:
        """
        Handles the processing and storage of daily weather data.

        This method processes the 'day' data, including the associated weather
        'condition'. It inserts a new day entry if one doesn't exist for the given
        location and date.

        :param location_id: The ID of the associated location.
        :type location_id: int
        :param date: The date string (YYYY-MM-DD) for this day's data.
        :type date: str
        :param day_data: The dictionary containing the day's weather details.
        :type day_data: Dict[str, Any]
        :returns: :obj:`True` if day data is successfully handled, :obj:`False` otherwise.
        :rtype: bool
        """
        if not day_data:
            logger.warning(f"Day data unavailable for {date}.")
            return True

        try:
            condition_dict = day_data.get('condition', {})
            if condition_dict and condition_dict.get('code') is not None:
                if not self.condition_db.get_condition(condition_dict.get('code')):
                    condition_inserted = self.condition_db.insert_condition(condition_dict)
                    if condition_inserted:
                        logger.info(f"Successfully stored condition data for code: {condition_dict.get('code')}.")
                    else:
                        logger.error(f"Failed to insert condition data for code: {condition_dict.get('code')}.")
                        return False
                else:
                    logger.info(f"Condition data already exists for code: {condition_dict.get('code')}.")
            else:
                logger.warning(f"Condition data is missing 'code' or is empty for {date}.")

            if not self.day_db.get_day_data(location_id, date):
                self.day_db.insert_day_data(location_id, date, day_data)
                logger.info(f"Successfully stored day data for {date}.")
                return True
            else:
                logger.info(f"Day data already exists for location ID {location_id} and date {date}.")
                return True
        except Exception as e:
            logger.exception(f"An unexpected error occurred handling day data for {date}: {e}")
            return False

    def _handle_astro_data(self, location_id: int, date: str, astro_data: Dict[str, Any]) -> bool:
        """
        Handles the processing and storage of astronomical data (sunrise, sunset, etc.).

        This method inserts new astronomical data for a given date and location if it
        doesn't already exist in the database.

        :param location_id: The ID of the associated location.
        :type location_id: int
        :param date: The date string (YYYY-MM-DD) for this astro data.
        :type date: str
        :param astro_data: The dictionary containing the astronomical details.
        :type astro_data: Dict[str, Any]
        :returns: :obj:`True` if astro data is successfully handled, :obj:`False` otherwise.
        :rtype: bool
        """
        if not astro_data:
            logger.warning(f"Astro data unavailable for {date}.")
            return True

        try:
            if not self.astro_db.get_astro_data(location_id, date):
                self.astro_db.insert_astro_data(location_id, date, astro_data)
                logger.info(f"Successfully stored astro data for {date}.")
                return True
            else:
                logger.info(f"Astro data already exists for location ID {location_id} and date {date}.")
                return True
        except Exception as e:
            logger.exception(f"An unexpected error occurred handling astro data for {date}: {e}")
            return False

    def _handle_hour_data(self, location_id: int, date: str, hour_data: List[Dict[str, Any]]) -> bool:
        """
        Handles the processing and storage of hourly weather data.

        This method processes a list of hourly data dictionaries for a specific date
        and inserts them into the database.

        :param location_id: The ID of the associated location.
        :type location_id: int
        :param date: The date string (YYYY-MM-DD) for this hour data.
        :type date: str
        :param hour_data: A list of dictionaries, each containing hourly weather details.
        :type hour_data: List[Dict[str, Any]]
        :returns: :obj:`True` if hourly data is successfully handled, :obj:`False` otherwise.
        :rtype: bool
        """
        if not hour_data:
            logger.warning(f"Hourly data unavailable for {date}.")
            return True

        try:
            self.hour_db.insert_hour_data(location_id, date, hour_data)
            logger.info(f"Successfully stored hourly data for {date}.")
            return True
        except Exception as e:
            logger.exception(f"An unexpected error occurred handling hourly data for {date}: {e}")
            return False

    def _validate_json_schema(self, data: Dict[str, Any], file_name: str) -> bool:
        """
        Validates the schema of the loaded raw JSON data to ensure it has the expected structure.

        :param data: The dictionary loaded from the JSON file.
        :type data: Dict[str, Any]
        :param file_name: The name of the file being processed, for logging.
        :type file_name: str
        :returns: :obj:`True` if the schema is valid, :obj:`False` otherwise.
        :rtype: bool
        """
        # Check for top-level keys
        required_keys = ['location', 'forecast']
        for key in required_keys:
            if key not in data:
                logger.error(f"Validation failed for {file_name}: Missing top-level key '{key}'.")
                return False

        # Check 'location' data structure and types
        location_data = data['location']
        if not all(k in location_data for k in ['name', 'lat', 'lon']):
            logger.error(f"Validation failed for {file_name}: 'location' data is incomplete.")
            return False
        if not isinstance(location_data['lat'], (int, float)) or not isinstance(location_data['lon'], (int, float)):
            logger.error(f"Validation failed for {file_name}: 'lat' or 'lon' are not numbers.")
            return False

        # Check 'forecast' data structure and types
        forecast_data = data['forecast']
        if 'forecastday' not in forecast_data or not isinstance(forecast_data['forecastday'], list):
            logger.error(f"Validation failed for {file_name}: 'forecastday' is missing or not a list.")
            return False

        # Check each 'forecastday' entry
        if not forecast_data['forecastday']:
            logger.warning(f"Validation warning for {file_name}: 'forecastday' list is empty.")
            # We will still return True here as it's not a critical error, just a warning.

        for day in forecast_data['forecastday']:
            if not all(k in day for k in ['date', 'day', 'astro', 'hour']):
                logger.error(f"Validation failed for {file_name}: A 'forecastday' entry is incomplete.")
                return False
            if not isinstance(day['hour'], list):
                logger.error(f"Validation failed for {file_name}: 'hour' is not a list in a forecast day entry.")
                return False

        logger.debug(f"Schema validation passed for {file_name}.")
        return True