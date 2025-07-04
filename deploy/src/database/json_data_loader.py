# vivarium/deploy/src/database/json_data_loader.py
import os
import json
import re
import sys
from typing import Optional, Dict, List, Any

# Ensure the project root is in the path for imports to work correctly
# This file is located at vivarium/deploy/src/database/json_data_loader.py
# So, three levels up will be the vivarium root: database/ -> src/ -> deploy/ -> vivarium/
if __name__ == "__main__":
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..','..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from utilities.src.logger import LogHelper
from utilities.src.database_operations import DatabaseOperations
from utilities.src.config import DatabaseConfig, FileConfig

from deploy.src.database.data_loader_strategy import DataLoaderStrategy

from weather.src.database.location_queries import LocationQueries
from weather.src.database.raw_data_queries import RawDataQueries
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

        :param folder_path: Path to the directory containing raw weather JSON files.
                            If None, defaults to `FileConfig.json_folder`.
        :type folder_path: Optional[str]
        :param db_config: Database configuration object. If None, a new `DatabaseConfig` instance
                          will be created.
        :type db_config: Optional[DatabaseConfig]
        """
        super().__init__()
        
        self.folder_path = folder_path if folder_path is not None else FileConfig.json_folder
        self.db_config = db_config if db_config is not None else DatabaseConfig()
        
        # Initialize DatabaseOperations with db_config to manage the connection
        self.database_ops = DatabaseOperations(self.db_config) 

        # Initialize all database query handlers with the same DatabaseOperations instance
        self.raw_data_db = RawDataQueries(self.database_ops)
        self.location_db = LocationQueries(self.database_ops)
        self.forecast_db = ForecastQueries(self.database_ops)
        self.day_db = DayQueries(self.database_ops)
        self.astro_db = AstroQueries(self.database_ops)
        self.condition_db = ConditionQueries(self.database_ops)
        self.hour_db = HourQueries(self.database_ops)

        logger.info(f"JSONDataLoader initialized. Raw JSON files folder: {self.folder_path}")

    def load_from_dump(self) -> bool:
        """
        This method is not applicable for JSONDataLoader. It logs a warning and returns False.

        :returns: False, as loading from a dump is not supported by this strategy.
        :rtype: bool
        """
        logger.warning(
            "JSONDataLoader does not support loading from database dumps. "
            "Ignoring dump_file_path parameter and proceeding with JSON data."
        )
        return False

    def load_json_data(self) -> bool:
        """
        Loads local JSON files from `self.folder_path` into the database.
        Processes each JSON file, extracts relevant data, and inserts/updates it
        into the various database tables via the query handlers.

        :returns: True if all files are processed successfully, False otherwise.
        :rtype: bool
        """
        logger.info(f"Starting JSON data loading process from folder: {self.folder_path}.")

        overall_success = True
        processed_count = 0

        try:
            if not os.path.exists(self.folder_path):
                logger.error(f"JSON data folder does not exist: {self.folder_path}")
                return False
            if not os.path.isdir(self.folder_path):
                logger.error(f"Provided path is not a directory: {self.folder_path}")
                return False
            
            json_files = [f for f in os.listdir(self.folder_path) if f.endswith('.json')]
            if not json_files:
                logger.info(f"No JSON files found in raw data folder: {self.folder_path}. Nothing to load.")
                return True

            for filename in sorted(json_files): 
                file_path = os.path.join(self.folder_path, filename)
                logger.debug(f"Processing data from file: {filename}...")
                
                if self._process_single_file(file_path):
                    processed_count += 1
                    logger.info(f"Successfully processed and persisted data from {filename}.")
                else:
                    overall_success = False
                    logger.error(f"Failed to process data from {filename}. See previous logs for details.")

        except Exception as e:
            logger.error(f"Error accessing JSON data directory {self.folder_path}: {e}", exc_info=True)
            overall_success = False

        logger.info(f"JSON data loading completed. Successfully processed {processed_count} files. Overall success: {overall_success}")
        return overall_success

    def execute_full_data_load(self) -> bool:
        """
        Orchestrates the full data loading process for raw climate data.

        For JSONDataLoader, this primarily means loading from raw JSON files
        located in the `self.folder_path`.

        :returns: True if the raw climate data loading is successful, False otherwise.
        :rtype: bool
        """
        logger.info("Executing full data load using JSONDataLoader strategy.")

        load_success = self.load_json_data()

        if load_success:
            logger.info("Full data load (raw JSON climate data) completed successfully.")
        else:
            logger.error("Full data load (raw JSON climate data) failed.")
        
        # Ensure the DatabaseOperations connection is closed after all loading attempts
        try:
            if self.database_ops:
                self.database_ops.close() 
                logger.info("JSONDataLoader: DatabaseOperations connection closed after full data loading.")
        except Exception as e:
            logger.error(f"Error closing database connection in JSONDataLoader: {e}", exc_info=True)

        return load_success

    # --- PRIVATE HELPER FUNCTIONS ---
    
    def _process_single_file(self, file_path: str) -> bool:
        """
        Processes a single raw JSON weather data file.

        This method reads the file, extracts the date from the filename, and then
        calls helper methods to insert the raw JSON and its nested data into
        the database tables.

        :param file_path: The absolute path to the JSON file to process.
        :type file_path: str
        :returns: True if the file is processed and data stored successfully, False otherwise.
        :rtype: bool
        """
        file_name = os.path.basename(file_path)
        logger.debug(f"Parsing and loading data from: {file_name}")

        try:
            match = re.match(r'(\d{4}-\d{2}-\d{2})\.json', file_name)
            if not match:
                logger.warning(f"Skipping '{file_name}': Filename does not match 'YYYY-MM-DD.json' format.")
                return False

            weather_date = match.group(1)

            with open(file_path, 'r', encoding='utf-8') as f:
                raw_data_dict = json.load(f)

            if not self._validate_json_schema(data = raw_data_dict, file_name = file_name):
                return False

            if not self._round_location_coordinates(raw_data_dict, file_path):
                return False

            raw_data_json_str = json.dumps(raw_data_dict)
            raw_data_inserted = self.raw_data_db.insert_raw_data(date=weather_date, raw_data=raw_data_json_str)

            if not raw_data_inserted:
                logger.error(f"Failed to insert/update raw climate data for {weather_date} from {file_name}.")
                return False

            logger.info(f"Successfully inserted/updated raw climate data for {weather_date}.")

            location_processed = self._handle_location_data(raw_data_dict, weather_date)
            forecast_processed = self._handle_forecast_data(raw_data_dict, weather_date)

            return location_processed and forecast_processed

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from {file_path}: {e}", exc_info=True)
            return False
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. This should be caught earlier.", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"An unexpected error occurred while processing {file_path}: {e}", exc_info=True)
            return False

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
        :returns: True if location data is successfully handled, False otherwise.
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
        :returns: True if all forecast days are processed successfully, False otherwise.
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
        :returns: True if day data is successfully handled, False otherwise.
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
        :returns: True if astro data is successfully handled, False otherwise.
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
        :returns: True if hourly data is successfully handled, False otherwise.
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

    def _round_location_coordinates(self, raw_data_dict: Dict[str, Any], file_path: str) -> bool:
        """
        Checks if location coordinates need rounding and updates the dictionary and file.

        This method inspects the 'lat' and 'lon' values in the loaded JSON data.
        If a coordinate has more than two decimal places of precision, it rounds
        the value to two decimal places. If a change is made, the JSON file on disk
        is updated with the new, rounded values.

        :param raw_data_dict: The dictionary loaded from the JSON file. This dictionary
                              will be modified in place if rounding occurs.
        :type raw_data_dict: Dict[str, Any]
        :param file_path: The path to the JSON file to be updated if needed.
        :type file_path: str
        :returns: True if the location data was rounded and the file updated, False otherwise.
        :rtype: bool
        """
        location_data = raw_data_dict.get('location', {})
        lat = location_data.get('lat')
        lon = location_data.get('lon')
        updated = False

        if lat is not None and isinstance(lat, (int, float)):
            rounded_lat = round(lat, 2)
            # Use string representation to avoid floating-point comparison issues
            if f'{lat:.10f}' != f'{rounded_lat:.10f}':
                location_data['lat'] = rounded_lat
                updated = True
                logger.warning(f"Latitude {lat} rounded to {rounded_lat} in file {os.path.basename(file_path)}.")

        if lon is not None and isinstance(lon, (int, float)):
            rounded_lon = round(lon, 2)
            if f'{lon:.10f}' != f'{rounded_lon:.10f}':
                location_data['lon'] = rounded_lon
                updated = True
                logger.warning(f"Longitude {lon} rounded to {rounded_lon} in file {os.path.basename(file_path)}.")

        if updated:
            try:
                # Write the modified dictionary back to the file
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(raw_data_dict, f, indent=4)
                logger.info(f"Updated JSON file {os.path.basename(file_path)} with rounded coordinates.")
                updated = True
            except Exception as e:
                logger.error(f"Failed to update JSON file {os.path.basename(file_path)} with rounded coordinates: {e}", exc_info=True)
                updated = False
        else:
            return True
    
    def _validate_json_schema(self, data: Dict[str, Any], file_name: str) -> bool:
        """
        Validates the schema of the loaded raw JSON data to ensure it has the expected structure.

        :param data: The dictionary loaded from the JSON file.
        :type data: Dict[str, Any]
        :param file_name: The name of the file being processed, for logging.
        :type file_name: str
        :returns: True if the schema is valid, False otherwise.
        :rtype: bool
        """
        # Check for top-level keys
        required_keys = ['location', 'current', 'forecast']
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

if __name__ == "__main__":
    pass