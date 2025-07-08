# vivarium/database/deploy_data_ops/data_loader/json_data_loader.py
"""
A concrete data loading strategy for ingesting raw climate data from local JSON files
into the database.
"""

import json
import re
from typing import Optional, Dict, List, Any
from pathlib import Path

from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations
from utilities.src.new_config import FileConfig
from utilities.src.path_utils import PathUtils

from database.deploy_data_ops.data_loader.data_loader_strategy import DataLoaderStrategy

from database.climate_data_ops.raw_data_queries import RawDataQueries
from database.climate_data_ops.location_queries import LocationQueries
from database.climate_data_ops.forecast_queries import ForecastQueries
from database.climate_data_ops.day_queries import DayQueries
from database.climate_data_ops.astro_queries import AstroQueries
from database.climate_data_ops.condition_queries import ConditionQueries
from database.climate_data_ops.hour_queries import HourQueries


logger = LogHelper.get_logger(__name__)


class JSONDataLoader(DataLoaderStrategy):
    """
    A concrete data loading strategy for ingesting raw climate data from local JSON files
    into the database.

    This class implements the DataLoaderStrategy interface, providing methods to
    load, process, and persist weather data from JSON files. It ensures that
    processed JSON files are only moved to a 'processed' folder upon successful
    database insertion of all associated data.
    """

    def __init__(self, db_operations: DBOperations,
                 folder_path: Optional[str] = None):
        """
        Initializes the JSONDataLoader with paths and database configuration.

        :param db_operations: An instance of DBOperations for database interaction.
        :type db_operations: DBOperations
        :param folder_path: Path to the directory containing raw weather JSON files.
                            If ``None``, defaults to `FileConfig().json_folder`.
                            This path will be resolved to an absolute path.
        :type folder_path: Optional[str]
        """
        super().__init__()

        self.raw_json_folder_path: Path = PathUtils.get_resource_path(
            folder_path if folder_path is not None else FileConfig().json_folder,
            must_exist=True
        )
        self.processed_json_folder_path: Path = PathUtils.get_resource_path(
            FileConfig().processed_json_folder,
            must_exist=False
        )
        self.processed_json_folder_path.mkdir(parents=True, exist_ok=True)
        self.database_ops: DBOperations = db_operations

        # Query handlers will be initialized in execute_full_data_load
        self.raw_data_ops:  Optional[RawDataQueries] = None
        self.location_ops:  Optional[LocationQueries] = None
        self.forecast_ops:  Optional[ForecastQueries] = None
        self.day_ops:       Optional[DayQueries] = None
        self.astro_ops:     Optional[AstroQueries] = None
        self.condition_ops: Optional[ConditionQueries] = None
        self.hour_ops:      Optional[HourQueries] = None

        logger.info(f"JSONDataLoader initialized. Raw JSON files folder: {self.raw_json_folder_path}")
        logger.info(f"Processed JSON files will be stored in: {self.processed_json_folder_path}")

    def load_from_dump(self) -> bool:
        """
        This method is not applicable for JSONDataLoader. It logs a warning and returns ``False``.

        :returns: ``False``, as loading from a dump is not supported by this strategy.
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
        into the various database tables via the query handlers. The processed
        JSON file is only saved to the 'processed' folder if all database
        operations for that file are successful.

        :returns: ``True`` if all files are processed successfully, ``False`` otherwise.
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

            json_files = [f for f in self.raw_json_folder_path.iterdir() if f.suffix == '.json' and f.is_file()]
            if not json_files:
                logger.info(f"No JSON files found in raw data folder: {self.raw_json_folder_path}. Nothing to load.")
                return True

            for original_file_path in sorted(json_files):
                logger.debug(f"Processing data from file: {original_file_path.name}...")

                processed_data_dict = self._copy_and_process_json_file(original_file_path)

                if processed_data_dict is None:
                    overall_success = False
                    logger.error(f"Failed to copy or process data from {original_file_path.name}. Skipping database insertion and file saving.")
                    continue

                match = re.match(r'(\d{4}-\d{2}-\d{2})\.json', original_file_path.name)
                weather_date = match.group(1)

                current_file_db_success = True

                logger.info(f"Attempting database insertion for data from {original_file_path.name}...")

                raw_data_json_str = json.dumps(processed_data_dict)
                if not self.raw_data_ops.insert(date=weather_date, raw_data=raw_data_json_str):
                    logger.error(f"Failed to insert/update raw climate data for {weather_date} from {original_file_path.name}.")
                    current_file_db_success = False
                else:
                    logger.info(f"Successfully inserted/updated raw climate data for {weather_date}.")

                if current_file_db_success:
                    location_processed = self._handle_location_data(processed_data_dict, weather_date)
                    forecast_processed = self._handle_forecast_data(processed_data_dict, weather_date)

                    if not (location_processed and forecast_processed):
                        current_file_db_success = False
                        logger.error(f"Failed to fully process and persist structured data (location/forecast/day/astro/hour) from {original_file_path.name}. See previous logs for details.")

                if current_file_db_success:
                    processed_file_path = self.processed_json_folder_path / original_file_path.name
                    logger.info(f"All DB operations successful for {original_file_path.name}. Attempting to save processed JSON file to: {processed_file_path}")
                    with open(processed_file_path, 'w', encoding='utf-8') as f:
                        json.dump(processed_data_dict, f, indent=4)
                    logger.info(f"Processed JSON file successfully saved to: {processed_file_path}")
                    processed_count += 1
                else:
                    overall_success = False
                    logger.warning(f"Database insertion failed for {original_file_path.name}. Processed JSON file WILL NOT be saved to the processed folder.")
                
                logger.info(f"Finished processing cycle for {original_file_path.name}. Success for this file: {current_file_db_success}")


        except Exception as e:
            logger.error(f"Error accessing JSON data directory {self.raw_json_folder_path}: {e}", exc_info=True)
            overall_success = False

        logger.info(f"JSON data loading completed. Successfully processed {processed_count} files (database + file move). Overall success: {overall_success}")
        return overall_success

    def execute_full_data_load(self) -> bool:
        """
        Orchestrates the full data loading process for raw climate data.

        For JSONDataLoader, this primarily means loading from raw JSON files
        located in the `self.raw_json_folder_path`.

        :returns: ``True`` if the raw climate data loading is successful, ``False`` otherwise.
        :rtype: bool
        """
        logger.info("Executing full data load using JSONDataLoader strategy.")

        self.raw_data_ops   = RawDataQueries(self.database_ops)
        self.location_ops   = LocationQueries(self.database_ops)
        self.forecast_ops   = ForecastQueries(self.database_ops)
        self.day_ops        = DayQueries(self.database_ops)
        self.astro_ops      = AstroQueries(self.database_ops)
        self.condition_ops  = ConditionQueries(self.database_ops)
        self.hour_ops       = HourQueries(self.database_ops)

        load_success = self.load_json_data()

        if load_success:
            logger.info("Full data load (raw JSON climate data) completed successfully.")
        else:
            logger.error("Full data load (raw JSON climate data) failed.")

        return load_success

    def _copy_and_process_json_file(self, original_file_path: Path) -> Optional[Dict[str, Any]]:
        """
        Reads an original JSON file, validates its filename and schema,
        and performs in-memory coordinate rounding.

        This method does not save the modified content to the processed folder.
        It returns the modified dictionary for further processing.

        :param original_file_path: The absolute path to the original JSON file.
        :type original_file_path: Path
        :returns: The modified dictionary if successful, ``None`` otherwise.
        :rtype: Optional[Dict[str, Any]]
        """
        file_name = original_file_path.name

        try:
            if not re.match(r'\d{4}-\d{2}-\d{2}\.json', file_name):
                logger.warning(f"Skipping '{file_name}': Filename does not match 'YYYY-MM-DD.json' format.")
                return None

            with open(original_file_path, 'r', encoding='utf-8') as f:
                raw_data_dict = json.load(f)

            if not self._validate_json_schema(data=raw_data_dict, file_name=file_name):
                logger.error(f"Schema validation failed for original file {file_name}. Skipping processing.")
                return None

            modified_data_dict = self._round_location_coordinates_in_memory(raw_data_dict, file_name)

            logger.debug(f"JSON file {original_file_path.name} processed in memory and returned for DB insertion.")

            return modified_data_dict

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from original file {original_file_path}: {e}", exc_info=True)
            return None
        except FileNotFoundError:
            logger.error(f"Original file not found: {original_file_path}.", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred during processing of {original_file_path}: {e}", exc_info=True)
            return None

    def _round_location_coordinates_in_memory(self, data_dict: Dict[str, Any], file_name: str) -> Dict[str, Any]:
        """
        Rounds 'lat' and 'lon' coordinates within the provided dictionary in memory to 2 decimal places.

        This method modifies the dictionary in place and returns it.

        :param data_dict: The dictionary containing location data to be rounded.
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
            if f'{lat:.10f}' != f'{rounded_lat:.10f}':
                location_data['lat'] = rounded_lat
                logger.warning(f"Latitude {lat} rounded to {rounded_lat} in data from {file_name}.")

        if lon is not None and isinstance(lon, (int, float)):
            rounded_lon = round(lon, 2)
            if f'{lon:.10f}' != f'{rounded_lon:.10f}':
                location_data['lon'] = rounded_lon
                logger.warning(f"Longitude {lon} rounded to {rounded_lon} in data from {file_name}.")

        return data_dict

    def _handle_location_data(self, raw_data: Dict[str, Any], weather_date: str) -> bool:
        """
        Handles the retrieval or insertion of location data from the raw weather dictionary.

        Checks if a location with the same latitude and longitude already exists. If not,
        it inserts the new location. The method stores the retrieved or new location ID
        in `self._current_location_id` for use by subsequent data handlers.

        :param raw_data: The dictionary containing the full raw weather API response.
        :type raw_data: Dict[str, Any]
        :param weather_date: The date string (YYYY-MM-DD) associated with the data.
        :type weather_date: str
        :returns: ``True`` if location data is successfully handled, ``False`` otherwise.
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

            location_id = self.location_ops.get(latitude, longitude)
            if location_id is None:
                location_id = self.location_ops.insert(location_data)
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

        The method iterates through each forecast day, checks for existing entries,
        and inserts new ones if they don't exist. It then calls nested handlers
        to process the daily, astronomical, and hourly data for each forecast day.

        :param raw_data: The dictionary containing the full raw weather API response.
        :type raw_data: Dict[str, Any]
        :param weather_date: The date string (YYYY-MM-DD) associated with the main data.
        :type weather_date: str
        :returns: ``True`` if all forecast days and their nested data are processed successfully,
                  ``False`` otherwise.
        :rtype: bool
        """
        if not hasattr(self, '_current_location_id') or self._current_location_id is None:
            logger.error(f"Location ID not set. Skipping forecast data processing for {weather_date}.")
            return False

        location_id = self._current_location_id
        forecast_data_list = raw_data.get('forecast', {}).get('forecastday', [])

        if not forecast_data_list:
            logger.warning(f"No forecast data found in raw JSON for {weather_date}. This might be expected if only current data is provided.")
            return True

        all_forecast_days_processed_successfully = True
        for forecast_day_dict in forecast_data_list:
            forecast_date_str = forecast_day_dict.get('date')
            if not forecast_date_str:
                logger.warning("Forecast day entry without a 'date' key. Skipping this entry.")
                all_forecast_days_processed_successfully = False
                continue

            try:
                existing_forecast_record = self.forecast_ops.get(location_id, forecast_date_str)
                
                if existing_forecast_record is None:
                    insert_successful = self.forecast_ops.insert(location_id, forecast_day_dict)
                    if insert_successful is None:
                        logger.error(f"Failed to insert forecast data for {forecast_date_str} (Location ID: {location_id}).")
                        all_forecast_days_processed_successfully = False
                        continue
                    logger.info(f"Inserted new forecast for location ID {location_id}, date {forecast_date_str}.")
                else:
                    logger.info(f"Forecast data already exists for location ID {location_id} and date {forecast_date_str}.")
                
                nested_data_processed_successfully = True

                try:
                    day_processed = self._handle_day_data(location_id, forecast_date_str, forecast_day_dict.get('day', {}))
                    astro_processed = self._handle_astro_data(location_id, forecast_date_str, forecast_day_dict.get('astro', {}))
                    hour_processed = self._handle_hour_data(location_id, forecast_date_str, forecast_day_dict.get('hour', []))

                    if not (day_processed and astro_processed and hour_processed):
                        nested_data_processed_successfully = False
                        logger.warning(f"Partial success for forecast date {forecast_date_str}. Some nested data (day/astro/hour) failed to insert correctly.")

                except Exception as e:
                    nested_data_processed_successfully = False
                    logger.exception(f"An unexpected error occurred while processing nested data for forecast date {forecast_date_str}: {e}")

                if not nested_data_processed_successfully:
                    all_forecast_days_processed_successfully = False

            except Exception as e:
                logger.exception(f"An unexpected error occurred processing forecast day {forecast_date_str} for location {location_id}: {e}")
                all_forecast_days_processed_successfully = False

        return all_forecast_days_processed_successfully

    def _handle_day_data(self, location_id: int, date: str, day_data: Dict[str, Any]) -> bool:
        """
        Handles the processing and storage of daily weather data.

        Processes the 'day' data, including the associated weather 'condition'.
        Inserts a new day entry if one doesn't exist for the given location and date.

        :param location_id: The ID of the associated location.
        :type location_id: int
        :param date: The date string (YYYY-MM-DD) for this day's data.
        :type date: str
        :param day_data: The dictionary containing the day's weather details.
        :type day_data: Dict[str, Any]
        :returns: ``True`` if day data is successfully handled, ``False`` otherwise.
        :rtype: bool
        """
        if not day_data:
            logger.warning(f"Day data unavailable for {date}. Proceeding as if successful.")
            return True

        try:
            condition_dict = day_data.get('condition', {})
            if condition_dict and condition_dict.get('code') is not None:
                if self.condition_ops.get(condition_dict.get('code')) is None:
                    condition_inserted_code = self.condition_ops.insert(condition_dict)
                    if condition_inserted_code is None:
                        logger.error(f"Failed to insert condition data for code: {condition_dict.get('code')}. This will impact day data insertion.")
                        return False 
                else:
                    logger.info(f"Condition data already exists for code: {condition_dict.get('code')}.")
            else:
                logger.warning(f"Condition data is missing 'code' or is empty for {date}. Proceeding without condition insertion.")

            if self.day_ops.get(location_id, date) is None:
                if not self.day_ops.insert(location_id, date, day_data):
                    logger.error(f"Failed to insert day data for {date} (Location ID: {location_id}).")
                    return False
                logger.info(f"Successfully stored day data for {date}.")
            else:
                logger.info(f"Day data already exists for location ID {location_id} and date {date}.")
            return True
        except Exception as e:
            logger.exception(f"An unexpected error occurred handling day data for {date}: {e}")
            return False

    def _handle_astro_data(self, location_id: int, date: str, astro_data: Dict[str, Any]) -> bool:
        """
        Handles the processing and storage of astronomical data (sunrise, sunset, etc.).

        Inserts new astronomical data for a given date and location if it
        doesn't already exist in the database.

        :param location_id: The ID of the associated location.
        :type location_id: int
        :param date: The date string (YYYY-MM-DD) for this astro data.
        :type date: str
        :param astro_data: The dictionary containing the astronomical details.
        :type astro_data: Dict[str, Any]
        :returns: ``True`` if astro data is successfully handled, ``False`` otherwise.
        :rtype: bool
        """
        if not astro_data:
            logger.warning(f"Astro data unavailable for {date}. Proceeding as if successful.")
            return True

        try:
            if self.astro_ops.get(location_id, date) is None:
                if not self.astro_ops.insert(location_id, date, astro_data):
                    logger.error(f"Failed to insert astro data for {date} (Location ID: {location_id}).")
                    return False
                logger.info(f"Successfully stored astro data for {date}.")
            else:
                logger.info(f"Astro data already exists for location ID {location_id} and date {date}.")
            return True
        except Exception as e:
            logger.exception(f"An unexpected error occurred handling astro data for {date}: {e}")
            return False

    def _handle_hour_data(self, location_id: int, date: str, hour_data_list: List[Dict[str, Any]]) -> bool:
        """
        Handles the processing and storage of hourly weather data.

        Processes a list of hourly data dictionaries for a specific date
        and inserts them into the database. This method assumes that `HourQueries.insert`
        internally handles the existence and insertion of associated condition codes.

        :param location_id: The ID of the associated location.
        :type location_id: int
        :param date: The date string (YYYY-MM-DD) for this hour data.
        :type date: str
        :param hour_data_list: A list of dictionaries, each containing hourly weather details.
        :type hour_data_list: List[Dict[str, Any]]
        :returns: ``True`` if hourly data is successfully handled, ``False`` otherwise.
        :rtype: bool
        """
        if not hour_data_list:
            logger.warning(f"Hourly data unavailable for {date}. Proceeding as if successful.")
            return True

        try:
            if not self.hour_ops.insert(location_id, date, hour_data_list):
                logger.error(f"Failed to insert hourly data for {date} (Location ID: {location_id}).")
                return False
            logger.info(f"Successfully stored hourly data for {date}.")
            return True
        except Exception as e:
            logger.exception(f"An unexpected error occurred handling hourly data for {date}: {e}")
            return False

    def _validate_json_schema(self, data: Dict[str, Any], file_name: str) -> bool:
        """
        Validates the schema of the loaded raw JSON data to ensure it has the expected structure.

        Performs checks for the presence of top-level keys ('location', 'forecast')
        and their essential sub-structures and data types.

        :param data: The dictionary loaded from the JSON file.
        :type data: Dict[str, Any]
        :param file_name: The name of the file being processed, for logging.
        :type file_name: str
        :returns: ``True`` if the schema is valid, ``False`` otherwise.
        :rtype: bool
        """
        required_keys = ['location', 'forecast']
        for key in required_keys:
            if key not in data:
                logger.error(f"Validation failed for {file_name}: Missing top-level key '{key}'.")
                return False

        location_data = data['location']
        if not all(k in location_data for k in ['name', 'lat', 'lon']):
            logger.error(f"Validation failed for {file_name}: 'location' data is incomplete.")
            return False
        if not isinstance(location_data['lat'], (int, float)) or not isinstance(location_data['lon'], (int, float)):
            logger.error(f"Validation failed for {file_name}: 'lat' or 'lon' are not numbers.")
            return False

        forecast_data = data['forecast']
        if 'forecastday' not in forecast_data or not isinstance(forecast_data['forecastday'], list):
            logger.error(f"Validation failed for {file_name}: 'forecastday' is missing or not a list.")
            return False

        if not forecast_data['forecastday']:
            logger.warning(f"Validation warning for {file_name}: 'forecastday' list is empty. This may be acceptable if only current data or no future forecast is provided.")

        for day in forecast_data['forecastday']:
            if not all(k in day for k in ['date', 'day', 'astro', 'hour']):
                logger.error(f"Validation failed for {file_name}: A 'forecastday' entry is incomplete (missing 'date', 'day', 'astro', or 'hour').")
                return False
            if not isinstance(day['hour'], list):
                logger.error(f"Validation failed for {file_name}: 'hour' is not a list in a forecast day entry.")
                return False

        logger.debug(f"Schema validation passed for {file_name}.")
        return True