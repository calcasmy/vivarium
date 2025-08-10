# vivarium/database/data_loader/json_data_loader.py

import json
import re
import shutil
import sys
from pathlib import Path
from typing import Optional, Dict, List, Any

# Ensure the vivarium root path is in sys.path for absolute imports
# Path from current file to vivarium root:
# data_loader/ -> database/ -> vivarium/
vivarium_root_path = Path(__file__).resolve().parents[2]
if str(vivarium_root_path) not in sys.path:
    sys.path.insert(0, str(vivarium_root_path))

from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations
from utilities.src.config import FileConfig
from utilities.src.path_utils import PathUtils

from database.data_loader_ops.data_loader_strategy import DataLoaderStrategy
from database.data_loader_ops.json_processor_ops.weather_json_processor import WeatherJSONProcessor 

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

    def __init__(self, file_config: FileConfig, db_operations: DBOperations):
        """
        Initializes the JSONDataLoader with database configuration and
        sets up necessary query handlers.

        The paths for raw and processed JSON files are derived from :class:`utilities.src.config.FileConfig`.

        :param db_operations: An instance of :class:`utilities.src.db_operations.DBOperations` for database interaction.
        :type db_operations: :class:`utilities.src.db_operations.DBOperations`
        """
        super().__init__()
        self.file_config                        = file_config
        self.database_ops: DBOperations         = db_operations

        # Define paths for raw and processed JSON folders based on the file configuration
        self.raw_json_folder_path: Path = Path(self.file_config.absolute_path) / self.file_config.json_folder
        self.processed_json_folder_path: Path = Path(self.file_config.absolute_path) / self.file_config.processed_json_folder

        # Ensure directories exist
        self.raw_json_folder_path.mkdir(parents=True, exist_ok=True)
        self.processed_json_folder_path.mkdir(parents=True, exist_ok=True)
        
        self.weather_json_processor = WeatherJSONProcessor()

        self.raw_data_ops:  RawDataQueries = RawDataQueries(self.database_ops)
        self.location_ops:  LocationQueries = LocationQueries(self.database_ops)
        self.forecast_ops:  ForecastQueries = ForecastQueries(self.database_ops)
        self.day_ops:       DayQueries = DayQueries(self.database_ops)
        self.astro_ops:     AstroQueries = AstroQueries(self.database_ops)
        self.condition_ops: ConditionQueries = ConditionQueries(self.database_ops)
        self.hour_ops:      HourQueries = HourQueries(self.database_ops)

        logger.info(f"JSONDataLoader initialized. Raw JSON files folder: {self.raw_json_folder_path}")
        logger.info(f"Processed JSON files will be stored in: {self.processed_json_folder_path}")

    def execute_data_load(self, file_path: Optional[str] = None, **kwargs: Any) -> bool:
        """
        Orchestrates the data loading process for JSON data.

        If a ``file_path`` is provided, it attempts to load that single JSON file.
        Otherwise, it loads all JSON files from the configured raw JSON folder.

        :param file_path: Optional. The absolute path to a specific JSON file to load.
                          If ``None``, all JSON files in the raw folder will be processed.
        :type file_path: Optional[str]
        :param kwargs: Additional keyword arguments (ignored by this loader).
        :type kwargs: Any
        :returns: ``True`` if the JSON data loading is successful, ``False`` otherwise.
        :rtype: bool
        """
        logger.info("Executing data load using JSONDataLoader strategy.")

        load_success = False
        if file_path:
            single_file_path = Path(file_path)
            if not single_file_path.is_absolute():
                logger.error(f"Provided file_path '{file_path}' is not an absolute path. Cannot process single file.")
                return False
            if not single_file_path.exists():
                logger.error(f"Specified single file does not exist: {single_file_path}. Cannot process.")
                return False
            if not single_file_path.is_file():
                logger.error(f"Provided file_path '{file_path}' is not a file. Cannot process.")
                return False

            if single_file_path.parent != self.raw_json_folder_path:
                logger.warning(f"Single file '{single_file_path}' is not in the configured raw JSON folder '{self.raw_json_folder_path}'. "
                                "It will still be processed, but consider moving it there first for consistency or adjusting configuration.")

            load_success = self._process_and_load_single_file(single_file_path)
        else:
            load_success = self._load_json_data_from_folder()

        if load_success:
            logger.info("JSON data load completed successfully.")
        else:
            logger.error("JSON data load failed.")

        return load_success
    
    def process_raw_data_file(self, raw_file_path: Path) -> bool:
        """
        Initiates the processing of a single raw JSON file, which has typically
        just been fetched and saved by a data retriever.

        This method provides a public entry point for processing a specific file
        without needing to iterate through the entire raw data folder. It is intended
        to be called by external modules (e.g., a data orchestration pipeline)
        that need to process a specific, known file.

        :param raw_file_path: The absolute path to the raw JSON file to be processed.
        :type raw_file_path: :class:`pathlib.Path`
        :returns: ``True`` if the file is processed successfully, ``False`` otherwise.
        :rtype: bool
        """
        logger.info(f"Public API call to process a single raw file: {raw_file_path}")
        if not isinstance(raw_file_path, Path) or not raw_file_path.is_file():
            logger.error(f"Provided path is not a valid file: {raw_file_path}")
            return False
        
        return self._process_and_load_single_file(raw_file_path)

# -- PRIVATE HELPER METHODS --

    def _process_and_load_single_file(self, file_path: Path) -> bool:
        """
        Internal method to read a single JSON file and initiate the data processing
        and loading into the database.

        Delegates file reading and validation to :class:`~database.data_loader_ops.json_processor_ops.weather_json_processor.WeatherJSONProcessor`
        and database insertion to :meth:`_process_and_store_data_from_dict`.

        :param file_path: The absolute path to the JSON file to be processed.
        :type file_path: :class:`pathlib.Path`
        :returns: ``True`` if the file is successfully read and its data loaded, ``False`` otherwise.
        :rtype: bool
        """
        logger.debug(f"Attempting to read and process single file: {file_path.name}")
        
        # Use the processor to read and validate the file content
        raw_data_dict = self.weather_json_processor.process_file(file_path = file_path)
        
        if raw_data_dict is None:
            logger.error(f"Failed to read or validate raw data from file: {file_path.name}. Skipping.")
            return False

        # Extract the date from the filename to pass to the processing method
        match = re.match(r'(\d{4}-\d{2}-\d{2})\.json', file_path.name)
        date_str = match.group(1) if match else "UNKNOWN_DATE"

        # Delegate the actual processing and storage to the new helper method
        processed_file_path = self._process_and_store_data_from_dict(
            raw_data_dict=raw_data_dict,
            date_str=date_str,
            source_raw_file_path=file_path # Pass the original file path for context
        )
        return processed_file_path is not None

    # def _process_and_load_single_file(self, file_path: Path) -> bool:
    #     """
    #     Internal method to process a single JSON file and load its data into the database.

    #     Delegates file reading, validation, and in-memory transformation to
    #     :class:`database.data_loader.json_processing.weather_json_processor.WeatherJSONProcessor`.
    #     Handles database insertion for all related entities (raw data, location, forecast,
    #     day, astro, hour) within a transaction. Upon successful database operations, the file
    #     is moved to the processed folder.

    #     :param file_path: The absolute path to the JSON file to be processed.
    #     :type file_path: :class:`pathlib.Path`
    #     :returns: ``True`` if the file is successfully processed and data loaded, ``False`` otherwise.
    #     :rtype: bool
    #     """
    #     logger.debug(f"Attempting to process and load single file: {file_path.name}")

    #     processed_data_dict = self.weather_json_processor.process_file(file_path)

    #     if processed_data_dict is None:
    #         logger.error(f"Failed to process or validate data from {file_path.name}. Skipping database insertion and file saving.")
    #         return False

    #     match = re.match(r'(\d{4}-\d{2}-\d{2})\.json', file_path.name)
    #     weather_date = match.group(1) if match else "UNKNOWN_DATE" # Should always match due to processor validation

    #     current_file_db_success = False

    #     try:
    #         self.database_ops.begin_transaction()

    #         raw_data_json_str = json.dumps(processed_data_dict)
    #         if not self.raw_data_ops.insert(date=weather_date, raw_data=raw_data_json_str):
    #             logger.error(f"Failed to insert/update raw climate data for {weather_date} from {file_path.name}.")
    #         else:
    #             logger.info(f"Successfully inserted/updated raw climate data for {weather_date}.")
                
    #             location_processed = self._handle_location_data(processed_data_dict, weather_date)
    #             forecast_processed = self._handle_forecast_data(processed_data_dict, weather_date)

    #             if location_processed and forecast_processed:
    #                 current_file_db_success = True
    #                 logger.info(f"All structured data (location/forecast/day/astro/hour) from {file_path.name} successfully persisted.")
    #             else:
    #                 logger.error(f"Failed to fully process and persist structured data (location/forecast/day/astro/hour) from {file_path.name}. See previous logs for details.")

    #         if current_file_db_success:
    #             self.database_ops.commit_transaction()
    #             processed_file_path = self.processed_json_folder_path / file_path.name
    #             logger.info(f"All DB operations successful for {file_path.name}. Moving original JSON file to: {processed_file_path}")
    #             shutil.move(file_path, processed_file_path)
    #             logger.info(f"Original JSON file moved to processed folder: {processed_file_path}")
    #         else:
    #             self.database_ops.rollback_transaction()
    #             logger.warning(f"Database insertion failed for {file_path.name}. Transaction rolled back. Original JSON file WILL NOT be moved.")
            
    #         return current_file_db_success

    #     except Exception as e:
    #         self.database_ops.rollback_transaction()
    #         logger.exception(f"An unexpected error occurred during database transaction for {file_path.name}: {e}")
    #         return False
    #     finally:
    #         logger.info(f"Finished processing cycle for {file_path.name}. Success for this file: {current_file_db_success}")

    def _load_json_data_from_folder(self) -> bool:
        """
        Loads local JSON files from `self.raw_json_folder_path` into the database.

        Processes each JSON file found in the designated raw data folder.
        Each file is processed as a single transaction unit, meaning if any part of
        its database insertion fails, that file's changes are rolled back, and the
        file is not moved to the processed folder.

        :returns: ``True`` if all files are processed successfully, ``False`` otherwise.
        :rtype: bool
        """
        if self.database_ops is None:
            logger.error("DatabaseOperations instance not set. Cannot load JSON data from folder.")
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
                if original_file_path.name.startswith('.') or original_file_path.name.startswith('._'):
                    logger.debug(f"Skipping hidden file: {original_file_path.name}")
                    continue
                if self._process_and_load_single_file(original_file_path):
                    processed_count += 1
                else:
                    overall_success = False
                    logger.warning(f"Failed to process {original_file_path.name}. Continuing with other files if any.")

        except Exception as e:
            logger.error(f"Error accessing JSON data directory {self.raw_json_folder_path}: {e}", exc_info=True)
            overall_success = False

        logger.info(f"JSON data loading from folder completed. Successfully processed {processed_count} files. Overall success: {overall_success}")
        return overall_success

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
        and inserts them into the database. This method assumes that :meth:`~database.climate_data_ops.hour_queries.HourQueries.insert`
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
        
    def _process_and_store_data_from_dict(self, raw_data_dict: Dict[str, Any], date_str: str, source_raw_file_path: Optional[Path] = None) -> Optional[Path]:
        """
        Processes a raw weather data dictionary, loads it into the database,
        and, if successful, creates a corresponding *processed* JSON file. # Docstring updated

        This method encapsulates the core logic for database insertion and file management
        after data has been parsed from a raw source.

        :param raw_data_dict: The raw weather data as a dictionary (parsed from API response or file).
        :type raw_data_dict: Dict[str, Any]
        :param date_str: The date string (YYYY-MM-DD) associated with the data.
        :type date_str: str
        :param source_raw_file_path: Optional. The path to the original raw JSON file from which
            this data was obtained. (This parameter is kept but its usage for moving is removed)
        :type source_raw_file_path: Optional[:class:`pathlib.Path`]
        :returns: The :class:`pathlib.Path` object of the newly created *processed* JSON file,
            or ``None`` if any processing or database operation fails.
        :rtype: Optional[:class:`pathlib.Path`]
        """
        logger.debug(f"Attempting to process and store data for date: {date_str}")
        
        current_data_db_success = False
        processed_file_path: Optional[Path] = None

        try:
            self.database_ops.begin_transaction()

            raw_data_json_str = json.dumps(raw_data_dict)
            if not self.raw_data_ops.insert(date=date_str, raw_data=raw_data_json_str):
                logger.error(f"Failed to insert/update raw climate data for {date_str}.")
            else:
                logger.info(f"Successfully inserted/updated raw climate data for {date_str}.")
                
                location_processed = self._handle_location_data(raw_data_dict, date_str)
                forecast_processed = self._handle_forecast_data(raw_data_dict, date_str)

                if location_processed and forecast_processed:
                    current_data_db_success = True
                    logger.info(f"All structured data (location/forecast/day/astro/hour) for {date_str} successfully persisted.")
                else:
                    logger.error(f"Failed to fully process and persist structured data (location/forecast/day/astro/hour) for {date_str}. See previous logs for details.")

            if current_data_db_success:
                self.database_ops.commit_transaction()
                logger.info(f"All DB operations successful for {date_str}. Attempting to save processed file.")
                
                # Create the processed JSON file
                processed_json_name = f"{date_str}_processed.json"
                processed_file_path = self.processed_json_folder_path / processed_json_name
                try:
                    with open(processed_file_path, 'w', encoding='utf-8') as outfile:
                        json.dump(raw_data_dict, outfile, indent=5)
                    logger.info(f"Processed JSON data saved to: {processed_file_path}")
                except IOError as e:
                    logger.error(f"Failed to save processed JSON data for {date_str} to {processed_file_path}: {e}")
                    processed_file_path = None # Mark as failure if processed file couldn't be saved                
            else:
                self.database_ops.rollback_transaction()
                logger.warning(f"Database insertion failed for {date_str}. Transaction rolled back.") # Removed mention of file not being moved
            
            return processed_file_path

        except Exception as e:
            self.database_ops.rollback_transaction()
            logger.exception(f"An unexpected error occurred during database transaction for {date_str}: {e}")
            return None