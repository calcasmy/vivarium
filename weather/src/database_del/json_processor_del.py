# vivarium/weather/src/database/json_processor.py
"""
A helper class for processing and storing a single climate weather data JSON.

This module provides functionalities to validate, round coordinates, and insert
various segments of climate data (location, forecast, daily, hourly, astronomical,
and condition data) into the database, utilizing dedicated query handlers.
"""
import os
import re
import sys
import json
from typing import Optional, Dict, List, Any

# Ensure project root is in path for imports
if __name__ == "__main__":
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations, ConnectionDetails

from weather.src.database_del.location_queries_del import LocationQueries
from weather.src.database_del.raw_data_queries_del import RawDataQueries
from weather.src.database_del.forecast_queries_del import ForecastQueries
from weather.src.database_del.day_queries_del import DayQueries
from weather.src.database_del.astro_queries_del import AstroQueries
from weather.src.database_del.condition_queries_del import ConditionQueries
from weather.src.database_del.hour_queries_del import HourQueries

logger = LogHelper.get_logger(__name__)


class JSONProcessor:
    """
    Processes and stores a single climate weather data JSON into the database.

    This class handles schema validation, coordinate rounding, and orchestrates
    the insertion of raw JSON data and its nested components into various
    database tables.
    """

    def __init__(self, db_operations: DBOperations):
        """
        Initializes the ClimateDataProcessor with a DBOperations instance.

        :param db_operations: An initialized instance of :class:`DBOperations`
                              for all database interactions.
        :type db_operations: DBOperations
        """
        self.db_ops = db_operations
        self._current_location_id: Optional[int] = None  # To hold location_id during processing
        
        # Initialize all database query handlers with the same DBOperations instance
        self.raw_data_db = RawDataQueries(self.db_ops)
        self.location_db = LocationQueries(self.db_ops)
        self.forecast_db = ForecastQueries(self.db_ops)
        self.day_db = DayQueries(self.db_ops)
        self.astro_db = AstroQueries(self.db_ops)
        self.condition_db = ConditionQueries(self.db_ops)
        self.hour_db = HourQueries(self.db_ops)
        logger.debug("ClimateDataProcessor initialized with database query handlers.")

    def process_json_file(self, file_path: str) -> bool:
        """
        Processes a single raw JSON weather data file from a given path.

        This method reads the file, extracts the date from the filename, and then
        calls the internal helper to process the parsed JSON dictionary.

        :param file_path: The absolute path to the JSON file to process.
        :type file_path: str
        :returns: `True` if the file is processed and data stored successfully, `False` otherwise.
        :rtype: bool
        """
        file_name = os.path.basename(file_path)
        logger.debug(f"Parsing and loading data from file: {file_name}")

        try:
            match = re.match(r'(\d{4}-\d{2}-\d{2})\.json', file_name)
            if not match:
                logger.warning(f"Skipping '{file_name}': Filename does not match 'YYYY-MM-DD.json' format.")
                return False

            weather_date = match.group(1)

            with open(file_path, 'r', encoding='utf-8') as f:
                raw_data_dict = json.load(f)

            # Pass the parsed dictionary and filename for context
            return self.process_json_data(raw_data_dict, weather_date, file_path)

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from {file_path}: {e}", exc_info=True)
            return False
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. This should be caught by calling code.", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"An unexpected error occurred while reading file {file_path}: {e}", exc_info=True)
            return False

    def process_json_data(self, raw_data_dict: Dict[str, Any], weather_date: str, source_name: str = "API_Fetch") -> bool:
        """
        Processes a single raw weather data dictionary (e.g., from API or a loaded JSON file).

        This method performs schema validation, rounds location coordinates, inserts
        the raw JSON into the database, and then delegates to handlers for nested data.

        :param raw_data_dict: The dictionary containing the full raw weather API response.
        :type raw_data_dict: Dict[str, Any]
        :param weather_date: The date string (YYYY-MM-DD) associated with the data being processed.
        :type weather_date: str
        :param source_name: An optional name indicating the source of the data (e.g., filename, "API_Fetch").
                            Used for logging context.
        :type source_name: str
        :returns: `True` if the data is processed and stored successfully, `False` otherwise.
        :rtype: bool
        """
        logger.debug(f"Processing data from source: {source_name} for date: {weather_date}")

        try:
            if not self._validate_json_schema(data=raw_data_dict, source_name=source_name):
                logger.error(f"Schema validation failed for data from {source_name}. Aborting processing.")
                return False

            # Round coordinates in the dictionary in place.
            # This method also handles updating the source file if it came from a file.
            if not self._round_location_coordinates(raw_data_dict, source_name):
                # If rounding failed to update a file, it's a critical error.
                logger.error(f"Failed to ensure rounded coordinates for data from {source_name}. Aborting processing.")
                return False

            # Convert the (potentially modified) dictionary back to JSON string for raw storage.
            raw_data_json_str = json.dumps(raw_data_dict)
            raw_data_inserted = self.raw_data_db.insert_raw_data(date=weather_date, raw_data=raw_data_json_str)

            if not raw_data_inserted:
                logger.error(f"Failed to insert/update raw climate data for {weather_date} from {source_name}.")
                return False

            logger.info(f"Successfully inserted/updated raw climate data for {weather_date} from {source_name}.")

            # Process nested data components.
            location_processed = self._handle_location_data(raw_data_dict, weather_date)
            forecast_processed = self._handle_forecast_data(raw_data_dict, weather_date)

            return location_processed and forecast_processed

        except Exception as e:
            logger.exception(f"An unexpected error occurred while processing data from {source_name}: {e}")
            return False

    def _handle_location_data(self, raw_data: Dict[str, Any], weather_date: str) -> bool:
        """
        Handles the retrieval or insertion of location data from the raw weather dictionary.

        It checks if a location with the same latitude and longitude already exists in the
        `climate_location` table. If not, it inserts the new location. The method stores the
        retrieved or new location ID in :attr:`self._current_location_id` for use by
        subsequent data handlers for the current data processing.

        :param raw_data: The dictionary containing the full raw weather API response.
        :type raw_data: Dict[str, Any]
        :param weather_date: The date string (YYYY-MM-DD) associated with the data.
        :type weather_date: str
        :returns: `True` if location data is successfully handled, `False` otherwise.
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
                logger.error(
                    f"Latitude or Longitude is missing in raw data for {weather_date}. "
                    "Cannot process location data."
                )
                return False

            # Attempt to get existing location ID
            location_id = self.location_db.get_location_id(latitude, longitude)
            if not location_id:
                # If not found, insert new location data.
                location_id = self.location_db.insert_location_data(location_data)
                if location_id is None:
                    logger.error(f"Failed to insert location data for {weather_date}.")
                    return False
                logger.info(f"Inserted new location with ID: {location_id} for {weather_date}.")
            else:
                logger.info(f"Location already exists with ID: {location_id} for {weather_date}.")
            
            # Store the current location ID for subsequent methods in the same file processing.
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
        :returns: `True` if all forecast days and their nested data are processed successfully, `False` otherwise.
        :rtype: bool
        """
        # Ensure _current_location_id has been set by _handle_location_data.
        if not hasattr(self, '_current_location_id') or self._current_location_id is None:
            logger.error(f"Location ID not set. Skipping forecast data processing for {weather_date}.")
            return False
            
        location_id = self._current_location_id
        forecast_data = raw_data.get('forecast', {}).get('forecastday', [])

        if not forecast_data:
            logger.warning(f"No forecast data found in raw JSON for {weather_date}. Skipping forecast processing.")
            return True # No forecast data is not necessarily a failure.

        all_forecast_days_processed_successfully = True
        for forecast_day_dict in forecast_data:
            forecast_date = forecast_day_dict.get('date')
            if not forecast_date:
                logger.warning("Forecast day entry without a 'date' key. Skipping this entry.")
                all_forecast_days_processed_successfully = False
                continue

            try:
                # Check if forecast entry already exists for this location and date.
                existing_forecast_id = self.forecast_db.get_forecast_by_location_and_date(location_id, forecast_date)
                if not existing_forecast_id:
                    # Insert new forecast data if it doesn't exist.
                    forecast_id = self.forecast_db.insert_forecast_data(location_id, forecast_day_dict)
                    if not forecast_id:
                        logger.error(f"Failed to insert forecast data for {forecast_date} (Location ID: {location_id}).")
                        all_forecast_days_processed_successfully = False
                        continue
                    logger.info(f"Inserted new forecast with ID: {forecast_id} for {forecast_date}.")
                else:
                    forecast_id = existing_forecast_id
                    logger.info(f"Forecast data already exists for location ID {location_id} and date {forecast_date}.")
                
                # Proceed to process nested daily, astronomical, and hourly data if forecast_id is valid.
                if forecast_id:
                    nested_data_processed_successfully = True

                    try:
                        day_processed = self._handle_day_data(location_id, forecast_date, forecast_day_dict.get('day', {}))
                        astro_processed = self._handle_astro_data(location_id, forecast_date, forecast_day_dict.get('astro', {}))
                        hour_processed = self._handle_hour_data(location_id, forecast_date, forecast_day_dict.get('hour', []))

                        if not (day_processed and astro_processed and hour_processed):
                            nested_data_processed_successfully = False
                            logger.warning(
                                f"Partial success for forecast date {forecast_date}. Some nested data "
                                "(day/astro/hour) failed to insert correctly."
                            )

                    except Exception as e:
                        nested_data_processed_successfully = False
                        logger.exception(
                            f"An unexpected error occurred while processing nested data for forecast date {forecast_date}: {e}"
                        )

                    if not nested_data_processed_successfully:
                        all_forecast_days_processed_successfully = False
                else:
                    # Should not happen if previous steps inserted or found an ID, but defensive check.
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
        :returns: `True` if day data is successfully handled, `False` otherwise.
        :rtype: bool
        """
        if not day_data:
            logger.warning(f"Day data unavailable for {date}. Skipping day data handling.")
            return True # No day data is not necessarily a failure.

        try:
            condition_dict = day_data.get('condition', {})
            if condition_dict and condition_dict.get('code') is not None:
                # Check and insert condition if it doesn't already exist.
                if not self.condition_db.get_condition(condition_dict.get('code')):
                    condition_inserted = self.condition_db.insert_condition(condition_dict)
                    if condition_inserted:
                        logger.info(f"Successfully stored condition data for code: {condition_dict.get('code')}.")
                    else:
                        logger.error(f"Failed to insert condition data for code: {condition_dict.get('code')}.")
                        return False # Fail if condition insertion fails.
                else:
                    logger.info(f"Condition data already exists for code: {condition_dict.get('code')}.")
            else:
                logger.warning(f"Condition data is missing 'code' or is empty for {date}. Skipping condition handling.")

            # Insert day data if it doesn't already exist for the location and date.
            if not self.day_db.get_day_data(location_id, date):
                self.day_db.insert_day_data(location_id, date, day_data)
                logger.info(f"Successfully stored day data for {date}.")
                return True
            else:
                logger.info(f"Day data already exists for location ID {location_id} and date {date}.")
                return True # Already exists is not a failure.

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
        :returns: `True` if astro data is successfully handled, `False` otherwise.
        :rtype: bool
        """
        if not astro_data:
            logger.warning(f"Astro data unavailable for {date}. Skipping astro data handling.")
            return True # No astro data is not necessarily a failure.

        try:
            if not self.astro_db.get_astro_data(location_id, date):
                self.astro_db.insert_astro_data(location_id, date, astro_data)
                logger.info(f"Successfully stored astro data for {date}.")
                return True
            else:
                logger.info(f"Astro data already exists for location ID {location_id} and date {date}.")
                return True # Already exists is not a failure.
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
        :returns: `True` if hourly data is successfully handled, `False` otherwise.
        :rtype: bool
        """
        if not hour_data:
            logger.warning(f"Hourly data unavailable for {date}. Skipping hourly data handling.")
            return True # No hourly data is not necessarily a failure.

        try:
            # The insert_hour_data method likely handles iterating and inserting each hour.
            self.hour_db.insert_hour_data(location_id, date, hour_data)
            logger.info(f"Successfully stored hourly data for {date}.")
            return True
        except Exception as e:
            logger.exception(f"An unexpected error occurred handling hourly data for {date}: {e}")
            return False

    def _round_location_coordinates(self, raw_data_dict: Dict[str, Any], source_name: str) -> bool:
        """
        Checks if location coordinates need rounding and updates the dictionary and source file.

        This method inspects the 'lat' and 'lon' values in the loaded JSON data.
        If a coordinate has more than two decimal places of precision, it rounds
        the value to two decimal places. If a change is made and the data originated
        from a file, the JSON file on disk is updated with the new, rounded values.
        This ensures consistency for primary key lookups (latitude, longitude)
        where precision might differ across API calls or file sources.

        :param raw_data_dict: The dictionary loaded from the JSON source. This dictionary
                              will be modified in place if rounding occurs.
        :type raw_data_dict: Dict[str, Any]
        :param source_name: The name of the source (e.g., file path, "API_Fetch").
                            Used for logging and to determine if a file update is needed.
        :type source_name: str
        :returns: `True` if the coordinates were processed (rounded or not) and file
                  updated (if needed) successfully, `False` otherwise.
        :rtype: bool
        """
        location_data = raw_data_dict.get('location', {})
        lat = location_data.get('lat')
        lon = location_data.get('lon')
        updated_in_memory = False

        # Round latitude if it's a number and has more than 2 decimal places in its string representation.
        if lat is not None and isinstance(lat, (int, float)):
            rounded_lat = round(lat, 2)
            # Compare string representations to avoid floating-point precision issues in comparison.
            if f'{lat:.10f}' != f'{rounded_lat:.10f}':
                location_data['lat'] = rounded_lat
                updated_in_memory = True
                logger.warning(f"Latitude {lat} rounded to {rounded_lat} in data from {source_name}.")

        # Round longitude if it's a number and has more than 2 decimal places in its string representation.
        if lon is not None and isinstance(lon, (int, float)):
            rounded_lon = round(lon, 2)
            if f'{lon:.10f}' != f'{rounded_lon:.10f}':
                location_data['lon'] = rounded_lon
                updated_in_memory = True
                logger.warning(f"Longitude {lon} rounded to {rounded_lon} in data from {source_name}.")

        if updated_in_memory:
            # Only attempt to write back to file if source_name is likely a file path.
            if os.path.exists(source_name) and os.path.isfile(source_name):
                try:
                    # Write the modified dictionary back to the file.
                    with open(source_name, 'w', encoding='utf-8') as f:
                        json.dump(raw_data_dict, f, indent=4)
                    logger.info(f"Updated JSON file {os.path.basename(source_name)} with rounded coordinates.")
                    return True # Successfully updated file.
                except Exception as e:
                    logger.error(
                        f"Failed to update JSON file {os.path.basename(source_name)} with rounded coordinates: {e}",
                        exc_info=True
                    )
                    return False # Failed to update file.
            else:
                logger.debug(f"Coordinates rounded in memory for {source_name}, but not writing back (not a file source).")
                return True # Changes were made in memory, which is sufficient if not a file.
        else:
            return True # No update needed, so considered successful.
    
    def _validate_json_schema(self, data: Dict[str, Any], source_name: str) -> bool:
        """
        Validates the schema of the loaded raw JSON data to ensure it has the expected structure.

        This method performs checks for the presence of required top-level keys
        ('location', 'current', 'forecast') and validates the structure and types
        of critical nested elements, particularly within 'location' and 'forecastday'.

        :param data: The dictionary loaded from the JSON source.
        :type data: Dict[str, Any]
        :param source_name: The name of the source (e.g., file path, "API_Fetch"), for logging.
        :type source_name: str
        :returns: `True` if the schema is valid, `False` otherwise.
        :rtype: bool
        """
        # Check for top-level required keys.
        required_keys = ['location', 'forecast']
        for key in required_keys:
            if key not in data:
                logger.error(f"Schema validation failed for {source_name}: Missing top-level key '{key}'.")
                return False

        # Check 'location' data structure and types.
        location_data = data['location']
        if not all(k in location_data for k in ['name', 'lat', 'lon']):
            logger.error(f"Schema validation failed for {source_name}: 'location' data is incomplete (missing name, lat, or lon).")
            return False
        if not isinstance(location_data['lat'], (int, float)) or not isinstance(location_data['lon'], (int, float)):
            logger.error(f"Schema validation failed for {source_name}: 'lat' or 'lon' in 'location' are not numbers.")
            return False
            
        # Check 'forecast' data structure and types.
        forecast_data = data['forecast']
        if 'forecastday' not in forecast_data or not isinstance(forecast_data['forecastday'], list):
            logger.error(f"Schema validation failed for {source_name}: 'forecastday' is missing or not a list under 'forecast'.")
            return False

        # Check each 'forecastday' entry within the forecast list.
        if not forecast_data['forecastday']:
            logger.warning(f"Schema validation warning for {source_name}: 'forecastday' list is empty. This may be expected for some data sets.")
            # Continue processing, as an empty forecast is not a critical error.
        
        for day in forecast_data['forecastday']:
            if not all(k in day for k in ['date', 'day', 'astro', 'hour']):
                logger.error(f"Schema validation failed for {source_name}: A 'forecastday' entry is incomplete (missing date, day, astro, or hour).")
                return False
            if not isinstance(day['hour'], list):
                logger.error(f"Schema validation failed for {source_name}: 'hour' is not a list in a 'forecastday' entry.")
                return False

        logger.debug(f"Schema validation passed for {source_name}.")
        return True