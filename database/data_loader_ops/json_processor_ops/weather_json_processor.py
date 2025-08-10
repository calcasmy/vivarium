# vivarium/database/data_loader/json_processing/weather_json_processor.py

import json
import re
import sys
from pathlib import Path
from typing import Optional, Dict, Any

# Ensure the vivarium root path is in sys.path for absolute imports
# Path from current file to vivarium root:
# json_processing/ -> data_loader/ -> database/ -> vivarium/
vivarium_root_path = Path(__file__).resolve().parents[3]
if str(vivarium_root_path) not in sys.path:
    sys.path.insert(0, str(vivarium_root_path))

from utilities.src.logger import LogHelper

logger = LogHelper.get_logger(__name__)


class WeatherJSONProcessor:
    """
    A utility class for validating, cleaning, and preparing raw weather JSON data.

    This includes filename validation, JSON schema validation, and in-memory data cleaning
    such as rounding location coordinates.
    """

    def __init__(self):
        """
        Initializes the WeatherJSONProcessor.
        """
        logger.debug("WeatherJSONProcessor: Initialized.")

    def process_file(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """
        Reads a JSON file, validates its filename and schema, and performs
        in-memory coordinate rounding.

        :param file_path: The absolute path to the JSON file to process.
        :type file_path: :class:`pathlib.Path`
        :returns: The modified dictionary if successful, ``None`` otherwise.
        :rtype: Optional[Dict[str, Any]]
        """
        file_name = file_path.name

        try:
            if not re.match(r'^\d{4}-\d{2}-\d{2}\.json$', file_name):
                logger.warning(f"Skipping '{file_name}': Filename does not match 'YYYY-MM-DD.json' format.")
                return None

            with open(file_path, 'r', encoding='utf-8') as f:
                raw_data_dict = json.load(f)

            if not self._validate_json_schema(data=raw_data_dict, file_name=file_name):
                logger.error(f"Schema validation failed for file {file_name}. Skipping processing.")
                return None

            modified_data_dict = self._round_location_coordinates_in_memory(raw_data_dict, file_name)
            logger.debug(f"JSON file {file_name} processed in memory.")
            return modified_data_dict

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from file {file_path}: {e}", exc_info=True)
            return None
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}.", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred during processing of {file_path}: {e}", exc_info=True)
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
            if f'{lat:.10f}' != f'{rounded_lat:.10f}': # Only log if actual rounding changes the value
                location_data['lat'] = rounded_lat
                logger.warning(f"Latitude {lat} rounded to {rounded_lat} in data from {file_name}.")

        if lon is not None and isinstance(lon, (int, float)):
            rounded_lon = round(lon, 2)
            if f'{lon:.10f}' != f'{rounded_lon:.10f}': # Only log if actual rounding changes the value
                location_data['lon'] = rounded_lon
                logger.warning(f"Longitude {lon} rounded to {rounded_lon} in data from {file_name}.")

        return data_dict

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
        required_top_level_keys = ['location', 'forecast']
        for key in required_top_level_keys:
            if key not in data:
                logger.error(f"Schema validation failed for {file_name}: Missing top-level key '{key}'.")
                return False

        location_data = data.get('location', {})
        if not all(k in location_data for k in ['name', 'lat', 'lon']):
            logger.error(f"Schema validation failed for {file_name}: 'location' data is incomplete or malformed.")
            return False
        if not isinstance(location_data['lat'], (int, float)) or not isinstance(location_data['lon'], (int, float)):
            logger.error(f"Schema validation failed for {file_name}: 'lat' or 'lon' in 'location' are not numbers.")
            return False

        forecast_data = data.get('forecast', {})
        if 'forecastday' not in forecast_data or not isinstance(forecast_data['forecastday'], list):
            logger.error(f"Schema validation failed for {file_name}: 'forecastday' is missing or not a list in 'forecast'.")
            return False

        if not forecast_data['forecastday']:
            logger.info(f"Schema validation info for {file_name}: 'forecastday' list is empty. This is acceptable if only current data or no future forecast is expected.")
        else:
            for i, day in enumerate(forecast_data['forecastday']):
                required_day_keys = ['date', 'day', 'astro', 'hour']
                if not all(k in day for k in required_day_keys):
                    logger.error(f"Schema validation failed for {file_name}: A 'forecastday' entry at index {i} is incomplete (missing one of {required_day_keys}).")
                    return False
                if not isinstance(day.get('hour'), list):
                    logger.error(f"Schema validation failed for {file_name}: 'hour' is not a list in 'forecastday' entry at index {i}.")
                    return False
        
        logger.debug(f"Schema validation passed for {file_name}.")
        return True