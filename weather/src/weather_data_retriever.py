# vivarium/weather/src/atmosphere/weather_data_retriever.py
import os
import sys
import json
from typing import Optional, Dict
from pathlib import Path

vivarium_root_path = Path(__file__).resolve().parents[3]
if str(vivarium_root_path) not in sys.path:
    sys.path.insert(0, str(vivarium_root_path))

from utilities.src.logger import LogHelper
from utilities.src.config import FileConfig, WeatherAPIConfig
from weather.src.weather_api_client import WeatherAPIClient

logger = LogHelper.get_logger(__name__)

class WeatherDataRetriever:
    """
    Manages the retrieval of raw weather data.

    Prioritizes reading from local JSON cache. If not found, fetches from API.
    Responsible for creating/updating the local raw JSON file.
    Does NOT interact with the database directly for raw_data_queries; that is delegated
    to the JSONDataLoader.
    """
    def __init__(self, weather_api_config: WeatherAPIConfig, file_config: FileConfig):
        """
        Initializes the WeatherDataRetriever.

        :param weather_api_config: An instance of :class:`utilities.src.config.WeatherAPIConfig` for API client setup.
        :type weather_api_config: :class:`utilities.src.config.WeatherAPIConfig`
        :param file_config: An instance of :class:`utilities.src.config.FileConfig` for file path configurations.
        :type file_config: :class:`utilities.src.config.FileConfig`
        """
        self.weather_api_config = weather_api_config
        self.file_config = file_config
        self.weather_client = WeatherAPIClient(self.weather_api_config)
        self.raw_files_dir: Path = Path(self.file_config.absolute_path) / self.file_config.json_folder
        self.raw_files_dir.mkdir(parents=True, exist_ok=True)
        logger.debug("WeatherDataRetriever initialized.")
    
    def get_local_raw_file_path(self, date: str) -> Path:
        """
        Generates the expected local path for a raw weather data file for a given date.
        This method does NOT check if the file actually exists.

        :param date: The date in 'YYYY-MM-DD' format.
        :type date: str
        :returns: The Path object for the expected raw data file.
        :rtype: :class:`pathlib.Path`
        """
        return self.raw_files_dir / f"{date}.json"
    
    def read_local_raw_file_content(self, raw_file_path: Path) -> Optional[dict]:
        """
        Reads the content of a local raw JSON file and returns it as a dictionary.

        :param raw_file_path: The path to the local raw JSON file.
        :type raw_file_path: :class:`pathlib.Path`
        :returns: The content of the file as a dictionary, or ``None`` if reading fails or file is empty.
        :rtype: Optional[dict]
        """
        if not raw_file_path.exists() or raw_file_path.stat().st_size == 0:
            logger.warning(f"Attempted to read empty or non-existent raw file: {raw_file_path}")
            return None
        try:
            with open(raw_file_path, 'r', encoding='utf-8') as infile:
                raw_data = json.load(infile)
            logger.info(f"Successfully read raw data from local file: {raw_file_path}")
            return raw_data
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed to read or decode raw data from {raw_file_path}: {e}")
            return None

    def _save_raw_data_to_local_file(self, date: str, raw_data_dict: dict) -> Optional[Path]:
        """
        Saves a given dictionary of raw weather data to a local JSON file.
        This is an internal helper method used by `fetch_raw_data_from_api`.

        :param date: The date associated with the data (used for filename) in 'YYYY-MM-DD' format.
        :type date: str
        :param raw_data_dict: The raw weather data as a dictionary.
        :type raw_data_dict: dict
        :returns: The :class:`pathlib.Path` object of the saved file, or ``None`` if saving fails.
        :rtype: Optional[:class:`pathlib.Path`]
        """
        file_path = self.get_local_raw_file_path(date)
        try:
            with open(file_path, 'w', encoding='utf-8') as outfile:
                json.dump(raw_data_dict, outfile, indent=5)
            logger.info(f"Successfully saved raw weather data to local file: {file_path}")
            return file_path
        except IOError as e:
            logger.error(f"Failed to save raw weather data to {file_path}: {e}")
            return None
        
    def fetch_raw_data_from_api(self, date: str, location_lat_long: Optional[str] = None) -> Optional[Path]:
        """
        Fetches raw weather data for a specific date from the external API
        and immediately saves it to a local JSON file.

        :param date: The date for which to fetch data in 'YYYY-MM-DD' format.
        :type date: str
        :param location_lat_long: Optional latitude,longitude string to override the default
            configured API location.
        :type location_lat_long: Optional[str]
        :returns: The :class:`pathlib.Path` object of the saved raw JSON file if successful,
            or ``None`` if fetching or saving fails.
        :rtype: Optional[:class:`pathlib.Path`]
        """
        logger.info(f"Attempting to fetch raw weather data for {date} from API.")
        try:
            weather_data_dict = self.weather_client.get_historical_data(date, location_lat_long)
            if weather_data_dict:
                logger.info(f"Successfully fetched raw weather data for {date} from API.")
                # Immediately save the fetched data to a local file
                saved_file_path = self._save_raw_data_to_local_file(date, weather_data_dict)
                return saved_file_path
            else:
                logger.warning(f"No weather data returned from API for {date}.")
                return None
        except Exception as e:
            logger.error(f"An error occurred while fetching data for {date} from API: {e}")
            return None
        
    def retrieve_raw_data(self, date: str, location_lat_long: Optional[str] = None) -> Optional[Path]:
        """
        Retrieves raw weather data for a specific date.
        First checks the local cache, then fetches from the API if not found.

        :param date: The date for which to retrieve data in 'YYYY-MM-DD' format.
        :type date: str
        :param location_latlong: Optional latitude,longitude string to override the default
            configured API location.
        :type location_latlong: Optional[str]
        :returns: The :class:`pathlib.Path` object of the local raw JSON file if found,
            or ``None`` if not found or fetching fails.
        :rtype: Optional[:class:`pathlib.Path`]
        """
        local_file_path = self.get_local_raw_file_path(date)
        
        # Check if the local file exists and is valid
        if local_file_path.exists():
            logger.info(f"Found local raw data file for {date}: {local_file_path}")
            return local_file_path
        
        # If not found locally, fetch from API
        logger.info(f"No local raw data file found for {date}. Attempting to fetch from API.")
        return self.fetch_raw_data_from_api(date, location_lat_long)