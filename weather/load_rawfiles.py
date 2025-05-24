# src/fetch_daily_weather.py
import os
import sys
import os.path
import json
import time

from typing import Optional, Dict
from datetime import datetime, timedelta

if __name__ == "__main__":
    vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    if vivarium_path not in sys.path:
        sys.path.insert(0, vivarium_path)

from weather.src.atmosphere.weather_api_client import WeatherAPIClient

from weather.src.database.database_operations import DatabaseOperations
from weather.src.database.location_queries import LocationQueries
from weather.src.database.raw_data_queries import RawDataQueries
from weather.src.database.forecast_queries import ForecastQueries
from weather.src.database.day_queries import DayQueries
from weather.src.database.astro_queries import AstroQueries
from weather.src.database.condition_queries import ConditionQueries
from weather.src.database.hour_queries import HourQueries

from utilities.src.logger import LogHelper
from utilities.src.config import DatabaseConfig, FileConfig
from utilities.src.path_utils import PathUtils

db_config = DatabaseConfig()

logger = LogHelper.get_logger(__name__)

class LoadRawfiels:
    """
    A class to store weather data from JSON files in local folder.
    """

    def __init__(self, db_operations):
        """
        Initializes the WeatherDataFetcher with database and API clients.
        """
        self.db_ops = db_operations

        self.weather_client = WeatherAPIClient()
        self.raw_data_db = RawDataQueries(db_operations)
        self.location_db = LocationQueries(db_operations)
        self.forecast_db = ForecastQueries(db_operations)
        self.day_db = DayQueries(db_operations)
        self.astro_db = AstroQueries(db_operations)
        self.condition_db = ConditionQueries(db_operations)
        self.hour_db = HourQueries(db_operations)
    
    @staticmethod
    def script_path() -> str:
        return os.path.abspath(__file__)
        
    def _perform_action_on_file(self, _file):            
        """
        Placeholder function to define the action to perform on each file.
        You will replace the content of this function with your specific logic.
        """
        file_config = FileConfig()

        try:
            # Example: Print file name and its first line
            print(f"\n--- Processing file: {os.path.basename(_file)} ---")
            with open(_file, 'r', errors='ignore') as infile:
                weather_data = json.load(infile)

            _forecastday = weather_data.get('forecast', {}).get('forecastday', [])
            for forecast_day in _forecastday:
                _date = forecast_day.get('date')
            
            if self.raw_data_db.get_raw_data_by_date(_date):
                logger.info(f"Raw climate data already exists for {_date}. Returning data from database.")
                return
            else:
                logger.info(f"Raw climate data does not exist for {_date}. Adding data to database.")
                if weather_data:
                    raw_data_id = self.raw_data_db.insert_raw_data(_date, weather_data)
                    if raw_data_id:
                        # 1. Handle Location Data
                        location_id = self.handle_location_data(weather_data, _date)

                        # 2. Handle Forecast Data
                        forecast_id = self.handle_forecast_data(weather_data, location_id, _date)
                        logger.info(f"Successfully persisted raw climate data for {_date}.")
                        return None
                    else:
                        logger.error(f"Failed to insert raw climate data for {_date}.")
                        return None
        except UnicodeDecodeError:
            print(f"  Error: Could not decode {os.path.basename(_file)} as UTF-8. It might be a binary file or different encoding.")
        except Exception as e:
            print(f"  An error occurred while processing {os.path.basename(_file)}: {e}")
            
    def handle_location_data(self, weather_data: Dict, date: str) -> Optional[int]:
        """Handles the retrieval or insertion of location data."""

        if not weather_data:
            logger.error(f"No weather data to process for {date}.")
            return
        else:
            try:
                location_data = weather_data.get('location', {})
                latitude = location_data.get('lat')
                longitude = location_data.get('lon')

                if latitude is None or longitude is None:
                    logger.error("Latitude or Longitude is missing. Skipping location and subsequent data processing.")
                    return None

                location_id = self.location_db.get_location_id(latitude, longitude)
                if not location_id:
                    location_id = self.location_db.insert_location_data(location_data)
                    if location_id is None:
                        logger.error("Failed to insert location data.")
                        return None
                    logger.info(f"Inserted new location with ID: {location_id}")
                else:
                    logger.info(f"Location already exists with ID: {location_id}")
                return location_id
            except Exception as e:
                logger.exception(f"An unexpected error occurred: {e}")

    def handle_forecast_data(self, weather_data: Dict, location_id: int, yesterday: str) -> Optional[int]:

        """Handles the processing of forecast data."""
        forecast_data = weather_data.get('forecast', {}).get('forecastday', [])
        if not forecast_data:
            logger.warning(f"No forecast data found for {yesterday}.")
            return

        for forecast_day in forecast_data:
            forecast_date = forecast_day.get('date')
            if not forecast_date:
                logger.warning("Forecast day without date.")
                continue  # Continue to the next forecast_day

            if not self.forecast_db.get_forecast_by_location_and_date(location_id, forecast_date):
                forecast_id = self.forecast_db.insert_forecast_data(location_id, forecast_day)
                if not forecast_id:
                    logger.error(f"Failed to insert forecast data for {forecast_date}.")
                    continue  # Continue to the next forecast_day
                logger.info(f"Successfully stored forecast data for {forecast_date}.")
            else:
                logger.info(f"Forecast data already exists for location ID {location_id} and date {forecast_date}.")

            # 2.1 Handle Day Data
            self.handle_day_data(location_id, forecast_date, forecast_day.get('day', {}))

            # 2.2 Handle Astro Data
            self.handle_astro_data(location_id, forecast_date, forecast_day.get('astro', {}))

            # 2.3
            self.handle_hour_data(location_id, forecast_date, forecast_day.get('hour', []))

    def handle_day_data(self, location_id: int, date: str, day_data: Dict) -> None:
        """Handles the processing of day data."""
        if day_data:
            condition_data = day_data.get('condition', {})
                # Inserting Condition data of the day.
            if condition_data:
                if not self.condition_db.get_condition(day_data.get('condition', {}).get('code')):
                    condition_id = self.condition_db.insert_condition(day_data.get('condition', []))
                    logger.info(f"Successfully stored condition data for {date}.")
                else:
                    logger.info(f"Condition data already exists for location ID {location_id} and date {date}.")
            else:
                logger.warning(f"Condition data is missing 'code' for {date}")

            if not self.day_db.get_day_data(location_id, date):
                self.day_db.insert_day_data(location_id, date, day_data)
                logger.info(f"Successfully stored day data for {date}.")
            else:
                logger.info(f"Day data already exists for location ID {location_id} and date {date}.")
        else:
            logger.warning(f"Day data unavailable for {date}")

    def handle_astro_data(self, location_id: int, date: str, astro_data: Dict) -> None:
        if not self.astro_db.get_astro_data(location_id, date):
            astro_id = self.astro_db.insert_astro_data(location_id, date, astro_data)
            logger.info(f"Successfully stored astro data for {date}.")
        else:
            logger.info(f"Astro data already exists for location ID {location_id} and date {date}.")

    def handle_hour_data(self, location_id: int, date: str, hour_data: Dict) -> None:
        """Handles the processing of hourly data."""
        self.hour_db.insert_hour_data(location_id, date, hour_data)
        logger.info(f"Successfully stored hour data for {date}.")

    def _persist_weather_data(self):
        """
        Opens a folder, iterates through each file in its top level, and performs an action.
        """
        try:
            file_config = FileConfig()
            _directory = os.path.join(os.path.dirname(LoadRawfiels.script_path()), file_config.raw_folder)

            if not os.path.isdir(_directory):
                print(f"Error: Folder '{_directory}' does not exist or is not a directory.")
                return
            
            # List all entries (files and subdirectories) in the folder
            for entry_name in os.listdir(_directory):
                _file = os.path.join(_directory, entry_name)

                # Check if the entry is a file (and not a directory)
                if os.path.isfile(_file):
                    self._perform_action_on_file(_file)
                else:
                    print(f"  Skipping directory: {entry_name}")

        except Exception as e:
            logger.exception(f"An unexpected error occurred: {e}")

def main():
    """
    Main function to persist weather data from files in rawdata folder.
    """
    db_operations = DatabaseOperations()
    db_operations.connect()
    try:
        get_weather_data = LoadRawfiels(db_operations)  # Pass the DatabaseOperations instance
        get_weather_data._persist_weather_data()
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
    finally:
        db_operations.close()  # Close the connection in the finally block

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    logger.info(f"Script execution time: {end_time - start_time:.2f} seconds")