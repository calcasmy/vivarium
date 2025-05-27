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

from weather.src.database.location_queries import LocationQueries
from weather.src.database.raw_data_queries import RawDataQueries
from weather.src.database.forecast_queries import ForecastQueries
from weather.src.database.day_queries import DayQueries
from weather.src.database.astro_queries import AstroQueries
from weather.src.database.condition_queries import ConditionQueries
from weather.src.database.hour_queries import HourQueries

from utilities.src.logger import LogHelper
from utilities.src.database_operations import DatabaseOperations
from utilities.src.config import DatabaseConfig, FileConfig
from utilities.src.path_utils import PathUtils

db_config = DatabaseConfig()

logger = LogHelper.get_logger(__name__)

class FetchDailyWeather:
    """
    A class to fetch and store weather data.
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

    def get_raw_weather_data(self, date: str, location_lat_long: str = None) -> Optional[Dict]:
        """
        Retrieves raw weather data from the database or the API.

        Args:
            date: The date for which to retrieve the data.

        Returns:
            The raw weather data as a dictionary, or None if it cannot be retrieved.
        """

        #Checking if the raw climate data file exists in the local directory.
        file_config = FileConfig()
        files_dir = os.path.join(os.path.dirname(FetchDailyWeather.script_path()), file_config.raw_folder)
        os.makedirs(files_dir, exist_ok=True)
        _file = os.path.join(files_dir, date + '.json')

        LogHelper.log_newline(logger)
        if not os.path.exists(_file) or os.path.getsize(_file) == 0:
            logger.info(f"Raw climate file does not exist for {date} at {files_dir}. Fetching from API.")
            weather_data = self.weather_client.get_historical_data(date, location_lat_long)
            if weather_data:
                logger.debug(f"weather_data: {weather_data}")
                with open(_file, 'w') as outfile:
                    json.dump(weather_data, outfile, indent=5)
        else:
            logger.info(f"Raw climate file already exists for {date} at {files_dir}.")
            with open(_file, 'r') as infile:
                weather_data = json.load(infile)

        existing_raw_data = self.raw_data_db.get_raw_data_by_date(date)
        if existing_raw_data:
            logger.info(f"Raw climate data already exists for {date}. Returning data from database.")
            return existing_raw_data
        else:
            logger.info(f"Raw climate data does not exist for {date}. Adding data to database.")
            if weather_data:
                raw_data_id = self.raw_data_db.insert_raw_data(date, weather_data)
                if raw_data_id:
                    logger.info(f"Successfully fetched and stored raw climate data for {date}.")
                    return weather_data
                else:
                    logger.error(f"Failed to insert raw climate data for {date}.")
                    return None
            else:
                logger.error(f"Failed to fetch weather data for {date} from API.")
                return None
            
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

    def fetch_and_store_weather_data(self, yesterday: str = None, location_latlong: str = None):
        """
        Fetches daily weather data and stores it in the database.
        """
        try:
            if not yesterday:
                yesterday = (datetime.now().date() - timedelta(days=1)).strftime('%Y-%m-%d')
            weather_data = self.get_raw_weather_data(yesterday, location_latlong)

            if not weather_data:
                logger.error(f"No weather data to process for {yesterday}.")
                return

            # 1. Handle Location Data
            location_id = self.handle_location_data(weather_data, yesterday)

            # 2. Handle Forecast Data
            forecast_id = self.handle_forecast_data(weather_data, location_id, yesterday)

        except Exception as e:
            logger.exception(f"An unexpected error occurred: {e}")

def main():
    """
    Main function to fetch weather data.
    """
    db_operations = DatabaseOperations()
    db_operations.connect()
    try:
        get_weather_data = FetchDailyWeather(db_operations)  # Pass the DatabaseOperations instance
        get_weather_data.fetch_and_store_weather_data()
        # Alternateive method calls if data for a specific date (or) date, location (lattitude and longitude) is required
        # get_weather_data.fetch_and_store_weather_data('2025-05-15',)
        # get_weather_data.fetch_and_store_weather_data('2025-05-18', '5.98,116.07')
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
    finally:
        db_operations.close()  # Close the connection in the finally block

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    logger.info(f"Script execution time: {end_time - start_time:.2f} seconds")
