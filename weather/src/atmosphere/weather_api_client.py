# src/atmosphere/weather_api_client.py
import requests
import datetime
from typing import Optional
from utilities.src.logger import LogHelper
from utilities.src.config import WeatherAPIConfig

logger = LogHelper.get_logger(__name__)
api_config = WeatherAPIConfig()

class WeatherAPIClient:
    def __init__(self):
        self.api_key = api_config.api_key
        self.base_url = api_config.url
        self.latlong = api_config.lat_long

    def get_historical_data(self, date_str: str, location_latlong: str = None) -> Optional[dict]:
        """Fetches historical weather data for a specific date and location."""
        params = {
            'key': self.api_key,
            'q': self.latlong,
            'dt': date_str
        }
        if location_latlong:
            params['q'] = location_latlong  # Override with provided value
            logger.info(f"Fetching weather data for {date_str} at {location_latlong}...")
        else:
            logger.info(f"Fetching weather data for {date_str} at default location {self.latlong}...")
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()  # Raise an exception for bad status codes
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching weather data for {date_str} at {params.get('q')}: {e}")
            return None

if __name__ == "__main__":
    client = WeatherAPIClient()
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    default_latlong = WeatherAPIConfig().weather_api_latlong
    data = client.get_historical_data(yesterday, default_latlong)
    if data:
        logger.info(f"Successfully fetched data for {yesterday} at ({default_latlong}): {data.keys()}")
    else:
        logger.error(f"Failed to fetch data for {yesterday} at ({default_latlong}).")