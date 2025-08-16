# src/atmosphere/weather_api_client.py
import requests
import datetime
from typing import Optional, Dict
from utilities.src.logger import LogHelper
from utilities.src.config import WeatherAPIConfig

logger = LogHelper.get_logger(__name__)

class WeatherAPIClient:
    """
    Client for interacting with the WeatherAPI.

    Handles fetching historical weather data for specified dates and locations.
    """
    def __init__(self, api_config: WeatherAPIConfig):
        """
        Initializes the WeatherAPIClient with configuration settings.

        :param api_config: An instance of :class:`utilities.src.config.WeatherAPIConfig`
            containing API key, base URL, and default location.
        :type api_config: :class:`utilities.src.config.WeatherAPIConfig`
        """
        self.api_key = api_config.api_key
        self.base_url = api_config.url
        self.default_location_lat_long = api_config.lat_long

    def get_historical_data(self, date_str: str, location_latlong: Optional[str] = None) -> Optional[Dict]:
        """
        Fetches historical weather data for a specific date and location.

        If `location_latlong` is provided, it overrides the default location configured.

        :param date_str: The date for which to fetch data in 'YYYY-MM-DD' format.
        :type date_str: str
        :param location_latlong: Optional latitude,longitude string to override the default
            configured API location (e.g., "37.7749,-122.4194").
        :type location_latlong: Optional[str]
        :returns: The raw weather data as a dictionary if the request is successful,
            otherwise ``None``.
        :rtype: Optional[dict]
        """
        params = {
            'key': self.api_key,
            'q': self.default_location_lat_long,
            'dt': date_str
        }
        if location_latlong:
            params['q'] = location_latlong
            logger.info(f"Fetching weather data for {date_str} at {location_latlong}...")
        else:
            logger.info(f"Fetching weather data for {date_str} at default location {self.default_location_lat_long}...")
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred while fetching weather data for {date_str} at {params.get('q')}: {http_err}")
            return None
        except requests.exceptions.ConnectionError as conn_err:
            logger.error(f"Connection error occurred while fetching weather data for {date_str} at {params.get('q')}: {conn_err}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching weather data for {date_str} at {params.get('q')}: {e}")
            return None

if __name__ == "__main__":
    config_for_test = WeatherAPIConfig()
    client = WeatherAPIClient(config_for_test)

    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    
    logger.info("\n--- Testing with default location with yesterdays's date---")
    data_default = client.get_historical_data(yesterday)
    if data_default:
        logger.info(f"Successfully fetched data for {yesterday} at default location: {data_default.keys()}")
    else:
        logger.error(f"Failed to fetch data for {yesterday} at default location.")

    logger.info("\n--- Testing with overridden location (New York) ---")
    new_york_latlong = "40.71,-74.01"
    data_ny = client.get_historical_data(yesterday, new_york_latlong)
    if data_ny:
        logger.info(f"Successfully fetched data for {yesterday} at New York: {data_ny.keys()}")
    else:
        logger.error(f"Failed to fetch data for {yesterday} at New York.")