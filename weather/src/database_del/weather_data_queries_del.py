# src/database/weather_data_queries.py
from typing import Optional, List, Dict

from utilities.src.logger import LogHelper
from utilities.src.database_operations import DatabaseOperations


logger = LogHelper.get_logger(__name__)

class WeatherDataQueries(DatabaseOperations): # Inherit from DatabaseOperations
    def __init__(self):
        super().__init__()  # Call the constructor of the parent class

    def create_table_if_not_exists(self) -> None:
        """Creates the weather_data table if it doesn't exist."""
        query = """
        CREATE TABLE IF NOT EXISTS weather_data (
            date DATE,
            location_name VARCHAR(255),
            region VARCHAR(255),
            country VARCHAR(255),
            latitude DECIMAL(10, 6),
            longitude DECIMAL(10, 6),
            avg_temp_c DECIMAL(5, 2),
            max_temp_c DECIMAL(5, 2),
            min_temp_c DECIMAL(5, 2),
            total_precip_mm DECIMAL(5, 2),
            PRIMARY KEY (date, latitude, longitude)
        );
        """
        self.execute_query(query)

    def insert_weather_data(self, data: dict) -> None:
        """Inserts weather data into the database. Handles potential errors."""
        query = """
        INSERT INTO weather_data (
            date,
            location_name,
            region,
            country,
            latitude,
            longitude,
            avg_temp_c,
            max_temp_c,
            min_temp_c,
            total_precip_mm
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (date, latitude, longitude) DO UPDATE SET
            avg_temp_c = EXCLUDED.avg_temp_c,
            max_temp_c = EXCLUDED.max_temp_c,
            min_temp_c = EXCLUDED.min_temp_c,
            total_precip_mm = EXCLUDED.total_precip_mm
        """
        try:
            location_data = data.get('location', {})
            forecast_data = data.get('forecast', {}).get('forecastday', [{}])[0].get('day',{})

            params = (
                forecast_data.get('date'),
                location_data.get('name'),
                location_data.get('region'),
                location_data.get('country'),
                location_data.get('lat'),
                location_data.get('lon'),
                forecast_data.get('avgtemp_c'),
                forecast_data.get('maxtemp_c'),
                forecast_data.get('mintemp_c'),
                forecast_data.get('totalprecip_mm'),
            )
            self.execute_query(query, params)
        except KeyError as e:
            logger.error(f"Missing key in weather data: {e}. Data: {data}")
            self.conn.rollback()
        except Exception as e:
            logger.error(f"Error inserting weather data: {e}. Data: {data}")
            self.conn.rollback()

    def fetch_weather_data(self) -> Optional[List[Dict]]:
        """Fetches all weather data from the database."""
        query = "SELECT * FROM weather_data;"
        return self.execute_query(query, fetch=True)
