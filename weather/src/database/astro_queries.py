# src/database/astro_queries.py
from weather.src.database.database_operations import DatabaseOperations
from typing import Dict, Optional

class AstroQueries(DatabaseOperations):
    def __init__(self, db_operations):
        super().__init__()
        self.conn = db_operations.get_connection()

    def insert_astro_data(self, location_id: int, forecast_date: str, astro_data: Dict) -> None:
        """Inserts daily forecast data."""
        query = """
            INSERT INTO public.climate_astro_data (
                location_id, forecast_date, sunrise, sunset, moonrise,
                moonset, moon_phase, moon_illumination
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (location_id, forecast_date) DO UPDATE SET
                sunrise = EXCLUDED.sunrise, sunset = EXCLUDED.sunset,
                moonrise = EXCLUDED.moonrise, moonset = EXCLUDED.moonset,
                moon_phase = EXCLUDED.moon_phase, moon_illumination = EXCLUDED.moon_illumination;
            """
        params = (
            location_id,
            forecast_date,
            astro_data.get('sunrise'), astro_data.get('sunset'),
            astro_data.get('moonrise'), astro_data.get('moonset'),
            astro_data.get('moon_phase'), astro_data.get('moon_illumination'),
        )
        self.execute_query(query, params)

    def get_astro_data(self, location_id: int, forecast_date: str) -> Optional[Dict]:
        """Retrieves astro data by location_id and forecast_date."""
        query = """
            SELECT
                location_id, forecast_date, sunrise, sunset, moonrise,
                moonset, moon_phase, moon_illumination
            FROM public.climate_astro_data
            WHERE location_id = %s AND forecast_date = %s;
            """
        params = (location_id, forecast_date)
        result = self.execute_query(query, params, fetch=True)
        if result:
            return result[0]
        else:
            return None
        
    def get_sunrise_sunset(self, location_id: int, forecast_date: str) -> Optional[Dict]:
        """Retrieves only sunrise and sunset times for a given location and date."""
        query = """
            SELECT sunrise, sunset
            FROM public.climate_astro_data
            WHERE location_id = %s AND forecast_date = %s;
            """
        params = (location_id, forecast_date)
        result = self.execute_query(query, params, fetch=True)
        if result:
            return {'sunrise': result[0]['sunrise'], 'sunset': result[0]['sunset']}
        else:
            return None