# src/database/day_queries.py

from typing import Dict, Optional
from utilities.src.database_operations import DatabaseOperations

class DayQueries(DatabaseOperations):
    def __init__(self, db_operations):
        super().__init__()
        self.conn = db_operations.get_connection()

    def insert_day_data(self, location_id: int, forecast_date: str, day_data: Dict) -> None:
        """Inserts daily forecast data."""
        query = """
            INSERT INTO public.climate_day_data (
                location_id, forecast_date, maxtemp_c, maxtemp_f,
                mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph,
                maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm,
                avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain,
                daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow,
                condition_code, uv
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (location_id, forecast_date) DO UPDATE SET
                maxtemp_c = EXCLUDED.maxtemp_c, maxtemp_f = EXCLUDED.maxtemp_f,
                mintemp_c = EXCLUDED.mintemp_c, mintemp_f = EXCLUDED.mintemp_f,
                avgtemp_c = EXCLUDED.avgtemp_c, avgtemp_f = EXCLUDED.avgtemp_f,
                maxwind_mph = EXCLUDED.maxwind_mph, maxwind_kph = EXCLUDED.maxwind_kph,
                totalprecip_mm = EXCLUDED.totalprecip_mm, totalprecip_in = EXCLUDED.totalprecip_in,
                totalsnow_cm = EXCLUDED.totalsnow_cm, avgvis_km = EXCLUDED.avgvis_km,
                avgvis_miles = EXCLUDED.avgvis_miles, avghumidity = EXCLUDED.avghumidity,
                daily_will_it_rain = EXCLUDED.daily_will_it_rain,
                daily_chance_of_rain = EXCLUDED.daily_chance_of_rain,
                daily_will_it_snow = EXCLUDED.daily_will_it_snow,
                daily_chance_of_snow = EXCLUDED.daily_chance_of_snow,
                condition_code = EXCLUDED.condition_code, uv = EXCLUDED.uv;
            """
        condition_code = day_data.get('condition', {}).get('code')
        params = (
            location_id,
            forecast_date,
            day_data.get('maxtemp_c'), day_data.get('maxtemp_f'),
            day_data.get('mintemp_c'), day_data.get('mintemp_f'),
            day_data.get('avgtemp_c'), day_data.get('avgtemp_f'),
            day_data.get('maxwind_mph'), day_data.get('maxwind_kph'),
            day_data.get('totalprecip_mm'), day_data.get('totalprecip_in'),
            day_data.get('totalsnow_cm'), day_data.get('avgvis_km'),
            day_data.get('avgvis_miles'), day_data.get('avghumidity'),
            day_data.get('daily_will_it_rain'), day_data.get('daily_chance_of_rain'),
            day_data.get('daily_will_it_snow'), day_data.get('daily_chance_of_snow'),
            condition_code,
            day_data.get('uv')
        )
        self.execute_query(query, params)

    def get_day_data(self, location_id: int, forecast_date: str) -> Optional[Dict]:
        """Retrieves daily forecast data by location_id and forecast_date."""
        query = """
            SELECT
                location_id, forecast_date, maxtemp_c, maxtemp_f,
                mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph,
                maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm,
                avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain,
                daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow,
                condition_code, uv
            FROM public.climate_day_data
            WHERE location_id = %s AND forecast_date = %s;
            """
        params = (location_id, forecast_date)
        result = self.execute_query(query, params, fetch=True)
        if result:
            return result[0]
        else:
            return None