# src/database/hour_queries.py

from typing import Dict, List, Optional
from utilities.src.database_operations import DatabaseOperations
from weather.src.database_del.condition_queries_del import ConditionQueries

class HourQueries(DatabaseOperations):
    def __init__(self, db_operations):
        super().__init__()
        self.condition_db = ConditionQueries(db_operations)
        self.conn = db_operations.get_connection()

    def insert_hour_data(self, location_id: int, forecast_date: str, hour_data_list: List[Dict]) -> None:
        """Inserts hourly forecast data for a given forecast day."""
        query = """
            INSERT INTO public.climate_hour_data (
                location_id, forecast_date, time_epoch, time, temp_c, temp_f, is_day,
                condition_code, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb,
                pressure_in, precip_mm, precip_in, snow_cm, humidity, cloud, feelslike_c,
                feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c,
                dewpoint_f, will_it_rain, chance_of_rain, will_it_snow, chance_of_snow,
                vis_km, vis_miles, gust_mph, gust_kph, uv
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (location_id, forecast_date, time_epoch) DO UPDATE SET
                time = EXCLUDED.time, temp_c = EXCLUDED.temp_c, temp_f = EXCLUDED.temp_f, is_day = EXCLUDED.is_day,
                condition_code = EXCLUDED.condition_code, wind_mph = EXCLUDED.wind_mph, wind_kph = EXCLUDED.wind_kph,
                wind_degree = EXCLUDED.wind_degree, wind_dir = EXCLUDED.wind_dir, pressure_mb = EXCLUDED.pressure_mb,
                pressure_in = EXCLUDED.pressure_in, precip_mm = EXCLUDED.precip_mm, precip_in = EXCLUDED.precip_in,
                snow_cm = EXCLUDED.snow_cm, humidity = EXCLUDED.humidity, cloud = EXCLUDED.cloud,
                feelslike_c = EXCLUDED.feelslike_c, feelslike_f = EXCLUDED.feelslike_f, windchill_c = EXCLUDED.windchill_c,
                windchill_f = EXCLUDED.windchill_f, heatindex_c = EXCLUDED.heatindex_c, heatindex_f = EXCLUDED.heatindex_f,
                dewpoint_c = EXCLUDED.dewpoint_c, dewpoint_f = EXCLUDED.dewpoint_f, will_it_rain = EXCLUDED.will_it_rain,
                chance_of_rain = EXCLUDED.chance_of_rain, will_it_snow = EXCLUDED.will_it_snow,
                chance_of_snow = EXCLUDED.chance_of_snow, vis_km = EXCLUDED.vis_km, vis_miles = EXCLUDED.vis_miles,
                gust_mph = EXCLUDED.gust_mph, gust_kph = EXCLUDED.gust_kph, uv = EXCLUDED.uv;
            """
        for hour_data in hour_data_list:
            condition_code = hour_data.get('condition', {}).get('code')

            # Insert hourly condition if it doesn't already exist
            if condition_code is not None and not self.condition_db.get_condition(condition_code):
                self.condition_db.insert_condition(hour_data.get('condition', {}))

            params = (
                location_id,
                forecast_date,
                hour_data.get('time_epoch'), hour_data.get('time'),
                hour_data.get('temp_c'), hour_data.get('temp_f'),
                hour_data.get('is_day'), condition_code,
                hour_data.get('wind_mph'), hour_data.get('wind_kph'),
                hour_data.get('wind_degree'), hour_data.get('wind_dir'),
                hour_data.get('pressure_mb'), hour_data.get('pressure_in'),
                hour_data.get('precip_mm'), hour_data.get('precip_in'),
                hour_data.get('snow_cm'), hour_data.get('humidity'),
                hour_data.get('cloud'), hour_data.get('feelslike_c'),
                hour_data.get('feelslike_f'), hour_data.get('windchill_c'),
                hour_data.get('windchill_f'), hour_data.get('heatindex_c'),
                hour_data.get('heatindex_f'), hour_data.get('dewpoint_c'),
                hour_data.get('dewpoint_f'), hour_data.get('will_it_rain'),
                hour_data.get('chance_of_rain'), hour_data.get('will_it_snow'),
                hour_data.get('chance_of_snow'), hour_data.get('vis_km'),
                hour_data.get('vis_miles'), hour_data.get('gust_mph'),
                hour_data.get('gust_kph'), hour_data.get('uv')
            )
            self.execute_query(query, params)

    def get_hour_data(self, location_id: int, forecast_date:str, time_epoch:int = 0000000000) -> Optional[Dict]:
        """
        Retrieves hour data by location_id, forecast_date and time_epoch.
        """
        query = """
            SELECT
                location_id, forecast_date, time_epoch, time, temp_c, temp_f, is_day,
                condition_code, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb,
                pressure_in, precip_mm, precip_in, snow_cm, humidity, cloud, feelslike_c,
                feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c,
                dewpoint_f, will_it_rain, chance_of_rain, will_it_snow,
                chance_of_snow, vis_km, vis_miles, gust_mph, gust_kph, uv
            FROM public.climate_hour_data
            WHERE location_id = %s AND forecast_date = %s AND time_epoch = %s;
            """
        params = (location_id, forecast_date, time_epoch)
        result = self.execute_query(query, params, fetch=True)
        if result:
            return result[0]
        else:
            return None
        
    def get_hourly_data_by_forecast_id(self, location_id: int, forecast_date:str) -> Optional[List[Dict]]:
        """
        Retrieves all hourly forecast data for a given forecast_id.
        """
        query = """
            SELECT
                location_id, forecast_date, time_epoch, time, temp_c, temp_f, is_day,
                condition_code, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb,
                pressure_in, precip_mm, precip_in, snow_cm, humidity, cloud, feelslike_c,
                feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c,
                dewpoint_f, will_it_rain, chance_of_rain, will_it_snow,
                chance_of_snow, vis_km, vis_miles, gust_mph, gust_kph, uv
            FROM public.climate_hour_data
            WHERE location_id = %s AND forecast_date = %s
            ORDER BY time_epoch;  -- Order by time
            """
        params = (location_id, forecast_date,)
        return self.execute_query(query, params, fetch=True)
    
    def fetch_time_epoch(self, location_id: int, forecast_date: str) -> Optional[List[Dict]]:
        """
        Fetches all time_epoch values from the climate_hour_data table for a given location_id and forecast_date.
       
        Args:
        db_operations: An instance of the DatabaseOperations class.
        location_id: The location ID.
        forecast_date: The forecast date (e.g., '2024-07-28').

        Returns:
            A list of time_epoch values (integers), or an empty list on error or if no data is found.
        """

        query = """
            SELECT time_epoch
            FROM climate_hour_data
            WHERE location_id = %s AND forecast_date = %s
            ORDER BY time_epoch;
        """
        params = (location_id, forecast_date,)
        return self.execute_query(query, params, fetch=True)