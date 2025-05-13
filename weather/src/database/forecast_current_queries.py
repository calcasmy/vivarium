# src/database/forecast_queries.py
from src.database.database_operations import DatabaseOperations
from typing import Optional, Dict, List

class ForecastQueries(DatabaseOperations):
    def __init__(self, db_operations):
        super().__init__()
        self.conn = db_operations.get_connection()

    def create_tables_if_not_exists(self) -> None:
        """Creates the forecast and hour tables if they don't exist."""
        forecast_query = """
            CREATE TABLE IF NOT EXISTS forecast (
                id SERIAL PRIMARY KEY,
                location_id INT REFERENCES location(id),
                date DATE,
                date_epoch BIGINT,
                max_temp_c DECIMAL(5, 2),
                max_temp_f DECIMAL(5, 2),
                min_temp_c DECIMAL(5, 2),
                min_temp_f DECIMAL(5, 2),
                avg_temp_c DECIMAL(5, 2),
                avg_temp_f DECIMAL(5, 2),
                maxwind_mph DECIMAL(5, 2),
                maxwind_kph DECIMAL(5, 2),
                totalprecip_mm DECIMAL(5, 2),
                totalprecip_in DECIMAL(5, 2),
                totalsnow_cm DECIMAL(5, 2),
                avgvis_km DECIMAL(5, 2),
                avgvis_miles DECIMAL(5, 2),
                avghumidity SMALLINT,
                daily_will_it_rain BOOLEAN,
                daily_chance_of_rain SMALLINT,
                daily_will_it_snow BOOLEAN,
                daily_chance_of_snow SMALLINT,
                condition_text VARCHAR(255),
                condition_icon VARCHAR(255),
                condition_code INT,
                uv SMALLINT,
                UNIQUE (location_id, date)
            );
            """

        hour_query = """
            CREATE TABLE IF NOT EXISTS hour (
                id SERIAL PRIMARY KEY,
                forecast_id INT REFERENCES forecast(id),
                time_epoch BIGINT,
                time TIMESTAMP,
                temp_c DECIMAL(5, 2),
                temp_f DECIMAL(5, 2),
                is_day BOOLEAN,
                condition_text VARCHAR(255),
                condition_icon VARCHAR(255),
                condition_code INT,
                wind_mph DECIMAL(5, 2),
                wind_kph DECIMAL(5, 2),
                wind_degree SMALLINT,
                wind_dir VARCHAR(10),
                pressure_mb DECIMAL(7, 2),
                pressure_in DECIMAL(5, 2),
                precip_mm DECIMAL(5, 2),
                precip_in DECIMAL(5, 2),
                humidity SMALLINT,
                cloud SMALLINT,
                feelslike_c DECIMAL(5, 2),
                feelslike_f DECIMAL(5, 2),
                windchill_c DECIMAL(5, 2),
                windchill_f DECIMAL(5, 2),
                heatindex_c DECIMAL(5, 2),
                heatindex_f DECIMAL(5, 2),
                dewpoint_c DECIMAL(5, 2),
                dewpoint_f DECIMAL(5, 2),
                will_it_rain BOOLEAN,
                chance_of_rain SMALLINT,
                will_it_snow BOOLEAN,
                chance_of_snow SMALLINT,
                vis_km DECIMAL(5, 2),
                vis_miles DECIMAL(5, 2),
                gust_mph DECIMAL(5, 2),
                gust_kph DECIMAL(5, 2),
                uv SMALLINT,
                UNIQUE (forecast_id, time_epoch)
            );
            """
        self.execute_query(forecast_query)
        self.execute_query(hour_query)


    def insert_forecast_data(self, location_id: int, forecast_data: Dict) -> Optional[int]:
        """Inserts forecast day data and returns the id of the inserted forecast row."""
        day_data = forecast_data.get('day', {})
        astro_data = forecast_data.get('astro', {})

        query = """
            INSERT INTO forecast (
                location_id, date, date_epoch, max_temp_c, max_temp_f,
                min_temp_c, min_temp_f, avg_temp_c, avg_temp_f, maxwind_mph,
                maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm,
                avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain,
                daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow,
                condition_text, condition_icon, condition_code, uv
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """
        params = (
            location_id, forecast_data.get('date'), forecast_data.get('date_epoch'),
            day_data.get('maxtemp_c'), day_data.get('maxtemp_f'),
            day_data.get('mintemp_c'), day_data.get('mintemp_f'),
            day_data.get('avgtemp_c'), day_data.get('avgtemp_f'),
            day_data.get('maxwind_mph'), day_data.get('maxwind_kph'),
            day_data.get('totalprecip_mm'), day_data.get('totalprecip_in'),
            day_data.get('totalsnow_cm'), day_data.get('avgvis_km'),
            day_data.get('avgvis_miles'), day_data.get('avghumidity'),
            day_data.get('daily_will_it_rain'), day_data.get('daily_chance_of_rain'),
            day_data.get('daily_will_it_snow'), day_data.get('daily_chance_of_snow'),
            day_data.get('condition', {}).get('text'),  # Get condition text
            day_data.get('condition', {}).get('icon'),  # and icon and code.
            day_data.get('condition', {}).get('code'),
            day_data.get('uv')
        )

        result = self.execute_query(query, params, fetch=True)
        if result:
            return result[0]['id']
        else:
            return None



    def insert_hour_data(self, forecast_id: int, hour_data_list: List[Dict]) -> None:
        """Inserts hourly forecast data for a given forecast day."""
        query = """
            INSERT INTO hour (
                forecast_id, time_epoch, time, temp_c, temp_f, is_day,
                condition_text, condition_icon, condition_code, wind_mph, wind_kph,
                wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm,
                precip_in, humidity, cloud, feelslike_c, feelslike_f,
                windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c,
                dewpoint_f, will_it_rain, chance_of_rain, will_it_snow,
                chance_of_snow, vis_km, vis_miles, gust_mph, gust_kph, uv
            ) VALUES (%s, %s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (forecast_id, time_epoch) DO UPDATE SET
                temp_c = EXCLUDED.temp_c, temp_f = EXCLUDED.temp_f,
                is_day = EXCLUDED.is_day, condition_text = EXCLUDED.condition_text,
                condition_icon = EXCLUDED.condition_icon,
                condition_code = EXCLUDED.condition_code,
                wind_mph = EXCLUDED.wind_mph, wind_kph = EXCLUDED.wind_kph,
                wind_degree = EXCLUDED.wind_degree, wind_dir = EXCLUDED.wind_dir,
                pressure_mb = EXCLUDED.pressure_mb, pressure_in = EXCLUDED.pressure_in,
                precip_mm = EXCLUDED.precip_mm, precip_in = EXCLUDED.precip_in,
                humidity = EXCLUDED.humidity, cloud = EXCLUDED.cloud,
                feelslike_c = EXCLUDED.feelslike_c, feelslike_f = EXCLUDED.feelslike_f,
                windchill_c = EXCLUDED.windchill_c, windchill_f = EXCLUDED.windchill_f,
                heatindex_c = EXCLUDED.heatindex_c, heatindex_f = EXCLUDED.heatindex_f,
                dewpoint_c = EXCLUDED.dewpoint_c, dewpoint_f = EXCLUDED.dewpoint_f,
                will_it_rain = EXCLUDED.will_it_rain, chance_of_rain = EXCLUDED.chance_of_rain,
                will_it_snow = EXCLUDED.will_it_snow, chance_of_snow = EXCLUDED.chance_of_snow,
                vis_km = EXCLUDED.vis_km, vis_miles = EXCLUDED.vis_miles,
                gust_mph = EXCLUDED.gust_mph, gust_kph = EXCLUDED.gust_kph,
                uv = EXCLUDED.uv,
                time = to_timestamp(EXCLUDED.time_epoch);
            """
        for hour_data in hour_data_list:
            condition_data = hour_data.get('condition', {})
            params = (
                forecast_id,
                hour_data.get('time_epoch'), hour_data.get('time_epoch'),
                hour_data.get('temp_c'), hour_data.get('temp_f'),
                hour_data.get('is_day'), condition_data.get('text'),
                condition_data.get('icon'), condition_data.get('code'),
                hour_data.get('wind_mph'), hour_data.get('wind_kph'),
                hour_data.get('wind_degree'), hour_data.get('wind_dir'),
                hour_data.get('pressure_mb'), hour_data.get('pressure_in'),
                hour_data.get('precip_mm'), hour_data.get('precip_in'),
                hour_data.get('humidity'), hour_data.get('cloud'),
                hour_data.get('feelslike_c'), hour_data.get('feelslike_f'),
                hour_data.get('windchill_c'), hour_data.get('windchill_f'),
                hour_data.get('heatindex_c'), hour_data.get('heatindex_f'),
                hour_data.get('dewpoint_c'), hour_data.get('dewpoint_f'),
                hour_data.get('will_it_rain'), hour_data.get('chance_of_rain'),
                hour_data.get('will_it_snow'), hour_data.get('chance_of_snow'),
                hour_data.get('vis_km'), hour_data.get('vis_miles'),
                hour_data.get('gust_mph'), hour_data.get('gust_kph'),
                hour_data.get('uv')
            )
            self.execute_query(query, params)

    def get_forecast_by_location_and_date(self, location_id: int, date: str) -> Optional[Dict]:
        """
        Retrieves a forecast record by location_id and date.
        """
        query = """
            SELECT
                id, location_id, date, date_epoch, max_temp_c, max_temp_f,
                min_temp_c, min_temp_f, avg_temp_c, avg_temp_f, maxwind_mph,
                maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm,
                avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain,
                daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow,
                condition_text, condition_icon, condition_code, uv
            FROM forecast
            WHERE location_id = %s AND date = %s;
            """
        params = (location_id, date)
        result = self.execute_query(query, params, fetch=True)
        if result:
            return result[0]  # Return the first (and should be only) result
        else:
            return None

    def get_hourly_data_by_forecast_id(self, forecast_id: int) -> Optional[List[Dict]]:
        """
        Retrieves all hourly forecast data for a given forecast_id.
        """
        query = """
            SELECT
                id, forecast_id, time_epoch, time, temp_c, temp_f, is_day,
                condition_text, condition_icon, condition_code, wind_mph, wind_kph,
                wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm,
                precip_in, humidity, cloud, feelslike_c, feelslike_f,
                windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c,
                dewpoint_f, will_it_rain, chance_of_rain, will_it_snow,
                chance_of_snow, vis_km, vis_miles, gust_mph, gust_kph, uv
            FROM hour
            WHERE forecast_id = %s
            ORDER BY time_epoch;  -- Order by time
            """
        params = (forecast_id,)
        return self.execute_query(query, params, fetch=True)