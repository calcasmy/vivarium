# src/database/current_weather_queries.py
from src.database.database_operations import DatabaseOperations
from typing import Optional, Dict

class CurrentWeatherQueries(DatabaseOperations):
    def __init__(self):
        super().__init__()

    def create_table_if_not_exists(self) -> None:
        query = """
        CREATE TABLE IF NOT EXISTS current_weather (
            id SERIAL PRIMARY KEY,
            location_id INT REFERENCES climate_location(location_id),
            last_updated_epoch BIGINT,
            last_updated VARCHAR(20),
            temp_c DECIMAL(5, 2),
            temp_f DECIMAL(5, 2),
            is_day BOOLEAN,
            condition_text VARCHAR(255),
            condition_icon VARCHAR(255),
            condition_code INT,
            wind_mph DECIMAL(5, 2),
            wind_kph DECIMAL(5, 2),
            wind_degree INTEGER,
            wind_dir VARCHAR(3),
            pressure_mb DECIMAL(7, 2),
            pressure_in DECIMAL(7, 2),
            precip_mm DECIMAL(5, 2),
            precip_in DECIMAL(5, 2),
            humidity INTEGER,
            cloud INTEGER,
            feelslike_c DECIMAL(5, 2),
            feelslike_f DECIMAL(5, 2),
            vis_km DECIMAL(5, 2),
            vis_miles DECIMAL(5, 2),
            uv DECIMAL(3, 1),
            gust_mph DECIMAL(5, 2),
            gust_kph DECIMAL(5, 2),
            UNIQUE (location_id, last_updated_epoch)
        );
        """
        self.execute_query(query)

    def insert_current_weather_data(self, data: Dict, location_id: int) -> None:
        """Inserts current weather data."""
        query = """
        INSERT INTO current_weather (
            location_id, last_updated_epoch, last_updated, temp_c, temp_f, is_day,
            condition_text, condition_icon, condition_code, wind_mph, wind_kph,
            wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in,
            humidity, cloud, feelslike_c, feelslike_f, vis_km, vis_miles, uv,
            gust_mph, gust_kph
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (location_id, last_updated_epoch) DO UPDATE SET
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
            vis_km = EXCLUDED.vis_km, vis_miles = EXCLUDED.vis_miles,
            uv = EXCLUDED.uv, gust_mph = EXCLUDED.gust_mph,
            gust_kph = EXCLUDED.gust_kph,
            last_updated = EXCLUDED.last_updated;
        """
        condition_data = data.get("condition", {})
        params = (
            location_id,
            data.get("last_updated_epoch"),
            data.get("last_updated"),  # No to_timestamp
            data.get("temp_c"),
            data.get("temp_f"),
            data.get("is_day"),
            condition_data.get("text"),
            condition_data.get("icon"),
            condition_data.get("code"),
            data.get("wind_mph"),
            data.get("wind_kph"),
            data.get("wind_degree"),
            data.get("wind_dir"),
            data.get("pressure_mb"),
            data.get("pressure_in"),
            data.get("precip_mm"),
            data.get("precip_in"),
            data.get("humidity"),
            data.get("cloud"),
            data.get("feelslike_c"),
            data.get("feelslike_f"),
            data.get("vis_km"),
            data.get("vis_miles"),
            data.get("uv"),
            data.get("gust_mph"),
            data.get("gust_kph"),
        )
        self.execute_query(query, params)