# src/database/forecast_queries.py

from typing import Optional, Dict
from utilities.src.database_operations import DatabaseOperations

class ForecastQueries(DatabaseOperations):
    def __init__(self, db_operations):
        super().__init__()
        self.conn = db_operations.get_connection()

    def create_tables_if_not_exists(self) -> None:
        """Creates the forecast and hour tables if they don't exist, matching the provided DDL."""
        forecast_query = """
            CREATE TABLE IF NOT EXISTS public.climate_forecast_day (
                location_id INTEGER,
                forecast_date DATE,
                forecast_date_epoch BIGINT,
                PRIMARY KEY (location_id, forecast_date),
                FOREIGN KEY (location_id) REFERENCES public.climate_location(location_id)
            );
            """

    def insert_forecast_data(self, location_id: int, forecast_data: Dict) -> Optional[int]:
        """Inserts forecast day data and returns the id of the inserted forecast row."""

        query = """
            INSERT INTO public.climate_forecast_day (
                location_id, forecast_date, forecast_date_epoch
            ) VALUES (%s, %s, %s)
            RETURNING location_id;
            """
        params = (
            location_id, forecast_data.get('date'), forecast_data.get('date_epoch'),
        )

        result = self.execute_query(query, params, fetch=True)
        if result:
            return result[0]['location_id']
        else:
            return None

    def get_forecast_by_location_and_date(self, location_id: int, date: str) -> Optional[Dict]:
        """
        Retrieves a forecast record by location_id and date.
        """
        query = """
            SELECT
                location_id, forecast_date, forecast_date_epoch
            FROM public.climate_forecast_day
            WHERE location_id = %s AND forecast_date = %s;
            """
        params = (location_id, date)
        result = self.execute_query(query, params, fetch=True)
        if result:
            return result[0]  # Return the first (and should be only) result
        else:
            return None