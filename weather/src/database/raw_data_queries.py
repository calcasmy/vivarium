# src/database/raw_data_queries.py
import json
from typing import Optional, Dict
from utilities.src.database_operations import DatabaseOperations


class RawDataQueries(DatabaseOperations):
    def __init__(self, db_operations):
        super().__init__()
        self.conn = db_operations.get_connection()

    def create_table_if_not_exists(self) -> None:
        """Creates the raw_climate_data table if it doesn't exist."""
        query = """
        CREATE TABLE IF NOT EXISTS raw_climate_data (
            weather_date DATE PRIMARY KEY,
            raw_data JSONB NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """
        self.execute_query(query)

    def insert_raw_data(self, date: str, raw_data: Dict) -> Optional[str]:
        """
        Inserts raw climate data into the database.

        Args:
            date: The date of the data.
            raw_data: The raw data as a dictionary (will be stored as JSONB).

        Returns:
            The date of the inserted row, or None on error.
        """
        query = """
        INSERT INTO raw_climate_data (weather_date, raw_data)
        VALUES (%s, %s)
        ON CONFLICT (weather_date) DO UPDATE SET raw_data = EXCLUDED.raw_data
        RETURNING weather_date;
        """
        params = (date, json.dumps(raw_data))  # Convert dict to JSON string
        result = self.execute_query(query, params, fetch=True)
        if result:
            return result[0]['weather_date']
        else:
            return None

    def get_raw_data_by_date(self, date: str) -> Optional[Dict]:
        """
        Retrieves raw climate data from the database by date.

        Args:
            date: The date of the data to retrieve.

        Returns:
            The raw data as a dictionary, or None if not found.
        """
        query = """
        SELECT raw_data FROM raw_climate_data
        WHERE weather_date = %s;
        """
        params = (date,)
        result = self.execute_query(query, params, fetch=True)
        if result:
            return result[0]['raw_data']
        else:
            return None
