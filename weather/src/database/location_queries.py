# src/database/location_queries.py

from typing import Optional, Dict
from utilities.src.database_operations import DatabaseOperations

class LocationQueries(DatabaseOperations):
    def __init__(self, db_operations):
        super().__init__()
        self.conn = db_operations.get_connection()

    def create_table_if_not_exists(self) -> None:
        query = """
        CREATE TABLE IF NOT EXISTS climate_location (
            location_id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            region VARCHAR(255),
            country VARCHAR(255),
            latitude DECIMAL(10, 6),
            longitude DECIMAL(10, 6),
            timezone_id VARCHAR(255),
            localtime_epoch BIGINT,
            "localtime" VARCHAR(20),
            CONSTRAINT unique_lat_lon UNIQUE (latitude, longitude)
        );
        """
        self.execute_query(query)

    def insert_location_data(self, data: Dict) -> Optional[int]:
        """
        Inserts location data and returns the id of the inserted row.
        If the location already exists, it updates the existing row
        and returns its id.
        """
        query = """
        INSERT INTO climate_location (
            name, region, country, latitude, longitude, timezone_id,
            localtime_epoch, "localtime"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (latitude, longitude) DO UPDATE SET
            name = EXCLUDED.name, region = EXCLUDED.region,
            country = EXCLUDED.country, timezone_id = EXCLUDED.timezone_id,
            localtime_epoch = EXCLUDED.localtime_epoch,
            "localtime" = EXCLUDED."localtime"
        RETURNING location_id;
        """
        params = (
            data.get("name"),
            data.get("region"),
            data.get("country"),
            data.get("lat"),
            data.get("lon"),
            data.get("tz_id"),
            data.get("localtime_epoch"),
            data.get("localtime"),
        )
        return self.execute_query_with_returning_id(query, params)

    def get_location_id(self, latitude: float, longitude: float) -> Optional[int]:
        """Gets a location ID from lat/long."""
        query = "SELECT location_id FROM climate_location WHERE latitude = %s AND longitude = %s;"
        params = (latitude, longitude)
        result = self.execute_query(query, params, fetch=True)
        if result:
            return result[0]["location_id"]
        else:
            return None