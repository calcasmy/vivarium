from utilities.src.database_operations import DatabaseOperations
from typing import Dict, Optional, List, Tuple

class SensorDataQueries(DatabaseOperations):
    def __init__(self, db_operations: DatabaseOperations):
        super().__init__(
            dbname=db_operations.dbname,
            user=db_operations.user,
            password=db_operations.password,
            host=db_operations.host,
            port=db_operations.port
        )
        self.conn = self.get_connection()

    def insert_sensor_reading(self, sensor_id: int, timestamp: str, humidity: Optional[float] = None, temperature_celsius: Optional[float] = None, light_level_lux: Optional[int] = None, other_value: Optional[float] = None, other_unit: Optional[str] = None) -> Optional[int]:
        """Inserts a new sensor reading and returns its ID."""
        query = """
            INSERT INTO public.sensor_readings (sensor_id, timestamp, humidity, temperature_celsius, light_level_lux, other_value, other_unit)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING reading_id;
        """
        params = (sensor_id, timestamp, humidity, temperature_celsius, light_level_lux, other_value, other_unit)
        result = self.execute_query(query, params, fetchone=True)
        if result:
            return result[0]
        else:
            return None

    def get_readings_by_sensor(self, sensor_id: int, limit: Optional[int] = None) -> Optional[List[Dict]]:
        """Retrieves readings for a specific sensor, optionally with a limit."""
        query = """
            SELECT reading_id, sensor_id, timestamp, humidity, temperature_celsius, light_level_lux, other_value, other_unit
            FROM public.sensor_readings
            WHERE sensor_id = %s
            ORDER BY timestamp DESC
            {limit_clause};
        """.format(limit_clause=f"LIMIT {limit}" if limit else "")
        params = (sensor_id,)
        results = self.execute_query(query, params, fetch=True)
        if results:
            return [
                {'reading_id': row[0], 'sensor_id': row[1], 'timestamp': row[2], 'humidity': row[3], 'temperature_celsius': row[4], 'light_level_lux': row[5], 'other_value': row[6], 'other_unit': row[7]}
                for row in results
            ]
        else:
            return None

    def get_readings_by_time_range(self, start_time: str, end_time: str) -> Optional[List[Dict]]:
        """Retrieves readings within a specific time range."""
        query = """
            SELECT reading_id, sensor_id, timestamp, humidity, temperature_celsius, light_level_lux, other_value, other_unit
            FROM public.sensor_readings
            WHERE timestamp BETWEEN %s AND %s
            ORDER BY timestamp;
        """
        params = (start_time, end_time)
        results = self.execute_query(query, params, fetch=True)
        if results:
            return [
                {'reading_id': row[0], 'sensor_id': row[1], 'timestamp': row[2], 'humidity': row[3], 'temperature_celsius': row[4], 'light_level_lux': row[5], 'other_value': row[6], 'other_unit': row[7]}
                for row in results
            ]
        else:
            return None