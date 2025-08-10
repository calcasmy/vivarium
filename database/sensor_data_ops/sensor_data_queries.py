from typing import Dict, Optional, List
from utilities.src.db_operations import DBOperations  # Assuming this is where DatabaseOperations is defined.

class SensorDataQueries(DBOperations):
    """
    This class provides methods to interact with the sensor_readings table in the database.
    The table schema is defined as:
    CREATE TABLE public.sensor_readings (
        reading_id bigserial NOT NULL,
        sensor_id int4 NOT NULL,
        "timestamp" timestamp NOT NULL,
        raw_data jsonb NULL,
        CONSTRAINT sensor_readings_pkey PRIMARY KEY (reading_id)
    );
    """

    def __init__(self, db_operations: DBOperations):
        """
        Initializes the SensorDataQueries object.

        Args:
            db_operations (DatabaseOperations): An instance of the DatabaseOperations class,
                which provides the database connection details.
        """
        super().__init__()
        self.conn = db_operations.get_connection()

    def insert_sensor_reading(self, sensor_id: int, timestamp: str, raw_data: Optional[dict] = None) -> Optional[int]:
        """
        Inserts a new sensor reading into the sensor_readings table.

        Args:
            sensor_id (int): The ID of the sensor.
            timestamp (str): The timestamp of the reading.
            raw_data (Optional[dict]): A dictionary containing the raw sensor data
                (e.g., humidity, temperature, light level).  This will be stored as a JSONB object.
                If None, it will insert NULL into the raw_data column.

        Returns:
            Optional[int]: The ID of the newly inserted reading, or None on failure.
        """
        query = """
            INSERT INTO public.sensor_readings (sensor_id, timestamp, raw_data)
            VALUES (%s, %s, %s)
            RETURNING reading_id;
        """
        params = (sensor_id, timestamp, raw_data)
        result = self.execute_query(query, params, fetch=True)
        if result:
            return result[0]
        else:
            return None

    def get_readings_by_sensor_id(self, sensor_id: int, limit: Optional[int] = 1) -> Optional[List[Dict]]:
        """
        Retrieves readings for a specific sensor.

        Args:
            sensor_id (int): The ID of the sensor.
            limit (Optional[int]): The maximum number of readings to retrieve.
                If None, all readings for the sensor are retrieved.

        Returns:
            Optional[List[Dict]]: A list of dictionaries, where each dictionary
                represents a sensor reading.  Returns None if no readings are found.
                The dictionary includes 'reading_id', 'sensor_id', 'timestamp', and 'raw_data'.
        """
        query = """
            SELECT reading_id, sensor_id, timestamp, raw_data
            FROM public.sensor_readings
            WHERE sensor_id = %s
            ORDER BY timestamp DESC
            {limit_clause};
        """.format(limit_clause=f"LIMIT {limit}" if limit else "")
        params = (sensor_id,)
        results = self.execute_query(query, params, fetch=True)
        if results:
            return [
                {'reading_id': row[0], 'sensor_id': row[1], 'timestamp': row[2], 'raw_data': row[3]}
                for row in results
            ]
        else:
            return None

    def get_readings_by_time_range(self, start_time: str, end_time: str) -> Optional[List[Dict]]:
        """
        Retrieves sensor readings within a specified time range.

        Args:
            start_time (str): The start timestamp for the query.
            end_time (str): The end timestamp for the query.

        Returns:
            Optional[List[Dict]]: A list of dictionaries, where each dictionary
                represents a sensor reading. Returns None if no readings are found.
                The dictionary includes  'reading_id', 'sensor_id', 'timestamp', and 'raw_data'.
        """
        query = """
            SELECT reading_id, sensor_id, timestamp, raw_data
            FROM public.sensor_readings
            WHERE timestamp BETWEEN %s AND %s
            ORDER BY timestamp;
        """
        params = (start_time, end_time)
        results = self.execute_query(query, params, fetch=True)
        if results:
            return [
                {'reading_id': row[0], 'sensor_id': row[1], 'timestamp': row[2], 'raw_data': row[3]}
                for row in results
            ]
        else:
            return None
        
    def get_latest_readings_by_sensor_id(self, sensor_id: int, limit: Optional[int] = 1) -> Optional[Dict]:
        """
        Retrieves readings for a specific sensor.

        Args:
            sensor_id (int): The ID of the sensor.
            limit (Optional[int]): The maximum number of readings to retrieve.
                If None, all readings for the sensor are retrieved.

        Returns:
            Optional[Dict]: A dictionaries of sensor reading information.  Returns None if no readings are found.
                The dictionary includes 'reading_id', 'sensor_id', 'timestamp', and 'raw_data'.
        """
        query = """
            SELECT reading_id, sensor_id, timestamp, raw_data
            FROM public.sensor_readings
            WHERE sensor_id = %s
            ORDER BY timestamp DESC
            {limit_clause};
        """.format(limit_clause=f"LIMIT {limit}" if limit else "")
        params = (sensor_id,)
        results = self.execute_query(query, params, fetch=True)
        if results:
            row = results[0]
            return{
                'reading_id': row['reading_id'],
                'sensor_id': row['sensor_id'],
                'timestamp': row['timestamp'],
                'raw_data': row['raw_data']
            }
        else:
            return None
