# vivarium/terrarium/src/database/sensor_data_queries.py

import json
from typing import Dict, Optional, List
from utilities.src.db_operations import DBOperations

class SensorDataQueries:
    """Provides methods to interact with the sensor_readings table.

    The table schema is defined as:
    .. code-block:: sql

        CREATE TABLE public.sensor_readings (
            reading_id bigserial NOT NULL,
            sensor_id int4 NOT NULL,
            "timestamp" timestamp NOT NULL,
            raw_data jsonb NULL,
            CONSTRAINT sensor_readings_pkey PRIMARY KEY (reading_id)
        );
    """
    def __init__(self, db_operations: DBOperations):
        """Initializes the SensorDataQueries object.

        :param db_operations: An instance of the DatabaseOperations class,
                              which provides the database connection details.
        :type db_operations: DBOperations
        """
        self.db_ops = db_operations

    def insert_sensor_reading(self, sensor_id: int, timestamp: str, raw_data: Optional[Dict] = None) -> Optional[int]:
        """Inserts a new sensor reading into the sensor_readings table.

        :param sensor_id: The ID of the sensor.
        :type sensor_id: int
        :param timestamp: The timestamp of the reading.
        :type timestamp: str
        :param raw_data: A dictionary containing the raw sensor data, to be stored as JSONB.
        :type raw_data: Optional[Dict]
        :returns: The ID of the newly inserted reading, or ``None`` on failure.
        :rtype: Optional[int]
        """
        query = """
            INSERT INTO public.sensor_readings (sensor_id, timestamp, raw_data)
            VALUES (%s, %s, %s)
            RETURNING reading_id;
        """
        params = (sensor_id, timestamp, json.dumps(raw_data) if raw_data else None)

        try:
            self.db_ops.begin_transaction()
            result = self.db_ops.execute_query(query, params, fetch_one=True)
            self.db_ops.commit_transaction()
            return result['reading_id'] if result else None
        except Exception:
            self.db_ops.rollback_transaction()
            raise

    def get_readings_by_sensor_id(self, sensor_id: int, limit: Optional[int] = 1) -> Optional[List[Dict]]:
        """Retrieves a specific number of the most recent readings for a sensor.

        :param sensor_id: The ID of the sensor.
        :type sensor_id: int
        :param limit: The maximum number of readings to retrieve.
        :type limit: Optional[int]
        :returns: A list of dictionaries, representing the sensor readings.
        :rtype: Optional[List[Dict]]
        """
        query = """
            SELECT reading_id, sensor_id, timestamp, raw_data
            FROM public.sensor_readings
            WHERE sensor_id = %s
            ORDER BY timestamp DESC
            LIMIT %s;
        """
        params = (sensor_id, limit)
        results = self.db_ops.execute_query(query, params, fetch=True)
        return results if results else None

    def get_latest_reading_by_sensor_id(self, sensor_id: int) -> Optional[Dict]:
        """Retrieves the latest reading for a specific sensor.

        :param sensor_id: The ID of the sensor.
        :type sensor_id: int
        :returns: A dictionary representing the latest sensor reading, or ``None`` if none is found.
        :rtype: Optional[Dict]
        """
        query = """
            SELECT reading_id, sensor_id, timestamp, raw_data
            FROM public.sensor_readings
            WHERE sensor_id = %s
            ORDER BY timestamp DESC
            LIMIT 1;
        """
        params = (sensor_id,)
        result = self.db_ops.execute_query(query, params, fetch_one=True)
        return result

    def get_readings_by_time_range(self, start_time: str, end_time: str) -> Optional[List[Dict]]:
        """Retrieves sensor readings within a specified time range.

        :param start_time: The start timestamp for the query.
        :type start_time: str
        :param end_time: The end timestamp for the query.
        :type end_time: str
        :returns: A list of dictionaries, where each dictionary represents a sensor reading.
        :rtype: Optional[List[Dict]]
        """
        query = """
            SELECT reading_id, sensor_id, timestamp, raw_data
            FROM public.sensor_readings
            WHERE timestamp BETWEEN %s AND %s
            ORDER BY timestamp;
        """
        params = (start_time, end_time)
        results = self.db_ops.execute_query(query, params, fetch=True)
        return results if results else None


# class SensorDataQueries:
#     """Provides methods to interact with the sensor_readings table.

#     The table schema is defined as:
#     .. code-block:: sql

#         CREATE TABLE public.sensor_readings (
#             reading_id bigserial NOT NULL,
#             sensor_id int4 NOT NULL,
#             "timestamp" timestamp NOT NULL,
#             raw_data jsonb NULL,
#             CONSTRAINT sensor_readings_pkey PRIMARY KEY (reading_id)
#         );
#     """
#     def __init__(self, db_operations: DBOperations):
#         """Initializes the SensorDataQueries object.

#         :param db_operations: An instance of the DatabaseOperations class,
#                               which provides the database connection details.
#         :type db_operations: DBOperations
#         """
#         self.db_ops = db_operations

#     def insert_sensor_reading(self, sensor_id: int, timestamp: str, raw_data: Optional[Dict] = None) -> Optional[int]:
#         """Inserts a new sensor reading into the sensor_readings table.

#         :param sensor_id: The ID of the sensor.
#         :type sensor_id: int
#         :param timestamp: The timestamp of the reading.
#         :type timestamp: str
#         :param raw_data: A dictionary containing the raw sensor data, to be stored as JSONB.
#         :type raw_data: Optional[dict]
#         :returns: The ID of the newly inserted reading, or ``None`` on failure.
#         :rtype: Optional[int]
#         """
#         query = """
#             INSERT INTO public.sensor_readings (sensor_id, timestamp, raw_data)
#             VALUES (%s, %s, %s)
#             RETURNING reading_id;
#         """
#         params = (sensor_id, timestamp, json.dumps(raw_data) if raw_data else None)
#         result = self.db_ops.execute_query(query, params, fetch=True)
#         if result:
#             return result[0]
#         return None

#     def get_readings_by_sensor_id(self, sensor_id: int, limit: Optional[int] = 1, latest_only: bool = False) -> Optional[List[Dict]]:
#         """Retrieves readings for a specific sensor.

#         :param sensor_id: The ID of the sensor.
#         :type sensor_id: int
#         :param limit: The maximum number of readings to retrieve.
#         :type limit: Optional[int]
#         :param latest_only: If ``True``, returns a single dictionary for the latest reading.
#         :type latest_only: bool
#         :returns: A list of dictionaries or a single dictionary, representing a sensor reading. Returns ``None`` if no readings are found.
#         :rtype: Optional[List[Dict]] or Optional[Dict]
#         """
#         base_query = """
#             SELECT reading_id, sensor_id, timestamp, raw_data
#             FROM public.sensor_readings
#             WHERE sensor_id = %s
#             ORDER BY timestamp DESC
#         """
#         params = [sensor_id]
        
#         if limit is not None:
#             base_query += " LIMIT %s"
#             params.append(limit)

#         results = self.db_ops.execute_query(base_query, tuple(params), fetch=True)
        
#         if results:
#             if latest_only:
#                 row = results[0]
#                 return {
#                     'reading_id': row['reading_id'],
#                     'sensor_id': row['sensor_id'],
#                     'timestamp': row['timestamp'],
#                     'raw_data': row['raw_data']
#                 }
#             return [
#                 {'reading_id': row['reading_id'], 'sensor_id': row['sensor_id'], 'timestamp': row['timestamp'], 'raw_data': row['raw_data']}
#                 for row in results
#             ]
#         return None

#     def get_readings_by_time_range(self, start_time: str, end_time: str) -> Optional[List[Dict]]:
#         """Retrieves sensor readings within a specified time range.

#         :param start_time: The start timestamp for the query.
#         :type start_time: str
#         :param end_time: The end timestamp for the query.
#         :type end_time: str
#         :returns: A list of dictionaries, where each dictionary represents a sensor reading. Returns ``None`` if no readings are found.
#         :rtype: Optional[List[Dict]]
#         """
#         query = """
#             SELECT reading_id, sensor_id, timestamp, raw_data
#             FROM public.sensor_readings
#             WHERE timestamp BETWEEN %s AND %s
#             ORDER BY timestamp;
#         """
#         params = (start_time, end_time)
#         results = self.db_ops.execute_query(query, params, fetch=True)
#         if results:
#             return [
#                 {'reading_id': row['reading_id'], 'sensor_id': row['sensor_id'], 'timestamp': row['timestamp'], 'raw_data': row['raw_data']}
#                 for row in results
#             ]
#         return None