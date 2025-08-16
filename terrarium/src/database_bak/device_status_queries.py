# vivarium/terrarium/src/database/device_status_queries.py

import json
from typing import Dict, Optional, List, Union
from utilities.src.db_operations import DBOperations

class DeviceStatusQueries:
    """Provides methods for querying and manipulating the device_status table.

    The table schema is defined as:
    .. code-block:: sql

        CREATE TABLE public.device_status (
            status_id BIGSERIAL PRIMARY KEY,
            device_id INT4 NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            is_on BOOLEAN NOT NULL,
            device_data JSONB NULL
        );
    """
    def __init__(self, db_operations: DBOperations):
        """Initializes the DeviceStatusQueries object.

        :param db_operations: An instance of the DatabaseOperations class,
                              which provides the database connection details.
        :type db_operations: DBOperations
        """
        self.db_ops = db_operations

    def insert_device_status(self, device_id: int, timestamp: str, is_on: bool, device_data: Optional[Union[Dict, str]] = None) -> Optional[int]:
        """Inserts a new device status record and returns its ID.

        :param device_id: The ID of the device.
        :type device_id: int
        :param timestamp: The timestamp of the status update.
        :type timestamp: str
        :param is_on: The new status of the device (True for ON, False for OFF).
        :type is_on: bool
        :param device_data: A dictionary containing additional device data, to be stored as JSONB.
        :type device_data: Optional[Union[Dict, str]]
        :returns: The ID of the newly inserted status record, or ``None`` on failure.
        :rtype: Optional[int]
        """
        query = """
            INSERT INTO public.device_status (device_id, timestamp, is_on, device_data)
            VALUES (%s, %s, %s, %s)
            RETURNING status_id;
        """
        params = (
            device_id,
            timestamp,
            is_on,
            json.dumps(device_data) if isinstance(device_data, dict) else device_data
        )

        try:
            self.db_ops.begin_transaction()
            result = self.db_ops.execute_query(query, params, fetch_one=True)
            self.db_ops.commit_transaction()
            return result['status_id'] if result else None
        except Exception:
            self.db_ops.rollback_transaction()
            raise

    def get_status_by_device_id(self, device_id: int, limit: Optional[int] = None) -> Optional[List[Dict]]:
        """Retrieves status records for a specific device, optionally with a limit.

        :param device_id: The ID of the device.
        :type device_id: int
        :param limit: The maximum number of records to retrieve.
        :type limit: Optional[int]
        :returns: A list of dictionaries representing the device status records, or ``None`` if none are found.
        :rtype: Optional[List[Dict]]
        """
        query = f"""
            SELECT status_id, device_id, timestamp, is_on, device_data
            FROM public.device_status
            WHERE device_id = %s
            ORDER BY timestamp DESC
            {'LIMIT %s' if limit else ''};
        """
        params = (device_id,) if not limit else (device_id, limit)
        results = self.db_ops.execute_query(query, params, fetch=True)
        return results if results else None

    def get_status_by_time_range(self, start_time: str, end_time: str) -> Optional[List[Dict]]:
        """Retrieves device status records within a specific time range.

        :param start_time: The start timestamp for the query.
        :type start_time: str
        :param end_time: The end timestamp for the query.
        :type end_time: str
        :returns: A list of dictionaries representing device status records, or ``None`` if none are found.
        :rtype: Optional[List[Dict]]
        """
        query = """
            SELECT status_id, device_id, timestamp, is_on, device_data
            FROM public.device_status
            WHERE timestamp BETWEEN %s AND %s
            ORDER BY timestamp;
        """
        params = (start_time, end_time)
        results = self.db_ops.execute_query(query, params, fetch=True)
        return results if results else None

    def get_latest_status_by_device_id(self, device_id: str) -> Optional[Dict]:
        """Retrieves the latest device status information for a given device_id.

        :param device_id: The ID of the device.
        :type device_id: int
        :returns: A dictionary representing the latest device status, or ``None`` if not found.
        :rtype: Optional[Dict]
        """
        query = """
            SELECT status_id, device_id, timestamp, is_on, device_data
            FROM public.device_status
            WHERE device_id = %s
            ORDER BY timestamp DESC
            LIMIT 1;
        """
        params = (int(device_id),)
        results = self.db_ops.execute_query(query, params, fetch_one=True)
        return results