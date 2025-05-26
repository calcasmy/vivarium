from utilities.src.database_operations import DatabaseOperations
from typing import Dict, Optional, List, Tuple, Union
import json

class DeviceStatusQueries(DatabaseOperations):
    def __init__(self, db_operations: DatabaseOperations = None):
        super().__init__()
        self.conn = db_operations.get_connection()


    def insert_device_status(self, device_id: int, timestamp: str, is_on: bool, device_data: Optional[Union[Dict, str]] = None) -> Optional[int]:
        """Inserts a new device status record and returns its ID."""
        query = """
            INSERT INTO public.device_status (device_id, timestamp, is_on, device_data)
            VALUES (%s, %s, %s, %s)
            RETURNING status_id;
        """
        params = (
        device_id, timestamp, is_on, json.dumps(device_data) if isinstance(device_data, dict) else device_data)
        result = self.execute_query(query, params, fetch=True)
        if result:
            return result[0]
        else:
            return None

    def get_status_by_device_id(self, device_id: int, limit: Optional[int] = None) -> Optional[List[Dict]]:
        """Retrieves status records for a specific device, optionally with a limit."""
        query = """
            SELECT status_id, device_id, timestamp, is_on, device_data
            FROM public.device_status
            WHERE device_id = %s
            ORDER BY timestamp DESC
            {limit_clause};
        """.format(limit_clause=f"LIMIT {limit}" if limit else "")
        params = (device_id,)
        results = self.execute_query(query, params, fetch=True)
        if results:
            return [
                {'status_id': row[0], 'device_id': row[1], 'timestamp': row[2], 'is_on': row[3], 'device_data': row[4]}
                for row in results
            ]
        else:
            return None

    def get_status_by_time_range(self, start_time: str, end_time: str) -> Optional[List[Dict]]:
        """Retrieves device status records within a specific time range."""
        query = """
            SELECT status_id, device_id, timestamp, is_on, device_data
            FROM public.device_status
            WHERE timestamp BETWEEN %s AND %s
            ORDER BY timestamp;
        """
        params = (start_time, end_time)
        results = self.execute_query(query, params, fetch=True)
        if results:
            return [
                {'status_id': row[0], 'device_id': row[1], 'timestamp': row[2], 'is_on': row[3], 'device_data': row[4]}
                for row in results
            ]
        else:
            return None
        

    def get_latest_status_by_device_id(self, device_id: str) -> Optional[Dict]:
        """
        Retrieves the latest device status information for a given device_id.
        """
        query = """
            SELECT status_id, device_id, timestamp, is_on, device_data
            FROM public.device_status
            WHERE device_id = %s
            ORDER BY timestamp DESC
            LIMIT 1;
        """
        params = (device_id,)
        
        # We expect at most one row, so we can directly fetch the first (and only) result
        results = self.execute_query(query, params, fetch=True)
        
        if results:
            row = results[0] # Get the first (and only) dictionary from the list
            return {
                'status_id': row['status_id'],
                'device_id': row['device_id'],
                'timestamp': row['timestamp'],
                'is_on': row['is_on'],
                'device_data': row['device_data']
            }
        else:
            return None