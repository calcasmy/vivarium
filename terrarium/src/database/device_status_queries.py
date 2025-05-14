from utilities.src.database_operations import DatabaseOperations
from typing import Dict, Optional, List, Tuple

class DeviceStatusQueries(DatabaseOperations):
    def __init__(self, db_operations: DatabaseOperations):
        super().__init__(
            dbname=db_operations.dbname,
            user=db_operations.user,
            password=db_operations.password,
            host=db_operations.host,
            port=db_operations.port
        )
        self.conn = self.get_connection()

    def insert_device_status(self, device_id: int, timestamp: str, is_on: bool, rpm: Optional[int] = None, level: Optional[float] = None, setting: Optional[str] = None) -> Optional[int]:
        """Inserts a new device status record and returns its ID."""
        query = """
            INSERT INTO public.device_status (device_id, timestamp, is_on, rpm, level, setting)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING status_id;
        """
        params = (device_id, timestamp, is_on, rpm, level, setting)
        result = self.execute_query(query, params, fetchone=True)
        if result:
            return result[0]
        else:
            return None

    def get_status_by_device(self, device_id: int, limit: Optional[int] = None) -> Optional[List[Dict]]:
        """Retrieves status records for a specific device, optionally with a limit."""
        query = """
            SELECT status_id, device_id, timestamp, is_on, rpm, level, setting
            FROM public.device_status
            WHERE device_id = %s
            ORDER BY timestamp DESC
            {limit_clause};
        """.format(limit_clause=f"LIMIT {limit}" if limit else "")
        params = (device_id,)
        results = self.execute_query(query, params, fetch=True)
        if results:
            return [
                {'status_id': row[0], 'device_id': row[1], 'timestamp': row[2], 'is_on': row[3], 'rpm': row[4], 'level': row[5], 'setting': row[6]}
                for row in results
            ]
        else:
            return None

    def get_status_by_time_range(self, start_time: str, end_time: str) -> Optional[List[Dict]]:
        """Retrieves device status records within a specific time range."""
        query = """
            SELECT status_id, device_id, timestamp, is_on, rpm, level, setting
            FROM public.device_status
            WHERE timestamp BETWEEN %s AND %s
            ORDER BY timestamp;
        """
        params = (start_time, end_time)
        results = self.execute_query(query, params, fetch=True)
        if results:
            return [
                {'status_id': row[0], 'device_id': row[1], 'timestamp': row[2], 'is_on': row[3], 'rpm': row[4], 'level': row[5], 'setting': row[6]}
                for row in results
            ]
        else:
            return None