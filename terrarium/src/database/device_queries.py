from utilities.src.database_operations import DatabaseOperations
from typing import Dict, Optional, List, Tuple

class DeviceQueries(DatabaseOperations):
    def __init__(self, db_operations: DatabaseOperations):
        super().__init__(
            dbname=db_operations.dbname,
            user=db_operations.user,
            password=db_operations.password,
            host=db_operations.host,
            port=db_operations.port
        )
        self.conn = self.get_connection()

    def insert_device(self, device_name: str, device_type: str, location: Optional[str] = None, model: Optional[str] = None, date_added: Optional[str] = None) -> Optional[int]:
        """Inserts a new device and returns its ID."""
        query = """
            INSERT INTO public.devices (device_name, device_type, location, model, date_added)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING device_id;
        """
        params = (device_name, device_type, location, model, date_added)
        result = self.execute_query(query, params, fetchone=True)
        if result:
            return result[0]
        else:
            return None

    def get_device_by_id(self, device_id: int) -> Optional[Dict]:
        """Retrieves device information by its ID."""
        query = """
            SELECT device_id, device_name, device_type, location, model, date_added
            FROM public.devices
            WHERE device_id = %s;
        """
        params = (device_id,)
        result = self.execute_query(query, params, fetchone=True)
        if result:
            return {
                'device_id': result[0],
                'device_name': result[1],
                'device_type': result[2],
                'location': result[3],
                'model': result[4],
                'date_added': result[5]
            }
        else:
            return None

    def get_device_by_name(self, device_name: str) -> Optional[Dict]:
        """Retrieves device information by its name."""
        query = """
            SELECT device_id, device_name, device_type, location, model, date_added
            FROM public.devices
            WHERE device_name = %s;
        """
        params = (device_name,)
        result = self.execute_query(query, params, fetchone=True)
        if result:
            return {
                'device_id': result[0],
                'device_name': result[1],
                'device_type': result[2],
                'location': result[3],
                'model': result[4],
                'date_added': result[5]
            }
        else:
            return None

    def list_all_devices(self) -> Optional[List[Dict]]:
        """Lists all devices in the database."""
        query = """
            SELECT device_id, device_name, device_type, location, model, date_added
            FROM public.devices;
        """
        results = self.execute_query(query, fetch=True)
        if results:
            return [
                {'device_id': row[0], 'device_name': row[1], 'device_type': row[2], 'location': row[3], 'model': row[4], 'date_added': row[5]}
                for row in results
            ]
        else:
            return None