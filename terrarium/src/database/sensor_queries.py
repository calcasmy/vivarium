from utilities.src.database_operations import DatabaseOperations
from typing import Dict, Optional, List, Tuple

class SensorsQueries(DatabaseOperations):
    def __init__(self, db_operations: DatabaseOperations):
        super().__init__()
        self.conn = db_operations.get_connection()

    def insert_sensor(self, sensor_name: str, sensor_type: str, location: Optional[str] = None, model: Optional[str] = None, date_installed: Optional[str] = None) -> Optional[int]:
        """Inserts a new sensor and returns its ID."""
        query = """
            INSERT INTO public.sensors (sensor_name, sensor_type, location, model, date_installed)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING sensor_id;
        """
        params = (sensor_name, sensor_type, location, model, date_installed)
        result = self.execute_query(query, params, fetch=True)
        if result:
            return result[0]
        else:
            return None

    def get_sensor_by_id(self, sensor_id: int) -> Optional[Dict]:
        """Retrieves sensor information by its ID."""
        query = """
            SELECT sensor_id, sensor_name, sensor_type, location, model, date_installed
            FROM public.sensors
            WHERE sensor_id = %s;
        """
        params = (sensor_id,)
        result = self.execute_query(query, params, fetch=True)
        if result:
            return {
                'sensor_id': result[0],
                'sensor_name': result[1],
                'sensor_type': result[2],
                'location': result[3],
                'model': result[4],
                'date_installed': result[5]
            }
        else:
            return None

    def get_sensor_by_name(self, sensor_name: str) -> Optional[Dict]:
        """Retrieves sensor information by its name."""
        query = """
            SELECT sensor_id, sensor_name, sensor_type, location, model, date_installed
            FROM public.sensors
            WHERE sensor_name = %s;
        """
        params = (sensor_name,)
        result = self.execute_query(query, params, fetch=True)
        if result:
            result = result[0]
            return {
                'sensor_id': result['sensor_id'],
                'sensor_name': result['sensor_name'],
                'sensor_type': result['sensor_type'],
                'location': result['location'],
                'model': result['model'],
                'date_installed': result['date_installed']
            }
        else:
            return None

    def list_all_sensors(self) -> Optional[List[Dict]]:
        """Lists all sensors in the database."""
        query = """
            SELECT sensor_id, sensor_name, sensor_type, location, model, date_installed
            FROM public.sensors;
        """
        results = self.execute_query(query, fetch=True)
        if results:
            return [
                {'sensor_id': row[0], 'sensor_name': row[1], 'sensor_type': row[2], 'location': row[3], 'model': row[4], 'date_installed': row[5]}
                for row in results
            ]
        else:
            return None