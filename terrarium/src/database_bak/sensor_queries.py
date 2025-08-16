from typing import Dict, Optional, List, Tuple
from utilities.src.db_operations import DBOperations

class SensorQueries:
    """Provides methods to interact with the public.sensors table.

    The table schema is defined as:
    .. code-block:: sql

        CREATE TABLE public.sensors (
            sensor_id bigserial NOT NULL,
            sensor_name varchar(255) NOT NULL,
            sensor_type varchar(255) NOT NULL,
            location varchar(255) NULL,
            model varchar(255) NULL,
            date_installed date NULL,
            CONSTRAINT sensors_pkey PRIMARY KEY (sensor_id)
        );
    """
    def __init__(self, db_operations: DBOperations):
        """Initializes the SensorQueries object.

        :param db_operations: An instance of the DatabaseOperations class.
        :type db_operations: DBOperations
        """
        self.db_ops = db_operations

    def insert_sensor(self, sensor_name: str, sensor_type: str, location: Optional[str] = None, model: Optional[str] = None, date_installed: Optional[str] = None) -> Optional[int]:
        """Inserts a new sensor and returns its ID.

        :param sensor_name: The name of the sensor.
        :type sensor_name: str
        :param sensor_type: The type of the sensor.
        :type sensor_type: str
        :param location: The physical location of the sensor.
        :type location: Optional[str]
        :param model: The model of the sensor.
        :type model: Optional[str]
        :param date_installed: The date the sensor was installed.
        :type date_installed: Optional[str]
        :returns: The ID of the newly inserted sensor, or ``None`` on failure.
        :rtype: Optional[int]
        """
        query = """
            INSERT INTO public.sensors (sensor_name, sensor_type, location, model, date_installed)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING sensor_id;
        """
        params = (sensor_name, sensor_type, location, model, date_installed)
        result = self.db_ops.execute_query(query, params, fetch=True)
        if result:
            return result[0]['sensor_id']
        return None

    def get_sensor_by_id(self, sensor_id: int) -> Optional[Dict]:
        """Retrieves sensor information by its ID.

        :param sensor_id: The ID of the sensor.
        :type sensor_id: int
        :returns: A dictionary of sensor details, or ``None`` if not found.
        :rtype: Optional[Dict]
        """
        query = """
            SELECT sensor_id, sensor_name, sensor_type, location, model, date_installed
            FROM public.sensors
            WHERE sensor_id = %s;
        """
        params = (sensor_id,)
        result = self.db_ops.execute_query(query, params, fetch=True)
        if result:
            row = result[0]
            return {
                'sensor_id': row['sensor_id'],
                'sensor_name': row['sensor_name'],
                'sensor_type': row['sensor_type'],
                'location': row['location'],
                'model': row['model'],
                'date_installed': row['date_installed']
            }
        return None

    def get_sensor_by_name(self, sensor_name: str) -> Optional[Dict]:
        """Retrieves sensor information by its name.

        :param sensor_name: The name of the sensor.
        :type sensor_name: str
        :returns: A dictionary of sensor details, or ``None`` if not found.
        :rtype: Optional[Dict]
        """
        query = """
            SELECT sensor_id, sensor_name, sensor_type, location, model, date_installed
            FROM public.sensors
            WHERE sensor_name = %s;
        """
        params = (sensor_name,)
        result = self.db_ops.execute_query(query, params, fetch=True)
        if result:
            row = result[0]
            return {
                'sensor_id': row['sensor_id'],
                'sensor_name': row['sensor_name'],
                'sensor_type': row['sensor_type'],
                'location': row['location'],
                'model': row['model'],
                'date_installed': row['date_installed']
            }
        return None

    def list_all_sensors(self) -> Optional[List[Dict]]:
        """Lists all sensors in the database.

        :returns: A list of dictionaries, each containing sensor details, or ``None`` if none are found.
        :rtype: Optional[List[Dict]]
        """
        query = """
            SELECT sensor_id, sensor_name, sensor_type, location, model, date_installed
            FROM public.sensors;
        """
        results = self.db_ops.execute_query(query, fetch=True)
        if results:
            return [
                {'sensor_id': row['sensor_id'], 'sensor_name': row['sensor_name'], 'sensor_type': row['sensor_type'], 'location': row['location'], 'model': row['model'], 'date_installed': row['date_installed']}
                for row in results
            ]
        return None