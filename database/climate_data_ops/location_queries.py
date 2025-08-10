import json
import psycopg2
from psycopg2 import sql
from typing import Optional, Dict, Any

from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations
from database.climate_data_ops.base_query_strategy import BaseQuery

logger = LogHelper.get_logger(__name__)


class LocationQueries(BaseQuery):
    """
    Manages database interactions for location data in the 'public.climate_location' table.
    """

    def __init__(self, db_operations: DBOperations):
        """
        Initializes the LocationQueries instance.

        :param db_operations: An active DBOperations instance for database connectivity.
        """
        super().__init__(db_operations)
        logger.debug("LocationQueries initialized.")

    def insert(self, data: Dict[str, Any]) -> Optional[int]:
        """
        Inserts new location data or updates existing data based on latitude and longitude.

        :param data: Dictionary containing location details. Expected keys: "name", "region",
                     "country", "lat", "lon", "tz_id", "localtime_epoch", "localtime".
        :return: The 'location_id' of the inserted or updated row if successful, None otherwise.
        """
        query = sql.SQL("""
            INSERT INTO public.climate_location (
                name, region, country, latitude, longitude, timezone_id,
                localtime_epoch, "localtime"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (latitude, longitude) DO UPDATE SET
                name = EXCLUDED.name, region = EXCLUDED.region,
                country = EXCLUDED.country, timezone_id = EXCLUDED.timezone_id,
                localtime_epoch = EXCLUDED.localtime_epoch,
                "localtime" = EXCLUDED."localtime"
            RETURNING location_id;
        """)
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
        try:
            result = self.db_ops.execute_query(query, params, fetch_one=True)
            if result and 'location_id' in result:
                logger.info(f"Location data for {data.get('name')} successfully inserted/updated with ID: {result['location_id']}.")
                return result['location_id']
            logger.warning(f"Insert/update for location '{data.get('name')}' returned no location_id.")
            return None
        except psycopg2.Error as e:
            logger.error(f"Database error during insert/update for location '{data.get('name')}': {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error during insert/update for location '{data.get('name')}': {e}", exc_info=True)
            return None

    def get(self, latitude: float, longitude: float) -> Optional[int]:
        """
        Retrieves the location_id for a given latitude and longitude from 'public.climate_location'.

        :param latitude: The latitude of the location.
        :param longitude: The longitude of the location.
        :return: The 'location_id' if found, None otherwise.
        """
        query = sql.SQL("""
            SELECT location_id FROM public.climate_location
            WHERE latitude = %s AND longitude = %s;
        """)
        params = (latitude, longitude)

        try:
            result = self.db_ops.execute_query(query, params, fetch_one=True)
            if result and 'location_id' in result:
                logger.info(f"Location ID {result['location_id']} found for ({latitude}, {longitude}).")
                return result['location_id']
            logger.info(f"No location found for latitude {latitude} and longitude {longitude}.")
            return None
        except psycopg2.Error as e:
            logger.error(f"Database error retrieving location for ({latitude}, {longitude}): {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error retrieving location for ({latitude}, {longitude}): {e}", exc_info=True)
            return None

    def update(self, location_id: int, new_data: Dict[str, Any]) -> bool:
        """
        Updates an existing location record in 'public.climate_location' by its ID.

        Note: To accurately verify affected rows for DML operations (like UPDATE/DELETE),
        the `DBOperations.execute_query` method would ideally return `cursor.rowcount`.
        As currently implemented, this method assumes success if no exception is raised.

        :param location_id: The ID of the location record to update.
        :param new_data: A dictionary containing the new values for location attributes.
                         Expected keys include: "name", "region", "country", etc.
        :return: True if the update was successful (no error), False otherwise.
        """
        # Construct SET clause dynamically from new_data, excluding location_id
        set_clauses = []
        params = []
        for key, value in new_data.items():
            if key != "location_id": # Ensure location_id is not updated as part of the data
                set_clauses.append(sql.Identifier(key) + sql.SQL(' = %s'))
                params.append(value)

        if not set_clauses:
            logger.warning(f"No valid fields provided to update for location ID {location_id}.")
            return False

        query = sql.SQL("""
            UPDATE public.climate_location
            SET {}
            WHERE location_id = %s;
        """).format(sql.SQL(', ').join(set_clauses))
        params.append(location_id) # Add location_id to the end of parameters

        try:
            self.db_ops.execute_query(query, tuple(params), fetch=False)
            logger.info(f"Location ID {location_id} updated. Verification of affected rows may be needed externally.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Database error updating location ID {location_id}: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error updating location ID {location_id}: {e}", exc_info=True)
            return False

    def delete(self, location_id: int) -> bool:
        """
        Deletes a location record from 'public.climate_location' by its ID.

        Note: To accurately verify affected rows for DML operations (like UPDATE/DELETE),
        the `DBOperations.execute_query` method would ideally return `cursor.rowcount`.
        As currently implemented, this method assumes success if no exception is raised.

        :param location_id: The ID of the location record to delete.
        :return: True if the deletion was successful (no error), False otherwise.
        """
        query = sql.SQL("""
            DELETE FROM public.climate_location
            WHERE location_id = %s;
        """)
        params = (location_id,)

        try:
            self.db_ops.execute_query(query, params, fetch=False)
            logger.info(f"Location ID {location_id} deleted. Verification of affected rows may be needed externally.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Database error deleting location ID {location_id}: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error deleting location ID {location_id}: {e}", exc_info=True)
            return False