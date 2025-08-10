# vivarium# vivarium/database/climate_data_ops/astro_queries.py

import psycopg2
from psycopg2 import sql
from typing import Optional, Dict, Any, Union

from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations
from database.climate_data_ops.base_query_strategy import BaseQuery

logger = LogHelper.get_logger(__name__)


class AstroQueries(BaseQuery):
    """
    Manages database interactions for astronomical forecast data in the 'public.climate_astro_data' table.
    """

    def __init__(self, db_operations: DBOperations):
        """
        Initializes the AstroQueries instance.

        :param db_operations: An active DBOperations instance for database connectivity.
        """
        super().__init__(db_operations)
        logger.debug("AstroQueries initialized.")

    def insert(self, location_id: int, forecast_date: str, astro_data: Dict[str, Any]) -> bool:
        """
        Inserts new astronomical forecast data or updates existing data if a conflict occurs
        on (location_id, forecast_date).

        Implements the abstract 'insert' method from BaseQuery.

        :param location_id: The ID of the associated location.
        :param forecast_date: The date (YYYY-MM-DD) of the forecast.
        :param astro_data: Dictionary containing astronomical forecast details.
                           Expected keys: 'sunrise', 'sunset', 'moonrise', 'moonset',
                           'moon_phase', 'moon_illumination'.
        :return: True if the operation was successful (no error), False otherwise.
        """
        query = sql.SQL("""
            INSERT INTO public.climate_astro_data (
                location_id, forecast_date, sunrise, sunset, moonrise,
                moonset, moon_phase, moon_illumination
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (location_id, forecast_date) DO UPDATE SET
                sunrise = EXCLUDED.sunrise, sunset = EXCLUDED.sunset,
                moonrise = EXCLUDED.moonrise, moonset = EXCLUDED.moonset,
                moon_phase = EXCLUDED.moon_phase, moon_illumination = EXCLUDED.moon_illumination;
        """)
        params = (
            location_id,
            forecast_date,
            astro_data.get('sunrise'), astro_data.get('sunset'),
            astro_data.get('moonrise'), astro_data.get('moonset'),
            astro_data.get('moon_phase'), astro_data.get('moon_illumination'),
        )
        try:
            self.db_ops.execute_query(query, params, fetch=False)
            logger.info(f"Astro data for location ID {location_id}, date '{forecast_date}' inserted/updated.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Database error during insert/update for astro data (location ID {location_id}, date '{forecast_date}'): {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error during insert/update for astro data (location ID {location_id}, date '{forecast_date}'): {e}", exc_info=True)
            return False

    def get(self, location_id: int, forecast_date: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves astronomical forecast data from 'public.climate_astro_data' by location ID and forecast date.

        Implements the abstract 'get' method from BaseQuery.

        :param location_id: The ID of the associated location.
        :param forecast_date: The date (YYYY-MM-DD) of the forecast.
        :return: A dictionary containing the astronomical forecast data if found, None otherwise.
        """
        query = sql.SQL("""
            SELECT
                location_id, forecast_date, sunrise, sunset, moonrise,
                moonset, moon_phase, moon_illumination
            FROM public.climate_astro_data
            WHERE location_id = %s AND forecast_date = %s;
        """)
        params = (location_id, forecast_date)
        try:
            result = self.db_ops.execute_query(query, params, fetch_one=True)
            if result:
                logger.info(f"Astro data found for location ID {location_id}, date '{forecast_date}'.")
                return result
            logger.info(f"No astro data found for location ID {location_id}, date '{forecast_date}'.")
            return None
        except psycopg2.Error as e:
            logger.error(f"Database error retrieving astro data for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error retrieving astro data for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return None

    def update(self, location_id: int, forecast_date: str, new_data: Dict[str, Any]) -> bool:
        """
        Updates an existing astronomical forecast record in 'public.climate_astro_data'.

        Note: This method assumes `DBOperations.execute_query` returns True for successful
        DML operations or raises an exception on failure.

        Implements the abstract 'update' method from BaseQuery.

        :param location_id: The ID of the location associated with the forecast.
        :param forecast_date: The date (YYYY-MM-DD) of the forecast to update.
        :param new_data: Dictionary containing the new values for astronomical forecast attributes.
        :return: True if the update was successful (no error), False otherwise.
        """
        set_clauses = []
        params = []

        updatable_fields = [
            'sunrise', 'sunset', 'moonrise', 'moonset', 'moon_phase', 'moon_illumination'
        ]

        for field in updatable_fields:
            if field in new_data:
                set_clauses.append(sql.Identifier(field) + sql.SQL(' = %s'))
                params.append(new_data[field])

        if not set_clauses:
            logger.warning(f"No valid fields provided to update for astro data (location ID {location_id}, date {forecast_date}).")
            return False

        query = sql.SQL("""
            UPDATE public.climate_astro_data
            SET {}
            WHERE location_id = %s AND forecast_date = %s;
        """).format(sql.SQL(', ').join(set_clauses))
        params.extend([location_id, forecast_date])

        try:
            self.db_ops.execute_query(query, tuple(params), fetch=False)
            logger.info(f"Astro data for location ID {location_id}, date '{forecast_date}' updated. Verification of affected rows may be needed externally.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Database error updating astro data for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error updating astro data for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return False

    def delete(self, location_id: int, forecast_date: str) -> bool:
        """
        Deletes an astronomical forecast record from 'public.climate_astro_data' by location ID and forecast date.

        Note: This method assumes `DBOperations.execute_query` returns True for successful
        DML operations or raises an exception on failure.

        Implements the abstract 'delete' method from BaseQuery.

        :param location_id: The ID of the location associated with the forecast.
        :param forecast_date: The date (YYYY-MM-DD) of the forecast to delete.
        :return: True if the deletion was successful (no error), False otherwise.
        """
        query = sql.SQL("""
            DELETE FROM public.climate_astro_data
            WHERE location_id = %s AND forecast_date = %s;
        """)
        params = (location_id, forecast_date)

        try:
            self.db_ops.execute_query(query, params, fetch=False)
            logger.info(f"Astro data for location ID {location_id}, date '{forecast_date}' deleted. Verification of affected rows may be needed externally.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Database error deleting astro data for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error deleting astro data for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return False

    def get_sunrise_sunset(self, location_id: int, forecast_date: str) -> Optional[Dict[str, str]]:
        """
        Retrieves only sunrise and sunset times for a given location and date from 'public.climate_astro_data'.

        :param location_id: The ID of the location.
        :param forecast_date: The date (YYYY-MM-DD) of the forecast.
        :return: A dictionary with 'sunrise' and 'sunset' times if found, None otherwise.
        """
        query = sql.SQL("""
            SELECT sunrise, sunset
            FROM public.climate_astro_data
            WHERE location_id = %s AND forecast_date = %s;
        """)
        params = (location_id, forecast_date)
        try:
            result = self.db_ops.execute_query(query, params, fetch_one=True)
            if result:
                logger.info(f"Sunrise/sunset found for location ID {location_id}, date '{forecast_date}'.")
                return {'sunrise': result.get('sunrise'), 'sunset': result.get('sunset')}
            logger.info(f"No sunrise/sunset data found for location ID {location_id}, date '{forecast_date}'.")
            return None
        except psycopg2.Error as e:
            logger.error(f"Database error retrieving sunrise/sunset for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error retrieving sunrise/sunset for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return None
    
    def get_latest_sunrise_sunset(self, location_id: int) -> Dict | None:
        """
        Fetches the latest available sunrise/sunset data from the table.
        """
        query = """
            SELECT sunrise, sunset 
            FROM climate_astro_data 
            WHERE location_id = %s
            ORDER BY date DESC 
            LIMIT 1;
        """
        params = (location_id,)
        result = self.db_operations.execute_query(query, params, fetch_one=True)
        return result