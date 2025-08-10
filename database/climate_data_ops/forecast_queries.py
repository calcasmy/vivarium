# vivarium/database/climate_data_ops/forecast_queries.py

import psycopg2
from psycopg2 import sql
from typing import Optional, Dict, Any, List, Union

from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations
from database.climate_data_ops.base_query_strategy import BaseQuery

logger = LogHelper.get_logger(__name__)


class ForecastQueries(BaseQuery):
    """
    Manages database interactions for forecast day data in the 'public.climate_forecast_day' table.
    """

    def __init__(self, db_operations: DBOperations):
        """
        Initializes the ForecastQueries instance.

        :param db_operations: An active DBOperations instance for database connectivity.
        """
        super().__init__(db_operations)
        logger.debug("ForecastQueries initialized.")

    def insert(self, location_id: int, forecast_data: Dict[str, Any]) -> Optional[int]:
        """
        Inserts new forecast day data or updates existing data if a conflict occurs
        on (location_id, forecast_date).

        Implements the abstract 'insert' method from BaseQuery.

        :param location_id: The ID of the associated location.
        :param forecast_data: Dictionary containing forecast day details. Expected keys:
                              'date', 'date_epoch'.
        :return: The 'location_id' of the inserted or updated forecast record if successful, None otherwise.
        """
        query = sql.SQL("""
            INSERT INTO public.climate_forecast_day (
                location_id, forecast_date, forecast_date_epoch
            ) VALUES (%s, %s, %s)
            ON CONFLICT (location_id, forecast_date) DO UPDATE SET
                forecast_date_epoch = EXCLUDED.forecast_date_epoch
            RETURNING location_id;
        """)
        params = (
            location_id,
            forecast_data.get('date'),
            forecast_data.get('date_epoch'),
        )

        try:
            result = self.db_ops.execute_query(query, params, fetch_one=True)
            if result and 'location_id' in result:
                logger.info(f"Forecast data for location ID {location_id}, date '{forecast_data.get('date')}' successfully inserted/updated.")
                return result['location_id']
            logger.warning(f"Insert/update for forecast (location ID {location_id}, date '{forecast_data.get('date')}') returned no location_id.")
            return None
        except psycopg2.Error as e:
            logger.error(f"Database error during insert/update for forecast (location ID {location_id}, date '{forecast_data.get('date')}'): {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error during insert/update for forecast (location ID {location_id}, date '{forecast_data.get('date')}'): {e}", exc_info=True)
            return None

    def get(self, location_id: int, forecast_date: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves a forecast record from 'public.climate_forecast_day' by location ID and date.

        Implements the abstract 'get' method from BaseQuery.

        :param location_id: The ID of the associated location.
        :param forecast_date: The date (YYYY-MM-DD) of the forecast.
        :return: A dictionary containing the forecast data if found, None otherwise.
        """
        query = sql.SQL("""
            SELECT
                location_id, forecast_date, forecast_date_epoch
            FROM public.climate_forecast_day
            WHERE location_id = %s AND forecast_date = %s;
        """)
        params = (location_id, forecast_date)

        try:
            result = self.db_ops.execute_query(query, params, fetch_one=True)
            if result:
                logger.info(f"Forecast data found for location ID {location_id}, date '{forecast_date}'.")
                return result
            logger.info(f"No forecast data found for location ID {location_id}, date '{forecast_date}'.")
            return None
        except psycopg2.Error as e:
            logger.error(f"Database error retrieving forecast for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error retrieving forecast for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return None

    def update(self, location_id: int, forecast_date: str, new_forecast_data: Dict[str, Any]) -> bool:
        """
        Updates an existing forecast record in 'public.climate_forecast_day'.

        Implements the abstract 'update' method from BaseQuery.

        :param location_id: The ID of the location associated with the forecast.
        :param forecast_date: The date (YYYY-MM-DD) of the forecast to update.
        :param new_forecast_data: Dictionary containing the new values for forecast attributes.
                                  Expected keys: 'forecast_date_epoch'.
        :return: True if the update was successful (no error), False otherwise.
        """
        set_clauses = []
        params = []
        
        if 'forecast_date_epoch' in new_forecast_data:
            set_clauses.append(sql.Identifier('forecast_date_epoch') + sql.SQL(' = %s'))
            params.append(new_forecast_data['forecast_date_epoch'])

        if not set_clauses:
            logger.warning(f"No valid fields provided to update for forecast (location ID {location_id}, date {forecast_date}).")
            return False

        query = sql.SQL("""
            UPDATE public.climate_forecast_day
            SET {}
            WHERE location_id = %s AND forecast_date = %s;
        """).format(sql.SQL(', ').join(set_clauses))
        params.extend([location_id, forecast_date])

        try:
            self.db_ops.execute_query(query, tuple(params), fetch=False)
            logger.info(f"Forecast for location ID {location_id}, date '{forecast_date}' updated. Verification of affected rows may be needed externally.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Database error updating forecast for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error updating forecast for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return False

    def delete(self, location_id: int, forecast_date: str) -> bool:
        """
        Deletes a forecast record from 'public.climate_forecast_day' by location ID and date.

        Implements the abstract 'delete' method from BaseQuery.

        :param location_id: The ID of the location associated with the forecast.
        :param forecast_date: The date (YYYY-MM-DD) of the forecast to delete.
        :return: True if the deletion was successful (no error), False otherwise.
        """
        query = sql.SQL("""
            DELETE FROM public.climate_forecast_day
            WHERE location_id = %s AND forecast_date = %s;
        """)
        params = (location_id, forecast_date)

        try:
            self.db_ops.execute_query(query, params, fetch=False)
            logger.info(f"Forecast for location ID {location_id}, date '{forecast_date}' deleted. Verification of affected rows may be needed externally.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Database error deleting forecast for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error deleting forecast for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return False