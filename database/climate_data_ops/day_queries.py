# vivarium# vivarium/database/climate_data_ops/day_queries.py

import psycopg2
from psycopg2 import sql
from typing import Optional, Dict, Any, Union

from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations
from database.climate_data_ops.base_query_strategy import BaseQuery

logger = LogHelper.get_logger(__name__)


class DayQueries(BaseQuery):
    """
    Manages database interactions for daily forecast data in the 'public.climate_day_data' table.
    """

    def __init__(self, db_operations: DBOperations):
        """
        Initializes the DayQueries instance.

        :param db_operations: An active DBOperations instance for database connectivity.
        """
        super().__init__(db_operations)
        logger.debug("DayQueries initialized.")

    def insert(self, location_id: int, forecast_date: str, day_data: Dict[str, Any]) -> bool:
        """
        Inserts new daily forecast data or updates existing data if a conflict occurs
        on (location_id, forecast_date).

        Implements the abstract 'insert' method from BaseQuery.

        :param location_id: The ID of the associated location.
        :param forecast_date: The date (YYYY-MM-DD) of the forecast.
        :param day_data: Dictionary containing daily forecast details.
                         Expected keys include: 'maxtemp_c', 'maxtemp_f', 'mintemp_c', etc.
        :return: True if the operation was successful (no error), False otherwise.
        """
        query = sql.SQL("""
            INSERT INTO public.climate_day_data (
                location_id, forecast_date, maxtemp_c, maxtemp_f,
                mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph,
                maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm,
                avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain,
                daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow,
                condition_code, uv
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (location_id, forecast_date) DO UPDATE SET
                maxtemp_c = EXCLUDED.maxtemp_c, maxtemp_f = EXCLUDED.maxtemp_f,
                mintemp_c = EXCLUDED.mintemp_c, mintemp_f = EXCLUDED.mintemp_f,
                avgtemp_c = EXCLUDED.avgtemp_c, avgtemp_f = EXCLUDED.avgtemp_f,
                maxwind_mph = EXCLUDED.maxwind_mph, maxwind_kph = EXCLUDED.maxwind_kph,
                totalprecip_mm = EXCLUDED.totalprecip_mm, totalprecip_in = EXCLUDED.totalprecip_in,
                totalsnow_cm = EXCLUDED.totalsnow_cm, avgvis_km = EXCLUDED.avgvis_km,
                avgvis_miles = EXCLUDED.avgvis_miles, avghumidity = EXCLUDED.avghumidity,
                daily_will_it_rain = EXCLUDED.daily_will_it_rain,
                daily_chance_of_rain = EXCLUDED.daily_chance_of_rain,
                daily_will_it_snow = EXCLUDED.daily_will_it_snow,
                daily_chance_of_snow = EXCLUDED.daily_chance_of_snow,
                condition_code = EXCLUDED.condition_code, uv = EXCLUDED.uv;
        """)
        condition_code = day_data.get('condition', {}).get('code')
        params = (
            location_id,
            forecast_date,
            day_data.get('maxtemp_c'), day_data.get('maxtemp_f'),
            day_data.get('mintemp_c'), day_data.get('mintemp_f'),
            day_data.get('avgtemp_c'), day_data.get('avgtemp_f'),
            day_data.get('maxwind_mph'), day_data.get('maxwind_kph'),
            day_data.get('totalprecip_mm'), day_data.get('totalprecip_in'),
            day_data.get('totalsnow_cm'), day_data.get('avgvis_km'),
            day_data.get('avgvis_miles'), day_data.get('avghumidity'),
            day_data.get('daily_will_it_rain'), day_data.get('daily_chance_of_rain'),
            day_data.get('daily_will_it_snow'), day_data.get('daily_chance_of_snow'),
            condition_code,
            day_data.get('uv')
        )
        try:
            self.db_ops.execute_query(query, params, fetch=False)
            logger.info(f"Daily forecast data for location ID {location_id}, date '{forecast_date}' inserted/updated.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Database error during insert/update for daily forecast (location ID {location_id}, date '{forecast_date}'): {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error during insert/update for daily forecast (location ID {location_id}, date '{forecast_date}'): {e}", exc_info=True)
            return False

    def get(self, location_id: int, forecast_date: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves daily forecast data from 'public.climate_day_data' by location ID and forecast date.

        Implements the abstract 'get' method from BaseQuery.

        :param location_id: The ID of the associated location.
        :param forecast_date: The date (YYYY-MM-DD) of the forecast.
        :return: A dictionary containing the daily forecast data if found, None otherwise.
        """
        query = sql.SQL("""
            SELECT
                location_id, forecast_date, maxtemp_c, maxtemp_f,
                mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph,
                maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm,
                avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain,
                daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow,
                condition_code, uv
            FROM public.climate_day_data
            WHERE location_id = %s AND forecast_date = %s;
        """)
        params = (location_id, forecast_date)
        try:
            result = self.db_ops.execute_query(query, params, fetch_one=True) # Changed from fetch=True to fetch_one=True
            if result:
                logger.info(f"Daily forecast data found for location ID {location_id}, date '{forecast_date}'.")
                return result
            logger.info(f"No daily forecast data found for location ID {location_id}, date '{forecast_date}'.")
            return None
        except psycopg2.Error as e:
            logger.error(f"Database error retrieving daily forecast for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error retrieving daily forecast for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return None

    def update(self, location_id: int, forecast_date: str, new_data: Dict[str, Any]) -> bool:
        """
        Updates an existing daily forecast record in 'public.climate_day_data'.

        Note: This method assumes `DBOperations.execute_query` returns True for successful
        DML operations or raises an exception on failure.

        Implements the abstract 'update' method from BaseQuery.

        :param location_id: The ID of the location associated with the forecast.
        :param forecast_date: The date (YYYY-MM-DD) of the forecast to update.
        :param new_data: Dictionary containing the new values for daily forecast attributes.
        :return: True if the update was successful (no error), False otherwise.
        """
        set_clauses = []
        params = []

        # Iterate through common fields for updates
        updatable_fields = [
            'maxtemp_c', 'maxtemp_f', 'mintemp_c', 'mintemp_f', 'avgtemp_c', 'avgtemp_f',
            'maxwind_mph', 'maxwind_kph', 'totalprecip_mm', 'totalprecip_in', 'totalsnow_cm',
            'avgvis_km', 'avgvis_miles', 'avghumidity', 'daily_will_it_rain',
            'daily_chance_of_rain', 'daily_will_it_snow', 'daily_chance_of_snow', 'uv'
        ]
        
        for field in updatable_fields:
            if field in new_data:
                set_clauses.append(sql.Identifier(field) + sql.SQL(' = %s'))
                params.append(new_data[field])
        
        # Handle condition_code separately if nested
        if 'condition' in new_data and 'code' in new_data['condition']:
            set_clauses.append(sql.Identifier('condition_code') + sql.SQL(' = %s'))
            params.append(new_data['condition']['code'])

        if not set_clauses:
            logger.warning(f"No valid fields provided to update for daily forecast (location ID {location_id}, date {forecast_date}).")
            return False

        query = sql.SQL("""
            UPDATE public.climate_day_data
            SET {}
            WHERE location_id = %s AND forecast_date = %s;
        """).format(sql.SQL(', ').join(set_clauses))
        params.extend([location_id, forecast_date])

        try:
            self.db_ops.execute_query(query, tuple(params), fetch=False)
            logger.info(f"Daily forecast for location ID {location_id}, date '{forecast_date}' updated. Verification of affected rows may be needed externally.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Database error updating daily forecast for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error updating daily forecast for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return False

    def delete(self, location_id: int, forecast_date: str) -> bool:
        """
        Deletes a daily forecast record from 'public.climate_day_data' by location ID and forecast date.

        Note: This method assumes `DBOperations.execute_query` returns True for successful
        DML operations or raises an exception on failure.

        Implements the abstract 'delete' method from BaseQuery.

        :param location_id: The ID of the location associated with the forecast.
        :param forecast_date: The date (YYYY-MM-DD) of the forecast to delete.
        :return: True if the deletion was successful (no error), False otherwise.
        """
        query = sql.SQL("""
            DELETE FROM public.climate_day_data
            WHERE location_id = %s AND forecast_date = %s;
        """)
        params = (location_id, forecast_date)

        try:
            self.db_ops.execute_query(query, params, fetch=False)
            logger.info(f"Daily forecast for location ID {location_id}, date '{forecast_date}' deleted. Verification of affected rows may be needed externally.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Database error deleting daily forecast for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error deleting daily forecast for location ID {location_id}, date '{forecast_date}': {e}", exc_info=True)
            return False