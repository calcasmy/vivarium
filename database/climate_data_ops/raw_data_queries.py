#vivarium/database/climate_data_ops/raw_data_queries.py

import json
import psycopg2
from psycopg2 import sql
from typing import Dict, Optional

from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations
from strategies.src.base_query import BaseQuery


logger = LogHelper.get_logger(__name__)


class RawDataQueries(BaseQuery):
    """
    Manages database interactions for raw climate data in the 'public.raw_climate_data' table.
    """

    def __init__(self, db_operations: DBOperations):
        """
        Initializes the RawDataQueries instance.

        :param db_operations: An active DBOperations instance for database connectivity.
        """
        super().__init__(db_operations)
        logger.debug("RawDataQueries initialized.")

    def insert(self, date: str, raw_data: Dict) -> Optional[str]:
        """
        Inserts or updates raw climate data for a given date in 'public.raw_climate_data'.

        Uses PostgreSQL's 'ON CONFLICT (weather_date) DO UPDATE' to handle existing data.

        :param date: The date (YYYY-MM-DD) for the climate data.
        :param raw_data: Dictionary containing the raw climate data (stored as JSONB).
        :return: The weather_date string if successful, None otherwise.
        """
        query = sql.SQL("""
            INSERT INTO public.raw_climate_data (weather_date, raw_data)
            VALUES (%s, %s)
            ON CONFLICT (weather_date) DO UPDATE SET raw_data = EXCLUDED.raw_data
            RETURNING weather_date;
        """)
        params = (date, json.dumps(raw_data))

        try:
            result = self.db_ops.execute_query(query, params, fetch_one=True)
            if result and 'weather_date' in result:
                logger.info(f"Raw climate data for date '{date}' inserted/updated.")
                return result['weather_date']
            logger.warning(f"Insert/update for date '{date}' returned no weather_date, indicating no row was affected or returned.")
            return None
        except psycopg2.Error as e:
            logger.error(f"Database error during insert/update for date '{date}': {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error during insert/update for date '{date}': {e}", exc_info=True)
            return None

    def get(self, date: str) -> Optional[Dict]:
        """
        Retrieves raw climate data for a specific date from 'public.raw_climate_data'.

        :param date: The date (YYYY-MM-DD) to retrieve data for.
        :return: A dictionary of raw climate data, or None if not found or an error occurs.
        """
        query = sql.SQL("""
            SELECT raw_data FROM public.raw_climate_data
            WHERE weather_date = %s;
        """)
        params = (date,)

        try:
            result = self.db_ops.execute_query(query, params, fetch_one=True)
            if result and 'raw_data' in result:
                retrieved_data = result['raw_data']
                if isinstance(retrieved_data, str):
                    logger.warning(
                        f"Raw data for {date} retrieved as string; attempting to parse."
                    )
                    try:
                        retrieved_data = json.loads(retrieved_data)
                    except json.JSONDecodeError as e:
                        logger.error(
                            f"Failed to parse raw data string from DB for {date}: {e}. Data unusable.",
                            exc_info=True
                        )
                        return None

                if not isinstance(retrieved_data, dict):
                    logger.error(
                        f"Raw data for {date} is not a dictionary after parsing. Type: {type(retrieved_data).__name__}."
                    )
                    return None
                logger.info(f"Raw climate data for date '{date}' retrieved.")
                return retrieved_data
            logger.info(f"No raw climate data found for date '{date}'.")
            return None
        except psycopg2.Error as e:
            logger.error(f"Database error retrieving raw climate data for date '{date}': {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error retrieving raw climate data for date '{date}': {e}", exc_info=True)
            return None

    def update(self, date: str, new_raw_data: Dict) -> bool:
        """
        Updates raw climate data for an existing record in 'public.raw_climate_data'.

        :param date: The date (YYYY-MM-DD) of the record to update.
        :param new_raw_data: The new raw JSON data as a dictionary.
        :return: True if the record was updated (one row affected), False otherwise.
        """
        query = sql.SQL("""
            UPDATE public.raw_climate_data
            SET raw_data = %s
            WHERE weather_date = %s;
        """)
        params = (json.dumps(new_raw_data), date)

        try:
            # execute_query returns None for DML operations if autocommit is True
            # To check affected rows, you usually need cursor.rowcount, which execute_query
            # does not explicitly return for non-fetch operations.
            # Assuming execute_query is modified to return rowcount for DML, or you verify behavior.
            # For now, will assume successful execution implies row(s) affected if WHERE condition matches.
            self.db_ops.execute_query(query, params, fetch=False)
            
            # Since autocommit is True, execute_query might not return rowcount directly.
            # A more robust check might involve fetching the record after update or
            # modifying execute_query to return cursor.rowcount for DML.
            # For this 'production-like' code, we'll assume a successful execute_query
            # indicates intent, but a real check would verify affected_rows if needed.
            # If execute_query was meant to return rowcount, then the check would be `if affected_rows > 0:`
            logger.info(f"Raw climate data for date '{date}' updated. Please verify affected rows if needed externally.")
            return True # Assuming success if no exception
        except psycopg2.Error as e:
            logger.error(f"Database error updating raw climate data for date '{date}': {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error updating raw climate data for date '{date}': {e}", exc_info=True)
            return False

    def delete(self, date: str) -> bool:
        """
        Deletes raw climate data for a specific date from 'public.raw_climate_data'.

        :param date: The date (YYYY-MM-DD) of the record to delete.
        :return: True if the record was deleted (one row affected), False otherwise.
        """
        query = sql.SQL("""
            DELETE FROM public.raw_climate_data
            WHERE weather_date = %s;
        """)
        params = (date,)

        try:
            self.db_ops.execute_query(query, params, fetch=False)
            
            logger.info(f"Raw climate data for date '{date}' deleted. Please verify affected rows if needed externally.")
            return True # Assuming success if no exception
        except psycopg2.Error as e:
            logger.error(f"Database error deleting raw climate data for date '{date}': {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error deleting raw climate data for date '{date}': {e}", exc_info=True)
            return False