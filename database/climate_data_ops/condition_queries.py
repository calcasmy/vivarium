# vivarium/database/climate_data_ops/condition_queries.py

import psycopg2
from psycopg2 import sql
from typing import Optional, Dict, Any, List, Union

from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations
from strategies.src.base_query import BaseQuery

logger = LogHelper.get_logger(__name__)


class ConditionQueries(BaseQuery):
    """
    Manages database interactions for condition data in the 'public.climate_condition' table.
    """

    def __init__(self, db_operations: DBOperations):
        """
        Initializes the ConditionQueries instance.

        :param db_operations: An active DBOperations instance for database connectivity.
        """
        super().__init__(db_operations)
        logger.debug("ConditionQueries initialized.")

    def insert(self, condition_data: Dict[str, Any]) -> Optional[int]:
        """
        Inserts a new condition record into 'public.climate_condition'.

        If a condition with the same 'condition_code' already exists, the operation
        does nothing (due to ON CONFLICT DO NOTHING clause).

        Implements the abstract 'insert' method from BaseQuery.

        :param condition_data: Dictionary containing condition data.
                               Expected keys: 'code' (int), 'text' (str), 'icon' (str).
        :return: The 'condition_code' of the inserted row if successful, None if no row
                 was inserted (e.g., due to conflict) or on error.
        """
        query = sql.SQL("""
            INSERT INTO public.climate_condition (
                condition_code, text, icon
            ) VALUES (%s, %s, %s)
            ON CONFLICT (condition_code) DO NOTHING
            RETURNING condition_code;
        """)
        params = (
            condition_data.get('code'),
            condition_data.get('text'),
            condition_data.get('icon'),
        )

        try:
            result = self.db_ops.execute_query(query, params, fetch_one=True)
            if result and 'condition_code' in result:
                logger.info(f"Condition code {result['condition_code']} successfully inserted.")
                return result['condition_code']
            logger.info(f"Condition code {condition_data.get('code')} already exists or no row was returned.")
            return None
        except psycopg2.Error as e:
            logger.error(f"Database error during insert for condition code {condition_data.get('code')}: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error during insert for condition code {condition_data.get('code')}: {e}", exc_info=True)
            return None

    def get(self, condition_code: int) -> Optional[Dict[str, Any]]:
        """
        Retrieves a condition record from 'public.climate_condition' by its code.

        Implements the abstract 'get' method from BaseQuery.

        :param condition_code: The condition code to retrieve.
        :return: A dictionary containing the condition data if found, None otherwise.
        """
        query = sql.SQL("""
            SELECT
                condition_code, text, icon
            FROM public.climate_condition
            WHERE condition_code = %s;
        """)
        params = (condition_code,)

        try:
            result = self.db_ops.execute_query(query, params, fetch_one=True)
            if result:
                logger.info(f"Condition data found for code: {condition_code}.")
                return result
            logger.info(f"No condition data found for code: {condition_code}.")
            return None
        except psycopg2.Error as e:
            logger.error(f"Database error retrieving condition for code {condition_code}: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error retrieving condition for code {condition_code}: {e}", exc_info=True)
            return None

    def update(self, condition_code: int, new_data: Dict[str, Any]) -> bool:
        """
        Updates an existing condition record in 'public.climate_condition' by its code.

        Note: This method assumes `DBOperations.execute_query` returns True for successful
        DML operations or raises an exception on failure.

        Implements the abstract 'update' method from BaseQuery.

        :param condition_code: The code of the condition record to update.
        :param new_data: A dictionary containing the new values for 'text' and/or 'icon'.
        :return: True if the update was successful (no error), False otherwise.
        """
        set_clauses = []
        params = []

        if 'text' in new_data:
            set_clauses.append(sql.Identifier('text') + sql.SQL(' = %s'))
            params.append(new_data['text'])
        if 'icon' in new_data:
            set_clauses.append(sql.Identifier('icon') + sql.SQL(' = %s'))
            params.append(new_data['icon'])

        if not set_clauses:
            logger.warning(f"No valid fields provided to update for condition code {condition_code}.")
            return False

        query = sql.SQL("""
            UPDATE public.climate_condition
            SET {}
            WHERE condition_code = %s;
        """).format(sql.SQL(', ').join(set_clauses))
        params.append(condition_code)

        try:
            self.db_ops.execute_query(query, tuple(params), fetch=False)
            logger.info(f"Condition code {condition_code} updated. Verification of affected rows may be needed externally.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Database error updating condition code {condition_code}: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error updating condition code {condition_code}: {e}", exc_info=True)
            return False

    def delete(self, condition_code: int) -> bool:
        """
        Deletes a condition record from 'public.climate_condition' by its code.

        Note: This method assumes `DBOperations.execute_query` returns True for successful
        DML operations or raises an exception on failure.

        Implements the abstract 'delete' method from BaseQuery.

        :param condition_code: The code of the condition record to delete.
        :return: True if the deletion was successful (no error), False otherwise.
        """
        query = sql.SQL("""
            DELETE FROM public.climate_condition
            WHERE condition_code = %s;
        """)
        params = (condition_code,)

        try:
            self.db_ops.execute_query(query, params, fetch=False)
            logger.info(f"Condition code {condition_code} deleted. Verification of affected rows may be needed externally.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Database error deleting condition code {condition_code}: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error deleting condition code {condition_code}: {e}", exc_info=True)
            return False