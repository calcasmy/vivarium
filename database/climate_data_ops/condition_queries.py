# vivarium/database/climate_data_ops/condition_queries.py

import psycopg2
from psycopg2 import sql
from typing import Optional, Dict, Any, List, Union

from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations
from database.climate_data_ops.base_query_strategy import BaseQuery

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
        Inserts a new weather condition into the 'public.climate_condition' table if it does not
        already exist (based on 'code').

        :param condition_data: A dictionary containing condition details, expected to have
                               'code' (int), 'text' (str), and 'icon' (str).
        :type condition_data: Dict[str, Any]
        :returns: The unique identifier (code) of the inserted or existing condition,
                  or ``None`` if an error occurs.
        :rtype: Optional[int]
        """
        code = condition_data.get('code')
        text = condition_data.get('text')
        icon = condition_data.get('icon')

        if code is None:
            logger.error("Attempted to insert condition without a 'code'.")
            return None

        # Check if condition already exists
        existing_code = self.get(code)
        if existing_code is not None:
            logger.info(f"Condition with code {code} already exists.")
            return existing_code

        query = """
            INSERT INTO public.climate_condition (condition_code, text, icon)
            VALUES (%s, %s, %s)
            ON CONFLICT (condition_code) DO UPDATE SET
                text = EXCLUDED.text,
                icon = EXCLUDED.icon
            RETURNING condition_code;
        """
        params = (code, text, icon)

        try:
            # ON CONFLICT DO UPDATE RETURNING code ensures we get the code even if updated
            inserted_code = self.db_ops.execute_query_with_returning_id(query, params)
            if inserted_code is not None:
                logger.info(f"Condition code {inserted_code} inserted/updated.")
                return inserted_code
            else:
                logger.error(f"Failed to insert or update condition with code {code}. No code returned.")
                return None
        except psycopg2.errors.UniqueViolation:
            logger.info(f"Condition with code {code} already exists (caught unique violation after initial check).")
            return code # Assume it exists and return the code
        except Exception as e:
            logger.error(f"Error inserting/updating condition with code {code}: {e}", exc_info=True)
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