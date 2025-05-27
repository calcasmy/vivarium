# src/database/condition_queries.py

from typing import Dict, Optional
from utilities.src.database_operations import DatabaseOperations

class ConditionQueries(DatabaseOperations):
    def __init__(self, db_operations):
        super().__init__()
        self.conn = db_operations.get_connection()

    def insert_condition(self, condition_data: Dict) -> Optional[int]:
        """
        Inserts a condition record into the database.

        Args:
            condition_data (Dict): A dictionary containing condition data.
                                    Expected keys: 'code', 'text', 'icon'.

        Returns:
            Optional[int]: The condition code if the insertion was successful, otherwise None.
        """
        query = """
            INSERT INTO public.climate_condition (
                condition_code, text, icon
            ) VALUES (%s, %s, %s)
            ON CONFLICT (condition_code) DO NOTHING  -- Skip if the code already exists
            RETURNING condition_code;
            """
        params = (
            condition_data.get('code'),
            condition_data.get('text'),
            condition_data.get('icon'),
        )

        try:
            result = self.execute_query(query, params, fetch=True)
            if result:
                return result[0]['condition_code']
            else:
                return None  # Return None if no insertion occurred due to conflict
        except Exception as e:
            print(f"Error in insert_condition: {e}")
            return None
    def get_condition(self, condition_code: int) -> Optional[Dict]:
        """
        Retrieves a condition record from the database by its code.

        Args:
            condition_code (int): The condition code to retrieve.

        Returns:
            Optional[Dict]: A dictionary containing the condition data if found, otherwise None.
        """
        query = """
            SELECT
                condition_code, text, icon
            FROM public.climate_condition
            WHERE condition_code = %s;
            """
        params = (condition_code,)
        result = self.execute_query(query, params, fetch=True)
        if result:
            return result[0]
        else:
            return None