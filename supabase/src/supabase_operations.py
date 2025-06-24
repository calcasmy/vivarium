# supabase/src/supabase_operations.py
from supabase import create_client, Client
import sys
import os

if __name__ == "__main__":
    vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..'))

    if vivarium_path not in sys.path:
        sys.path.insert(0, vivarium_path)
from utilities.src.config import SupabaseConfig

class SupabaseOperations:
    """
    Handles data operations (insert, select, upsert) for Supabase using the
    client (anon) key.
    """
    def __init__(self, table_name: str):
        self.config = SupabaseConfig()
        self.supabase: Client = create_client(self.config.url, self.config.anon_key)
        self.table_name = table_name

    def insert_data(self, data: dict | list) -> dict:
        """
        Inserts one or more records into the specified Supabase table.

        Args:
            data (dict | list): A dictionary for a single row or a list of dictionaries for multiple rows.

        Returns:
            dict: The response data from Supabase.
        Raises:
            Exception: If the insert operation fails.
        """
        try:
            response = self.supabase.table(self.table_name).insert(data).execute()
            # Supabase Python client returns a Response object
            # The actual data is in response.data
            if response.data:
                print(f"Successfully inserted {len(response.data)} records into '{self.table_name}'.")
                return response.data
            else:
                # Handle cases where insert might return minimal response but no error
                print(f"Insert operation completed, but no data returned for '{self.table_name}'.")
                return {}
        except Exception as e:
            print(f"ERROR: Failed to insert data into '{self.table_name}': {e}", file=sys.stderr)
            raise

    def select_data(self, filters: dict = None, columns: str = "*") -> list:
        """
        Selects data from the specified Supabase table with optional filters.

        Args:
            filters (dict, optional): A dictionary of column-value pairs to filter by (e.g., {"column_name": "value"}).
                                      Currently supports only equality filters.
            columns (str, optional): A comma-separated string of columns to select. Defaults to "*".

        Returns:
            list: A list of dictionaries, where each dictionary represents a row.
        Raises:
            Exception: If the select operation fails.
        """
        try:
            query = self.supabase.table(self.table_name).select(columns)
            if filters:
                for column, value in filters.items():
                    query = query.eq(column, value) # Apply equality filter

            response = query.execute()
            if response.data is not None:
                print(f"Successfully selected {len(response.data)} records from '{self.table_name}'.")
                return response.data
            return []
        except Exception as e:
            print(f"ERROR: Failed to select data from '{self.table_name}': {e}", file=sys.stderr)
            raise

    def upsert_data(self, data: dict | list, on_conflict: str = None) -> dict:
        """
        Inserts or updates data in the specified Supabase table.
        If a row with the same primary key (or columns specified in on_conflict) exists, it's updated.

        Args:
            data (dict | list): A dictionary for a single row or a list of dictionaries for multiple rows.
                                Must include primary key columns for upsert to work.
            on_conflict (str, optional): A comma-separated string of columns that define the unique
                                         constraint for conflict resolution (e.g., "id", "column1,column2").
                                         If None, Supabase attempts to infer the primary key.

        Returns:
            dict: The response data from Supabase.
        Raises:
            Exception: If the upsert operation fails.
        """
        try:
            query = self.supabase.table(self.table_name).upsert(data)
            if on_conflict:
                query = query.on_conflict(on_conflict)

            response = query.execute()

            if response.data:
                print(f"Successfully upserted {len(response.data)} records into '{self.table_name}'.")
                return response.data
            else:
                print(f"Upsert operation completed, but no data returned for '{self.table_name}'.")
                return {}
        except Exception as e:
            print(f"ERROR: Failed to upsert data into '{self.table_name}': {e}", file=sys.stderr)
            raise

# Example Usage (for testing purposes, if you run this file directly)
if __name__ == "__main__":
    # Ensure SUPABASE_URL_CLIENT and SUPABASE_ANON_KEY are set as environment variables
    # DO NOT COMMIT ACTUAL KEYS TO VERSION CONTROL

    # Create a dummy table in your Supabase project for testing, e.g., 'test_data'
    # with columns: id (int, PK), name (text), value (int)

    test_table_name = "test_data"
    try:
        supabase_ops = SupabaseOperations(table_name=test_table_name)

        # 1. Insert a single record
        print("\n--- Inserting single record ---")
        single_record = {"id": 1, "name": "item_A", "value": 100}
        inserted_data = supabase_ops.insert_data(single_record)
        print(f"Inserted: {inserted_data}")

        # 2. Upsert multiple records (some new, some updating existing)
        print("\n--- Upserting multiple records ---")
        multi_records = [
            {"id": 2, "name": "item_B", "value": 200}, # New
            {"id": 3, "name": "item_C", "value": 300}, # New
            {"id": 1, "name": "item_A_updated", "value": 150} # Update existing ID 1
        ]
        upserted_data = supabase_ops.upsert_data(multi_records, on_conflict="id")
        print(f"Upserted: {upserted_data}")

        # 3. Select all data
        print("\n--- Selecting all data ---")
        all_data = supabase_ops.select_data()
        print(f"All Data: {all_data}")

        # 4. Select data with a filter
        print("\n--- Selecting data with filter ---")
        filtered_data = supabase_ops.select_data(filters={"name": "item_B"})
        print(f"Filtered Data (name='item_B'): {filtered_data}")

    except Exception as e:
        print(f"An error occurred during Supabase operations example: {e}", file=sys.stderr)