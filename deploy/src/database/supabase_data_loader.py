# vivarium/deploy/src/database/supabase_data_loader.py
"""
A concrete data loading strategy for Supabase, which combines data retention
with data loading from a dump file or JSON files.

This module implements a composite data loader that first enforces a data
retention policy by deleting old records and then uses another data loader
strategy (e.g., PostgresDataLoader or JSONDataLoader) to load new data.
"""
import os
import sys
from datetime import datetime, timedelta
from typing import Optional

# Ensure the project root is in the path for imports to work correctly.
# This file is located at vivarium/deploy/src/database/supabase_data_loader.py,
# so three levels up will be the vivarium root: database/ -> src/ -> deploy/ -> vivarium/.
if __name__ == "__main__":
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..','..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from utilities.src.logger import LogHelper
from utilities.src.config import DatabaseConfig
from utilities.src.database_operations import DatabaseOperations

from deploy.src.database.data_loader_strategy import DataLoaderStrategy
from deploy.src.database.postgres_data_loader import PostgresDataLoader
from deploy.src.database.json_data_loader import JSONDataLoader

logger = LogHelper.get_logger(__name__)

class SupabaseDataLoader(DataLoaderStrategy):
    """
    A composite data loader strategy for Supabase, combining data retention
    with data ingestion from specified sources (SQL dump or JSON files).
    """
    # The number of months for which data should be retained.
    DATA_RETENTION_MONTHS = 12 
    
    def __init__(self, db_config: Optional[DatabaseConfig] = None,
                 dump_file_path: Optional[str] = None,
                 json_folder_path: Optional[str] = None):
        """
        Initializes the SupabaseDataLoader.

        This loader can be configured to use a Postgres dump or a folder of JSON
        files as its data source. It performs a data retention cleanup before
        loading new data.

        :param db_config: An optional :class:`DatabaseConfig` object for database connection.
                          If `None`, a new one will be created.
        :type db_config: Optional[DatabaseConfig]
        :param dump_file_path: The absolute path to a PostgreSQL dump file to load.
        :type dump_file_path: Optional[str]
        :param json_folder_path: The absolute path to a folder containing JSON files.
        :type json_folder_path: Optional[str]
        :raises ValueError: If both `dump_file_path` and `json_folder_path` are provided.
        """
        super().__init__()
        self.db_config = db_config if db_config is not None else DatabaseConfig()
        
        if dump_file_path and json_folder_path:
            raise ValueError(
                "Cannot load from both a dump file and a JSON folder. "
                "Please provide only one data source."
            )
        
        self.dump_file_path = dump_file_path
        self.json_folder_path = json_folder_path
        
        # db_ops is initialized here but its connection will be managed within
        # execute_full_data_load for retention and delegated to the sub-loader.
        self.db_ops = DatabaseOperations(self.db_config)
        logger.info("SupabaseDataLoader: Initialized for Supabase data loading with retention policy.")

    def load_from_dump(self) -> bool:
        """
        Loads data from a PostgreSQL dump file using the PostgresDataLoader strategy.

        This method is a proxy to the `PostgresDataLoader.load_from_dump` method.

        :returns: `True` if the dump is successfully restored, `False` otherwise.
        :rtype: bool
        """
        if self.dump_file_path:
            loader = PostgresDataLoader(file_path=self.dump_file_path, db_config=self.db_config)
            return loader.load_from_dump()
        logger.warning("SupabaseDataLoader: No dump file path provided. Skipping dump load.")
        return False

    def load_json_data(self) -> bool:
        """
        Loads data from JSON files using the JSONDataLoader strategy.

        This method is a proxy to the `JSONDataLoader.load_json_data` method.

        :returns: `True` if JSON data is successfully loaded, `False` otherwise.
        :rtype: bool
        """
        if self.json_folder_path:
            loader = JSONDataLoader(folder_path=self.json_folder_path, db_config=self.db_config)
            return loader.load_json_data()
        logger.warning("SupabaseDataLoader: No JSON folder path provided. Skipping JSON load.")
        return False

    def execute_full_data_load(self) -> bool:
        """
        Orchestrates the full data loading process for Supabase.

        This method first enforces the data retention policy by deleting old records,
        and then executes the chosen data loading strategy (dump or JSON files).

        :returns: `True` if both retention and data loading are successful, `False` otherwise.
        :rtype: bool
        """
        logger.info("SupabaseDataLoader: Starting full data loading process with retention enforcement.")
        
        # Step 1: Enforce data retention policy
        if not self._enforce_data_retention():
            logger.error("SupabaseDataLoader: Data retention enforcement failed. Aborting data load.")
            return False

        # Step 2: Delegate to the appropriate data loader based on configuration
        load_success = False
        if self.dump_file_path:
            logger.info(f"SupabaseDataLoader: Delegating to PostgresDataLoader for dump file load from {self.dump_file_path}.")
            loader = PostgresDataLoader(file_path=self.dump_file_path, db_config=self.db_config)
            load_success = loader.execute_full_data_load()
        elif self.json_folder_path:
            logger.info(f"SupabaseDataLoader: Delegating to JSONDataLoader for JSON folder load from {self.json_folder_path}.")
            loader = JSONDataLoader(folder_path=self.json_folder_path, db_config=self.db_config)
            load_success = loader.execute_full_data_load()
        else:
            logger.info("SupabaseDataLoader: No data source (dump or JSON) specified. Only retention was performed.")
            load_success = True # If no data source is specified, retention is the only task, so it's a success.

        if load_success:
            logger.info("SupabaseDataLoader: Full data loading process completed successfully.")
        else:
            logger.error("SupabaseDataLoader: Full data loading process failed.")

        return load_success
    
    # --- PRIVATE METHODS ---
    
    def _enforce_data_retention(self) -> bool:
        """
        Deletes all climate data older than the specified retention period.

        Data older than `DATA_RETENTION_MONTHS` will be deleted from the relevant tables.
        This is a critical operation for managing database size.

        :returns: `True` if the retention policy is successfully enforced, `False` otherwise.
        :rtype: bool
        """
        logger.info(f"Enforcing data retention policy: Deleting records older than {self.DATA_RETENTION_MONTHS} months.")
        
        # Calculate the cutoff date.
        cutoff_date = (datetime.now() - timedelta(days=self.DATA_RETENTION_MONTHS * 30)).strftime('%Y-%m-%d')
        logger.info(f"Cutoff date for data retention is: {cutoff_date}. All data before this date will be deleted.")

        # List of tables to apply retention to, ordered by foreign key dependencies.
        # This order is crucial to avoid foreign key constraint violations.
        tables_to_clean = [
            'weather_hour_data',
            'weather_day_data',
            'weather_astro_data',
            'weather_forecast_data',
            'weather_raw_data'
        ]

        try:
            # The Supabase 'service_role' user is needed for DELETE operations with RLS.
            # So we use the `service_role` configuration to connect.
            self.db_ops.connect(connection_type='service_role')
            conn = self.db_ops.connection
            
            with conn.cursor() as cursor:
                for table in tables_to_clean:
                    query = f"DELETE FROM {table} WHERE date < %s;"
                    try:
                        cursor.execute(query, (cutoff_date,))
                        deleted_rows = cursor.rowcount
                        logger.info(f"Successfully deleted {deleted_rows} rows from '{table}' with date before {cutoff_date}.")
                    except Exception as e:
                        logger.error(f"Failed to delete old data from '{table}': {e}", exc_info=True)
                        # We return False on the first failure to maintain data integrity.
                        return False
                
                # Commit the transaction after all deletions are successful.
                conn.commit()
                logger.info("Data retention successfully enforced and committed.")
                return True
        except Exception as e:
            logger.error(f"An error occurred during data retention enforcement: {e}", exc_info=True)
            return False
        finally:
            if self.db_ops:
                self.db_ops.close()