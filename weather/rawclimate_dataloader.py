# vivarium/deploy/src/database/json_data_load.py

import os
import json
import re
from datetime import datetime
import sys
from typing import Optional, Dict, List

# Ensure the project root is in the path for imports to work correctly
if __name__ == "__main__":
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..','..'))
    if project_root not in sys.path:
        sys.sys.path.insert(0, project_root)

from utilities.src.logger import LogHelper
from utilities.src.database_operations import DatabaseOperations
from utilities.src.config import FileConfig, WeatherAPIConfig, DatabaseConfig

from deploy.src.database.data_loader_strategy import DataLoaderStrategy


logger = LogHelper.get_logger(__name__)

class JSONDataLoader(DataLoaderStrategy):
    """
    A concrete data loading strategy for ingesting raw climate data from local JSON files
    into the database. This class implements the DataLoaderStrategy interface.
    """
    def __init__(self, folder_path: Optional[str] = None,
                 processed_data_path: Optional[str] = None, # Re-added for file moving capability
                 db_config: Optional[DatabaseConfig] = None):
        """
        Initializes the JSONDataLoader with paths and database configuration.

        :param folder_path: Path to the directory containing raw weather JSON files.
                                Defaults to the value from FileConfig.
        :type folder_path: Optional[str]
        :param processed_data_path: Path where processed data files are moved after loading.
                                    Defaults to the value from FileConfig.
                                    Set to None or empty string if no archiving is desired.
        :type processed_data_path: Optional[str]
        :param db_config: Database configuration object. Defaults to a new DatabaseConfig instance.
        :type db_config: Optional[DatabaseConfig]
        """
        super().__init__()

        # Corrected variable name consistency: using folder_path consistently
        self.folder_path = folder_path if folder_path is not None else FileConfig.RAW_WEATHER_DATA_PATH
        self.processed_data_path = processed_data_path if processed_data_path is not None else FileConfig.PROCESSED_WEATHER_DATA_PATH
        self.db_config = db_config if db_config is not None else DatabaseConfig()

        # Initialize DatabaseOperations instance
        self.database_ops = DatabaseOperations(self.db_config)

        logger.info(f"JSONDataLoader initialized. Raw files folder: {self.folder_path}")
        if self.processed_data_path:
            os.makedirs(self.processed_data_path, exist_ok=True)
            logger.info(f"Processed data will be moved to: {self.processed_data_path}")
        else:
            logger.info("No processed data path specified. Processed files will remain in raw folder.")


    def load_from_dump(self, dump_file_path: str) -> bool:
        """
        This method is not applicable for JSONDataLoader. It logs a warning and returns False.

        :param dump_file_path: The absolute path to the database dump file (ignored).
        :type dump_file_path: str
        :returns: False, as loading from a dump is not supported by this strategy.
        :rtype: bool
        """
        logger.warning(
            f"JSONDataLoader does not support loading from database dumps. "
            f"Ignoring dump_file_path: {dump_file_path}"
        )
        return False

    def load_raw_climate_data(self) -> bool:
        """
        Loads raw climate data from local JSON files into the database.

        This method reads JSON files from the specified `folder_path`,
        processes the data, inserts/updates it into the database,
        and optionally moves the processed files to `processed_data_path`.

        :returns: True if all files are processed successfully, False otherwise.
        :rtype: bool
        """
        logger.info("Starting JSON data loading process from local files.")

        processed_count = 0
        success = True

        try:
            if not os.path.exists(self.folder_path):
                logger.error(f"JSON data folder does not exist: {self.folder_path}")
                return False
            if not os.path.isdir(self.folder_path):
                logger.error(f"Provided path is not a directory: {self.folder_path}")
                return False
            
            json_files = [f for f in os.listdir(self.folder_path) if f.endswith('.json')]
            if not json_files:
                logger.info(f"No JSON files found in raw data folder: {self.folder_path}")
                return True # No files to process, considered a success for this operation

            # Sort files to ensure consistent processing order, useful for chronological data
            for filename in sorted(json_files): 
                file_path = os.path.join(self.folder_path, filename)
                try: 
                    logger.debug(f"Processing and loading data from {filename}...")

                    # Call the helper method to process and insert a single file
                    if self._process_single_file(file_path):
                        processed_count += 1
                        logger.info(f"Successfully processed {filename}")
                        # Move file if processed_data_path is configured
                        if self.processed_data_path:
                            new_file_path = os.path.join(self.processed_data_path, filename)
                            os.rename(file_path, new_file_path)
                            logger.debug(f"Moved {filename} to {self.processed_data_path}")
                    else:
                        success = False # A single file processing failure marks overall as False

                except json.JSONDecodeError:
                    logger.error(f"Error decoding JSON from file: {filename}")
                    success = False
                except FileNotFoundError:
                    logger.error(f"File not found: {filename}. Skipping.")
                    success = False
                except Exception as e:
                    logger.error(f"An unexpected error occurred while processing {filename}: {e}", exc_info=True)
                    success = False

        except Exception as e:
            logger.error(f"Error accessing raw climate data directory {self.folder_path}: {e}", exc_info=True)
            success = False

        logger.info(f"Raw climate data loading completed. Processed {processed_count} files. Overall success: {success}")
        return success

    def execute_data_load(self, dump_file_path: Optional[str] = None) -> bool:
        """
        Orchestrates the full data loading process for raw climate data.

        For JSONDataLoader, this primarily means loading from raw files.
        It will not utilize a database dump file.

        :param dump_file_path: Optional absolute path to a database dump file. If provided,
                               this parameter will be ignored by this strategy.
        :type dump_file_path: Optional[str]
        :returns: True if the raw climate data loading is successful, False otherwise.
        :rtype: bool
        """
        logger.info("Executing full data load using JSONDataLoader strategy.")

        if dump_file_path:
            logger.warning(
                f"Dump file path '{dump_file_path}' provided, but JSONDataLoader "
                "is designed for raw file ingestion, not database dumps. "
                "The dump will be ignored."
            )
            # You could optionally call self.load_from_dump(dump_file_path) here,
            # which would log the warning as per its implementation above.

        # Proceed with loading raw climate data
        raw_load_success = self.load_raw_climate_data()

        if raw_load_success:
            logger.info("Full data load (raw climate data) completed successfully.")
            return True
        else:
            logger.error("Full data load (raw climate data) failed.")
            return False

    def _check_raw_data_exists(self, weather_date: str) -> bool:
        """
        Checks if raw climate data for a given date already exists in the database.

        :param weather_date: The date string (YYYY-MM-DD) to check.
        :type weather_date: str
        :returns: True if data exists, False otherwise.
        :rtype: bool
        """
        query = """
            SELECT raw_data FROM raw_climate_data
            WHERE weather_date = %s;
        """
        try:
            # Use self.database_ops which is initialized in __init__
            result = self.database_ops.execute_query(query, (weather_date,), fetch=True)
            if result:
                logger.info(f"Raw climate data already exists for {weather_date}.")
                return True
            return False
        except Exception as e:
            logger.error(f"Error checking raw climate data existence for {weather_date}: {e}", exc_info=True)
            # If the table doesn't exist, this will also raise an error,
            # so we assume it doesn't exist and proceed to insert.
            return False

    def _insert_raw_climate_data(self, weather_date: str, raw_data: Dict) -> bool:
        """
        Inserts raw climate data into the database.
        Uses ON CONFLICT DO UPDATE to handle existing entries, effectively an upsert.

        :param weather_date: The date string (YYYY-MM-DD) for which data is being inserted.
        :type weather_date: str
        :param raw_data: The dictionary containing the raw JSON data to be stored.
        :type raw_data: Dict
        :returns: True if the data is successfully inserted or updated, False otherwise.
        :rtype: bool
        """
        query = """
            INSERT INTO raw_climate_data (weather_date, raw_data)
            VALUES (%s, %s)
            ON CONFLICT (weather_date) DO UPDATE SET raw_data = EXCLUDED.raw_data
            RETURNING weather_date;
        """
        try:
            # Convert dictionary to JSON string for storage
            raw_data_json_str = json.dumps(raw_data)
            # Use self.database_ops which is initialized in __init__
            inserted_date = self.database_ops.execute_query_with_returning_id(query, (weather_date, raw_data_json_str))
            if inserted_date:
                logger.info(f"Successfully inserted/updated raw climate data for {weather_date}.")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to insert raw climate data for {weather_date}: {e}", exc_info=True)
            return False

    def _process_single_file(self, file_path: str) -> bool:
        """
        Processes a single raw JSON weather data file.
        Reads the file, extracts relevant data, and inserts it into the database.

        :param file_path: The absolute path to the JSON file to process.
        :type file_path: str
        :returns: True if the file is processed and data stored successfully, False otherwise.
        :rtype: bool
        """
        try:
            file_name = os.path.basename(file_path)
            logger.info(f"--- Processing file: {file_name} ---")

            # Extract date from filename (e.g., '2025-04-22.json')
            match = re.match(r'(\d{4}-\d{2}-\d{2})\.json', file_name)
            if not match:
                logger.warning(f"Skipping '{file_name}': Filename does not match YYYY-MM-DD.json format.")
                return False

            weather_date = match.group(1)

            with open(file_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)

            if self._insert_raw_climate_data(weather_date, raw_data):
                return True
            else:
                logger.error(f"Failed to process and store data from {file_name}.")
                return False

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from {file_path}: {e}")
            return False
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            return False
        except Exception as e:
            logger.error(f"An unexpected error occurred while processing {file_path}: {e}", exc_info=True)
            return False

# Example of how you might run this (for testing within this file):
if __name__ == "__main__":
    logger.info("--- Testing JSONDataLoader Standalone ---")

    # Mock FileConfig and DatabaseConfig for standalone testing
    class MockFileConfig:
        RAW_WEATHER_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'weather', 'rawfiles'))
        PROCESSED_WEATHER_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'weather', 'processed_files')) # New processed folder

    # Create dummy raw data folder and a test JSON file if they don't exist
    os.makedirs(MockFileConfig.RAW_WEATHER_DATA_PATH, exist_ok=True)
    os.makedirs(MockFileConfig.PROCESSED_WEATHER_DATA_PATH, exist_ok=True)

    test_json_file_path = os.path.join(MockFileConfig.RAW_WEATHER_DATA_PATH, '2025-06-24.json')
    if not os.path.exists(test_json_file_path):
        with open(test_json_file_path, 'w') as f:
            json.dump({"date": "2025-06-24", "temp_c": 20, "humidity": 70, "condition": "Sunny"}, f)
        logger.info(f"Created test JSON file: {test_json_file_path}")
    else:
        logger.info(f"Test JSON file already exists: {test_json_file_path}")

    # Use your actual DatabaseConfig, or mock it if you don't want real DB connection for test
    # from utilities.src.config import DatabaseConfig # Uncomment if you want to use real config

    # Assuming DatabaseConfig needs dummy values for standalone testing if not connecting to real DB
    class MockDatabaseConfig:
        host = "localhost"
        port = 5432
        database_name = "vivarium"
        user = "vivarium"
        password = "your_db_password" # Replace with actual or env var for real test

    # Instantiate the loader
    loader = JSONDataLoader(
        folder_path=MockFileConfig.RAW_WEATHER_DATA_PATH,
        processed_data_path=MockFileConfig.PROCESSED_WEATHER_DATA_PATH,
        db_config=MockDatabaseConfig() # Use MockDatabaseConfig or your actual DatabaseConfig
    )

    # Run the full data load
    logger.info("Attempting full data load...")
    result = loader.execute_data_load()
    logger.info(f"Full data load test result: {result}")

    # Clean up dummy files/folders if created for testing
    # if os.path.exists(test_json_file_path):
    #     os.remove(test_json_file_path)
    #     logger.info(f"Removed test JSON file: {test_json_file_path}")
    # Consider removing MockFileConfig.PROCESSED_WEATHER_DATA_PATH contents too if files were moved.