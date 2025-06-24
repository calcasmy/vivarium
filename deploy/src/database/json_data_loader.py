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
        sys.path.insert(0, project_root)

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
                 db_config: Optional[DatabaseConfig] = None):
        """
        Initializes the JSONDataLoader with paths and database configuration.

        :param files_path       : Path to the directory containing raw weather JSON files.
                                    Defaults to the value from FileConfig.
        :type files_path        : Optional[str]

        :param db_config        : Database configuration object. Defaults to a new DatabaseConfig instance.
        :type db_config         : Optional[DatabaseConfig]
        """
        super().__init__()

        self.folder_path = folder_path if folder_path is not None else FileConfig.RAW_WEATHER_DATA_PATH
        self.db_config = db_config if db_config is not None else DatabaseConfig()

        logger.info(f"JSONDataLoader initialized. Raw files path: {self.files_path}")

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

    def load_json_data(self) -> bool:
        """
        Loads local JSON files into the database.

        This method reads JSON files from the specified files_path,
        processes the data, inserts it into the database (using DatabaseOperations),

        :returns: True if all files are processed successfully, False otherwise.
        :rtype: bool
        """
        logger.info("Starting JSON data loading process from local files.")

        # Initialize DatabaseOperations here if you intend to write to DB within this method
        # db_operations = DatabaseOperations(self.db_config)
        # You'll need to pass 'db_operations' instance or directly use its methods for inserts.

        processed_count = 0
        success = True

        try:
            if not os.path.exists(self.folder_path):
                logger.error(f"JSON data path does not exist: {self.folder_path}")
                return False
            
            json_files = [f for f in os.listdir(self.folder_path) if f.endswith('.json')]
            if not json_files:
                logger.info(f"No JSON files found in raw data folder: {self.folder_path}")
                return

            for filename in sorted(json_files): # Process files in date order if named YYYY-MM-DD.json
                file_path = os.path.join(self.folder_path, filename)
                try: 

                    logger.debug(f"Processing and loading data from {filename}...")

                    self._process_single_file(file_path)

                    logger.info(f"Successfully processed {filename}")

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
            logger.error(f"Error accessing raw climate data directory {self.files_path}: {e}", exc_info=True)
            success = False

        logger.info(f"Raw climate data loading completed. Processed {processed_count} files. Overall success: {success}")
        return success

    def execute_full_data_load(self, dump_file_path: Optional[str] = None) -> bool:
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
        """
        query = """
            SELECT raw_data FROM raw_climate_data
            WHERE weather_date = %s;
        """
        try:
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
        Uses ON CONFLICT DO UPDATE to handle existing entries.
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
            inserted_date = self.database_ops.execute_query_with_returning_id(query, (weather_date, raw_data_json_str))
            if inserted_date:
                logger.info(f"Successfully inserted/updated raw climate data for {weather_date}.")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to insert raw climate data for {weather_date}: {e}", exc_info=True)
            return False

    def _process_single_file(self, file_path: str) -> None:
        """
        Processes a single raw JSON weather data file.
        Reads the file, extracts relevant data, and inserts it into the database.
        """
        try:
            file_name = os.path.basename(file_path)
            logger.info(f"--- Processing file: {file_name} ---")

            # Extract date from filename (e.g., '2025-04-22.json')
            match = re.match(r'(\d{4}-\d{2}-\d{2})\.json', file_name)
            if not match:
                logger.warning(f"Skipping '{file_name}': Filename does not matchYYYY-MM-DD.json format.")
                return

            weather_date = match.group(1)

            with open(file_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)

            # Check if data already exists, then insert/update
            # Still attempt to insert/update even if check fails,
            # as ON CONFLICT handles it, and the check itself might fail
            # if the table doesn't exist yet.
            if not self._insert_raw_climate_data(weather_date, raw_data):
                logger.error(f"Failed to process and store data from {file_name}.")

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from {file_path}: {e}")
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while processing {file_path}: {e}", exc_info=True)

if __name__ == "__main__":
    logger.info("--- Testing JSONDataLoader Standalone ---")

    # Ensure your FileConfig and DatabaseConfig are set up for testing
    # For a quick test, you might mock them or create dummy paths/configs
    # For example:
    # class MockFileConfig:
    #     RAW_WEATHER_DATA_PATH = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data', 'raw_weather_test')
    # os.makedirs(MockFileConfig.RAW_WEATHER_DATA_PATH, exist_ok=True)
    # # Create a dummy JSON file for testing
    # with open(os.path.join(MockFileConfig.RAW_WEATHER_DATA_PATH, 'test_data_1.json'), 'w') as f:
    #     json.dump({"date": "2023-01-01", "temp": 25.5, "humidity": 60}, f)

    # loader = JSONDataLoader(
    #     files_path=MockFileConfig.RAW_WEATHER_DATA_PATH,
    #     db_config=DatabaseConfig() # Use your actual DatabaseConfig
    # )

    # result = loader.execute_full_data_load()
    # logger.info(f"Full data load test result: {result}")

    # Clean up dummy files/folders if created for testing