# vivarium/deploy/src/database/data_loading/postgres_data_loader.py
import os
import sys
import subprocess
import getpass
from typing import Optional, Tuple

if __name__ == "__main__":
    vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
    if vivarium_path not in sys.path:
        sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.config import DatabaseConfig
from utilities.src.database_operations import DatabaseOperations

from weather.rawclimate_dataloader import main as load_rawfiles_main 
from deploy.src.database.data_loader_strategy import DataLoaderStrategy

logger = LogHelper.get_logger(__name__)

class PostgresDataLoader(DataLoaderStrategy):
    """
    Concrete strategy for loading data into a PostgreSQL database.
    Implements the DataLoaderStrategy abstract methods.
    """
    def __init__(self):
        """
        Initializes the PostgresDataLoader strategy.
        Loads database configuration and initializes DatabaseOperations.
        """
        super().__init__()
        self.db_config = DatabaseConfig()
        # db_ops is initialized here but its connection will be closed at the end of execute_full_data_load
        self.db_ops = DatabaseOperations(self.db_config)
        logger.info("PostgresDataLoader: Initialized for PostgreSQL data loading.")

    def _get_app_user_password(self) -> str:
        """
        Prompts the user for the application user's password for database connection,
        or retrieves it from configuration.

        :returns: The password entered by the user or from config.
        :rtype: str
        :raises ValueError: If the user provides an empty password and it's not in config.
        """
        if self.db_config.password:
            return self.db_config.password
            
        password = getpass.getpass(f"Enter password for application user '{self.db_config.user}': ")
        if not password:
            logger.error("Application user password cannot be empty. Please provide a valid password.")
            raise ValueError("Application user password cannot be empty.")
        return password

    def load_from_dump(self, dump_file_path: str) -> bool:
        """
        Loads data into the PostgreSQL database from a specified dump file.
        This method uses the 'psql' command-line utility.

        :param dump_file_path: The absolute path to the database dump file (e.g., a .sql file).
        :type dump_file_path: str
        :returns: True if the dump is successfully restored, False otherwise.
        :rtype: bool
        """
        if not os.path.exists(dump_file_path):
            logger.error(f"PostgresDataLoader: Dump file not found at: {dump_file_path}.")
            return False

        logger.info(f"PostgresDataLoader: Loading data from dump file: {dump_file_path}")

        # Construct connection string for psql
        db_user = self.db_config.user
        db_name = self.db_config.dbname
        db_host = self.db_config.host # Default to local host
        db_port = str(self.db_config.port)

        # If remote, use remote host
        if self.db_config.remote_host and self.db_config.remote_host != "localhost":
             db_host = self.db_config.remote_host

        # IUsing PGPASSWORD environment variable for non-interactive psql
        try:
            app_password = self._get_app_user_password()
            os.environ['PGPASSWORD'] = app_password # Temporarily set PGPASSWORD

            # psql command to restore a plain SQL dump
            command = [
                'psql',
                '-h', db_host,
                '-p', db_port,
                '-U', db_user,
                '-d', db_name,
                '-f', dump_file_path, # Read commands from file
                '-v', 'ON_ERROR_STOP=1' # Stop on first error
            ]

            logger.info(f"PostgresDataLoader: Executing psql command: {' '.join(command[:-1])} < {os.path.basename(dump_file_path)}")
            
            process = subprocess.run(command, capture_output=True, text=True, check=False) # check=False to handle errors manually

            if process.returncode != 0:
                logger.error(f"PostgresDataLoader: Failed to load dump file '{dump_file_path}'.")
                logger.error(f"STDOUT:\n{process.stdout}")
                logger.error(f"STDERR:\n{process.stderr}")
                return False
            else:
                logger.info(f"PostgresDataLoader: Successfully loaded data from dump file '{dump_file_path}'.")
                logger.debug(f"STDOUT:\n{process.stdout}")
                return True
        except FileNotFoundError:
            logger.error("PostgresDataLoader: 'psql' command not found. Ensure PostgreSQL client tools are installed and in PATH.")
            return False
        except ValueError as e:
            logger.error(f"PostgresDataLoader: {e}")
            return False
        except Exception as e:
            logger.error(f"PostgresDataLoader: An unexpected error occurred during dump loading: {e}", exc_info=True)
            return False
        finally:
            if 'PGPASSWORD' in os.environ:
                del os.environ['PGPASSWORD'] # Clean up environment variable


    def load_raw_climate_data(self) -> bool:
        """
        Loads raw climate data from local files into the PostgreSQL database.
        This method calls the `main` function from `weather.rawclimate_dataloader`.

        :returns: True if the raw climate data is successfully loaded, False otherwise.
        :rtype: bool
        """
        logger.info("PostgresDataLoader: Attempting to load initial raw climate data.")
        try:
            load_rawfiles_main()
            logger.info("PostgresDataLoader: Raw climate data loading complete.")
            return True
        except Exception as e:
            logger.error(f"PostgresDataLoader: Failed to load raw climate data: {e}", exc_info=True)
            return False
        finally:
            pass # No action needed here, connection closed by orchestrator


    def execute_full_data_load(self, dump_file_path: Optional[str] = None) -> bool:
        """
        Orchestrates the full data loading process for PostgreSQL.

        :param dump_file_path: Optional absolute path to a database dump file. If provided,
                               data will be loaded from this dump.
        :type dump_file_path: Optional[str]
        :returns: True if all specified data loading steps are successful, False otherwise.
        :rtype: bool
        """
        logger.info("PostgresDataLoader: Starting full data loading process.")
        try:
            # Step 1: Load from database dump (if specified)
            if dump_file_path:
                logger.info(f"PostgresDataLoader: Initiating dump file load from {dump_file_path}.")
                if not self.load_from_dump(dump_file_path):
                    logger.error("PostgresDataLoader: Failed to load data from dump file.")
                    return False
            else:
                logger.info("PostgresDataLoader: No dump file path provided. Skipping dump load.")

            # Step 2: Load raw climate data
            logger.info("PostgresDataLoader: Initiating raw climate data load.")
            if not self.load_raw_climate_data():
                logger.error("PostgresDataLoader: Failed to load raw climate data.")
                return False

            logger.info("PostgresDataLoader: Full data loading process completed successfully.")
            return True
        except Exception as e:
            logger.error(f"PostgresDataLoader: An unexpected error occurred during full data loading: {e}", exc_info=True)
            return False
        finally:
            # Ensure the primary DatabaseOperations connection is closed after all loading attempts
            self.db_ops.close() 
            logger.info("PostgresDataLoader: DatabaseOperations connection closed after full data loading.")