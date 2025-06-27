# vivarium/deploy/src/database/postgres_data_loader.py
import os
import sys
import subprocess
import getpass
from typing import Optional, Tuple

# Ensure the vivarium root path is in sys.path
# Three levels up to vivarium root: database/ -> src/ -> deploy/ -> vivarium/
if __name__ == "__main__":
    vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
    if vivarium_path not in sys.path:
        sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.config import DatabaseConfig, FileConfig
from utilities.src.database_operations import DatabaseOperations

from deploy.src.database.data_loader_strategy import DataLoaderStrategy

logger = LogHelper.get_logger(__name__)

class PostgresDataLoader(DataLoaderStrategy):
    """
    Concrete strategy for loading data into a PostgreSQL database.
    Implements the DataLoaderStrategy abstract methods.
    """
    def __init__(self, file_path: Optional[str],
                 db_config: Optional[DatabaseConfig] = None):
        """
        Initializes the PostgresDataLoader strategy.
        Loads database configuration and initializes DatabaseOperations.

        :param file_path: The absolute path to the database dump file to be loaded.
                          Can be None if only other loading operations are expected (though not typical for this loader).
        :type file_path: Optional[str]
        :param db_config: An optional DatabaseConfig object. If None, a new one will be created.
        :type db_config: Optional[DatabaseConfig]
        """
        super().__init__()
        self.file_path = file_path
        self.db_config = db_config if db_config is not None else DatabaseConfig()
        # db_ops is initialized here but its connection will be closed at the end of execute_full_data_load
        self.db_ops = DatabaseOperations(self.db_config)
        logger.info("PostgresDataLoader: Initialized for PostgreSQL data loading.")

    def load_from_dump(self) -> bool:
        """
        Loads data into the PostgreSQL database from the dump file specified during initialization (`self.file_path`).
        This method uses the 'psql' command-line utility.

        :returns: True if the dump is successfully restored, False otherwise.
        :rtype: bool
        """
        if not self.file_path:
            logger.error("PostgresDataLoader: No dump file path provided during initialization to load from.")
            return False

        if not os.path.exists(self.file_path):
            logger.error(f"PostgresDataLoader: Dump file not found at: {self.file_path}.")
            return False

        logger.info(f"PostgresDataLoader: Loading data from dump file: {self.file_path}")

        # Construct connection string for psql
        db_user = self.db_config.user
        db_name = self.db_config.dbname
        db_host = self.db_config.host # Default to localhost
        db_port = str(self.db_config.port)

        # If remote, use remote host
        if self.db_config.remote_host and self.db_config.remote_host != "localhost":
             db_host = self.db_config.remote_host

        # Using PGPASSWORD environment variable for non-interactive psql
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
                '-f', self.file_path, # Read commands from file
                '-v', 'ON_ERROR_STOP=1' # Stop on first error
            ]

            logger.info(f"PostgresDataLoader: Executing psql command: {' '.join(command[:-1])} < {os.path.basename(self.file_path)}")
            
            process = subprocess.run(command, capture_output=True, text=True, check=False) # check=False to handle errors manually

            if process.returncode != 0:
                logger.error(f"PostgresDataLoader: Failed to load dump file '{self.file_path}'.")
                logger.error(f"STDOUT:\n{process.stdout}")
                logger.error(f"STDERR:\n{process.stderr}")
                return False
            else:
                logger.info(f"PostgresDataLoader: Successfully loaded data from dump file '{self.file_path}'.")
                # logger.debug(f"STDOUT:\n{process.stdout}") # Uncomment if you want verbose psql output in debug logs
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

    def load_json_data(self) -> bool:
        """
        PostgresDataLoader does not support loading raw JSON data directly.
        This method will log a warning and return False.

        :returns: False, as this operation is not supported by this loader.
        :rtype: bool
        """
        logger.warning("PostgresDataLoader: 'load_json_data' is not supported by this loader strategy. Please use JSONDataLoader for this purpose.")
        return False

    def execute_full_data_load(self) -> bool:
        """
        Orchestrates the full data loading process for PostgreSQL.
        This typically involves loading data from the dump file specified during initialization.

        :returns: True if all specified data loading steps are successful, False otherwise.
        :rtype: bool
        """
        logger.info("PostgresDataLoader: Starting full data loading process.")
        try:
            if self.file_path:
                logger.info(f"PostgresDataLoader: Initiating dump file load from {self.file_path}.")
                if not self.load_from_dump():
                    logger.error("PostgresDataLoader: Failed to load data from dump file.")
                    return False
            else:
                logger.info("PostgresDataLoader: No dump file path provided during initialization. Skipping dump load.")

            logger.info("PostgresDataLoader: Full data loading process completed successfully.")
            return True
        except Exception as e:
            logger.error(f"PostgresDataLoader: An unexpected error occurred during full data loading: {e}", exc_info=True)
            return False
        finally:
            # Ensure the primary DatabaseOperations connection is closed after all loading attempts
            if self.db_ops: # Check if db_ops was successfully initialized
                self.db_ops.close() 
                logger.info("PostgresDataLoader: DatabaseOperations connection closed after full data loading.")

    # --- PRIVATE FUNCTIONS ---
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