# vivarium/deploy/src/database/supabase_setup.py
"""
Implements the database setup strategy for a Supabase PostgreSQL instance.

This module focuses on applying the schema to the existing Supabase database
using the 'psql' command-line tool, as direct superuser management for
database/user creation is typically not available on managed cloud services.
"""

import os
import sys
import subprocess
from typing import Tuple, Optional

# Ensure vivarium root path is in sys.path to resolve imports correctly.
# This block allows the script to be run from any directory within the project structure.
vivarium_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
if vivarium_root_path not in sys.path:
    sys.path.insert(0, vivarium_root_path)

from utilities.src.logger import LogHelper
from utilities.src.new_config import SupabaseConfig
from utilities.src.db_operations import DBOperations, ConnectionDetails
from utilities.src.path_utils import PathUtils
from deploy.src.database_setup.db_setup_strategy import DBSetupStrategy

logger = LogHelper.get_logger(__name__)


class SupabaseSetup(DBSetupStrategy):
    """
    Concrete implementation of DBSetupStrategy for Supabase PostgreSQL.

    This class handles the application of the database schema to a Supabase
    instance using the 'psql' command, leveraging credentials from SupabaseConfig.
    It does not perform database or user creation, as these are managed by Supabase.
    """

    def __init__(self, supabase_config: SupabaseConfig):
        """
        Initializes the SupabaseSetup strategy with Supabase configuration details.

        :param supabase_config: An instance of `SupabaseConfig` containing
                                connection credentials for the Supabase database.
        :type supabase_config: SupabaseConfig
        """
        self.supabase_config = supabase_config
        # db_ops is initialized here but its connection details are used from self.supabase_config
        # for connecting the application. The psql command uses connection_details directly.
        self.db_ops = DBOperations()
        self.connection_details = self.supabase_config.supabase_connection_details
        logger.info("SupabaseSetup initialized.")

    def _build_psql_command(self, sql_script_path: str) -> list[str]:
        """
        Constructs the 'psql' command with arguments to apply an SQL script.

        The command includes host, port, user, database name, and the script file.
        The password will be handled via the PGPASSWORD environment variable.

        :param sql_script_path: The full file path to the SQL schema script.
        :type sql_script_path: str
        :returns: A list of strings representing the 'psql' command and its arguments.
        :rtype: list[str]
        """
        conn_details = self.connection_details
        command = [
            'psql',
            '-h', conn_details.host,
            '-p', str(conn_details.port),
            '-U', conn_details.user,
            '-d', conn_details.database,
            '-f', sql_script_path,
            '-v', 'ON_ERROR_STOP=1' # Ensure script stops on the first error encountered
        ]
        return command

    def _run_psql_command(self, command: list[str]) -> bool:
        """
        Executes a 'psql' command as a subprocess, handling output and errors.

        The `PGPASSWORD` environment variable is used to securely pass the
        database password to 'psql'.

        :param command: A list of strings representing the 'psql' command and its arguments.
        :type command: list[str]
        :returns: True if the 'psql' command executes successfully (exit code 0), False otherwise.
        :rtype: bool
        """
        env = os.environ.copy()
        env['PGPASSWORD'] = self.connection_details.password

        logger.debug(f"Executing psql command: {' '.join(command)}")
        try:
            # check=False ensures subprocess.run doesn't raise an exception for non-zero exit codes.
            # We check result.returncode manually.
            result = subprocess.run(command, env=env, check=False, capture_output=True, text=True)

            if result.returncode == 0:
                logger.info("psql command executed successfully.")
                if result.stdout:
                    logger.debug(f"psql stdout:\n{result.stdout}")
                return True
            else:
                logger.error(f"psql command failed with exit code {result.returncode}.")
                if result.stdout:
                    logger.error(f"psql stdout:\n{result.stdout}")
                if result.stderr:
                    logger.error(f"psql stderr:\n{result.stderr}")
                return False
        except FileNotFoundError:
            logger.critical(
                "psql command not found. Please ensure PostgreSQL client tools are "
                "installed and accessible in your system's PATH. On Debian/Ubuntu, install 'postgresql-client'."
            )
            return False
        except Exception as e:
            logger.critical(f"An unexpected error occurred while running psql command: {e}", exc_info=True)
            return False

    def execute_full_setup(self, sql_script_name: str) -> Tuple[bool, Optional[DBOperations]]:
        """
        Applies the SQL schema script to the Supabase database.

        This method implements the `DBSetupStrategy`'s abstract method.
        It identifies the schema file, constructs the 'psql' command, and executes it.
        It does not manage database or user creation, as these are handled by the Supabase platform.

        :param sql_script_name: The name of the SQL schema script file (e.g., 'supabase_schema.sql').
        :type sql_script_name: str
        :returns: A tuple containing:
                  - True if schema application was successful, False otherwise.
                  - An active `DBOperations` instance initialized with the
                    Supabase application database credentials, or `None` if setup failed.
        :rtype: Tuple[bool, Optional[DBOperations]]
        """
        logger.info(f"Attempting to apply schema '{sql_script_name}' to Supabase database.")

        try:
            # Construct the full path to the SQL schema script
            # Assuming schema files are located in 'vivarium/deploy/sql'
            sql_script_path = PathUtils.get_full_path("deploy", "sql", sql_script_name)

            if not os.path.exists(sql_script_path):
                logger.error(f"SQL schema file not found at expected path: {sql_script_path}")
                return False, None

            psql_command = self._build_psql_command(sql_script_path)
            success = self._run_psql_command(psql_command)

            if success:
                logger.info("Supabase schema applied successfully.")
                # Return the DBOperations instance which is ready to use
                # with the application's connection details (supabase_connection_details).
                return True, self.db_ops
            else:
                logger.error("Failed to apply Supabase schema using psql.")
                return False, None
        except Exception as e:
            logger.critical(f"A critical error occurred during Supabase schema application: {e}", exc_info=True)
            return False, None

# Example Usage for direct testing (not part of the main application flow)
if __name__ == "__main__":
    # Adjust sys.path to find config if running this script directly
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Import config classes here to avoid circular dependencies when run standalone
    from utilities.src.config import SupabaseConfig, FileConfig

    try:
        logger.info("Starting direct test of SupabaseSetup.")
        # Load Supabase configuration from config files
        supabase_cfg = SupabaseConfig()
        file_cfg = FileConfig() # To get the schema file name

        # Instantiate the SupabaseSetup strategy
        supabase_setup_strategy = SupabaseSetup(supabase_config=supabase_cfg)

        # Define the schema file to use (e.g., from FileConfig)
        schema_file_name = file_cfg.supabase_schema # Assuming this points to 'supabase_schema.sql'

        # Execute the full setup (which means applying the schema for Supabase)
        setup_success, db_ops_instance = supabase_setup_strategy.execute_full_setup(
            sql_script_name=schema_file_name
        )

        if setup_success:
            logger.info("Supabase database setup (schema application) test completed successfully.")
            if db_ops_instance:
                logger.info("Successfully obtained a DBOperations instance for Supabase.")
                # You can now use db_ops_instance to perform further operations
                # connected to your Supabase project (as the application user).
                try:
                    logger.info("Attempting a test query with the obtained DBOperations instance...")
                    db_ops_instance.connect(supabase_cfg.supabase_connection_details)
                    # Example: Query for existing tables (exclude system schemas)
                    result = db_ops_instance.execute_query(
                        "SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'auth', 'storage', 'graphql', 'realtime', 'supabase_migrations') ORDER BY schemaname, tablename;",
                        fetch_one=False
                    )
                    if result:
                        logger.info("Tables found in Supabase:")
                        for row in result:
                            logger.info(f"  Schema: {row[0]}, Table: {row[1]}")
                    else:
                        logger.info("No application-specific tables found in Supabase (or query failed).")
                except Exception as e:
                    logger.error(f"Error during post-setup test query: {e}", exc_info=True)
                finally:
                    db_ops_instance.close()

        else:
            logger.error("Supabase database setup (schema application) test failed.")

    except ImportError as ie:
        logger.critical(
            f"Import error: {ie}. Ensure your PYTHONPATH is set up correctly "
            "or run this script from the Vivarium project root directory."
        )
        sys.exit(1)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during SupabaseSetup example execution: {e}", exc_info=True)
        sys.exit(1)