# vivarium/deploy/src/database/supabase_setup.py
import os
import re
import sys
import time
import getpass
import argparse
import psycopg2
from typing import Optional, Dict, Any
from psycopg2 import sql, OperationalError, Error as Psycopg2Error


# Ensure vivarium root is in sys.path to resolve imports correctly
# This is a standalone script, so this block is good practice
if __name__ == "__main__":
    # If run directly, go up three levels from 'database' to 'vivarium' root
    vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
    if vivarium_path not in sys.path:
        sys.path.insert(0, vivarium_path)

from deploy.src.database.db_setup_strategy import DBSetupStrategy

from utilities.src.logger import LogHelper
from utilities.src.database_operations import DatabaseOperations
from utilities.src.config import SupabaseConfig, FileConfig, WeatherAPIConfig
from utilities.src.path_utils import PathUtils

logger = LogHelper.get_logger(__name__)


class SupabaseSetup(DBSetupStrategy):
    """
    Concrete strategy for setting up a Supabase-managed PostgreSQL database.

    This class implements the `DBSetupStrategy` abstract methods to handle the
    specific requirements of a managed cloud environment like Supabase. It
    orchestrates the process of verifying connectivity, creating the schema, and
    ensuring the setup is compatible with a shared, managed PostgreSQL instance.
    """

    def __init__(self):
        """
        Initializes the SupabaseSetup strategy.

        This method sets up the configuration and the database operations helper
        for interacting with the Supabase database.
        """
        super().__init__()
        self.db_config = SupabaseConfig()
        # Initialize DatabaseOperations without a specific config in __init__.
        # We will pass the connection configuration to the connect method,
        # which is the most flexible approach for this class's design.
        self.db_ops = DatabaseOperations()
        logger.info("SupabaseSetup: Initialized for Supabase database setup.")

    def prompt_for_restart(self) -> bool:
        """
        Supabase is a managed cloud service, so no manual service restart is required.

        This method is implemented to satisfy the abstract base class but will
        log a message indicating that the step is skipped.

        Returns:
            Always returns True, as no user action is required.
        """
        logger.info("Supabase is a managed cloud service. No manual service restart is required.")
        return True

    def create_database_and_user(self) -> bool:
        """
        Verifies that a connection can be established to the Supabase database.

        Supabase manages database and user creation, so this method serves as a
        verification step to ensure the provided credentials are valid.

        Returns:
            True if the connection is successful, False otherwise.
        """
        logger.info("Supabase manages database and user creation. Verifying connection to the managed database...")
        # Use a temporary DatabaseOperations instance for this verification step
        # to avoid interfering with the main connection for table creation.
        temp_db_ops = DatabaseOperations()
        
        try:
            # We use the custom_config parameter in DatabaseOperations.connect to pass
            # the specific Supabase connection details from the configuration file.
            temp_db_ops.connect(custom_config=self._custom_connection_configuration())
            logger.info("Successfully connected to the Supabase database. Database and user are ready.")
            return True
        except OperationalError as e:
            logger.error(
                f"Failed to connect to the Supabase database. "
                f"Please check your config file for the correct host, port, user, and password. Error: {e}",
                exc_info=True
            )
            return False
        except Exception as e:
            logger.error(f"An unexpected error occurred while verifying the Supabase connection: {e}", exc_info=True)
            return False
        finally:
            # Ensure the temporary verification connection is always closed.
            temp_db_ops.close()

    def create_tables(self, sql_script_name: str) -> bool:
        """
        Creates tables in the Supabase database using the provided SQL schema script.

        This method reads the SQL script, pre-processes it to remove commands
        incompatible with a managed service (e.g., `ALTER ... OWNER TO`), and then
        executes the cleaned commands against the database.

        Args:
            sql_script_name: The name of the SQL schema script (e.g., 'supabase_schema.sql').

        Returns:
            True if table creation is successful, False otherwise.
        """
        logger.info(f"SupabaseSetup: Creating tables using schema script: {sql_script_name}.")
        
        full_script_path = os.path.join(self.sql_schema_base_path, sql_script_name)
        if not os.path.exists(full_script_path):
            logger.error(f"SQL schema file not found at: {full_script_path}. Cannot create tables.")
            return False
        
        try:
            logger.info(f"SupabaseSetup: Reading SQL schema from: {full_script_path}")
            with open(full_script_path, 'r') as f:
                sql_commands = f.read()

            # --- Pre-process the SQL script to remove incompatible statements ---
            # 1. Remove comments (both multi-line and single-line).
            sql_commands = re.sub(r"^[ \t]*--.*$", "", sql_commands, flags=re.MULTILINE)
            sql_commands = re.sub(r"/\*.*?\*/", "", sql_commands, flags=re.DOTALL)
            
            # 2. Remove 'ALTER ... OWNER TO ...' statements. These are problematic with
            # managed services like Supabase, where you cannot change object ownership.
            # The connected user will automatically own the created objects.
            logger.info("Removing 'ALTER ... OWNER TO' statements from the schema script.")
            sql_commands = re.sub(
                r"ALTER\s+(TABLE|SEQUENCE|VIEW|MATERIALIZED VIEW|FOREIGN TABLE|SCHEMA)\s+.*?OWNER TO\s+\w+;",
                "",
                sql_commands,
                flags=re.IGNORECASE | re.DOTALL,
            )

            # 3. Remove 'DROP ... IF EXISTS ...' statements.
            # While generally harmless, a well-formed schema should create, not drop.
            # The script will continue on 'already exists' errors anyway.
            logger.info("Removing 'DROP ... IF EXISTS' statements from the schema script.")
            sql_commands = re.sub(
                r"DROP\s+(TABLE|INDEX|CONSTRAINT|SEQUENCE).*?;",
                "",
                sql_commands,
                flags=re.IGNORECASE | re.DOTALL,
            )

            commands_executed_successfully = True
            # Split the cleaned script into individual commands and execute them.
            for command in sql_commands.split(';'):
                stripped_command = command.strip()
                if stripped_command:
                    try:
                        # This will use the persistent connection established in execute_full_setup.
                        self.db_ops.execute_query(stripped_command)
                    except Psycopg2Error as e:
                        if "already exists" in str(e).lower() or "multiple primary keys" in str(e).lower() or "already has a primary key" in str(e).lower():
                            # Log these non-fatal errors as a warning and continue.
                            logger.warning(f"Non-fatal error (object already exists): {stripped_command[:100]}... Error: {e}")
                        else:
                            # For all other errors, log as a critical error and stop.
                            logger.error(f"Critical error executing SQL command: {stripped_command[:100]}...\nError: {e}")
                            commands_executed_successfully = False
                            break # Stop on critical errors.
                    except Exception as e:
                        logger.error(f"An unexpected error occurred executing SQL command: {stripped_command[:100]}...\nError: {e}", exc_info=True)
                        commands_executed_successfully = False
                        break # Stop on any unexpected error.
            
            if commands_executed_successfully:
                logger.info("SQL script executed successfully. Tables created/verified.")
            else:
                logger.error("Some SQL commands failed during table creation. Check logs for details.")
                return False

            return commands_executed_successfully

        except FileNotFoundError:
            logger.error(f"SupabaseSetup: SQL schema file not found at {full_script_path}")
            return False
        except Exception as e:
            logger.error(f"SupabaseSetup: An error occurred while creating tables: {e}", exc_info=True)
            return False
        # The connection is managed by the calling method (execute_full_setup),
        # so we do not close it here to prevent premature disconnection.


    def execute_full_setup(self, sql_script_name: str) -> bool:
        """
        Orchestrates the full Supabase setup process.
        
        This method executes the entire setup sequence, including connection
        verification, schema creation, and database restart (if applicable).
        
        Args:
            sql_script_name: The name of the SQL schema script to be executed.

        Returns:
            True if the full setup is successful, False otherwise.
        """
        logger.info("SupabaseSetup: Starting full setup execution.")
        
        success = False
        try:
            # 1. Verify credentials and connection before starting the full setup.
            if not self.create_database_and_user():
                logger.error("Supabase connection verification failed. Cannot proceed with setup.")
                return False

            # 2. Establish a persistent connection for the duration of the schema setup.
            # This is crucial for `create_tables` to reuse the same connection.
            self.db_ops.connect(custom_config=self._custom_connection_configuration())
            logger.info("Supabase connection established for schema setup.")

            # 3. Supabase is a managed service, so no manual restart is needed.
            if not self.prompt_for_restart():
                logger.error("Prompt for restart was not confirmed, aborting.")
                return False
            
            # 4. Create tables using the schema script.
            if not self.create_tables(sql_script_name):
                logger.error("Failed to create tables using the SQL schema script.")
                return False
                
            logger.info("Supabase setup process completed successfully.")
            success = True
            return success

        except Exception as e:
            logger.error(f"An unexpected error occurred during Supabase setup: {e}", exc_info=True)
            return False
        finally:
            # Ensure the primary connection opened by execute_full_setup is always closed.
            self.db_ops.close()

    def _custom_connection_configuration(self) -> Dict[str, Any]:
        """
        Builds and returns the connection parameters dictionary from SupabaseConfig.

        Returns:
            A dictionary containing the connection parameters for psycopg2.
        """
        return {
            'user': self.db_config.user,
            'password': self.db_config.password,
            'host': self.db_config.host,
            'port': int(self.db_config.port),
            'dbname': self.db_config.dbname,
            'sslmode': self.db_config.sslmode  # Supabase requires SSL
        }