# vivarium/deploy/src/database_setup/postgres_setup.py
# """
# Implements the database setup strategy for a PostgreSQL instance.

# This module handles the full lifecycle of setting up a PostgreSQL database,
# including creating the database, creating and configuring a dedicated user,
# and applying the necessary schema, using credentials provided via DatabaseConfig.
# """

# import os, sys, re
# from typing import Tuple, Optional
# from psycopg2 import sql, OperationalError as Psycopg2Error

# from utilities.src.logger import LogHelper
# from utilities.src.config import DatabaseConfig
# from utilities.src.db_operations import DBOperations, ConnectionDetails
# from utilities.src.path_utils import PathUtils
# from database.database_setup_ops.db_setup_strategy import DBSetupStrategy

# logger = LogHelper.get_logger(__name__)


# class PostgresSetup(DBSetupStrategy):
#     """
#     Concrete implementation of DBSetupStrategy for PostgreSQL.

#     This class orchestrates the setup of a PostgreSQL database,
#     including database creation, user creation with appropriate privileges,
#     and the application of the schema.
#     """

#     def __init__(self, db_config: DatabaseConfig):
#         """
#         Initializes the PostgresSetup strategy with PostgreSQL configuration details.

#         The `is_remote` parameter determines which set of application user credentials
#         (local or remote) will be used for the final `DBOperations` instance.

#         :param db_config: An instance of `DatabaseConfig` containing
#                           all PostgreSQL connection credentials.
#         :type db_config: DatabaseConfig
#         """

#         self.db_config = db_config
#         # Connection details for superuser operations (e.g., create db/user)
#         self.superuser_conn_details = self.db_config.postgres_superuser_connection
#         # Connection details for the application user (where schema will be applied and app connects)
#         # Choose local or remote based on db_config's internal logic, or external 'is_remote' hint
#         self.app_conn_details = self.db_config.postgres_remote_connection \
#                                 if self.db_config._is_remote_db else self.db_config.postgres_local_connection
    
#         try:
#             # Initialize DBOperations for superuser tasks
#             self.superuser_db_ops = DBOperations()
#             # Initialize DBOperations for application user tasks and final return
#             self.app_db_ops = DBOperations()
#             logger.info("PostgresSetup initialized.")
#             logger.debug(f"Superuser connection: {self.superuser_conn_details.host}:{self.superuser_conn_details.port}/{self.superuser_conn_details.dbname} as {self.superuser_conn_details.user}")
#             logger.debug(f"App user connection: {self.app_conn_details.host}:{self.app_conn_details.port}/{self.app_conn_details.dbname} as {self.app_conn_details.user}")

#         except Exception as e:
#             logger.critical(f"Error initializing PostgresSetup: {e}", exc_info=True)
        
#     def _connect_as_superuser(self) -> bool:
#         """
#         Establishes a connection to PostgreSQL as the superuser.

#         :returns: True if connection is successful, False otherwise.
#         :rtype: bool
#         """
#         logger.info(f"Connecting to PostgreSQL as superuser '{self.superuser_conn_details.user}'...")
#         try:
#             # Connect the superuser_db_ops instance
#             self.superuser_db_ops.connect(self.superuser_conn_details)
#             logger.info("Successfully connected as superuser.")
#             return True
#         except Exception as e:
#             logger.error(f"Failed to connect as superuser: {e}", exc_info=True)
#             return False

#     def _check_database_exists(self, dbname: str) -> bool:
#         """
#         Checks if a database with the given name already exists.

#         Requires superuser connection.

#         :param dbname: The name of the database to check.
#         :type dbname: str
#         :returns: True if the database exists, False otherwise.
#         :rtype: bool
#         """
#         logger.info(f"Checking if database '{dbname}' exists...")
#         query = "SELECT 1 FROM pg_database WHERE datname = %s;"
#         try:
#             result = self.superuser_db_ops.execute_query(query, (dbname,), fetch_one=True)
#             exists = result is not None
#             logger.info(f"Database '{dbname}' {'exists' if exists else 'does not exist'}.")
#             return exists
#         except Exception as e:
#             logger.error(f"Error checking database existence for '{dbname}': {e}", exc_info=True)
#             return False

#     def _drop_database(self, dbname: str) -> bool:
#         """
#         Drops a PostgreSQL database, including terminating all active connections to it.

#         This method is primarily intended for **development, testing, or initial setup**
#         scenarios where a clean slate for the application database is required.
#         **WARNING:** This is a highly destructive operation. It will permanently delete
#         all data, schema, and objects within the specified database. It should
#         **NEVER** be used in production environments for data preservation or
#         without absolute certainty of its impact.

#         Requires an active superuser connection (via `self.superuser_db_ops`)
#         which must NOT be connected to the database being dropped (typically
#         connected to 'postgres' or another default administrative database).

#         :param dbname: The name of the database to drop.
#         :type dbname: str
#         :returns: :obj:`True` if the database was successfully dropped or did not exist,
#                   :obj:`False` otherwise.
#         :rtype: bool
#         :raises psycopg2.Error: If a database-specific error occurs during the operation
#                                 (e.g., permissions, or if connections persist despite termination attempts).
#         :raises Exception: For any other unexpected errors during the process.

#         **Internal Steps:**

#         1.  Checks if the database exists using :py:meth:`~.PostgresSetup._check_database_exists`.
#         2.  If it exists, attempts to terminate all active backend connections to it.
#             This is crucial to prevent "database is being accessed by other users" errors
#             during the `DROP DATABASE` command.
#         3.  Executes the ``DROP DATABASE`` command. An optional ``WITH (FORCE)`` clause
#             is included for PostgreSQL 13+ for more aggressive termination, but might
#             need to be removed for older versions.

#         .. seealso::
#            :py:meth:`~.DatabaseOperations.execute_command`
#            :py:meth:`~.PostgresSetup._check_database_exists`
#         """
#         # Defensive check: Ensure the superuser_db_ops is initialized and connected
#         if not hasattr(self, 'superuser_db_ops') or not self.superuser_db_ops or not self.superuser_db_ops.conn:
#             logger.error("Superuser database operations object is not initialized or connected. Cannot drop database.")
#             return False

#         # Step 1: Check if the database exists to avoid unnecessary operations/errors
#         if not self._check_database_exists(dbname):
#             logger.info(f"Database '{dbname}' does not exist. Skipping database drop.")
#             return True

#         logger.warning(f"Initiating drop sequence for database '{dbname}'. This is a DESTRUCTIVE operation!")
        
#         try:
#             # Step 2: Terminate all active connections to the target database
#             # This is crucial to avoid "database is being accessed by other users" errors
#             terminate_connections_query = sql.SQL("""
#                 SELECT pg_terminate_backend(pg_stat_activity.pid)
#                 FROM pg_stat_activity
#                 WHERE pg_stat_activity.datname = {}
#                   AND pid <> pg_backend_pid();
#             """).format(sql.Literal(dbname)) # Use sql.Literal for the database name in the WHERE clause

#             logger.info(f"Attempting to terminate active connections to database '{dbname}'...")
#             # Use execute_command which wraps execute_query for non-fetching operations
#             if not self.superuser_db_ops.execute_command(terminate_connections_query):
#                  logger.warning(f"Failed to terminate all connections to '{dbname}'. Proceeding with DROP DATABASE, but it might still fail if connections persist.")
#             else:
#                  logger.info(f"Successfully sent termination signals to connections for database '{dbname}'.")

#             # Step 3: Drop the database
#             # The WITH (FORCE) option (PostgreSQL 13+) can forcefully disconnect users.
#             # Remove this if targeting older PostgreSQL versions.
#             drop_db_query = sql.SQL("DROP DATABASE {} WITH (FORCE);").format(sql.Identifier(dbname))
            
#             logger.info(f"Executing DROP DATABASE '{dbname}'...")
#             if self.superuser_db_ops.execute_command(drop_db_query):
#                 logger.info(f"Database '{dbname}' dropped successfully.")
#                 return True
#             else:
#                 # execute_command already logs the error, just return False
#                 logger.error(f"Failed to drop database '{dbname}'. Check previous logs for details from execute_command.")
#                 return False

#         except Psycopg2Error as e:
#             # Catch specific PostgreSQL errors (e.g., permissions, still active connections if terminate failed)
#             logger.error(f"Database error while dropping database '{dbname}': {e}", exc_info=True)
#             return False
#         except Exception as e:
#             # Any other unexpected errors
#             logger.error(f"An unexpected error occurred while dropping database '{dbname}': {e}", exc_info=True)
#             return False

#     def _create_database(self, dbname: str, owner_user: str) -> bool:
#         """
#         Creates a new database and assigns an owner.

#         Requires superuser connection.

#         :param dbname: The name of the database to create.
#         :type dbname: str
#         :param owner_user: The user to be set as the owner of the new database.
#         :type owner_user: str
#         :returns: True if the database is created successfully or already exists, False otherwise.
#         :rtype: bool
#         """
#         if self._check_database_exists(dbname):
#             logger.info(f"Database '{dbname}' already exists. Skipping creation.")
#             return True

#         logger.info(f"Creating database '{dbname}' owned by '{owner_user}'...")
#         # Use a separate connection for CREATE DATABASE as it cannot be run in a transaction block
#         # opened by psycopg2 (which execute_query wraps). Or, ensure autocommit is on.
#         # For simplicity and robustness with superuser, a direct cursor execution without transaction block is often safer.
#         create_db_query = f"CREATE DATABASE {dbname} WITH OWNER = {owner_user};"
#         try:
#             # Assuming superuser_db_ops has autocommit enabled for CREATE DATABASE,
#             # or we manually commit/handle it. DBOperations is designed to manage this.
#             self.superuser_db_ops.execute_command(create_db_query)
#             logger.info(f"Database '{dbname}' created successfully.")
#             return True
#         except Exception as e:
#             logger.error(f"Failed to create database '{dbname}': {e}", exc_info=True)
#             return False

#     def _check_user_exists(self, username: str) -> bool:
#         """
#         Checks if a user with the given username already exists.

#         Requires superuser connection.

#         :param username: The name of the user to check.
#         :type username: str
#         :returns: True if the user exists, False otherwise.
#         :rtype: bool
#         """
#         logger.info(f"Checking if user '{username}' exists...")
#         query = "SELECT 1 FROM pg_roles WHERE rolname = %s;"
#         try:
#             result = self.superuser_db_ops.execute_query(query, (username,), fetch_one=True)
#             exists = result is not None
#             logger.info(f"User '{username}' {'exists' if exists else 'does not exist'}.")
#             return exists
#         except Exception as e:
#             logger.error(f"Error checking user existence for '{username}': {e}", exc_info=True)
#             return False

#     def _drop_user_if_exists(self, username: str, reassign_to_user: str) -> bool:
#         """
#         Drops a PostgreSQL user if they exist, including reassigning/dropping owned objects.
#         This prevents dependency issues when recreating the user.

#         Requires superuser connection.

#         :param username: The name of the user to drop.
#         :type username: str
#         :param reassign_to_user: The username to reassign owned objects to before dropping.
#                                  This should typically be a superuser (e.g., 'postgres').
#         :type reassign_to_user: str
#         :returns: :obj:`True` if the user was successfully dropped or did not exist, :obj:`False` otherwise.
#         :rtype: bool
#         """
#         if not self._check_user_exists(username):
#             logger.info(f"User '{username}' does not exist. Skipping user drop.")
#             return True

#         logger.warning(f"User '{username}' found. Attempting to clean up dependencies before dropping user...")

#         try:
#             # Reassign owned objects to the specified superuser
#             reassign_query = sql.SQL("REASSIGN OWNED BY {} TO {};").format(
#                 sql.Identifier(username), sql.Identifier(reassign_to_user)
#             )
#             if self.superuser_db_ops.execute_command(reassign_query): # execute_command expects string query
#                 logger.info(f"Ownership reassigned for objects owned by '{username}' in current DB.")
#             else:
#                 logger.warning(f"Could not reassign owned by '{username}'. Check logs for details. Continuing with drop.")
#         except Exception as e:
#             logger.error(f"Unexpected error during ownership reassignment for '{username}': {e}", exc_info=True)
#             # Continue to drop, as reassignment might not always be critical or possible

#         try:
#             # Drop owned objects (which were not reassigned or are unowned)
#             drop_owned_query = sql.SQL("DROP OWNED BY {};").format(sql.Identifier(username))
#             if self.superuser_db_ops.execute_command(drop_owned_query):
#                 logger.info(f"Objects owned by '{username}' dropped and privileges revoked in current DB.")
#             else:
#                 logger.warning(f"Could not drop owned by '{username}'. Check logs for details. Continuing with user drop.")
#         except Exception as e:
#             logger.error(f"Unexpected error during owned objects drop for '{username}': {e}", exc_info=True)
#             # Continue to drop user

#         logger.info(f"Dropping user '{username}'...")
#         try:
#             drop_user_query = sql.SQL("DROP USER {}").format(sql.Identifier(username))
#             if self.superuser_db_ops.execute_command(drop_user_query):
#                 logger.info(f"User '{username}' dropped successfully.")
#                 return True
#             else:
#                 logger.error(f"Failed to drop user '{username}'. See logs for database error.")
#                 return False # Indicate failure if execute_command returned False
#         except Exception as e:
#             logger.error(f"Unexpected error dropping user '{username}': {e}", exc_info=True)
#             return False

#     def _create_user(self, username: str, password: str) -> bool:
#         """
#         Creates a new user with a specified password.

#         Requires superuser connection.

#         :param username: The name of the user to create.
#         :type username: str
#         :param password: The password for the new user.
#         :type password: str
#         :returns: True if the user is created successfully or already exists, False otherwise.
#         :rtype: bool
#         """
#         if self._check_user_exists(username):
#             logger.info(f"User '{username}' already exists. Skipping creation.")
#             return True

#         logger.info(f"Creating user '{username}'...")
#         create_user_query = f"CREATE USER {username} WITH PASSWORD %s;"
#         try:
#             self.superuser_db_ops.execute_command(create_user_query, (password,))
#             logger.info(f"User '{username}' created successfully.")
#             return True
#         except Exception as e:
#             logger.error(f"Failed to create user '{username}': {e}", exc_info=True)
#             return False

#     def _grant_privileges(self, username: str, dbname: str) -> bool:
#         """
#         Grants necessary privileges to the user on the specified database.

#         Requires superuser connection.

#         :param username: The user to whom privileges will be granted.
#         :type username: str
#         :param dbname: The database on which privileges will be granted.
#         :type dbname: str
#         :returns: True if privileges are granted successfully, False otherwise.
#         :rtype: bool
#         """
#         logger.info(f"Granting privileges to user '{username}' on database '{dbname}'...")
#         # CONNECT privilege is needed to connect to the database
#         # This must be done on the database by the owner/superuser
#         grant_connect_query = f"GRANT CONNECT ON DATABASE {dbname} TO {username};"

#         # Granting all privileges on future tables/sequences in the public schema
#         # This is a common pattern to ensure the app user has full control
#         grant_future_all_on_tables = f"ALTER DEFAULT PRIVILEGES FOR USER {username} IN SCHEMA public GRANT ALL ON TABLES TO {username};"
#         grant_future_all_on_sequences = f"ALTER DEFAULT PRIVILEGES FOR USER {username} IN SCHEMA public GRANT ALL ON SEQUENCES TO {username};"

#         try:
#             # Connect to the target database before granting schema-level privileges
#             # Or, execute these commands while connected as superuser to postgres database,
#             # but specifying 'ON DATABASE {dbname}'.
#             self.superuser_db_ops.execute_command(grant_connect_query)
#             self.superuser_db_ops.execute_command(grant_future_all_on_tables)
#             self.superuser_db_ops.execute_command(grant_future_all_on_sequences)

#             logger.info(f"Privileges granted to user '{username}' on database '{dbname}'.")
#             return True
#         except Exception as e:
#             logger.error(f"Failed to grant privileges to user '{username}' on database '{dbname}': {e}", exc_info=True)
#             return False

#     def _apply_schema(self, sql_script_name: str) -> bool:
#         """
#         Applies the SQL schema script to the newly created database.

#         Connects as the application user to the application database for this step.

#         :param sql_script_name: The name of the SQL schema script file (e.g., 'postgres_schema.sql').
#         :type sql_script_name: str
#         :returns: True if the schema is applied successfully, False otherwise.
#         :rtype: bool
#         """
#         logger.info(f"Applying schema '{sql_script_name}' to database '{self.app_conn_details.dbname}' as user '{self.app_conn_details.user}'...")
#         try:
#             # Establish connection using app user credentials
#             self.app_db_ops.connect(self.app_conn_details)

#             # Get the full path to the SQL schema script
#             vivarium_package_root = PathUtils.get_project_root()
#             sql_script_path = os.path.abspath(os.path.join(PathUtils.get_project_root(), sql_script_name))
#             # sql_script_path = PathUtils.get_full_path("deploy", "sql", sql_script_name)

#             if not os.path.exists(sql_script_path):
#                 logger.error(f"SQL schema file not found: {sql_script_path}")
#                 return False

#             # Read the SQL schema from the file
#             logger.info(f"PostgresSetup: Reading SQL schema from: {sql_script_path}")
#             with open(sql_script_path, 'r') as f:
#                 sql_commands = f.read()

#             # -- Clean up / Remove comments and split into individual commands --
#             # This regex removes -- style comments and /* */ style comments.
#             sql_commands = re.sub(r"^[ \t]*--.*$", "", sql_commands, flags=re.MULTILINE)
#             sql_commands = re.sub(r"/\*.*?\*/", "", sql_commands, flags=re.DOTALL)

#             commands_executed_successfully = True
#             # Split by semicolon, but handle cases where semicolons might be inside quoted strings
#             # This simple split might break for complex SQL with escaped semicolons.
#             # For robust parsing, a dedicated SQL parser might be needed, but for typical schema files, this is often sufficient.
#             for command in sql_commands.split(';'):
#                 stripped_command = command.strip()
#                 if stripped_command:
#                     try:
#                         self.app_db_ops.execute_query(stripped_command)
#                     except Psycopg2Error as e:
#                         # Specific error handling for permission denied on public schema
#                         if "permission denied for schema public" in str(e).lower():
#                             logger.error(f"FATAL: Permission denied to create object in 'public' schema. "
#                                             f"Ensure user has CREATE privilege on public schema. "
#                                             f"Command failed: {stripped_command[:100]}... Error: {e}")
#                             commands_executed_successfully = False
#                             break
#                         elif "already exists" in str(e).lower():
#                             logger.warning(f"Table or object already exists (non-fatal): {stripped_command[:100]}... Error: {e}")
#                             # Continue processing other commands even if one exists
#                         else:
#                             logger.error(f"Error executing SQL command: {stripped_command[:100]}...\nError: {e}")
#                             commands_executed_successfully = False
#                             break # Breaking on any unhandled psycopg2 error
#                     except Exception as e:
#                         logger.error(f"An unexpected error occurred executing SQL command: {stripped_command[:100]}...\nError: {e}", exc_info=True)
#                         commands_executed_successfully = False
#                         break # Breaking on any unexpected error
                    
#             if commands_executed_successfully:
#                 logger.info("SQL script executed successfully. Tables created/verified.")
#             else:
#                 logger.error("Some SQL commands failed during table creation. Check logs for details. Aborting further setup.")
#                 return False # Indicate failure if any command failed

#             return commands_executed_successfully

#         except Exception as e:
#             logger.error(f"Failed to apply schema: {e}", exc_info=True)
#             return False

#     def execute_full_setup(self, sql_script_name: str) -> Tuple[bool, Optional[DBOperations]]:
#         """
#         Executes the full PostgreSQL database setup process.

#         This method orchestrates the following steps:
#         0. Connects as a superuser.
#         1. Drops the application database if it exists.
#         2. Creates the application user if it doesn't exist.
#         3. Creates the application database if it doesn't exist, owned by the application user.
#         4. Grants necessary privileges to the application user on the database.
#         5. Applies the SQL schema to the database using the application user.

#         :param sql_script_name: The name of the SQL schema script to apply
#                                 (e.g., 'postgres_schema.sql').
#         :type sql_script_name: str
#         :returns: A tuple containing:
#                   - True if the entire setup process was successful, False otherwise.
#                   - An active `DBOperations` instance connected as the
#                     application user to the application database, or `None` if setup failed.
#         :rtype: Tuple[bool, Optional[DBOperations]]
#         """
#         logger.info("Starting full PostgreSQL database setup process.")

#         # Step 0: Connect as superuser
#         if not self._connect_as_superuser():
#             logger.error("Initial superuser connection failed. Aborting setup.")
#             self.superuser_db_ops.close()
#             return False, None
        
#         # --- Step 1: Drop Application Database ---
#         if not self._drop_database(self.db_config.app_dbname):
#             logger.error(f"Failed to drop application database '{self.db_config.app_dbname}'. Aborting setup.")
#             return False
        
#         # --- Step 2: Drop and Recreate Application User ---
#         if not self._drop_user_if_exists(self.db_config.app_user, self.db_config._superuser):
#             logger.error(f"Failed to reset (drop) existing application user '{self.db_config.app_user}'. Aborting setup.")
#             return False
        
#         if not self._create_user(self.db_config.app_user, self.db_config.app_password):
#             logger.error(f"Failed to create application user '{self.db_config.app_user}'. Aborting setup.")
#             return False

#         # Step 3: Create application database
#         app_db_name = self.app_conn_details.dbname
#         if not self._create_database(app_db_name, self.db_config.app_user):
#             logger.error(f"Failed to create or verify application database '{app_db_name}'. Aborting setup.")
#             self.superuser_db_ops.close()
#             return False, None

#         # Step 4: Grant privileges to application user
#         if not self._grant_privileges(self.db_config.app_user, app_db_name):
#             logger.error(f"Failed to grant privileges to user '{self.db_config.app_user}' on database '{app_db_name}'. Aborting setup.")
#             self.superuser_db_ops.close()
#             return False, None

#         # Close superuser connection as it's no longer needed after setup steps
#         self.superuser_db_ops.close()
#         logger.info("Superuser connection closed.")

#         # Step 5: Apply schema using the application user
#         if not self._apply_schema(sql_script_name):
#             logger.error(f"Failed to apply schema '{sql_script_name}'. Setup failed.")
#             self.app_db_ops.close() # Close if schema application failed
#             return False, None

#         logger.info("PostgreSQL database setup completed successfully.")
#         # Return the app_db_ops instance which is now connected to the application database
#         # as the application user and has the schema applied.
#         return True, self.app_db_ops

# # Example Usage for direct testing (not part of the main application flow)
# if __name__ == "__main__":
#     # Adjust sys.path to find config if running this script directly
#     project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
#     if project_root not in sys.path:
#         sys.path.insert(0, project_root)

#     # Import config classes here to avoid circular dependencies when run standalone
#     from utilities.src.config import DatabaseConfig, FileConfig

#     try:
#         logger.info("Starting direct test of PostgresSetup.")
#         # Load PostgreSQL configuration from config files
#         db_cfg = DatabaseConfig()
#         file_cfg = FileConfig() # To get the schema file name

#         # Instantiate the PostgresSetup strategy
#         postgres_setup_strategy = PostgresSetup(db_config=db_cfg)

#         # Define the schema file to use
#         schema_file_name = file_cfg.schema_file # Assuming this points to 'postgres_schema.sql'

#         # Execute the full setup
#         setup_success, db_ops_instance = postgres_setup_strategy.execute_full_setup(
#             sql_script_name=schema_file_name
#         )

#         if setup_success:
#             logger.info("PostgreSQL database setup test completed successfully.")
#             if db_ops_instance:
#                 logger.info("Successfully obtained a DBOperations instance for PostgreSQL (application user).")
#                 # You can now use db_ops_instance to perform further operations
#                 # connected to your PostgreSQL database (as the application user).
#                 try:
#                     logger.info("Attempting a test query with the obtained DBOperations instance...")
#                     # Example: Query for existing tables (exclude system schemas)
#                     result = db_ops_instance.execute_query(
#                         "SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema') ORDER BY schemaname, tablename;",
#                         fetch_one=False
#                     )
#                     if result:
#                         logger.info("Tables found in PostgreSQL:")
#                         for row in result:
#                             logger.info(f"  Schema: {row[0]}, Table: {row[1]}")
#                     else:
#                         logger.info("No application-specific tables found in PostgreSQL (or query failed).")
#                 except Exception as e:
#                     logger.error(f"Error during post-setup test query: {e}", exc_info=True)
#                 finally:
#                     # Ensure the application connection is closed
#                     db_ops_instance.close()

#         else:
#             logger.error("PostgreSQL database setup test failed.")

#     except ImportError as ie:
#         logger.critical(
#             f"Import error: {ie}. Ensure your PYTHONPATH is set up correctly "
#             "or run this script from the Vivarium project root directory."
#         )
#         sys.exit(1)
#     except Exception as e:
#         logger.critical(f"An unexpected error occurred during PostgresSetup example execution: {e}", exc_info=True)
#         sys.exit(1)


# vivarium/database/database_setup_ops/postgres_setup.py

import os
import re
from typing import Tuple
from psycopg2 import sql, OperationalError as Psycopg2Error

from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations, ConnectionDetails
from utilities.src.config import FileConfig
from database.database_setup_ops.db_setup_strategy import DBSetupStrategy

logger = LogHelper.get_logger(__name__)

# Vivarium root path, assuming relative from this script
vivarium_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

class PostgresSetup(DBSetupStrategy):
    """
    Manages the setup and configuration of a PostgreSQL database for the Vivarium application.

    This class implements the `DBSetupStrategy` abstract base class, providing
    concrete methods for creating/dropping databases and users, granting privileges,
    and applying SQL schema files.
    """

    def __init__(self, app_connection_details: ConnectionDetails, superuser_connection_details: ConnectionDetails, file_config: FileConfig, db_type: str):
        """
        Initializes the PostgresSetup strategy with connection details, file configurations, and database type.

        :param app_connection_details: Connection details for the application user.
        :type app_connection_details: :class:`utilities.src.db_operations.ConnectionDetails`
        :param superuser_connection_details: Connection details for the superuser.
        :type superuser_connection_details: :class:`utilities.src.db_operations.ConnectionDetails`
        :param file_config: Configuration object containing file paths, including schema.
        :type file_config: :class:`utilities.src.config.FileConfig`
        :param db_type: The type of database being set up ('postgres' or 'supabase'). Used for schema file selection.
        :type db_type: str
        """
        self.app_conn_details = app_connection_details
        self.superuser_conn_details = superuser_connection_details
        self.file_config = file_config
        self.db_type = db_type.lower()


    def full_setup(self) -> bool:
        """
        Executes the full PostgreSQL database setup process.

        This method orchestrates the following steps:
        1. Connects to PostgreSQL as a superuser.
        2. Terminates existing connections to the target database (if it exists).
        3. Drops the application database (if it exists).
        4. Reassigns and drops objects owned by the application user (if the user exists).
        5. Drops the application user (if it exists).
        6. Creates the application user.
        7. Creates the application database owned by the application user.
        8. Grants necessary privileges to the application user on the database.
        9. Applies the SQL schema to the database using the application user.

        :returns: :obj:`True` if the entire setup process was successful, :obj:`False` otherwise.
        :rtype: bool
        """
        logger.info("Starting full PostgreSQL database setup process.")

        superuser_db_ops = DBOperations()
        app_db_ops = DBOperations()

        try:
            logger.info(f"Connecting to PostgreSQL as superuser '{self.superuser_conn_details.user}' to host '{self.superuser_conn_details.host}'...")
            superuser_db_ops.connect(self.superuser_conn_details)
            superuser_db_ops.set_autocommit(True)
            logger.info("Successfully connected as superuser.")

            app_db_name = self.app_conn_details.dbname
            app_user = self.app_conn_details.user
            app_password = self.app_conn_details.password
            superuser_user_name = self.superuser_conn_details.user

            logger.info(f"Checking if database '{app_db_name}' exists...")
            db_exists = superuser_db_ops.execute_query("SELECT 1 FROM pg_database WHERE datname = %s;", (app_db_name,), fetch_one=True)
            if db_exists:
                logger.warning(f"Initiating drop sequence for database '{app_db_name}'. This is a DESTRUCTIVE operation!")
                try:
                    terminate_connections_query = sql.SQL("""
                        SELECT pg_terminate_backend(pg_stat_activity.pid)
                        FROM pg_stat_activity
                        WHERE pg_stat_activity.datname = {}
                          AND pid <> pg_backend_pid();
                    """).format(sql.Literal(app_db_name))
                    logger.info(f"Attempting to terminate active connections to database '{app_db_name}'...")
                    superuser_db_ops.execute_command(terminate_connections_query)
                    logger.info(f"Termination signals sent to connections for database '{app_db_name}'.")

                    drop_db_query = sql.SQL("DROP DATABASE {} WITH (FORCE);").format(sql.Identifier(app_db_name))
                    logger.info(f"Executing DROP DATABASE '{app_db_name}'...")
                    if not superuser_db_ops.execute_command(drop_db_query):
                        logger.error(f"Failed to drop database '{app_db_name}'. Aborting setup.")
                        return False
                    logger.info(f"Database '{app_db_name}' dropped successfully.")
                except Psycopg2Error as e:
                    logger.error(f"Database error while dropping database '{app_db_name}': {e}", exc_info=True)
                    return False
                except Exception as e:
                    logger.error(f"An unexpected error occurred while dropping database '{app_db_name}': {e}", exc_info=True)
                    return False
            else:
                logger.info(f"Database '{app_db_name}' does not exist. Skipping database drop.")

            logger.info(f"Checking if user '{app_user}' exists...")
            user_exists = superuser_db_ops.execute_query("SELECT 1 FROM pg_roles WHERE rolname = %s;", (app_user,), fetch_one=True)
            
            if user_exists:
                logger.warning(f"User '{app_user}' found. Attempting to clean up dependencies before dropping user...")
                try:
                    reassign_query = sql.SQL("REASSIGN OWNED BY {} TO {};").format(
                        sql.Identifier(app_user), sql.Identifier(superuser_user_name)
                    )
                    if superuser_db_ops.execute_command(reassign_query):
                        logger.info(f"Ownership reassigned for objects owned by '{app_user}'.")
                    else:
                        logger.warning(f"Could not reassign owned by '{app_user}'. Continuing with drop.")

                    drop_owned_query = sql.SQL("DROP OWNED BY {};").format(sql.Identifier(app_user))
                    if superuser_db_ops.execute_command(drop_owned_query):
                        logger.info(f"Objects owned by '{app_user}' dropped and privileges revoked.")
                    else:
                        logger.warning(f"Could not drop owned by '{app_user}'. Continuing with user drop.")
                        
                    logger.info(f"Dropping user '{app_user}'...")
                    drop_user_query = sql.SQL("DROP USER {};").format(sql.Identifier(app_user))
                    if not superuser_db_ops.execute_command(drop_user_query):
                        logger.error(f"Failed to drop user '{app_user}'. Aborting setup.")
                        return False
                    logger.info(f"User '{app_user}' dropped successfully.")

                except Psycopg2Error as e:
                    logger.error(f"Database error while dropping user '{app_user}': {e}", exc_info=True)
                    return False
                except Exception as e:
                    logger.error(f"An unexpected error occurred while dropping user '{app_user}': {e}", exc_info=True)
                    return False
            else:
                logger.info(f"User '{app_user}' does not exist. Skipping user drop.")

            logger.info(f"Creating user '{app_user}'...")
            create_user_query = sql.SQL("CREATE USER {} WITH PASSWORD %s;").format(sql.Identifier(app_user))
            if not superuser_db_ops.execute_command(create_user_query, (app_password,)):
                logger.error(f"Failed to create application user '{app_user}'. Aborting setup.")
                return False
            logger.info(f"User '{app_user}' created successfully.")

            logger.info(f"Creating database '{app_db_name}' owned by '{app_user}'...")
            create_db_query = sql.SQL("CREATE DATABASE {} WITH OWNER = {};").format(
                sql.Identifier(app_db_name), sql.Identifier(app_user)
            )
            if not superuser_db_ops.execute_command(create_db_query):
                logger.error(f"Failed to create application database '{app_db_name}'. Aborting setup.")
                return False
            logger.info(f"Database '{app_db_name}' created successfully.")

            logger.info(f"Granting privileges to user '{app_user}' on database '{app_db_name}'...")
            grant_connect_query = sql.SQL("GRANT CONNECT ON DATABASE {} TO {};").format(
                sql.Identifier(app_db_name), sql.Identifier(app_user)
            )
            grant_future_all_on_tables = sql.SQL("ALTER DEFAULT PRIVILEGES FOR USER {} IN SCHEMA public GRANT ALL ON TABLES TO {};").format(
                sql.Identifier(app_user), sql.Identifier(app_user)
            )
            grant_future_all_on_sequences = sql.SQL("ALTER DEFAULT PRIVILEGES FOR USER {} IN SCHEMA public GRANT ALL ON SEQUENCES TO {};").format(
                sql.Identifier(app_user), sql.Identifier(app_user)
            )

            if not superuser_db_ops.execute_command(grant_connect_query) or \
               not superuser_db_ops.execute_command(grant_future_all_on_tables) or \
               not superuser_db_ops.execute_command(grant_future_all_on_sequences):
                logger.error(f"Failed to grant privileges to user '{app_user}' on database '{app_db_name}'. Aborting setup.")
                return False
            logger.info(f"Privileges granted to user '{app_user}' on database '{app_db_name}'.")

        except Exception as e:
            logger.error(f"Error during superuser operations: {e}", exc_info=True)
            return False
        finally:
            superuser_db_ops.close()
            logger.info("Superuser connection closed.")

        sql_script_name_for_schema = self.file_config.supabase_schema if self.db_type == 'supabase' else self.file_config.schema_file

        logger.info(f"Applying schema '{sql_script_name_for_schema}' to database '{self.app_conn_details.dbname}' as user '{self.app_conn_details.user}'...")
        try:
            app_db_ops.connect(self.app_conn_details)
            app_db_ops.set_autocommit(True)

            sql_script_path = os.path.abspath(os.path.join(vivarium_root_path, sql_script_name_for_schema))

            if not os.path.exists(sql_script_path):
                logger.error(f"SQL schema file not found: {sql_script_path}")
                return False

            with open(sql_script_path, 'r') as f:
                sql_commands_raw = f.read()

            sql_commands_cleaned = re.sub(r"^[ \t]*--.*$", "", sql_commands_raw, flags=re.MULTILINE)
            sql_commands_cleaned = re.sub(r"/\*.*?\*/", "", sql_commands_cleaned, flags=re.DOTALL)

            commands_executed_successfully = True
            for command in sql_commands_cleaned.split(';'):
                stripped_command = command.strip()
                if stripped_command:
                    try:
                        app_db_ops.execute_command(stripped_command)
                    except Psycopg2Error as e:
                        if "permission denied for schema public" in str(e).lower():
                            logger.error(f"FATAL: Permission denied to create object in 'public' schema. "
                                            f"Ensure user has CREATE privilege on public schema. "
                                            f"Command failed: {stripped_command[:100]}... Error: {e}")
                            commands_executed_successfully = False
                            break
                        elif "already exists" in str(e).lower() or "cannot create" in str(e).lower() and "already exists" in str(e).lower():
                            logger.warning(f"Table or object already exists (non-fatal, usually ok for idempotency): {stripped_command[:100]}... Error: {e}")
                        else:
                            logger.error(f"Error executing SQL command: {stripped_command[:100]}...\nError: {e}")
                            commands_executed_successfully = False
                            break
                    except Exception as e:
                        logger.error(f"An unexpected error occurred executing SQL command: {stripped_command[:100]}...\nError: {e}", exc_info=True)
                        commands_executed_successfully = False
                        break

            if not commands_executed_successfully:
                logger.error("Some SQL commands failed during schema application. Check logs for details. Aborting further setup.")
                return False

            logger.info("SQL script executed successfully. Tables created/verified.")

        except Exception as e:
            logger.error(f"Failed to apply schema: {e}", exc_info=True)
            return False
        finally:
            app_db_ops.close()
            logger.info("Application user connection closed after schema application.")

        logger.info("PostgreSQL database setup completed successfully.")
        return True