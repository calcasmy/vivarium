# vivarium/deploy/src/database/postgres_setup.py
import os
import re
import sys
import getpass
import psycopg2
from typing import Dict
from psycopg2 import sql, OperationalError, Error as Psycopg2Error
from psycopg2.extensions import cursor as Psycopg2Cursor

if __name__ == "__main__":
    vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
    if vivarium_path not in sys.path:
        sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.config import DatabaseConfig, FileConfig
from utilities.src.database_operations import DatabaseOperations
from deploy.src.database.db_setup_strategy import DBSetupStrategy

logger = LogHelper.get_logger(__name__)

class PostgresSetup(DBSetupStrategy):
    """
    Concrete strategy for setting up a PostgreSQL database.
    Implements the DBSetupStrategy abstract methods.
    """
    def __init__(self, is_remote: bool):
        """
        Initializes the PostgresSetup strategy.

        :param is_remote: Boolean indicating if the connection is remote.
                          This might influence how certain operations are performed (e.g., host).
        :type is_remote: bool
        """
        super().__init__()  # Call the parent constructor to set self.sql_schema_base_path
        self.is_remote = is_remote
        self.db_config = DatabaseConfig()
        self.db_ops = DatabaseOperations(self.db_config)
        logger.info(f"PostgresSetup: Initialized for {'remote' if is_remote else 'local'} PostgreSQL setup.")

    def prompt_for_restart(self) -> bool:
        """
        Prompts the user to manually restart the PostgreSQL service and waits for confirmation.

        This step is often crucial after database creation or user privilege changes
        to ensure PostgreSQL reloads its configuration.

        :returns: True if the user confirms the restart, False otherwise (though currently always returns True after prompt).
        :rtype: bool
        """

        logger.info("\n" + "="*80)
        logger.info("  IMPORTANT: PostgreSQL Service Restart Required ")
        logger.info("="*80)
        logger.info("Please manually restart your PostgreSQL database service before continuing.")
        logger.info("This ensures a clean state for database operations.")

        # Provide common commands for the user
        logger.info("\nCommon restart commands:")
        logger.info("  Linux (systemd):   sudo systemctl restart postgresql")
        logger.info("  macOS (Homebrew):  pg_ctl restart -D /opt/homebrew/var/postgresql@15 (adjust path if different)")
        logger.info("  Windows (Service): Restart 'PostgreSQL' service via Services (services.msc) or from command line (run as Administrator):")
        logger.info("                     net stop postgresql-x64-15 && net start postgresql-x64-15 (adjust service name)")
        logger.info("\nAfter executing the appropriate command, press Enter to continue...")

        input("Press Enter to confirm PostgreSQL service has been restarted...")
        logger.info("User confirmed service restart. Continuing with database setup.")
        logger.info("="*80 + "\n")
        return True

    def create_database_and_user(self) -> bool:
        """
        Orchestrates the creation and setup of the Vivarium database and a dedicated application user.
        This operation requires connecting as a PostgreSQL superuser (e.g., 'postgres').

        The process includes:
        1. Checking for and optionally dropping an existing database.
        2. Handling the creation/dropping of the application user.
        3. Creating the new application database.
        4. Granting initial connection and schema permissions to the application user.

        :returns: True if the entire database and user creation process is successful or relevant entities already exist, False otherwise.
        :rtype: bool
        :raises ValueError: If a required admin password is not provided.
        :raises OperationalError: If a connection to the PostgreSQL server as admin fails.
        :raises Exception: For any other unexpected errors during database/user creation.
        """

        try:
            admin_db_params = self._get_admin_db_params()

            # 1: Dropping the database if exists
            logger.info("PostgresSetup: Checking and dropping existing database (if any) before user setup...")
            if not self._drop_database_if_exists(admin_db_params):
                logger.error("PostgresSetup: Failed to handle existing database. Aborting setup.")
                return False

            # 2: Handle user setup (drop user if exists, then create user)
            logger.info("PostgresSetup: Starting user setup process...")
            if not self._handle_user_setup(admin_db_params):
                logger.error("PostgresSetup: User setup failed.")
                return False

            # 3: Create database
            logger.info("PostgresSetup: Starting database creation process...")
            if not self._create_database(admin_db_params):
                logger.error("PostgresSetup: Database creation failed.")
                return False

            # 4: Grant initial database permissions to the new user.
            logger.info("PostgresSetup: Starting initial database permissions setup...")
            if not self._grant_initial_db_permissions(admin_db_params):
                logger.error("PostgresSetup: Initial database permissions setup failed.")
                return False

            logger.info("PostgresSetup: Database and user setup complete.")
            return True

        except ValueError as e:
            logger.error(f"PostgresSetup: Configuration error: {e}")
            return False
        except OperationalError as e:
            logger.error(f"PostgresSetup: Could not connect to PostgreSQL as admin user. "
                         f"Please ensure PostgreSQL is running and admin credentials are correct. Error: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"PostgresSetup: An unexpected error occurred during the overall setup process: {e}", exc_info=True)
            return False

    def create_tables(self, sql_script_name: str) -> bool:
        """
        Creates tables in the Vivarium database using the provided SQL schema script.

        This method connects to the target database using the application user credentials
        (managed by `self.db_ops`). It reads the SQL script, removes comments, and
        executes each SQL command. It handles cases where tables already exist
        and provides specific error logging for permission issues.

        :param sql_script_name: The name of the SQL schema script (e.g., 'postgres_schema.sql').
        :type sql_script_name: str
        :returns: True if table creation is successful, False otherwise.
        :rtype: bool

        :raises FileNotFoundError: If the SQL schema file does not exist at the expected path.
        :raises Exception: For any other errors during SQL file reading or execution.
        """
        logger.info(f"PostgresSetup: Creating tables using schema script: {sql_script_name}.")
        
        full_script_path = os.path.join(self.sql_schema_base_path, sql_script_name)
        if not os.path.exists(full_script_path):
            logger.error(f"SQL schema file not found at: {full_script_path}. Cannot create tables.")
            return False
        
        try:
            logger.info(f"PostgresSetup: Reading SQL schema from: {full_script_path}")
            with open(full_script_path, 'r') as f:
                sql_commands = f.read()

            # -- Clean up / Remove comments and split into individual commands --
            # This regex removes -- style comments and /* */ style comments.
            sql_commands = re.sub(r"^[ \t]*--.*$", "", sql_commands, flags=re.MULTILINE)
            sql_commands = re.sub(r"/\*.*?\*/", "", sql_commands, flags=re.DOTALL)

            commands_executed_successfully = True
            # Split by semicolon, but handle cases where semicolons might be inside quoted strings
            # This simple split might break for complex SQL with escaped semicolons.
            # For robust parsing, a dedicated SQL parser might be needed, but for typical schema files, this is often sufficient.
            for command in sql_commands.split(';'):
                stripped_command = command.strip()
                if stripped_command:
                    try:
                        self.db_ops.execute_query(stripped_command)
                    except Psycopg2Error as e:
                        # Specific error handling for permission denied on public schema
                        if "permission denied for schema public" in str(e).lower():
                            logger.error(f"FATAL: Permission denied to create object in 'public' schema. "
                                            f"Ensure user has CREATE privilege on public schema. "
                                            f"Command failed: {stripped_command[:100]}... Error: {e}")
                            commands_executed_successfully = False
                            break
                        elif "already exists" in str(e).lower():
                            logger.warning(f"Table or object already exists (non-fatal): {stripped_command[:100]}... Error: {e}")
                            # Continue processing other commands even if one exists
                        else:
                            logger.error(f"Error executing SQL command: {stripped_command[:100]}...\nError: {e}")
                            commands_executed_successfully = False
                            break # Breaking on any unhandled psycopg2 error
                    except Exception as e:
                        logger.error(f"An unexpected error occurred executing SQL command: {stripped_command[:100]}...\nError: {e}", exc_info=True)
                        commands_executed_successfully = False
                        break # Breaking on any unexpected error
                    
            if commands_executed_successfully:
                logger.info("SQL script executed successfully. Tables created/verified.")
            else:
                logger.error("Some SQL commands failed during table creation. Check logs for details. Aborting further setup.")
                return False # Indicate failure if any command failed

            return commands_executed_successfully
            
        except FileNotFoundError:
            logger.error(f"PostgresSetup: SQL schema file not found at {full_script_path}")
            return False
        except Exception as e:
            logger.error(f"PostgresSetup: An error occurred while creating tables: {e}")
            return False
        # Removed the self.db_ops.close() from here, as it's now handled by execute_full_setup.

    def execute_full_setup(self, sql_script_name: str) -> bool:
        """
        Orchestrates the full PostgreSQL setup process.

        This method performs the following steps:
        1. Creates the Vivarium database and application user (dropping if exists).
           This step connects as a superuser.
        2. Prompts the user to restart the PostgreSQL service (if required).
        3. Creates all necessary tables and schema objects in the Vivarium database
           using the provided SQL script. This step uses the application user.

        The `self.db_ops` connection (initialized in `__init__`) is managed throughout
        these steps and is closed in the `finally` block of this method to ensure
        resource cleanup.

        :param sql_script_name: The name of the SQL schema script (e.g., 'postgres_schema.sql').
                                This script defines the database schema (tables, indexes, etc.).
        :type sql_script_name: str
        :returns: True if the full setup is successful, False otherwise.
        :rtype: bool
        """
        logger.info(f"PostgresSetup: Starting full setup execution for {self.db_config.dbname}.")
        
        success = False
        try:
            # 1. Create database and user (requires admin password and temporary admin connection)
            if not self.create_database_and_user():
                logger.error("PostgresSetup: Failed to create database and/or user.")
                return False
            
            # 2. Prompt for restart (if needed, ensures changes from step 1 are applied)
            # Note: This restart is crucial if you created the database or user for the first time
            # or enabled new extensions, as PostgreSQL might need to reload config.
            if not self.prompt_for_restart():
                # If user cancels restart, stop setup
                logger.info("PostgresSetup: Setup aborted by user.")
                return False

            # 3. Create tables (uses the application user connection managed by self.db_ops)
            if not self.create_tables(sql_script_name):
                logger.error("PostgresSetup: Failed to create tables.")
                return False
            
            logger.info("PostgresSetup: Full PostgreSQL setup process completed.")
            success = True
            return success
            
        finally:
            self.db_ops.close()
            logger.info("DatabaseOperations connection closed after setup execution.")
    
# --- Private Fuctions ---

    def _drop_database_if_exists(self, base_db_params: Dict) -> bool:
        """
        Checks if the application database exists and drops it.

        This method connects to the 'postgres' (or superuser's default) database
        using the provided superuser credentials. If the target application database
        exists, it attempts to terminate any active connections to it and then
        drops the database. It uses explicit, isolated connections for these
        sensitive administrative commands.

        :param base_db_params: A dictionary of connection parameters for the superuser.
                               It should include 'user', 'host', 'port', 'password',
                               and 'dbname' (typically 'postgres').
        :type base_db_params: Dict
        :returns: True if the database drop phase completed successfully (whether
                  the database was dropped or did not exist), False otherwise.
        :rtype: bool
        """
        db_exists = False
        try:
            # Check Database Existence
            with psycopg2.connect(**base_db_params) as check_conn:
                check_conn.autocommit = True
                with check_conn.cursor() as check_cursor:
                    check_cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [self.db_config.dbname])
                    db_exists = check_cursor.fetchone() is not None
            
        except (OperationalError, Psycopg2Error) as e:
            logger.error(f"PostgresSetup: Database error checking database existence: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"PostgresSetup: Unexpected error during database existence check: {e}", exc_info=True)
            return False

        if db_exists:
            logger.warning(f"Database '{self.db_config.dbname}' found. Attempting to drop existing database.")
            
            terminate_conn = None
            drop_conn = None
            try:
                # Terminate active connections to the target database
                terminate_conn = psycopg2.connect(**base_db_params)
                terminate_cursor = terminate_conn.cursor()
                terminate_conn.autocommit = True

                logger.info(f"Terminating active connections to database '{self.db_config.dbname}'...")
                terminate_cursor.execute(sql.SQL("""
                    SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = %s
                    AND pid <> pg_backend_pid();
                """), [self.db_config.dbname])
                logger.info(f"Successfully sent terminate signals.")
                
                # No need for rollback with autocommit=True. Rollback only makes sense for transactions.
                # terminate_conn.rollback() # Defensive rollback (safe with autocommit=True)
                
                terminate_cursor.close()
                terminate_conn.close()

                # Execute DROP DATABASE on a fresh, isolated connection
                drop_conn = psycopg2.connect(**base_db_params)
                drop_cursor = drop_conn.cursor()
                drop_conn.autocommit = True 

                logger.info(f"Executing DROP DATABASE for '{self.db_config.dbname}'...")
                drop_cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(self.db_config.dbname)))
                logger.info(f"Database '{self.db_config.dbname}' dropped successfully.")
                drop_cursor.close()
                drop_conn.close()

            except (OperationalError, Psycopg2Error) as e:
                logger.error(f"PostgresSetup: Database error during DROP DATABASE: {e}", exc_info=True)
                logger.error("Ensure no direct connections to the database persist or that it exists.")
                return False
            except Exception as e:
                logger.error(f"PostgresSetup: Unexpected error during DROP DATABASE: {e}", exc_info=True)
                return False
            finally:
                if terminate_conn and not terminate_conn.closed:
                    terminate_conn.close()
                if drop_conn and not drop_conn.closed:
                    drop_conn.close()
        else:
            logger.info(f"Database '{self.db_config.dbname}' does not exist. Skipping database drop.")
        
        return True # Database drop phase completed successfully (whether dropped or not)

    def _create_database(self, base_db_params: Dict) -> bool:
        """
        Creates the application database using a superuser connection.

        This method connects to a default database (e.g., 'postgres') as a superuser
        and executes the `CREATE DATABASE` command. It includes robust error handling
        for cases where the database already exists or other operational issues occur.

        :param base_db_params: A dictionary of connection parameters for the superuser.
                               It should include 'user', 'host', 'port', 'password',
                               and 'dbname' (typically 'postgres').
        :type base_db_params: Dict
        :returns: True if the database is successfully created or already exists, False otherwise.
        :rtype: bool
        """
        create_conn = None
        try:
            create_conn = psycopg2.connect(**base_db_params)
            create_cursor = create_conn.cursor()
            create_conn.autocommit = True 

            template_db_to_use = 'template0' # Force template0 for collation compatibility
            logger.info(f"Executing CREATE DATABASE '{self.db_config.dbname}' with owner '{self.db_config.user}' using template '{template_db_to_use}'...")
            create_cursor.execute(sql.SQL("CREATE DATABASE {} WITH OWNER {} TEMPLATE {} ENCODING 'UTF8' LC_COLLATE='C' LC_CTYPE='C'").format(
                sql.Identifier(self.db_config.dbname),
                sql.Identifier(self.db_config.user),
                sql.Identifier(template_db_to_use)
            ))
            logger.info(f"Database '{self.db_config.dbname}' created successfully.")

        except (OperationalError, Psycopg2Error) as e:
            if 'already exists' in str(e):
                logger.warning(f"PostgresSetup: Database '{self.db_config.dbname}' already exists. Skipping database creation.")
            else:
                logger.error(f"PostgresSetup: Database error during CREATE DATABASE: {e}", exc_info=True)
                return False
        except Exception as e:
            logger.error(f"PostgresSetup: Unexpected error during CREATE DATABASE: {e}", exc_info=True)
            return False
        finally:
            if create_conn and not create_conn.closed:
                create_cursor.close()
                create_conn.close()
        
        return True # Database creation phase completed successfully (whether created or skipped)

    def _handle_user_setup(self, base_db_params: Dict) -> bool:
        """
        Handles the dropping and creation of the PostgreSQL application user.

        This method connects as a superuser to manage the lifecycle of the application user.
        It attempts to drop the user if it exists, including reassigning/dropping owned objects
        to prevent dependency issues. After ensuring the old user is gone, it creates a new user
        with the specified password.

        :param base_db_params: A dictionary of connection parameters for the superuser.
                               It should include 'user', 'host', 'port', 'password',
                               and 'dbname' (typically 'postgres').
        :type base_db_params: Dict
        :returns: True if the user setup (drop and/or create) is successful, False otherwise.
        :rtype: bool
        """
        try:
            with psycopg2.connect(**base_db_params) as conn:
                conn.autocommit = True
                with conn.cursor() as cursor:
                    # --- Drop User if Exists (with robust cleanup) ---
                    logger.info(f"Checking if user '{self.db_config.user}' exists for dropping...")
                    cursor.execute(sql.SQL("SELECT 1 FROM pg_roles WHERE rolname = %s"), [self.db_config.user])
                    if cursor.fetchone():
                        logger.warning(f"User '{self.db_config.user}' found. Attempting to clean up dependencies before dropping user...")
                        try:
                            cursor.execute(sql.SQL("REASSIGN OWNED BY {} TO {};").format(
                                sql.Identifier(self.db_config.user), sql.Identifier(base_db_params['user'])
                            ))
                            logger.info(f"Ownership reassigned for objects owned by '{self.db_config.user}' in current DB.")
                        except Psycopg2Error as e:
                            logger.warning(f"Could not reassign owned by '{self.db_config.user}' (may not own objects here): {e}")

                        try:
                            cursor.execute(sql.SQL("DROP OWNED BY {};").format(
                                sql.Identifier(self.db_config.user)
                            ))
                            logger.info(f"Objects owned by '{self.db_config.user}' dropped and privileges revoked in current DB.")
                        except Psycopg2Error as e:
                            logger.warning(f"Could not drop owned by '{self.db_config.user}' (may not own objects or have privileges here): {e}")

                        logger.info(f"Dropping user '{self.db_config.user}'...")
                        cursor.execute(sql.SQL("DROP USER {}").format(sql.Identifier(self.db_config.user)))
                        logger.info(f"User '{self.db_config.user}' dropped successfully.")
                    else:
                        logger.info(f"User '{self.db_config.user}' does not exist. Skipping user drop.")

                    # --- Create User ---
                    try:
                        cursor.execute(sql.SQL("CREATE USER {} WITH PASSWORD %s").format(
                            sql.Identifier(self.db_config.user)), [self.db_config.password]) 
                        logger.info(f"PostgresSetup: User '{self.db_config.user}' created.") 
                    except Psycopg2Error as e:
                        if 'duplicate key value violates unique constraint' in str(e) or 'already exists' in str(e):
                            logger.warning(f"PostgresSetup: User '{self.db_config.user}' already exists. Skipping user creation.") 
                        else:
                            logger.error(f"PostgresSetup: Error creating user '{self.db_config.user}': {e}", exc_info=True) 
                            return False
            return True # User setup successful

        except (OperationalError, Psycopg2Error) as e:
            logger.error(f"PostgresSetup: A database error occurred during user setup: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"PostgresSetup: An unexpected non-database error occurred during user setup: {e}", exc_info=True)
            return False

    def _grant_initial_db_permissions(self, admin_db_params: Dict) -> bool:
        """
        Grants initial necessary privileges on the newly created database to the application user.

        This method connects to the *newly created* database as a superuser and grants
        CONNECT privilege to the application user. It then calls `_grant_user_permissions`
        to set up schema and default object privileges within that database.

        :param admin_db_params: A dictionary of connection parameters for the superuser.
                                It should include 'user', 'host', 'port', 'password',
                                and 'dbname' (the newly created application database name).
        :type admin_db_params: Dict
        :returns: True if initial permissions are granted successfully, False otherwise.
        :rtype: bool
        """
        try:
            # Need to connect to the *newly created* database as superuser to grant permissions on it
            conn_to_new_db_params = admin_db_params.copy()
            conn_to_new_db_params['dbname'] = self.db_config.dbname # Connect to the newly created database

            with psycopg2.connect(**conn_to_new_db_params) as perm_conn:
                perm_conn.autocommit = True
                with perm_conn.cursor() as perm_cursor:
                    logger.info(f"Granting CONNECT privilege on database '{self.db_config.dbname}' to user '{self.db_config.user}'...")
                    perm_cursor.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                        sql.Identifier(self.db_config.dbname), sql.Identifier(self.db_config.user)
                    ))
                    logger.info(f"CONNECT privilege granted on database '{self.db_config.dbname}' to user '{self.db_config.user}'.")

                    # Prepare params for existing _grant_user_permissions method
                    manual_prompt_params = {
                        'user': admin_db_params.get('user', 'N/A'),
                        'host': admin_db_params.get('host', 'N/A'),
                        'port': admin_db_params.get('port', 'N/A'),
                        'db_name': self.db_config.dbname,
                        'app_user': self.db_config.user
                    }
                    if not self._grant_user_permissions(super_params=manual_prompt_params, super_curor=perm_cursor):
                        logger.error("PostgresSetup: Failed to fully automate user permissions after database creation. Manual steps may be required.")
                        return False
            return True
        except (OperationalError, Psycopg2Error) as e:
            logger.error(f"PostgresSetup: Database error during initial permissions setup: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"PostgresSetup: An unexpected non-database error occurred during initial permissions setup: {e}", exc_info=True)
            return False

    def _grant_user_permissions(self, super_params: Dict, super_curor: Psycopg2Cursor) -> bool:
        """
        Grants necessary privileges on the 'public' schema and sets default privileges
        for future objects created by the application user.

        This method attempts to:
        1. REVOKE default public access from the 'public' schema.
        2. GRANT CREATE and USAGE privileges on the 'public' schema to the
           application user (`self.db_config.user`).
        3. Set default privileges for future tables created by the application user
           in the 'public' schema (SELECT, INSERT, UPDATE, DELETE, etc.).
        4. Set default privileges for future sequences created by the application user
           in the 'public' schema (USAGE, SELECT, UPDATE).

        If the automated granting of public schema permissions fails (e.g., due
        to specific server configurations or insufficient superuser privileges),
        it falls back to prompting the user for manual intervention.

        :param super_params: A dictionary containing parameters necessary for manual
                             instructions if automation fails (e.g., superuser, host, port).
        :type super_params: Dict
        :param super_curor: An active psycopg2 cursor connected as a superuser to the
                            target application database.
        :type super_curor: Psycopg2Cursor
        :returns: True if all specified permissions are granted successfully (automated or manual), False otherwise.
        :rtype: bool
        """
        try:
            # --- AUTOMATE PUBLIC SCHEMA PERMISSIONS (if possible) ---
            logger.info(f"Attempting to grant CREATE, USAGE on public schema to '{self.db_config.user}' in '{self.db_config.dbname}'...")

            try:
                # REVOKE default PUBLIC access on schema public
                super_curor.execute(sql.SQL("REVOKE ALL ON SCHEMA public FROM PUBLIC;"))
                logger.info("REVOKED ALL ON SCHEMA public FROM PUBLIC.")

                # GRANT CREATE, USAGE on public schema to the application user
                super_curor.execute(sql.SQL("GRANT CREATE, USAGE ON SCHEMA public TO {};").format(
                    sql.Identifier(self.db_config.user)
                ))
                logger.info(f"GRANTED CREATE, USAGE ON SCHEMA public TO '{self.db_config.user}' in '{self.db_config.dbname}'.")

                # Set default privileges for future tables created by the application user in public schema
                logger.info(f"Setting default privileges for future tables created by '{self.db_config.user}' in schema 'public'...")
                super_curor.execute(sql.SQL("""
                    ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA public
                    GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
                    ON TABLES TO {};
                """).format(sql.Identifier(self.db_config.user), sql.Identifier(self.db_config.user)))
                logger.info("Default privileges for tables set successfully.")

                # Set default privileges for future sequences created by the application user in public schema
                logger.info(f"Setting default privileges for sequences created by '{self.db_config.user}' in schema 'public'...")
                super_curor.execute(sql.SQL("""
                    ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA public
                    GRANT USAGE, SELECT, UPDATE
                    ON SEQUENCES TO {};
                """).format(sql.Identifier(self.db_config.user), sql.Identifier(self.db_config.user)))
                logger.info("Default privileges for sequences set successfully.")
                
                return True # Automated permissions granted successfully

            except Psycopg2Error as e:
                logger.error(f"Error automating public schema permissions: {e}. Manual steps might be required.")
                self._prompt_for_manual_public_schema_permissions(super_params)
                return False # Automated attempt failed, manual prompt given.

        except (OperationalError, Psycopg2Error) as e:
            logger.error(f"Database error during granting schema and default privileges: {e}")
            return False
        except Exception as e:
            logger.error(f"An unexpected non-database error occurred during granting schema and default privileges: {e}", exc_info=True)
            return False

    def _prompt_for_manual_public_schema_permissions(self, db_params: Dict):
        """
        Helper to prompt the user for manual public schema permissions if automated
        permission granting fails.

        This method displays detailed instructions for the user to log into
        PostgreSQL as a superuser and manually execute SQL commands to grant
        CREATE and USAGE privileges on the 'public' schema to the application user.
        It then waits for user confirmation before proceeding.

        :param db_params: A dictionary containing parameters necessary for the manual
                          instructions, typically including:
                          - 'user': The PostgreSQL superuser name (e.g., 'postgres').
                          - 'host': The PostgreSQL host (e.g., 'localhost' or remote IP).
                          - 'port': The PostgreSQL port (e.g., 5432).
                          - (Optional, but recommended for clarity in prompt)
                            'app_user': The application user name to grant permissions to.
                            'db_name': The database name on which to grant permissions.
        :type db_params: Dict
        :returns: None
        """
        logger.info("\n" + "="*80)
        logger.info("  IMPORTANT: Manual Public Schema Permissions Required ")
        logger.info("="*80)
        logger.info(f"Automatic setup of public schema permissions failed or was skipped.")
        logger.info(f"Please log into your PostgreSQL server as superuser (e.g., '{db_params['user']}')")
        logger.info(f"and execute the following SQL commands to grant '{self.db_config.user}' CREATE permissions")
        logger.info(f"on the 'public' schema within the '{self.db_config.dbname}' database:")

        logger.info("\n")
        logger.info(f"  1. Switch to the PostgreSQL user:            sudo su {db_params['user']}")
        logger.info(f"     (Enter your system's sudo password if prompted.)")
        logger.info("\n")
        logger.info(f"  2. Connect to psql as the superuser:        psql -h {db_params['host']} -p {db_params['port']} -U {db_params['user']} -d {self.db_config.dbname}")
        logger.info(f"     (Enter the PostgreSQL superuser password when prompted.)")
        logger.info("\n")
        logger.info("  3. Inside psql (at the 'vivarium=#' prompt), execute the following commands:")
        logger.info("     REVOKE ALL ON SCHEMA public FROM PUBLIC;")
        logger.info(f"     GRANT CREATE, USAGE ON SCHEMA public TO {self.db_config.user};")
        logger.info("\n")
        logger.info("  4. Exit psql:                                \\q")
        logger.info(f"  5. (Optional) Exit the postgres user shell: exit")
        logger.info("\n")
        input("After executing these commands, press Enter here to continue...")
        logger.info("User confirmed public schema permissions. Continuing with table setup.")
        logger.info("="*80 + "\n")

    def _get_admin_password(self) -> str:
        """
        Prompts the user for the PostgreSQL administrative (e.g., 'postgres') user's password.

        This method uses `getpass.getpass` for secure password input without echoing.
        It also validates that a non-empty password is provided.

        :returns: The password entered by the user.
        :rtype: str
        :raises ValueError: If an empty password is provided.
        """
        password = getpass.getpass(f"Enter password for PostgreSQL admin user '{self.db_config.superuser}': ")
        if not password:
            logger.error("Admin password cannot be empty. Please provide a valid password.")
            raise ValueError("Admin password cannot be empty.")
        return password
    
    def _get_admin_db_params(self) -> Dict:
        """
        Prepares base connection parameters for superuser operations.

        This method selects the appropriate PostgreSQL connection parameters
        (local or remote) from `self.db_config` and then overrides the user,
        database name (to connect as a superuser), and prompts for the admin password.

        :returns: A dictionary of connection parameters suitable for connecting as a superuser.
        :rtype: Dict
        """
        admin_db_params = {}
        if self.is_remote:
            logger.info("PostgresSetup: Building connection for remote superuser operations.")
            admin_db_params = self.db_config.postgres_remote.copy()
        else:
            logger.info("PostgresSetup: Building connection for local superuser operations.")
            admin_db_params = self.db_config.postgres.copy()

        admin_db_params['user'] = self.db_config.superuser
        admin_db_params['dbname'] = self.db_config.superuser_dbname # Connect to 'postgres' for admin tasks
        admin_db_params['password'] = self._get_admin_password()
        return admin_db_params