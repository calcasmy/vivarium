import os
import re
import sys
import argparse
import psycopg2
from psycopg2 import sql
import getpass

if __name__ == "__main__":
    vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    if vivarium_path not in sys.path:
        sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.database_operations import DatabaseOperations
from utilities.src.config import DatabaseConfig, FileConfig

logger = LogHelper.get_logger(__name__)

class DBSetup:
    def __init__(self, 
                 isremote: bool = False, override_db_user: str = None, 
                 override_host: str = None, override_super_password: str = None):
        """
        Initializes DBSetup with configuration and optional overrides.

        Args:
            is_remote_setup (bool): True if setting up database/user for remote access, False for local.
            override_host (str, optional): Overrides the host for superuser connection. Defaults to None.
            override_db_user (str, optional): Overrides the target application database user. Defaults to None.
            initial_super_password (str, optional): Provides a superuser password for the initial setup.
                                                    If None, it will try to get it from config_secrets.ini
                                                    or prompt securely. Defaults to None.
        """
        self.db_config = DatabaseConfig()
        self.file_config = FileConfig()
        self.isremote = isremote
        self.host = override_host
        self.dbuser = override_db_user
        self.super_password = override_super_password

    def create_database_and_user(self) -> bool:
        """
        Connects to the default 'postgres' database using superuser credentials
        to create the target application database and user if they do not already exist.

        Returns:
            bool: True if database and user exists or setup was successful, False otherwise.
        """
        # -- 1. Super connectoin Parameters --
        superuser_conn_params = self.db_config.postgres_super
        
        super_user = superuser_conn_params.get('superuser')
        super_port = superuser_conn_params.get('superport')
        super_dbname = superuser_conn_params.get('superdbname')
        super_host = self.host

        super_password = self.super_password

        if not super_password:
            logger.warning(f"Superuser password not provided via CLI or found in config_secrets.ini for user '{super_user}'. Prompting securely.")
            try:
                super_password = getpass.getpass(f"Enter password for superuser '{super_user}': ") 
                if super_password == '':
                    super_password = superuser_conn_params.get('superpassword')
            except Exception as e:
                logger.error(f"Error while securely prompting for password: {e}")
            if not super_password:
                logger.error("Superuser password is required but not provided. Aborting.")
        
        # Use override_host if provided, otherwise fall back to config
        super_host = self.host if self.host is not None else superuser_conn_params.get('superhost')
        if not super_host:
            super_host = 'localhost' 
            logger.warning(f"Superuser host not specified in config or via override. Defaulting to '{super_host}'.")

        # --- Determine target application user/database parameters ---
        if self.isremote:
            app_params = self.db_config.postgres_remote
        else:
            app_params = self.db_config.postgres
        
        # Use override_db_user if provided, otherwise fall back to config
        db_user = self.dbuser if self.dbuser is not None else app_params.get('user')
        db_password = app_params.get('password') # Application user password always from config
        db_name = app_params.get('dbname')

        # Basic validation for essential parameters
        if not all([super_user, super_password, super_host, super_port, super_dbname, db_user, db_password, db_name]):
            logger.error("Missing essential database connection parameters. Please check your config files and ensure all required values are present.")
            return False
        
        dbconnection = None
        try:
            # Connect as superuser to the default 'postgres' database
            logger.info(f"Attempting to connect as superuser '{super_user}' to database '{super_dbname}' at {super_host}:{super_port}...")
            dbconnection = psycopg2.connect(
                dbname=super_dbname,
                user=super_user,
                password=super_password,
                host=super_host,
                port=super_port
            )
            dbconnection.autocommit = True # Auto-commit for DDL operations

            dbcursor = dbconnection.cursor()

            # 1. Create the application database
            logger.info(f"Checking for database '{db_name}'...")
            dbcursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s").format(
                dbname=sql.Identifier(db_name)
            ), [db_name])
            
            if not dbcursor.fetchone():
                logger.info(f"Database '{db_name}' not found. Creating...")
                dbcursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
                logger.info(f"Database '{db_name}' created successfully.")
            else:
                logger.info(f"Database '{db_name}' already exists. Skipping creation.")

            # 2. Create the application user
            logger.info(f"Checking for user '{db_user}'...")
            dbcursor.execute(sql.SQL("SELECT 1 FROM pg_roles WHERE rolname = %s").format(
                rolname=sql.Identifier(db_user)
            ), [db_user])
            
            if not dbcursor.fetchone():
                logger.info(f"User '{db_user}' not found. Creating...")
                dbcursor.execute(sql.SQL("CREATE USER {} WITH ENCRYPTED PASSWORD %s").format(
                    sql.Identifier(db_user)
                ), [db_password])
                logger.info(f"User '{db_user}' created successfully.")
            else:
                logger.info(f"User '{db_user}' already exists. Skipping creation.")

            # 3. Grant privileges to the remote user on the database
            logger.info(f"Granting all privileges on database '{db_name}' to user '{db_user}'...")
            dbcursor.execute(sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {}").format(
                sql.Identifier(db_name), sql.Identifier(db_user)
            ))
            logger.info(f"Privileges granted on database '{db_name}' to user '{db_user}'.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Error during database and user creation: {e}")
            return False
        finally:
            if dbconnection:
                dbconnection.close()
                logger.info("Superuser connection closed.")

    def create_tables(self, sql_script_path: str = 'db_schema.sql') -> bool:
        """
        Connects to the application database using the application user credentials
        and executes an SQL script to create tables.

        Args:
            sql_script_path (str): The path to the SQL schema script relative to the project root.

        Returns:
            bool: True if table creation was successful, False otherwise.
        """
        # --- Determine target application user/database parameters ---
        if self.isremote:
            app_conn_params = self.db_config.postgres_remote
        else:
            app_conn_params = self.db_config.postgres
        
        
        app_user = app_conn_params.get('user')
        app_password = app_conn_params.get('password')
        app_host = app_conn_params.get('host')
        app_port = app_conn_params.get('port')
        app_dbname = app_conn_params.get('dbname')

        if not all([app_user, app_password, app_host, app_port, app_dbname]):
            logger.error("Missing essential database connection parameters. Please check your config files and ensure all required values are present.")
            return False

        conn = None
        try:
            logger.info(f"Attempting to connect as application user '{app_user}' to database '{app_dbname}' at {app_host}:{app_port}...")
            conn = psycopg2.connect(
                dbname=app_dbname,
                user=app_user,
                password=app_password,
                host=app_host,
                port=app_port
            )
            conn.autocommit = True

            cur = conn.cursor()

            sql_script_full_path = os.path.join(self.file_config.absolute_path, sql_script_path)
            if not os.path.exists(sql_script_full_path):
                logger.error(f"SQL schema file not found at: {sql_script_full_path}. Cannot create tables.")
                return False # Indicate failure due to missing file

            logger.info(f"Executing SQL script '{sql_script_full_path}' to create tables...")
            with open(sql_script_full_path, 'r') as f:
                full_sql_script = f.read()
                
                # 1. Remove single-line comments (starts with -- to end of line)
                #    re.M makes ^ and $ match start/end of line
                #    re.sub(r"^[ \t]*--.*$", "", ..., flags=re.M) would remove the whole line if it's only a comment.
                #    Here, we'll strip comments only, so the rest of the line is still processed.
                #    A simpler approach: remove all single-line comments that are at the beginning of a line
                #    or preceded by a space.
                
                # Remove -- style comments. This regex handles comments at the start of a line
                # or after some whitespace, and removes them up to the newline.
                cleaned_sql_script = re.sub(r"^[ \t]*--.*$", "", full_sql_script, flags=re.MULTILINE)
                
                # Also remove multi-line comments /* ... */
                # re.DOTALL makes . match newlines too.
                sql_commands = re.sub(r"/\*.*?\*/", "", cleaned_sql_script, flags=re.DOTALL)

                # Now split by semicolon. The split() method will handle cases where there are
                # empty strings due to multiple semicolons or semicolons at start/end,
                # which are then filtered out by the 'if command.strip()' check.
                


                commands_executed_successfully = True
                for command in sql_commands.split(';'):
                    if command.strip():
                        try:
                            cur.execute(command)
                            # logger.debug(f"Executed: {command.strip()}")
                        except psycopg2.Error as e:
                            if "already exists" in str(e):
                                logger.warning(f"Table or object already exists (non-fatal): {command.strip()[:100]}... Error: {e}")
                            else:
                                logger.error(f"Error executing SQL command: {command.strip()}\nError: {e}")
                                commands_executed_successfully = False
                                # Continue to next command if you want to attempt all,
                                # or break if one critical failure should stop the rest.
                                # For setup, often better to log and try other tables.
            
            if commands_executed_successfully:
                logger.info("SQL script executed successfully. Tables created/verified.")
            else:
                logger.warning("Some SQL commands failed during table creation. Check logs for details.")
            
            return commands_executed_successfully # Return overall success status

        except psycopg2.Error as e:
            logger.error(f"Critical error during table creation: {e}")
            return False # Indicate failure
        finally:
            if conn:
                conn.close()
                logger.info("Application user connection closed.")

def main():


    parser = argparse.ArgumentParser(
        description="Initial DB operation, setting up DB user, DB and respective tables."
    )
    parser.add_argument(
        "--isremote",
        action="store_true",
        help="Specify if the DB setup is for a remote database."
    )
    parser.add_argument(
        "--islocal",
        action="store_true",
        help="Specify if the DB setup is for a local database."
    )
    parser.add_argument(
        "--host",
        type=str,
        help="Override the host (IP address or hostname) for the superuser connection. Defaults to value in config."
    )
    parser.add_argument(
        "--db_user",
        type=str,
        help="Override the application database user name to create. Defaults to value in config."
    )
    parser.add_argument(
        "--super_password",
        type=str,
        help="One-time password for the superuser (postgres) for initial setup."
    )

    args = parser.parse_args()

    # Validate --isremote and --islocal usage
    if args.isremote and args.islocal:
        logger.error("Cannot specify both --isremote and --islocal. Choose one.")
        
    
    db_setup_instance = DBSetup(
        isremote=args.isremote,
        override_host=args.host,
        override_db_user=args.db_user,
        override_super_password=args.super_password # Pass CLI argument if provided
    )
    
    setup_successful = True

    # Step 1: Create database and application user
    logger.info("Step 1: Creating database and application user...")
    if not db_setup_instance.create_database_and_user():
        logger.error("Database and user creation failed. Aborting further setup.")
        setup_successful = False
    
    # Step 2: Create tables (only if Step 1 was successful)
    if setup_successful:
        logger.info("Step 2: Creating tables...")
        if not db_setup_instance.create_tables(sql_script_path='db_schema.sql'):
            logger.error("Table creation failed. Database setup is incomplete.")
            setup_successful = False

    if setup_successful:
        logger.info("Database setup process completed successfully.")
    else:
        logger.error("Database setup process finished with errors. Check logs for details.")


if __name__ == "__main__":
    main()
#     logger.info("Starting database setup process (standalone run).")
#     db_setup_instance = DBSetup()
    
#     setup_successful = True

#     # Step 1: Create database and application user
#     logger.info("Step 1: Creating database and application user...")
#     if not db_setup_instance.create_database_and_user():
#         logger.error("Database and user creation failed. Aborting further setup.")
#         setup_successful = False