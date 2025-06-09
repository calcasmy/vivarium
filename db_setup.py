import os
import re
import sys
import argparse
import psycopg2
from psycopg2 import sql
import getpass
import time

if __name__ == "__main__":
    vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if vivarium_path not in sys.path:
        sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.database_operations import DatabaseOperations
from utilities.src.config import DatabaseConfig, FileConfig
from weather.load_rawfiles import main as load_rawfiles_main

logger = LogHelper.get_logger(__name__)

class DBSetup:
    def __init__(self, 
                 isremote: bool = False):
        """
        Initializes DBSetup with configuration.
        """
        self.db_config = DatabaseConfig()
        self.file_config = FileConfig()
        self.isremote = isremote

    def prompt_for_restart(self) -> bool:
        """
        Prompts the user to manually restart the PostgreSQL service and waits for confirmation.
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
        return True # Always returns True as we rely on user confirmation

    def create_database_and_user(self) -> bool:
        """
        Connects to the default 'postgres' database using superuser credentials
        to drop/create the target application database and user,
        and grants specific, necessary permissions. Prompts for details if isremote=True.

        Returns:
            bool: True if database and user setup was successful, False otherwise.
        """
        # --- Determine application user/database parameters from config (defaults) ---
        app_params = self.db_config.postgres_remote if self.isremote else self.db_config.postgres
        
        # --- Determine superuser connection parameters from config (defaults) ---
        superuser_conn_params = self.db_config.postgres_super
        
        # --- Determine Superuser Host ---
        super_host = None
        if self.isremote:
            super_host = input("Enter remote database host (IP or hostname): ").strip()
            if not super_host:
                logger.error("Remote database host cannot be empty. Aborting.")
                return False
        else: # Local setup: use config default or 'localhost'
            super_host = superuser_conn_params.get('superhost', '192.168.68.72') # Using the IP you provided in logs
            logger.info(f"Using local superuser host: '{super_host}' from config or default.")

        # --- Determine Superuser Password ---
        super_password = None
        if self.isremote:
            super_password = getpass.getpass("Enter password for remote superuser: ").strip()
        else: # Local setup: try config, then prompt if not found
            super_password = superuser_conn_params.get('superpassword')
            if not super_password:
                logger.warning(f"Superuser password not found in config for local setup. Prompting securely.")
                super_password = getpass.getpass(f"Enter password for local superuser '{superuser_conn_params.get('superuser', 'postgres')}': ").strip()
        
        if not super_password:
            logger.error("Superuser password is required but not provided. Aborting.")
            return False

        # --- Determine Application Username ---
        db_user = None
        if self.isremote:
            db_user = input("Enter new application username for remote database: ").strip()
            if not db_user:
                logger.error("Application username cannot be empty. Aborting.")
                return False
        else: # Local setup: use config default
            db_user = app_params.get('user', 'vivarium') # Default if not in config
            if not db_user:
                logger.error("Application user name not found in config for local setup. Aborting.")
                return False
        
        # --- Determine Application User Password ---
        db_password = None
        if self.isremote:
            db_password = getpass.getpass(f"Enter password for new application user '{db_user}': ").strip()
            if not db_password:
                logger.error("Application user password cannot be empty. Aborting.")
                return False
        else: # Local setup: use config default
            db_password = app_params.get('password')
            if not db_password:
                logger.warning(f"Application user password not found in config for local setup. Prompting securely.")
                db_password = getpass.getpass(f"Enter password for application user '{db_user}': ").strip()

        # Gather remaining connection parameters (mostly from config or derived)
        super_user = superuser_conn_params.get('superuser', 'postgres') # Default superuser
        super_port = superuser_conn_params.get('superport', 5432)     # Default superuser port
        super_dbname = superuser_conn_params.get('superdbname', 'postgres') # Default superuser dbname
        
        # For app connection, use the determined host and port or fallback to superuser's
        app_host = app_params.get('host', 'localhost') # Default to localhost if not in config
        app_port = app_params.get('port', 5432)        # Default to 5432 if not in config
        db_name = app_params.get('dbname', 'vivarium') # Default to vivarium if not in config
        
        if not db_name:
            logger.error("Application database name (dbname) not found in config. Aborting.")
            return False


        # Final validation of essential parameters before connecting
        if not all([super_user, super_password, super_host, super_port, super_dbname, db_user, db_password, db_name, app_host, app_port]):
            logger.error("Missing essential database connection parameters after all attempts. Please check config files and prompts.")
            return False
        
        superuser_conn = None
        temp_app_conn = None

        try:
            # Connect as superuser to the default 'postgres' database
            logger.info(f"Attempting to connect as superuser '{super_user}' to database '{super_dbname}' at {super_host}:{super_port}...")
            superuser_conn = psycopg2.connect(
                dbname=super_dbname,
                user=super_user,
                password=super_password,
                host=super_host,
                port=super_port
            )
            superuser_conn.autocommit = True
            superuser_cursor = superuser_conn.cursor()

            # --- Drop Database if Exists ---
            logger.info(f"Checking if database '{db_name}' exists...")
            superuser_cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [db_name])
            if superuser_cursor.fetchone():
                logger.warning(f"Database '{db_name}' found. Terminating existing connections and dropping database...")
                # Terminate all active connections to the target database
                superuser_cursor.execute(sql.SQL("""
                    SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = %s
                      AND pid <> pg_backend_pid();
                """), [db_name])
                logger.info(f"Terminated active connections to database '{db_name}'.")
                
                # Now drop the database
                superuser_cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(db_name)))
                logger.info(f"Database '{db_name}' dropped successfully.")
            else:
                logger.info(f"Database '{db_name}' does not exist. Skipping drop.")
            
            # --- Drop User if Exists (with robust cleanup) ---
            logger.info(f"Checking if user '{db_user}' exists for dropping...")
            superuser_cursor.execute(sql.SQL("SELECT 1 FROM pg_roles WHERE rolname = %s"), [db_user])
            if superuser_cursor.fetchone():
                logger.warning(f"User '{db_user}' found. Attempting to clean up dependencies before dropping user...")

                # Reconnect to default 'postgres' database if needed, for global cleanup
                if superuser_conn.info.dbname != super_dbname:
                    superuser_conn.close()
                    logger.info("Superuser connection changed. Reconnecting to default DB for user cleanup.")
                    superuser_conn = psycopg2.connect(
                        dbname=super_dbname,
                        user=super_user,
                        password=super_password,
                        host=super_host,
                        port=super_port
                    )
                    superuser_conn.autocommit = True
                    superuser_cursor = superuser_conn.cursor()

                # Attempt to reassign ownership and drop owned objects/privileges globally
                # These commands are often sufficient even if they produce warnings for "no objects owned"
                try:
                    superuser_cursor.execute(sql.SQL("REASSIGN OWNED BY {} TO {};").format(
                        sql.Identifier(db_user), sql.Identifier(super_user)
                    ))
                    logger.info(f"Ownership reassigned for objects owned by '{db_user}'.")
                except psycopg2.Error as e:
                    logger.warning(f"Could not reassign owned by '{db_user}' (may not own objects here): {e}")

                try:
                    superuser_cursor.execute(sql.SQL("DROP OWNED BY {};").format(
                        sql.Identifier(db_user)
                    ))
                    logger.info(f"Objects owned by '{db_user}' dropped and privileges revoked in current DB.")
                except psycopg2.Error as e:
                    logger.warning(f"Could not drop owned by '{db_user}' (may not own objects or have privileges here): {e}")
                
                # Now, attempt to drop the user
                logger.info(f"Dropping user '{db_user}'...")
                superuser_cursor.execute(sql.SQL("DROP USER {}").format(sql.Identifier(db_user)))
                logger.info(f"User '{db_user}' dropped successfully.")
            else:
                logger.info(f"User '{db_user}' does not exist. Skipping drop.")


            # 1. Create the application database (This step always runs after the potential drop)
            logger.info(f"Creating database '{db_name}'...")
            superuser_cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
            logger.info(f"Database '{db_name}' created successfully.")

            # 2. Create the application user (This step always runs after the potential drop)
            logger.info(f"Creating user '{db_user}'...")
            superuser_cursor.execute(sql.SQL("CREATE USER {} WITH ENCRYPTED PASSWORD %s").format(
                sql.Identifier(db_user)
            ), [db_password])
            logger.info(f"User '{db_user}' created successfully.")

            # 3. Grant CONNECT privilege on the database to the application user
            logger.info(f"Granting CONNECT privilege on database '{db_name}' to user '{db_user}'...")
            superuser_cursor.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(db_name), sql.Identifier(db_user)
            ))
            logger.info(f"CONNECT privilege granted on database '{db_name}' to user '{db_user}'.")
            
            # Close superuser connection as it's done with its tasks
            superuser_conn.close() 
            logger.info("Superuser connection closed.")
            
            # --- NEW: User Interaction for Public Schema Permissions ---
            logger.info("\n" + "="*80)
            logger.info("  IMPORTANT: Manual Public Schema Permissions Required ")
            logger.info("="*80)
            logger.info(f"Please log into your PostgreSQL server as superuser (e.g., '{super_user}')")
            logger.info(f"and execute the following SQL commands to grant '{db_user}' CREATE permissions")
            logger.info(f"on the 'public' schema within the '{db_name}' database:")
            
            logger.info("\n")
            logger.info(f"  1. Switch to the PostgreSQL user:            sudo su {super_user}")
            logger.info(f"     (Enter your system's sudo password if prompted.)")
            logger.info("\n")
            logger.info(f"  2. Connect to psql as the superuser:        psql -h {super_host} -p {super_port} -U {super_user} -d {db_name}")
            logger.info(f"     (Enter the PostgreSQL superuser password when prompted.)")
            logger.info("\n")
            logger.info("  3. Inside psql (at the 'vivarium=#' prompt), execute the following commands:")
            logger.info("     REVOKE ALL ON SCHEMA public FROM PUBLIC;")
            logger.info(f"     GRANT CREATE, USAGE ON SCHEMA public TO {db_user};")
            logger.info("\n")
            logger.info("  4. Exit psql:                                \\q")
            logger.info(f"  5. (Optional) Exit the postgres user shell: exit")
            logger.info("\n")
            logger.info("After executing these commands, press Enter here to continue...")


            input("Press Enter to confirm public schema permissions have been set...")
            logger.info("User confirmed public schema permissions. Continuing with table setup.")
            logger.info("="*80 + "\n")

            # Establish a new connection as the application user to the *newly created database*
            logger.info(f"Connecting to '{db_name}' as '{db_user}' to set detailed permissions...")
            temp_app_conn = psycopg2.connect(
                dbname=db_name,
                user=db_user,
                password=db_password,
                host=app_host,
                port=app_port
            )
            temp_app_conn.autocommit = True
            temp_app_cursor = temp_app_conn.cursor()
            
            # Set default privileges for future tables created by vivarium in the public schema
            logger.info(f"Setting default privileges for tables created by '{db_user}' in schema 'public'...")
            temp_app_cursor.execute(sql.SQL("""
                ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA public
                GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
                ON TABLES TO {};
            """).format(sql.Identifier(db_user), sql.Identifier(db_user)))
            logger.info("Default privileges for tables set successfully.")

            # Set default privileges for future sequences created by vivarium in the public schema
            logger.info(f"Setting default privileges for sequences created by '{db_user}' in schema 'public'...")
            temp_app_cursor.execute(sql.SQL("""
                ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA public
                GRANT USAGE, SELECT, UPDATE
                ON SEQUENCES TO {};
            """).format(sql.Identifier(db_user), sql.Identifier(db_user)))
            logger.info("Default privileges for sequences set successfully.")

            return True

        except psycopg2.Error as e:
            logger.error(f"Error during database and user creation: {e}")
            return False
        finally:
            if superuser_conn and not superuser_conn.closed:
                superuser_conn.close()
                logger.info("Superuser connection closed in finally block.")
            if temp_app_conn and not temp_app_conn.closed:
                temp_app_conn.close()
                logger.info("Temporary application user connection closed in finally block.")


    def create_tables(self, sql_script_path: str = 'vivarium_schema.sql') -> bool:
        """
        Connects to the application database using the application user credentials
        and executes an SQL script to create tables.
        """
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
            logger.error("Missing essential database connection parameters for table creation. Please check your config files.")
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
                return False

            logger.info(f"Executing SQL script '{sql_script_full_path}' to create tables...")
            with open(sql_script_full_path, 'r') as f:
                full_sql_script = f.read()
                
                cleaned_sql_script = re.sub(r"^[ \t]*--.*$", "", full_sql_script, flags=re.MULTILINE)
                sql_commands = re.sub(r"/\*.*?\*/", "", cleaned_sql_script, flags=re.DOTALL)
                
                commands_executed_successfully = True
                for command in sql_commands.split(';'):
                    if command.strip():
                        try:
                            cur.execute(command)
                        except psycopg2.Error as e:
                            if "permission denied for schema public" in str(e).lower():
                                logger.error(f"FATAL: Permission denied to create object in 'public' schema. "
                                             f"Ensure user '{app_user}' has CREATE privilege on public schema. "
                                             f"Command failed: {command.strip()[:100]}... Error: {e}")
                                commands_executed_successfully = False
                                break # Stop processing if this critical permission error occurs
                            elif "already exists" in str(e):
                                logger.warning(f"Table or object already exists (non-fatal): {command.strip()[:100]}... Error: {e}")
                            else:
                                logger.error(f"Error executing SQL command: {command.strip()}\nError: {e}")
                                commands_executed_successfully = False
            
            if commands_executed_successfully:
                logger.info("SQL script executed successfully. Tables created/verified.")
            else:
                logger.warning("Some SQL commands failed during table creation. Check logs for details.")
            
            return commands_executed_successfully

        except psycopg2.Error as e:
            logger.error(f"Critical error during table creation: {e}")
            return False
        finally:
            if conn and not conn.closed:
                conn.close()
                logger.info("Application user connection closed.")

def main():
    parser = argparse.ArgumentParser(
        description="Initial DB operation, setting up DB user, DB and respective tables."
    )
    parser.add_argument(
        "--isremote",
        action="store_true",
        help="Specify if the DB setup is for a remote database. If set, will prompt for host, superuser password, new username, and new user password. Otherwise, defaults from config are used."
    )

    args = parser.parse_args()

    is_remote_setup = args.isremote
    if not is_remote_setup:
        logger.info("Neither --isremote specified. Assuming local database setup. Using configuration defaults.")
        
    db_setup_instance = DBSetup(
        isremote=is_remote_setup
    )
    
    setup_successful = True

    logger.info("Step 0: User interaction required for PostgreSQL service restart.")
    db_setup_instance.prompt_for_restart()

    logger.info("Step 1: Creating database and application user (dropping if exists)...")
    if not db_setup_instance.create_database_and_user():
        logger.error("Database and user creation failed. Aborting further setup.")
        setup_successful = False
    
    if setup_successful:
        logger.info("Step 2: Creating tables...")
        if not db_setup_instance.create_tables(sql_script_path='vivarium_schema.sql'):
            logger.error("Table creation failed. Database setup is incomplete.")
            setup_successful = False

    if setup_successful:
        logger.info("Step 3: Loading initial raw climate data from files...")
        try:
            load_rawfiles_main() 
            logger.info("Initial raw climate data loading completed.")
        except Exception as e:
            logger.exception(f"Error during initial raw climate data loading: {e}")
            setup_successful = False

    if setup_successful:
        logger.info("Database setup process completed successfully.")
    else:
        logger.error("Database setup process finished with errors. Check logs for details.")


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    logger.info(f"Total script execution time: {end_time - start_time:.2f} seconds")