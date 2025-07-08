# vivarium/deploy/src/orchestrator.py
"""
The Orchestrator module is the main entry point for managing database setup and
data loading for the Vivarium application.

It uses a strategy pattern to delegate operations to specific database and
data loader implementations based on command-line arguments.
"""

import os
import sys
import time
import argparse
from typing import Optional, Tuple, Any

# Ensure vivarium root is in sys.path to resolve imports correctly.
# This block must be at the very top, before other project-specific imports.
# It allows the script to be run from any directory within the project structure.
if __name__ == "__main__":
    vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if vivarium_path not in sys.path:
        sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.config import FileConfig, DatabaseConfig, SupabaseConfig
from utilities.src.database_operations import DatabaseOperations, ConnectionDetails
from deploy.src.database.db_setup_strategy import DBSetupStrategy
from deploy.src.database.postgres_setup import PostgresSetup
from deploy.src.database.supabase_setup import SupabaseSetup
from deploy.src.database.json_data_loader import JSONDataLoader
from deploy.src.database.postgres_data_loader import PostgresDataLoader
from deploy.src.database.data_loader_strategy import DataLoaderStrategy

# Initialize core configurations and logger
file_config = FileConfig()
logger = LogHelper.get_logger(__name__)


class Orchestrator:
    """
    Orchestrates the database setup process and data loading by delegating
    the logic to specific strategies based on the selected database type.
    """

    def __init__(self, database_type: str, connection_type: str = "local"):
        """
        Initializes the Orchestrator by selecting the appropriate database setup strategy.

        :param database_type: The type of database (e.g., 'postgres', 'supabase').
        :type database_type: str
        :param connection_type: The type of connection ('local' or 'remote').
        :type connection_type: str
        :raises RuntimeError: If a supported database type is provided but no
                              valid setup strategy can be determined.
        """
        self.db_setup_strategy: Optional[DBSetupStrategy] = None
        self.db_type: str = database_type.lower()
        self.connection_type: str = connection_type.lower()
        
        # DatabaseConfig is always initialized to provide config for loaders and setups.
        self.db_config: DatabaseConfig = DatabaseConfig()
        
        logger.info(
            f"Orchestrator: Initializing for database type '{self.db_type}' "
            f"with connection type '{self.connection_type}'."
        )

        # Map database_type and connection_type to the appropriate strategy.
        if self.db_type == 'postgres':
            is_remote_postgres = (self.connection_type == 'remote')
            self.db_setup_strategy = PostgresSetup(is_remote=is_remote_postgres)
        elif self.db_type == 'supabase':
            # Supabase is a managed cloud service. The connection type is typically
            # always treated as 'remote' by the Supabase setup strategy.
            self.db_setup_strategy = SupabaseSetup()
            if self.connection_type == 'local':
                 logger.warning(
                     "Supabase is a cloud service. 'local' connection type specified, "
                     "but will be treated as 'remote' by the Supabase setup."
                 )
        elif self.db_type == "none":
            # This type is used internally when setup is explicitly skipped.
            logger.info("Orchestrator initialized without a specific database setup strategy (setup skipped).")
        else: 
            logger.error(f"Automatic set-up not yet supported for the '{self.db_type}' database.")
            self.db_setup_strategy = None

        if self.db_setup_strategy is None and self.db_type not in ["none"]:
            raise RuntimeError(
                "Failed to initialize database setup strategy for a supported type. "
                "Check the database type and configuration."
            )

    def run_setup(self, sql_script_name: str = 'postgres_schema.sql') -> Tuple[bool, Optional[DatabaseOperations]]:
        """
        Delegates the database setup process to the chosen strategy and returns
        the active connection object on success.

        :param sql_script_name: The name of the SQL schema script to execute.
        :type sql_script_name: str
        :return: A tuple containing the setup success status (bool) and the
                 active `DatabaseOperations` instance with the connection, or `None` on failure.
        :rtype: Tuple[bool, Optional[DatabaseOperations]]
        """
        if self.db_setup_strategy is None:
            logger.warning(f"No setup strategy defined for '{self.db_type}'. Skipping automated setup.")
            return False, None

        logger.info(f"Orchestrator: Delegating full setup execution to the {self.db_type} strategy.")
        try:
            # The execute_full_setup method now returns the connection object.
            success, db_ops_conn = self.db_setup_strategy.execute_full_setup(sql_script_name=sql_script_name)
            return success, db_ops_conn
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during database setup delegation: {e}",
                exc_info=True
            )
            return False, None
    
    def run_data_loading(self, data_loader_strategy: DataLoaderStrategy) -> bool:
        """
        Delegates the data loading process to the chosen data loader strategy.

        The data loader strategy should be initialized with its required paths and configurations.

        :param data_loader_strategy: An initialized instance of a `DataLoaderStrategy`
                                     (e.g., `JSONDataLoader`, `PostgresDataLoader`).
        :type data_loader_strategy: DataLoaderStrategy
        :return: True if data loading is successful, False otherwise.
        :rtype: bool
        """
        logger.info(
            f"Orchestrator: Delegating data loading to the {type(data_loader_strategy).__name__} strategy."
        )
        try:
            return data_loader_strategy.execute_full_data_load()
        except Exception as e:
            logger.error(f"An unexpected error occurred during data loading delegation: {e}", exc_info=True)
            return False


def _parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments for the orchestrator script.

    :return: An `argparse.Namespace` object containing the parsed arguments.
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="Initializes the database, creates users and tables, and loads data.",
        formatter_class=argparse.RawTextHelpFormatter  # For better formatting of help message
    )

    db_type_group = parser.add_mutually_exclusive_group()
    db_type_group.add_argument(
        "-P", "--postgres",
        action="store_true",
        help="Use PostgreSQL database."
    )
    db_type_group.add_argument(
        "-S", "--supabase",
        action="store_true",
        help="Use Supabase database."
    )
    db_type_group.add_argument(
        "-O", "--other",
        type=str,
        metavar="DB_TYPE",
        help="Specify another database type (e.g., 'oracle', 'mongodb')."
    )

    conn_type_group = parser.add_mutually_exclusive_group()
    conn_type_group.add_argument(
        "-l", "--local",
        action="store_true",
        help="Use a local database connection."
    )
    conn_type_group.add_argument(
        "-r", "--remote",
        action="store_true",
        help="Use a remote database connection."
    )

    parser.add_argument(
        "--skip",
        action="store_true",
        help="Skip the database setup process. Only perform data loading if specified."
    )

    # --- Data Loading Arguments ---
    data_load_group = parser.add_mutually_exclusive_group()
    data_load_group.add_argument(
        "--load-json-data",
        type=str,
        metavar="JSON_FILES_DIR",
        nargs='?',  # Optional argument
        const='__DEFAULT_JSON_PATH__',  # A sentinel value if flag is present but no path is given
        help="Load raw JSON data files from the specified directory.\n"
             "Provide the path (absolute or relative) to the directory containing JSON files."
    )
    data_load_group.add_argument(
        "--load-db-dump",
        type=str,
        metavar="DUMP_FILE_PATH",
        nargs='?',  # Optional argument
        const='__DEFAULT_DUMP_PATH__',  # A sentinel value if flag is present but no path is given
        help="Load data from a database dump file using `PostgresDataLoader`.\n"
             "Provide the path (absolute or relative) to the .sql dump file."
    )

    return parser.parse_args()


def _resolve_and_validate_paths(args: argparse.Namespace) -> None:
    """
    Resolves any relative paths to absolute paths and validates their existence and type.

    This function modifies the `args` object in place.

    :param args: The `argparse.Namespace` object with parsed arguments.
    :type args: argparse.Namespace
    :raises SystemExit: If any path validation fails, the script exits with a status code of 1.
    """
    # --- Path Validation for --load-db-dump ---
    if args.load_db_dump:
        if args.load_db_dump == '__DEFAULT_DUMP_PATH__':
            logger.error(
                "Error: --load-db-dump requires a file path. "
                "Please provide the path to the .sql dump file."
            )
            sys.exit(1)
        
        # Convert to absolute path
        if not os.path.isabs(args.load_db_dump):
            args.load_db_dump = os.path.abspath(args.load_db_dump)
            logger.info(f"Resolved --load-db-dump path (was relative): {args.load_db_dump}")

        if not os.path.exists(args.load_db_dump) or not os.path.isfile(args.load_db_dump):
            logger.error(
                f"Error: Database dump file not found or is not a file at '{args.load_db_dump}'. "
                "Please provide a valid path."
            )
            sys.exit(1)
        logger.info(f"Validated database dump file path: {args.load_db_dump}")

    # --- Path Validation for --load-json-data ---
    if args.load_json_data:
        if args.load_json_data == '__DEFAULT_JSON_PATH__':
            logger.error(
                "Error: --load-json-data requires a directory path. "
                "Please provide the path to the folder containing JSON files."
            )
            sys.exit(1)
        
        # Convert to absolute path
        if not os.path.isabs(args.load_json_data):
            args.load_json_data = os.path.abspath(args.load_json_data)
            logger.info(f"Resolved --load-json-data path (was relative): {args.load_json_data}")

        if not os.path.exists(args.load_json_data) or not os.path.isdir(args.load_json_data):
            logger.error(
                f"Error: JSON data directory not found or is not a directory at '{args.load_json_data}'. "
                "Please provide a valid path."
            )
            sys.exit(1)
        logger.info(f"Validated JSON data directory path: {args.load_json_data}")


def _run_db_setup(args: argparse.Namespace) -> Tuple[Optional[Orchestrator], bool, Optional[DatabaseOperations]]:
    """
    Handles the database setup phase based on command-line arguments.

    Initializes the orchestrator, runs the setup, and returns the active
    database connection object if successful.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :return: A tuple containing:
             - An `Orchestrator` instance.
             - A boolean indicating whether the setup was successful or skipped.
             - The active `DatabaseOperations` connection instance, or `None`.
    :rtype: Tuple[Optional[Orchestrator], bool, Optional[DatabaseOperations]]
    """
    orchestrator_instance: Optional[Orchestrator] = None
    setup_successful: bool = False
    db_ops_conn: Optional[DatabaseOperations] = None
    
    explicit_db_setup_flag_provided = args.postgres or args.supabase or args.other

    if args.skip:
        logger.info("Skipping database setup as --skip flag was provided.")
        setup_successful = True
        # Initialize an orchestrator without a specific setup strategy for data loading.
        orchestrator_instance = Orchestrator(database_type="none", connection_type="none")
    elif explicit_db_setup_flag_provided:
        db_type: str = 'postgres' if args.postgres else ('supabase' if args.supabase else args.other.lower())
        
        connection_type: str
        if args.remote:
            connection_type = 'remote'
        elif args.local:
            connection_type = 'local'
        else:
            # Default connection type if a setup flag is given but no connection type is specified.
            logger.info("No connection type specified, defaulting to 'local'.")
            connection_type = 'local' 

        try:
            orchestrator_instance = Orchestrator(database_type=db_type, connection_type=connection_type)
            logger.info("Starting database setup phase...")
            
            # --- Select the appropriate schema file based on database type ---
            sql_script_name: str
            if db_type == 'supabase':
                sql_script_name = file_config.supabase_schema
            else:
                sql_script_name = file_config.schema_file

            # Run the setup and capture the returned success status and connection object.
            setup_successful, db_ops_conn = orchestrator_instance.run_setup(sql_script_name=sql_script_name)

            if setup_successful:
                logger.info("Database setup process completed successfully.")
            else:
                logger.error("Database setup process finished with errors. Data loading will be skipped.")
                # No need to sys.exit(1) here; the caller will handle it and ensure cleanup.
        except (ValueError, NotImplementedError, RuntimeError) as e:
            logger.error(f"Error during orchestrator initialization or setup: {e}", exc_info=True)
            sys.exit(1) # Exit immediately on critical initialization errors.
    else:
        # If no explicit setup flags and no --skip, assume setup is not required.
        logger.info("No database setup flags provided. Assuming database is already set up.")
        setup_successful = True  # Assume success for data loading purposes.
        orchestrator_instance = Orchestrator(database_type="none", connection_type="none")
        # In this case, we don't have a connection, so db_ops_conn remains None.
    
    return orchestrator_instance, setup_successful, db_ops_conn


def _run_data_loading(
    args: argparse.Namespace,
    orchestrator_instance: Optional[Orchestrator],
    setup_successful: bool,
    db_ops_conn: Optional[DatabaseOperations]
) -> None:
    """
    Handles the data loading phase based on command-line arguments.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :param orchestrator_instance: The initialized `Orchestrator` instance.
    :type orchestrator_instance: Optional[Orchestrator]
    :param setup_successful: A boolean indicating if the database setup phase
                             was successful or skipped.
    :type setup_successful: bool
    :param db_ops_conn: The active `DatabaseOperations` connection instance returned
                        by the setup phase, or `None`.
    :type db_ops_conn: Optional[DatabaseOperations]
    :raises SystemExit: If data loading fails, the script exits with a status code of 1.
    """
    # Proceed to data loading ONLY if a loader flag is given AND setup was successful (or skipped).
    if (args.load_json_data or args.load_db_dump) and setup_successful:
        if orchestrator_instance is None:
            logger.error("Orchestrator instance not initialized. Cannot proceed with data loading.")
            sys.exit(1)

        # JSON Data Loading
        if args.load_json_data:
            logger.info("Starting JSON data loading phase...")
            # JSONDataLoader needs a db_config. We'll use the one from the orchestrator.
            json_loader = JSONDataLoader(
                folder_path=args.load_json_data,
                db_config=orchestrator_instance.db_config
            )
            if orchestrator_instance.run_data_loading(json_loader): 
                logger.info("JSON data loading process completed successfully.")
            else:
                logger.error("JSON data loading process finished with errors.")
                sys.exit(1)

        # Database Dump Loading (Mutually exclusive with JSON loading)
        elif args.load_db_dump:
            logger.info(f"Starting database dump loading from: {args.load_db_dump}...")
            
            # --- Pass the active connection and config to the data loader ---
            # NOTE: We need the ConnectionDetails for the psql command to know the host/port/user.
            # We get this from the appropriate config class based on the db_type.
            if orchestrator_instance.db_type == 'supabase':
                config_instance = SupabaseConfig()
            else:
                # Default to a standard database config
                config_instance = DatabaseConfig() 
            
            conn_details = ConnectionDetails(
                host=config_instance.host,
                port=int(config_instance.port),
                user=config_instance.user,
                password=config_instance.password,
                database=config_instance.dbname
            )
            
            # === MODIFIED SECTION ===
            # Initialize PostgresDataLoader with the active connection and connection details.
            db_dump_loader = PostgresDataLoader(
                file_path=args.load_db_dump,
                db_ops=db_ops_conn, # Pass the active DatabaseOperations instance
                connection_details=conn_details # Pass connection details for psql command
            )
            # === END MODIFIED SECTION ===

            if orchestrator_instance.run_data_loading(db_dump_loader):
                logger.info("Database dump loading process completed successfully.")
            else:
                logger.error("Database dump loading process finished with errors.")
                sys.exit(1)
    else:
        if not (args.load_json_data or args.load_db_dump):
            logger.info("No data loading flags (--load-json-data, --load-db-dump) provided. Skipping data loading.")
        elif not setup_successful:
            logger.error("Skipping data loading as the database setup failed or was not completed successfully.")


def main() -> None:
    """
    Main entry point for the Vivarium Orchestrator application.

    This function coordinates the database setup and data loading processes
    based on command-line arguments provided by the user.
    """
    start_time = time.time()
    db_ops_conn: Optional[DatabaseOperations] = None # Initialize connection to None

    try:
        # Phase 1: Parse and validate command-line arguments and paths.
        args = _parse_args()
        _resolve_and_validate_paths(args)

        # Phase 2: Run the database setup, getting back the connection if successful.
        orchestrator_instance, setup_successful, db_ops_conn = _run_db_setup(args)

        # Phase 3: Run the data loading using the returned connection.
        _run_data_loading(args, orchestrator_instance, setup_successful, db_ops_conn)

    except Exception as e:
        logger.critical(f"A critical, unhandled exception occurred in the main execution block: {e}", exc_info=True)
        sys.exit(1) # Exit with a failure status
        
    finally:
        # Phase 4: Always ensure the database connection is closed.
        if db_ops_conn:
            logger.info("Closing the database connection managed by the orchestrator.")
            db_ops_conn.close()
            
        end_time = time.time()
        logger.info(f"Total script execution time: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    main()