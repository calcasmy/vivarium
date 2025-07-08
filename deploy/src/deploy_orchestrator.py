# vivarium/deploy/src/deploy_orchestrator.py
"""
The DeployOrchestrator module is the main entry point for managing database setup and
data loading strategies for the Vivarium application in a deployment context.

It uses a strategy pattern to delegate operations to specific PostgreSQL or Supabase
database setup and data loader implementations based on command-line arguments.
"""

import os
import sys
import time
import argparse
from typing import Optional, Tuple, Any

# Ensure vivarium root is in sys.path to resolve imports correctly.
# This block must be at the very top, before other project-specific imports.
# It allows the script to be run from any directory within the project structure.
vivarium_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vivarium_root_path not in sys.path:
    sys.path.insert(0, vivarium_root_path)

from utilities.src.logger import LogHelper
from utilities.src.new_config import FileConfig, DatabaseConfig, SupabaseConfig
from utilities.src.db_operations import DBOperations, ConnectionDetails
from utilities.src.path_utils import PathUtils

# -- DATABASE SETUP STRATEGY
from database.deploy_data_ops.database_setup.db_setup_strategy import DBSetupStrategy
from database.deploy_data_ops.database_setup.postgres_setup import PostgresSetup
from database.deploy_data_ops.database_setup.supabase_setup import SupabaseSetup

# -- DATA LOADER STRATEGY
from database.deploy_data_ops.data_loader.data_loader_strategy import DataLoaderStrategy
from database.deploy_data_ops.data_loader.json_data_loader import JSONDataLoader
from database.deploy_data_ops.data_loader.pgdump_data_loader import PGDumpDataLoader

# Initialize core configurations and logger
file_config = FileConfig()
logger = LogHelper.get_logger(__name__)

class DeployOrchestrator:
    """
    Orchestrates the database setup process and data loading by delegating
    the logic to specific strategies based on the selected database type (PostgreSQL or Supabase).
    """

    db_setup_strategy: Optional[DBSetupStrategy]
    db_type: str
    connection_type: str
    db_config: DatabaseConfig
    supabase_config: SupabaseConfig

    def __init__(self, database_type: str, connection_type: str = "local", skip: bool = False):
        """
        Initializes the DeployOrchestrator by selecting the appropriate database setup strategy.

        :param database_type: The type of database ('postgres' or 'supabase').
        :type database_type: str
        :param connection_type: The type of connection ('local' or 'remote').
        :type connection_type: str
        :param skip: If True, skips the database setup initialization.
        :type skip: bool
        :raises ValueError: If an unsupported database type is provided.
        :raises RuntimeError: If a supported database type is provided but no
                              valid setup strategy can be determined.
        """
        self.db_setup_strategy = None
        self.db_type = database_type.lower()
        self.connection_type = connection_type.lower()

        self.db_config = DatabaseConfig()
        self.supabase_config = SupabaseConfig()

        logger.info(
            f"DeployOrchestrator: Initializing for database type '{self.db_type}' "
            f"with connection type '{self.connection_type}'."
        )

        if self.db_type == 'postgres' and not skip:
            self.db_setup_strategy = PostgresSetup(db_config=self.db_config)
        elif self.db_type == 'supabase' and not skip:
            self.db_setup_strategy = SupabaseSetup(supabase_config=self.supabase_config)
            if self.connection_type == 'local':
                logger.warning(
                    "Supabase is a cloud service. 'local' connection type specified, "
                    "but will be treated as 'remote' by the Supabase setup strategy."
                )
        elif skip:
            logger.info("DeployOrchestrator initialized without a specific database setup strategy (setup skipped).")
        else:
            raise ValueError(f"Unsupported database type: '{self.db_type}'. "
                             "Only 'postgres' and 'supabase' are supported.")

        if self.db_setup_strategy is None and skip is False:
            raise RuntimeError(
                "Failed to initialize database setup strategy for a supported type. "
                "Check the database type and configuration."
            )

    def run_setup(self, sql_script_name: str) -> Tuple[bool, Optional[DBOperations]]:
        """
        Delegates the database setup process to the chosen strategy and returns
        the active connection object on success.

        :param sql_script_name: The name of the SQL schema script to execute.
        :type sql_script_name: str
        :return: A tuple containing the setup success status (bool) and the
                 active `DBOperations` instance with the connection, or `None` on failure.
        :rtype: Tuple[bool, Optional[DBOperations]]
        """
        if self.db_setup_strategy is None:
            logger.warning(f"No setup strategy defined for '{self.db_type}'. Skipping automated setup.")
            return False, None

        logger.info(f"DeployOrchestrator: Delegating full setup execution to the {self.db_type} strategy.")
        try:
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

        :param data_loader_strategy: An initialized instance of a `DataLoaderStrategy`
                                     (e.g., `JSONDataLoader`, `PGDumpDataLoader`).
        :type data_loader_strategy: DataLoaderStrategy
        :param db_operations: An initialized `DBOperations` instance for database interaction.
        :type db_operations: DBOperations
        :return: True if data loading is successful, False otherwise.
        :rtype: bool
        """
        logger.info(
            f"DeployOrchestrator: Delegating data loading to the {type(data_loader_strategy).__name__} strategy."
        )
        try:
            return data_loader_strategy.execute_full_data_load()
        except Exception as e:
            logger.error(f"An unexpected error occurred during data loading delegation: {e}", exc_info=True)
            return False

def _parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments for the orchestrator script, supporting only
    PostgreSQL and Supabase.

    :return: An `argparse.Namespace` object containing the parsed arguments.
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="Initializes either a PostgreSQL or Supabase database, "
                    "creates users and tables, and loads data.",
        formatter_class=argparse.RawTextHelpFormatter
    )

    db_type_group = parser.add_mutually_exclusive_group(required=True)
    db_type_group.add_argument(
        "-P", "--postgres",
        action="store_true",
        help="Use PostgreSQL database for setup and data loading."
    )
    db_type_group.add_argument(
        "-S", "--supabase",
        action="store_true",
        help="Use Supabase database for setup and data loading."
    )

    conn_type_group = parser.add_mutually_exclusive_group()
    conn_type_group.add_argument(
        "-l", "--local",
        action="store_true",
        help="Use a local database connection. (Primarily for PostgreSQL)."
    )
    conn_type_group.add_argument(
        "-r", "--remote",
        action="store_true",
        help="Use a remote database connection. (Applies to both PostgreSQL and Supabase)."
    )

    parser.add_argument(
        "--skip",
        action="store_true",
        help="Skip the database setup process. Only perform data loading if specified."
    )

    # data_load_group = parser.add_mutually_exclusive_group()
    parser.add_argument(
        "--load-json-data",
        type=str,
        metavar="JSON_FILES_DIR",
        nargs='?',
        const='__DEFAULT_JSON_PATH__',
        help="Load raw JSON data files from the specified directory.\n"
             "Provide the path (absolute or relative) to the directory containing JSON files."
    )
    parser.add_argument(
        "--load-db-dump",
        type=str,
        metavar="DUMP_FILE_PATH",
        nargs='?',
        const='__DEFAULT_DUMP_PATH__',
        help="Load data from a database dump file using `PGDumpDataLoader`.\n"
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
    if args.load_db_dump:
        if args.load_db_dump == '__DEFAULT_DUMP_PATH__':
            logger.error(
                "Error: --load-db-dump requires a file path. "
                "Please provide the path to the .sql dump file."
            )
            sys.exit(1)

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

    if args.load_json_data:
        if args.load_json_data == '__DEFAULT_JSON_PATH__':
            logger.error(
                "Error: --load-json-data requires a directory path. "
                "Please provide the path to the folder containing JSON files."
            )
            sys.exit(1)

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

def _run_db_setup(args: argparse.Namespace) -> Tuple[Optional[DeployOrchestrator], bool, Optional[DBOperations]]:
    """
    Handles the database setup phase based on command-line arguments.

    Initializes the orchestrator, runs the setup, and returns the active
    database connection object if successful.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :return: A tuple containing:
             - A `DeployOrchestrator` instance.
             - A boolean indicating whether the setup was successful or skipped.
             - The active `DBOperations` connection instance, or `None`.
    :rtype: Tuple[Optional[DeployOrchestrator], bool, Optional[DBOperations]]
    """
    orchestrator_instance: Optional[DeployOrchestrator] = None
    setup_successful: bool = False
    db_ops_conn: Optional[DBOperations] = None

    db_type: str = 'postgres' if args.postgres else 'supabase'

    connection_type: str
    if args.remote:
        connection_type = 'remote'
    elif args.local:
        connection_type = 'local'
    else:
        logger.info("No connection type specified, defaulting to 'local'.")
        connection_type = 'local'

    if args.skip:
        logger.info("Skipping database setup as --skip flag was provided.")
        setup_successful = True
        orchestrator_instance = DeployOrchestrator(database_type=db_type, connection_type=connection_type, skip=True)

        if args.load_json_data or args.load_db_dump:
            try:
                # --- Select connection details based on db_type and connection_type ---
                if db_type == 'postgres':
                    if connection_type == 'local':
                        
                        current_host = orchestrator_instance.db_config._local_host
                        current_port = orchestrator_instance.db_config._local_port
                        logger.debug(f"Using local PostgreSQL connection details: {current_host}:{current_port}")
                    else: # 'remote'
                        current_host = orchestrator_instance.db_config._remote_host
                        current_port = orchestrator_instance.db_config._remote_port
                        logger.debug(f"Using remote PostgreSQL connection details: {current_host}:{current_port}")
                    
                    # For Postgres, user/password/database come from db_config
                    conn_details = ConnectionDetails(
                        host=current_host,
                        port=current_port,
                        dbname=orchestrator_instance.db_config._app_dbname,
                        user=orchestrator_instance.db_config._app_user,
                        password=orchestrator_instance.db_config._app_password
                    )
                elif db_type == 'supabase':

                    # For Supabase, user/password/database come from supabase_config
                    conn_details = ConnectionDetails(
                        host=orchestrator_instance.supabase_config._host,
                        port=orchestrator_instance.supabase_config._port,
                        dbname=orchestrator_instance.supabase_config._dbname,
                        user=orchestrator_instance.supabase_config._user,
                        password=orchestrator_instance.supabase_config._password,
                    )
                else:
                    # This case should ideally not be reached due to initial db_type validation
                    raise ValueError(f"Unexpected database type '{db_type}' encountered during DBOperations instantiation.")

                db_ops_conn = DBOperations()
                db_ops_conn.connect(conn_details)
                logger.info("DBOperations instance created for data loading despite skipped setup.")
            except Exception as e:
                logger.error(
                    f"Failed to create DBOperations instance for data loading with --skip: {e}",
                    exc_info=True
                )
                setup_successful = False

    else:
        try:
            orchestrator_instance = DeployOrchestrator(database_type=db_type, connection_type=connection_type)
            logger.info("Starting database setup phase...")

            sql_script_name: str
            if db_type == 'supabase':
                sql_script_name = file_config.supabase_schema
            else:
                sql_script_name = file_config.schema_file

            setup_successful, db_ops_conn = orchestrator_instance.run_setup(sql_script_name=sql_script_name)

            if setup_successful:
                logger.info("Database setup process completed successfully.")
            else:
                logger.error("Database setup process finished with errors. Data loading will be skipped.")
        except (ValueError, NotImplementedError, RuntimeError) as e:
            logger.critical(f"Error during DeployOrchestrator initialization or setup: {e}", exc_info=True)
            sys.exit(1)

    return orchestrator_instance, setup_successful, db_ops_conn

def _run_data_loading(
    args: argparse.Namespace,
    orchestrator_instance: Optional[DeployOrchestrator],
    setup_successful: bool,
    db_ops_conn: Optional[DBOperations]
) -> None:
    """
    Handles the data loading phase based on command-line arguments.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :param orchestrator_instance: The initialized `DeployOrchestrator` instance.
    :type orchestrator_instance: Optional[DeployOrchestrator]
    :param setup_successful: A boolean indicating if the database setup phase
                             was successful or skipped.
    :type setup_successful: bool
    :param db_ops_conn: The active `DBOperations` connection instance returned
                        by the setup phase, or `None`.
    :type db_ops_conn: Optional[DBOperations]
    :raises SystemExit: If data loading fails, the script exits with a status code of 1.
    """
    if (args.load_json_data or args.load_db_dump) and setup_successful:
        if orchestrator_instance is None:
            logger.error("DeployOrchestrator instance not initialized. Cannot proceed with data loading.")
            sys.exit(1)

        if db_ops_conn is None:
             logger.error("Database connection object (db_operations) is missing. Cannot proceed with data loading that requires DB interaction.")
             sys.exit(1)
        
        # -- HANDLE DATABASE DUMP LOADING --
        if args.load_db_dump:
            logger.info(f"Starting database dump loading from: {args.load_db_dump}...")

            db_dump_loader = PGDumpDataLoader(
                file_path=args.load_db_dump,
                db_operations=db_ops_conn
            )

            if orchestrator_instance.run_data_loading(db_dump_loader):
                logger.info("Database dump loading process completed successfully.")
            else:
                logger.error("Database dump loading process finished with errors.")
                sys.exit(1)

        # -- HANDLE JSON DATA LOADING --
        if args.load_json_data:
            logger.info("Starting JSON data loading phase...")
            json_loader = JSONDataLoader(
                folder_path=args.load_json_data,
                db_operations=db_ops_conn 
            )
            if orchestrator_instance.run_data_loading(json_loader):
                logger.info("JSON data loading process completed successfully.")
            else:
                logger.error("JSON data loading process finished with errors.")
                sys.exit(1)
    else:
        if not (args.load_json_data or args.load_db_dump):
            logger.info("No data loading flags (--load-json-data, --load-db-dump) provided. Skipping data loading.")
        elif not setup_successful:
            logger.error("Skipping data loading as the database setup failed or was not completed successfully.")

def main() -> None:
    """
    Main entry point for the Vivarium DeployOrchestrator application.

    This function coordinates the database setup and data loading processes
    for PostgreSQL or Supabase based on command-line arguments provided by the user.
    """
    start_time = time.time()
    db_ops_conn: Optional[DBOperations] = None

    try:
        args = _parse_args()
        _resolve_and_validate_paths(args)

        orchestrator_instance, setup_successful, db_ops_conn = _run_db_setup(args)

        _run_data_loading(args, orchestrator_instance, setup_successful, db_ops_conn)

    except Exception as e:
        logger.critical(f"A critical, unhandled exception occurred in the main execution block: {e}", exc_info=True)
        sys.exit(1)

    finally:
        if db_ops_conn:
            logger.info("Closing the database connection managed by the DeployOrchestrator.")
            db_ops_conn.close()

        end_time = time.time()
        logger.info(f"Total script execution time: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    main()