# vivarium/deploy/src/deploy_orchestrator.py

import os
import sys
import time
import argparse
from typing import Optional, Tuple

# Ensure vivarium root is in sys.path to resolve imports correctly.
vivarium_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vivarium_root_path not in sys.path:
    sys.path.insert(0, vivarium_root_path)

from utilities.src.logger import LogHelper
from utilities.src.config import FileConfig, DatabaseConfig, SupabaseConfig
from utilities.src.db_operations import DBOperations, ConnectionDetails

from database.database_setup_ops.db_setup_strategy import DBSetupStrategy
from database.database_setup_ops.postgres_setup import PostgresSetup
from database.database_setup_ops.supabase_setup import SupabaseSetup # Assuming SupabaseSetup will also use ConnectionDetails

from database.data_loader_ops.data_loader_strategy import DataLoaderStrategy
from database.data_loader_ops.json_data_loader import JSONDataLoader
from database.data_loader_ops.pgdump_data_loader import PGDumpDataLoader

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

    def __init__(self, database_type: str, connection_type: str = "local"):
        """
        Initializes the DeployOrchestrator by storing database and connection types.
        The actual setup strategy is instantiated later after connection details are resolved.

        :param database_type: The type of database ('postgres' or 'supabase').
        :type database_type: str
        :param connection_type: The type of connection ('local' or 'remote').
        :type connection_type: str
        :raises ValueError: If an unsupported database type is provided.
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

        if self.db_type not in ['postgres', 'supabase']:
            raise ValueError(
                f"Unsupported database type: '{self.db_type}'. "
                "Only 'postgres' and 'supabase' are supported."
            )
        
        if self.db_type == 'supabase' and self.connection_type == 'local':
            logger.warning(
                "Supabase is a cloud service. 'local' connection type specified, "
                "but will be treated as 'remote' for Supabase operations."
            )

    def _build_postgres_connection_details(self) -> Tuple[ConnectionDetails, ConnectionDetails]:
        """
        Builds `ConnectionDetails` for PostgreSQL based on the connection type.

        :returns: A tuple containing (app_connection_details, superuser_connection_details).
        :rtype: Tuple[ConnectionDetails, ConnectionDetails]
        """
        if self.connection_type == 'local':
            app_host = self.db_config.postgres_local_connection.host
            app_port = self.db_config.postgres_local_connection.port
            app_user = self.db_config.postgres_local_connection.user
            app_password = self.db_config.postgres_local_connection.password
            app_dbname = self.db_config.postgres_local_connection.dbname
        else:  # 'remote'
            app_host = self.db_config.postgres_remote_connection.host
            app_port = self.db_config.postgres_remote_connection.port
            app_user = self.db_config.postgres_remote_connection.user
            app_password = self.db_config.postgres_remote_connection.password
            app_dbname = self.db_config.postgres_remote_connection.dbname

        app_conn_details = ConnectionDetails(
            host=app_host,
            port=app_port,
            user=app_user,
            password=app_password,
            dbname=app_dbname,
            sslmode=None  # PostgreSQL generally doesn't force SSL by default like Supabase
        )

        superuser_conn_details = self.db_config.postgres_superuser_connection
        return app_conn_details, superuser_conn_details

    def _build_supabase_connection_details(self) -> Tuple[ConnectionDetails, ConnectionDetails]:
        """
        Builds `ConnectionDetails` for Supabase.

        Supabase generally uses the same details for app and superuser access
        with the 'postgres' user, requiring SSL.

        :returns: A tuple containing (app_connection_details, superuser_connection_details).
        :rtype: Tuple[ConnectionDetails, ConnectionDetails]
        """
        # Supabase requires SSL and often connects to 'postgres' db as admin user
        # The project-level 'postgres' user usually acts as both app and superuser for setup.
        app_host = self.supabase_config.host
        app_port = self.supabase_config.port
        app_user = self.supabase_config.user
        app_password = self.supabase_config.password
        app_dbname = self.supabase_config.dbname # Often 'postgres' for initial admin, or specific project DB
        
        # Supabase almost always requires sslmode='require'
        app_conn_details = ConnectionDetails(
            host=app_host,
            port=app_port,
            user=app_user,
            password=app_password,
            dbname=app_dbname,
            sslmode='require',
            extra_params={"options": "--client_encoding=UTF8"} # Common for Supabase
        )
        superuser_conn_details = app_conn_details # For Supabase, the main user often serves as superuser

        return app_conn_details, superuser_conn_details

    def run_setup(self, app_conn_details: ConnectionDetails, superuser_conn_details: ConnectionDetails, sql_script_name: str) -> bool:
        """
        Delegates the database setup process to the chosen strategy.

        :param app_conn_details: Connection details for the application user.
        :type app_conn_details: :class:`utilities.src.db_operations.ConnectionDetails`
        :param superuser_conn_details: Connection details for the superuser.
        :type superuser_conn_details: :class:`utilities.src.db_operations.ConnectionDetails`
        :param sql_script_name: The name of the SQL schema script to execute.
        :type sql_script_name: str
        :returns: :obj:`True` if the setup is successful, :obj:`False` otherwise.
        :rtype: bool
        """
        if self.db_type == 'postgres':
            self.db_setup_strategy = PostgresSetup(
                app_connection_details=app_conn_details,
                superuser_connection_details=superuser_conn_details
            )
        elif self.db_type == 'supabase':
            self.db_setup_strategy = SupabaseSetup( # Assuming SupabaseSetup also takes ConnectionDetails now
                app_connection_details=app_conn_details,
                superuser_connection_details=superuser_conn_details
            )
        
        if self.db_setup_strategy is None:
            logger.error(f"Failed to initialize database setup strategy for type '{self.db_type}'.")
            return False

        logger.info(f"DeployOrchestrator: Delegating full setup execution to the {self.db_type} strategy.")
        try:
            success = self.db_setup_strategy.full_setup() # Call the new 'full_setup'
            return success
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during database setup delegation: {e}",
                exc_info=True
            )
            return False

    def run_data_loading(self, data_loader_strategy: DataLoaderStrategy) -> bool:
        """
        Delegates the data loading process to the chosen data loader strategy.

        :param data_loader_strategy: An initialized instance of a `DataLoaderStrategy`
                                     (e.g., `JSONDataLoader`, `PGDumpDataLoader`).
        :type data_loader_strategy: DataLoaderStrategy
        :returns: :obj:`True` if data loading is successful, :obj:`False` otherwise.
        :rtype: bool
        """
        logger.info(
            f"DeployOrchestrator: Delegating data loading to the {type(data_loader_strategy).__name__} strategy."
        )
        try:
            return data_loader_strategy.execute_data_load()
        except Exception as e:
            logger.error(f"An unexpected error occurred during data loading delegation: {e}", exc_info=True)
            return False


def _parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments for the orchestrator script, supporting only
    PostgreSQL and Supabase.

    :returns: An `argparse.Namespace` object containing the parsed arguments.
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
            logger.error("Error: --load-db-dump requires a file path. Please provide the path to the .sql dump file.")
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
    database connection object if successful or skipped for data loading.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :returns: A tuple containing:
             - A `DeployOrchestrator` instance.
             - A boolean indicating whether the setup was successful or skipped.
             - The active `DBOperations` connection instance for data loading, or `None`.
    :rtype: Tuple[Optional[DeployOrchestrator], bool, Optional[DBOperations]]
    """
    orchestrator_instance: Optional[DeployOrchestrator] = None
    setup_successful: bool = False
    db_ops_conn: Optional[DBOperations] = None

    db_type: str = 'postgres' if args.postgres else 'supabase'
    connection_type: str = 'remote' if args.remote else 'local'

    try:
        orchestrator_instance = DeployOrchestrator(database_type=db_type, connection_type=connection_type)

        app_conn_details: ConnectionDetails
        superuser_conn_details: ConnectionDetails
        sql_script_name: str

        if db_type == 'postgres':
            app_conn_details, superuser_conn_details = orchestrator_instance._build_postgres_connection_details()
            sql_script_name = file_config.schema_file
        elif db_type == 'supabase':
            app_conn_details, superuser_conn_details = orchestrator_instance._build_supabase_connection_details()
            sql_script_name = file_config.supabase_schema
        else:
            raise ValueError(f"Unknown database type: {db_type}")

        if args.skip:
            logger.info("Skipping database setup as --skip flag was provided.")
            setup_successful = True
        else:
            logger.info("Starting database setup phase...")
            setup_successful = orchestrator_instance.run_setup(
                app_conn_details=app_conn_details,
                superuser_conn_details=superuser_conn_details,
                sql_script_name=sql_script_name
            )

            if setup_successful:
                logger.info("Database setup process completed successfully.")
            else:
                logger.error("Database setup process finished with errors. Data loading will be skipped.")

        if setup_successful and (args.load_json_data or args.load_db_dump):
            try:
                db_ops_conn = DBOperations()
                db_ops_conn.connect(app_conn_details)
                logger.info("DBOperations instance created and connected for data loading.")
            except Exception as e:
                logger.error(
                    f"Failed to create DBOperations instance for data loading: {e}",
                    exc_info=True
                )
                setup_successful = False # Data loading connection failure means setup is effectively failed for the whole process

    except (ValueError, NotImplementedError, RuntimeError) as e:
        logger.critical(f"Error during DeployOrchestrator initialization or setup: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during database setup phase: {e}", exc_info=True)
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

        if args.load_json_data:
            logger.info("Starting JSON data loading phase...")
            json_loader = JSONDataLoader(
                db_operations=db_ops_conn,
                raw_folder_path=args.load_json_data
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