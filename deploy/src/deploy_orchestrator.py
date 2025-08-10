# vivarium/deploy/src/deploy_orchestrator.py

import os
import sys
import time
import argparse
from typing import Optional, Tuple
from dataclasses import dataclass
from enum import Enum

# -- UTILITIES IMPORTS --
from utilities.src.logger import LogHelper
from utilities.src.config import FileConfig, DatabaseConfig, SupabaseConfig
from utilities.src.db_operations import DBOperations, ConnectionDetails
from utilities.src.enums.global_enums import ErrorCodes
from utilities.src.enums.database_enums import DatabaseType, ConnectionType

# -- DATABASE IMPORTS --
from database.database_setup_ops.db_setup_strategy import DBSetupStrategy
from database.database_setup_ops.postgres_setup import PostgresSetup
from database.database_setup_ops.supabase_setup import SupabaseSetup
from database.data_loader_ops.data_loader_strategy import DataLoaderStrategy
from database.data_loader_ops.json_data_loader import JSONDataLoader
from database.data_loader_ops.pgdump_data_loader import PGDumpDataLoader

@dataclass
class SetupResult:
    """Result object for setup operations."""
    success: bool
    orchestrator: Optional['DeployOrchestrator']
    db_connection: Optional[DBOperations]
    error_message: Optional[str] = None

logger = LogHelper.get_logger(__name__)

class DeployOrchestrator:
    """
    Orchestrates the database setup process and data loading by delegating
    the logic to specific strategies based on the selected database type (PostgreSQL or Supabase).
    """

    def __init__(self, 
                 database_type: DatabaseType, 
                 connection_type: ConnectionType, 
                 file_config: FileConfig) -> None:
        """
        Initializes the DeployOrchestrator.
        
        :param database_type: The type of database.
        :type database_type: DatabaseType
        :param connection_type: The type of connection.
        :type connection_type: ConnectionType
        :param file_config: FileConfig instance containing path configurations.
        :type file_config: FileConfig
        :raises RuntimeError: If configuration initialization or validation fails.
        """
        self.db_setup_strategy: Optional[DBSetupStrategy] = None
        
        self.db_type = database_type
        self.connection_type = connection_type

        logger.info(
            f"Initializing DeployOrchestrator for {self.db_type.value} "
            f"with {self.connection_type.value} connection."
        )

        # Validate database type and connection type combination
        self._validate_database_type_and_connection()

        # Initialize configurations with validation
        try:
            self.db_config = DatabaseConfig()
            self.supabase_config = SupabaseConfig()
            self.file_config = file_config
            self._validate_configurations()
        except Exception as e:
            logger.error(f"Failed to initialize configurations: {e}")
            raise RuntimeError(f"Configuration initialization failed: {e}")

        # Log warning for Supabase with local connection
        if self.db_type == DatabaseType.SUPABASE and self.connection_type == ConnectionType.LOCAL:
            logger.warning(
                "Supabase is a cloud service. 'local' connection type specified, "
                "but will be treated as 'remote' for Supabase operations."
            )

    def _validate_database_type_and_connection(self) -> None:
        """
        Validates the database type and connection type combination.

        Forces Supabase connections to be REMOTE.

        :raises ValueError: If an unsupported database type or invalid connection type combination is provided.
        """
        valid_combinations = {
            DatabaseType.POSTGRES: [ConnectionType.LOCAL, ConnectionType.REMOTE],
            DatabaseType.SUPABASE: [ConnectionType.REMOTE]
        }
        
        if self.db_type not in valid_combinations:
            raise ValueError(
                f"Unsupported database type: '{self.db_type.value}'. "
                f"Supported types: {list(valid_combinations.keys())}"
            )
        
        # If Supabase is selected and connection type is LOCAL, warn and override.
        if self.db_type == DatabaseType.SUPABASE and self.connection_type == ConnectionType.LOCAL:
            logger.warning("Supabase database type requires a REMOTE connection. Overriding connection type to REMOTE.")
            self.connection_type = ConnectionType.REMOTE

        if self.connection_type not in valid_combinations[self.db_type]:
            raise ValueError(
                f"Invalid connection type '{self.connection_type.value}' for database type '{self.db_type.value}'. "
                f"Valid connection types for {self.db_type.value}: {[c.value for c in valid_combinations[self.db_type]]}"
            )

    def _validate_configurations(self) -> None:
        """
        Validates that required configuration parameters are available based on DB type.

        :raises ValueError: If any required configuration attribute is missing.
        """
        try:
            if self.db_type == DatabaseType.POSTGRES:
                config = (self.db_config.postgres_local_connection if self.connection_type == ConnectionType.LOCAL
                          else self.db_config.postgres_remote_connection)
                    
                required_attrs = ['host', 'port', 'user', 'password', 'dbname']
                for attr in required_attrs:
                    if not hasattr(config, attr) or getattr(config, attr) is None:
                        raise ValueError(f"Missing required PostgreSQL connection configuration: {attr}")
                
                superuser_config = self.db_config.postgres_superuser_connection
                if not all(hasattr(superuser_config, attr) and getattr(superuser_config, attr) is not None for attr in ['user', 'dbname', 'host', 'port']):
                    raise ValueError("Incomplete PostgreSQL superuser connection configuration.")
            
            elif self.db_type == DatabaseType.SUPABASE:
                supabase_conn = self.supabase_config.supabase_connection_details
                required_attrs = ['host', 'port', 'user', 'password', 'dbname']
                for attr in required_attrs:
                    if not hasattr(supabase_conn, attr) or getattr(supabase_conn, attr) is None:
                        raise ValueError(f"Missing required Supabase connection configuration: {attr}")
                
                if not self.supabase_config.url or not self.supabase_config.service_key or not self.supabase_config.anon_key:
                    raise ValueError("Missing Supabase URL, Service Key, or Anon Key.")
                        
            # Validate schema file path based on the selected database type
            if self.db_type == DatabaseType.POSTGRES and not self.file_config.schema_file:
                 raise ValueError("PostgreSQL schema file path is not configured.")
            if self.db_type == DatabaseType.SUPABASE and not self.file_config.supabase_schema:
                 raise ValueError("Supabase schema file path is not configured.")
                        
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            raise

    def _build_postgres_connection_details(self) -> Tuple[ConnectionDetails, ConnectionDetails]:
        """
        Builds ConnectionDetails for PostgreSQL based on the connection type.
        
        :rtype: Tuple[ConnectionDetails, ConnectionDetails]
        :returns: A tuple containing the application connection details and superuser connection details.
        :raises RuntimeError: If connection details construction or conversion fails.
        """
        try:
            conn_config = (self.db_config.postgres_local_connection if self.connection_type == ConnectionType.LOCAL
                           else self.db_config.postgres_remote_connection)
                            
            app_conn_details = ConnectionDetails(
                host=conn_config.host,
                port=int(conn_config.port),
                user=conn_config.user,
                password=conn_config.password,
                dbname=conn_config.dbname,
                sslmode=None
            )
            
            # Superuser connection details are already validated to exist in _validate_configurations
            superuser_conn_details = ConnectionDetails(
                host=self.db_config.postgres_superuser_connection.host,
                port=int(self.db_config.postgres_superuser_connection.port),
                user=self.db_config.postgres_superuser_connection.user,
                password=self.db_config.postgres_superuser_connection.password,
                dbname=self.db_config.postgres_superuser_connection.dbname,
                sslmode=None
            )
            
            logger.debug(f"PostgreSQL application connection details built for {self.connection_type.value} connection.")
            return app_conn_details, superuser_conn_details
            
        except Exception as e:
            logger.error(f"Failed to build PostgreSQL connection details: {e}")
            raise RuntimeError(f"Connection details construction failed: {e}")

    def _build_supabase_connection_details(self) -> Tuple[ConnectionDetails, ConnectionDetails]:
        """
        Builds ConnectionDetails for Supabase.
        
        :rtype: Tuple[ConnectionDetails, ConnectionDetails]
        :returns: A tuple containing the application connection details and superuser connection details.
        :raises RuntimeError: If connection details construction or conversion fails.
        """
        try:
            supabase_conn = self.supabase_config.supabase_connection_details
            app_conn_details = ConnectionDetails(
                host=supabase_conn.host,
                port=int(supabase_conn.port),
                user=supabase_conn.user,
                password=supabase_conn.password,
                dbname=supabase_conn.dbname,
                sslmode='require',
                extra_params={"options": "--client_encoding=UTF8"}
            )
            
            # For Supabase, the main user often serves as the superuser for setup.
            superuser_conn_details = app_conn_details
            logger.debug("Supabase connection details built.")
            return app_conn_details, superuser_conn_details
            
        except Exception as e:
            logger.error(f"Failed to build Supabase connection details: {e}")
            raise RuntimeError(f"Connection details construction failed: {e}")

    def _setup_with_retry(self, app_conn_details: ConnectionDetails, 
                         superuser_conn_details: ConnectionDetails, 
                         max_retries: int = 3, 
                         delay: float = 2.0) -> bool:
        """
        Sets up the database with retry logic for transient failures.

        :param app_conn_details: Application connection details.
        :type app_conn_details: ConnectionDetails
        :param superuser_conn_details: Superuser connection details.
        :type superuser_conn_details: ConnectionDetails
        :param max_retries: Maximum number of retry attempts.
        :type max_retries: int
        :param delay: Initial delay between retries (exponential backoff).
        :type delay: float
        :returns: True if setup successful, False otherwise.
        :rtype: bool
        """
        for attempt in range(max_retries):
            try:
                logger.info(f"Setup attempt {attempt + 1} of {max_retries}")
                
                if self.db_type == DatabaseType.POSTGRES:
                    self.db_setup_strategy = PostgresSetup(
                        app_connection_details=app_conn_details,
                        superuser_connection_details=superuser_conn_details,
                        file_config=self.file_config,
                        db_type=self.db_type.value
                    )
                elif self.db_type == DatabaseType.SUPABASE:
                    self.db_setup_strategy = SupabaseSetup(
                        app_connection_details=app_conn_details,
                        superuser_connection_details=superuser_conn_details,
                        file_config=self.file_config, # FIX: Corrected typo from 'fiel_config' to 'file_config'
                        db_type=self.db_type.value
                    )
                else:
                    logger.error(f"Unsupported database type: {self.db_type.value}")
                    return False
                
                if self.db_setup_strategy is None:
                    # FIX: Used .value for clarity in logging enum type
                    logger.error(f"Failed to initialize database setup strategy for type '{self.db_type.value}'.")
                    return False
                
                if self.db_setup_strategy.full_setup():
                    logger.info(f"Database setup successful on attempt {attempt + 1}")
                    return True
                    
                logger.warning(f"Setup attempt {attempt + 1} failed, retrying...")
                
            except Exception as e:
                # FIX: Added exc_info=True for full traceback in error logs
                logger.error(f"Setup attempt {attempt + 1} failed with error: {e}", exc_info=True)
                
            if attempt < max_retries - 1:
                logger.info(f"Waiting {delay:.1f} seconds before retry...")
                time.sleep(delay)
                delay *= 2
        
        logger.error(f"All {max_retries} setup attempts failed")
        return False

    def run_setup(self, app_conn_details: ConnectionDetails, 
                  superuser_conn_details: ConnectionDetails
                  ) -> bool:
        """
        Delegates the database setup process to the chosen strategy with retry logic.

        :param app_conn_details: Connection details for the application user.
        :type app_conn_details: ConnectionDetails
        :param superuser_conn_details: Connection details for the superuser.
        :type superuser_conn_details: ConnectionDetails
        :returns: True if the setup is successful, False otherwise.
        :rtype: bool
        """
        logger.info(f"Starting database setup for {self.db_type.value}.")
        
        try:
            return self._setup_with_retry(
                app_conn_details, 
                superuser_conn_details
            )
        except Exception as e:
            logger.error(f"An unexpected error occurred during database setup: {e}", exc_info=True)
            return False

    def run_data_loading(self, data_loader_strategy: DataLoaderStrategy) -> bool:
        """
        Delegates the data loading process to the chosen data loader strategy.

        :param data_loader_strategy: An initialized instance of a DataLoaderStrategy.
        :type data_loader_strategy: DataLoaderStrategy
        :returns: True if data loading is successful, False otherwise.
        :rtype: bool
        """
        logger.info(f"Starting data loading with {type(data_loader_strategy).__name__}.")
        
        try:
            return data_loader_strategy.execute_data_load()
        except Exception as e:
            logger.error(f"An unexpected error occurred during data loading: {e}", exc_info=True)
            return False

def _parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments for the orchestrator script.
    
    :returns: An argparse.Namespace object containing the parsed arguments.
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
        "--schema-file",
        type=str,
        metavar="PATH",
        help="Override the default path to the PostgreSQL schema SQL file."
    )
    parser.add_argument(
        "--supabase-schema-file",
        type=str,
        metavar="PATH",
        help="Override the default path to the Supabase schema SQL file."
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
        help="Load raw JSON data files from the specified directory.\n"
             "Provide the path (absolute or relative) to the directory containing JSON files."
    )
    
    parser.add_argument(
        "--load-db-dump",
        type=str,
        metavar="DUMP_FILE_PATH",
        help="Load data from a database dump file using PGDumpDataLoader.\n"
             "Provide the path (absolute or relative) to the .sql dump file."
    )
    parser.add_argument(
        "--json_folder",
        type=str,
        metavar="PATH",
        help="Override the default path to the raw JSON data folder."
    )
    parser.add_argument(
        "--processed_json_folder",
        type=str,
        metavar="PATH",
        help="Override the default path to the processed JSON data folder."
    )
    parser.add_argument(
        "--data-file",
        type=str,
        metavar="PATH",
        help="Override the default path to the data file used for loading into the database."
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Database operation timeout in seconds (default: 300)."
    )

    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum number of retry attempts for database operations (default: 3)."
    )
    return parser.parse_args()

def _resolve_and_validate_paths(args: argparse.Namespace) -> None:
    """
    Resolves any relative paths to absolute paths and validates their existence and type.
    
    :param args: The argparse.Namespace object with parsed arguments.
    :type args: argparse.Namespace
    :raises SystemExit: If any path validation fails.
    """
    if args.load_db_dump:
        if not os.path.isabs(args.load_db_dump):
            args.load_db_dump = os.path.abspath(args.load_db_dump)
            logger.info(f"Resolved --load-db-dump path to absolute: {args.load_db_dump}") 

        if not os.path.exists(args.load_db_dump):
            logger.error(f"Database dump file not found: {args.load_db_dump}")
            sys.exit(ErrorCodes.FILE_ERROR)
            
        if not os.path.isfile(args.load_db_dump):
            logger.error(f"Path is not a file: {args.load_db_dump}")
            sys.exit(ErrorCodes.FILE_ERROR)
            
        if not os.access(args.load_db_dump, os.R_OK):
            logger.error(f"No read permission for file: {args.load_db_dump}")
            sys.exit(ErrorCodes.FILE_ERROR)
            
        if os.path.getsize(args.load_db_dump) == 0:
            logger.error(f"Database dump file is empty: {args.load_db_dump}")
            sys.exit(ErrorCodes.FILE_ERROR)
            
        # Validate file extension
        if not args.load_db_dump.lower().endswith(('.sql', '.dump')):
            logger.warning(f"File doesn't have expected extension (.sql, .dump): {args.load_db_dump}")
            
        logger.info(f"Validated database dump file: {args.load_db_dump}")

    if args.load_json_data:
        if not os.path.isabs(args.load_json_data):
            args.load_json_data = os.path.abspath(args.load_json_data)
            logger.info(f"Resolved --load-json-data path to absolute: {args.load_json_data}")

        if not os.path.exists(args.load_json_data):
            logger.error(f"JSON data directory not found: {args.load_json_data}")
            sys.exit(ErrorCodes.FILE_ERROR)
            
        if not os.path.isdir(args.load_json_data):
            logger.error(f"Path is not a directory: {args.load_json_data}")
            sys.exit(ErrorCodes.FILE_ERROR)
            
        if not os.access(args.load_json_data, os.R_OK):
            logger.error(f"No read permission for directory: {args.load_json_data}")
            sys.exit(ErrorCodes.FILE_ERROR)
            
        # Check if directory contains JSON files
        json_files = [f for f in os.listdir(args.load_json_data) if f.lower().endswith('.json')]
        if not json_files:
            logger.warning(f"No JSON files found in directory: {args.load_json_data}")
            
        logger.info(f"Validated JSON data directory: {args.load_json_data}")

def _run_db_setup(args: argparse.Namespace) -> SetupResult:
    """
    Handles the database setup phase based on command-line arguments.
    
    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :returns: SetupResult object containing setup status and components.
    :rtype: SetupResult
    """
    database_type: DatabaseType = DatabaseType.POSTGRES if args.postgres else DatabaseType.SUPABASE
    
    if args.remote:
        connection_type: ConnectionType = ConnectionType.REMOTE
    elif args.local:
        connection_type: ConnectionType = ConnectionType.LOCAL
    else:
        connection_type: ConnectionType = ConnectionType.LOCAL if database_type == DatabaseType.POSTGRES else ConnectionType.REMOTE

    # Validate for conflicting schema file arguments
    if args.schema_file and database_type == DatabaseType.SUPABASE:
        logger.error("Cannot specify --schema-file with --supabase database type. Use --supabase-schema-file.")
        return SetupResult(success=False, orchestrator=None, db_connection=None, error_message="Conflicting schema arguments.")
    if args.supabase_schema_file and database_type == DatabaseType.POSTGRES:
        logger.error("Cannot specify --supabase-schema-file with --postgres database type. Use --schema-file.")
        return SetupResult(success=False, orchestrator=None, db_connection=None, error_message="Conflicting schema arguments.")

    orchestrator_instance: Optional[DeployOrchestrator] = None
    try:
        override_file_config = FileConfig(
            schema_file_override=args.schema_file,
            supabase_schema_override=args.supabase_schema_file,
            json_folder_override=args.json_folder,
            processed_json_folder_override=args.processed_json_folder,
            data_file_override=args.data_file
        )
    except FileNotFoundError as e:
        logger.error(f"Required configuration file not found: {e}. Please ensure config.ini and config_secrets.ini exist.")
        return SetupResult(success=False, orchestrator=None, db_connection=None, error_message=f"Configuration file not found: {e}")
    except Exception as e:
        logger.error(f"Failed to load configurations: {e}", exc_info=True)
        return SetupResult(success=False, orchestrator=None, db_connection=None, error_message=f"Failed to load configurations: {e}")

    try:
        logger.info(f"Initializing DeployOrchestrator for {database_type.value} with {connection_type.value} connection")
        orchestrator_instance = DeployOrchestrator(
            database_type=database_type, 
            connection_type=connection_type, 
            file_config=override_file_config
        )

        # Build connection details
        if database_type == DatabaseType.POSTGRES:
            app_conn_details, superuser_conn_details = orchestrator_instance._build_postgres_connection_details()
        else:  # Supabase
            app_conn_details, superuser_conn_details = orchestrator_instance._build_supabase_connection_details()

        # Handle setup or skip
        if args.skip:
            logger.info("Skipping database setup as --skip flag was provided.")
            setup_successful = True
        else:
            logger.info("Starting database setup phase...")
            setup_successful = orchestrator_instance.run_setup(
                app_conn_details=app_conn_details,
                superuser_conn_details=superuser_conn_details,
            )

            if setup_successful:
                logger.info("Database setup process completed successfully.")
            else:
                logger.error("Database setup process failed.")
                return SetupResult(
                    success=False,
                    orchestrator=orchestrator_instance,
                    db_connection=None,
                    error_message="Database setup failed"
                )

        # Create database connection for data loading if needed
        db_ops_conn = None
        if setup_successful and (args.load_json_data or args.load_db_dump):
            try:
                db_ops_conn = DBOperations()
                db_ops_conn.connect(app_conn_details)
                
                # Test connection
                if not db_ops_conn.test_connection():
                    logger.error("Database connection test failed")
                    return SetupResult(
                        success=False,
                        orchestrator=orchestrator_instance,
                        db_connection=None,
                        error_message="Database connection test failed"
                    )
                    
                logger.info("Database connection established successfully for data loading.")
                
            except Exception as e:
                logger.error(f"Failed to create database connection for data loading: {e}", exc_info=True)
                return SetupResult(
                    success=False,
                    orchestrator=orchestrator_instance,
                    db_connection=None,
                    error_message=f"Database connection failed: {e}"
                )

        return SetupResult(
            success=setup_successful,
            orchestrator=orchestrator_instance,
            db_connection=db_ops_conn
        )

    except Exception as e:
        logger.error(f"Error during database setup phase: {e}", exc_info=True)
        return SetupResult(
            success=False,
            orchestrator=orchestrator_instance,
            db_connection=None,
            error_message=f"Setup initialization failed: {e}"
        )

def _handle_data_loading(args: argparse.Namespace, 
                     setup_result: SetupResult) -> bool:
    """
    Handles the data loading phase based on command-line arguments and setup results.
    
    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :param setup_result: Result of the database setup phase.
    :type setup_result: SetupResult
    :returns: True if data loading is successful, False otherwise.
    :rtype: bool
    """
    orchestrator_instance: Optional[DeployOrchestrator] = setup_result.orchestrator
    db_ops_conn: Optional[DBOperations] = setup_result.db_connection

    if not orchestrator_instance or not db_ops_conn:
        logger.error("Data loading cannot proceed due to missing orchestrator or database connection from setup phase.")
        return False

    if not (args.load_json_data or args.load_db_dump):
        logger.info("No data loading specified. Skipping data loading phase.")
        return True

    logger.info("Starting data loading phase...")
    overall_success = True

    # --- Handle JSON data loading ---
    if args.load_json_data:
        logger.info(f"Using JSONDataLoader for data loading from: {args.load_json_data}")
        json_data_loader = JSONDataLoader(
            file_config=orchestrator_instance.file_config,
            db_operations=db_ops_conn
        )
        if not orchestrator_instance.run_data_loading(json_data_loader):
            logger.error("JSON data loading failed.")
            overall_success = False
        else:
            logger.info("JSON data loading completed successfully.")

    # --- Handle DB Dump data loading ---
    if args.load_db_dump:
        logger.info(f"Using PGDumpDataLoader for data loading from: {args.load_db_dump}")
        if orchestrator_instance.db_type != DatabaseType.POSTGRES:
            logger.error(f"PGDumpDataLoader is only supported for PostgreSQL databases. Current DB type: {orchestrator_instance.db_type.value}.")
            overall_success = False # Mark overall as failure if incompatible DB type
        else:
            pgdump_data_loader = PGDumpDataLoader(
                file_config=orchestrator_instance.file_config,
                db_operations=db_ops_conn
            )
            if not orchestrator_instance.run_data_loading(pgdump_data_loader):
                logger.error("PGDump data loading failed.")
                overall_success = False # Mark overall as failure
            else:
                logger.info("PGDump data loading completed successfully.")

    return overall_success

def main() -> None:
    """
    Main entry point for the Vivarium DeployOrchestrator application.
    
    Coordinates the database setup and data loading processes for PostgreSQL 
    or Supabase based on command-line arguments.
    """
    start_time = time.time()
    exit_code = ErrorCodes.SUCCESS
    
    try:
        # Parse arguments and validate paths
        args = _parse_args()
        _resolve_and_validate_paths(args)

        # Run database setup
        setup_result = _run_db_setup(args)
        
        if not setup_result.success:
            logger.error(f"Database setup failed: {setup_result.error_message}")
            exit_code = ErrorCodes.DATABASE_ERROR
        else:
            # Run data loading if setup was successful
            if args.load_json_data or args.load_db_dump:
                logger.info("Proceeding with data loading phase as requested.")
                if not _handle_data_loading(args, setup_result):
                    logger.error("Data loading failed.")
                    exit_code = ErrorCodes.DATABASE_ERROR # Set exit_code on data loading failure
            else:
                logger.info("No data loading flags provided. Skipping data loading phase.")

    except KeyboardInterrupt:
        logger.info("Operation interrupted by user.")
        exit_code = ErrorCodes.GENERAL_ERROR
    except Exception as e:
        logger.critical(f"Critical error in main execution: {e}", exc_info=True)
        exit_code = ErrorCodes.GENERAL_ERROR
    finally:
        if setup_result is not None and setup_result.db_connection:
            try:
                setup_result.db_connection.close()
                logger.info("Database connection closed successfully.")
            except Exception as e:
                logger.warning(f"Error closing database connection: {e}")

        # Log execution time
        end_time = time.time()
        logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")

        # FIX: Move the final success message here, after all processing and cleanup
        if exit_code == ErrorCodes.SUCCESS:
            logger.info("All requested operations completed successfully.")

        # Exit with appropriate code
        if exit_code != ErrorCodes.SUCCESS:
            sys.exit(exit_code)

if __name__ == "__main__":
    main()