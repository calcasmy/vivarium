# vivarium/deploy/src/orchestrator.py
import os
import sys
import time
import argparse

from typing import Optional

if __name__ == "__main__":
    vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if vivarium_path not in sys.path:
        sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.config import FileConfig, DatabaseConfig

from deploy.src.database.postgres_setup import PostgresSetup
from deploy.src.database.supabase_setup import SupabaseSetup

from deploy.src.database.json_data_loader import JSONDataLoader
from deploy.src.database.postgres_data_loader import PostgresDataLoader
from deploy.src.database.data_loader_strategy import DataLoaderStrategy


file_config = FileConfig()

logger = LogHelper.get_logger(__name__)

class Orchestrator:
    """
    Orchestrates the database setup process by delegating the entire setup logic
    to specific database setup strategies based on a generic database type and connection type.
    """

    def __init__(self, database_type: str, connection_type: str = "local"):
        """
        Initializes the orchestrator by selecting the appropriate database setup strategy.
        
        :param database_type: The type of database (e.g., 'postgres', 'supabase', 'oracle', 'mongodb').
        :type database_type: str
        :param connection_type: The type of connection (e.g., 'local', 'remote').
        :type connection_type: str
        """
        self.db_setup_strategy = None
        self.db_type = database_type.lower()
        self.is_supported_db = True # Flag to track if the DB type is supported for automated setup
        self.connection_type = connection_type.lower()

        self.db_config = DatabaseConfig()

        logger.info(f"Orchestrator: Initializing for database type '{self.db_type}' with connection type '{self.connection_type}'.")

        # Map database_type and connection_type to the appropriate strategy
        if self.db_type == 'postgres':
            # For PostgreSQL, 'connection_type' directly maps to 'is_remote'
            is_remote_postgres = (self.connection_type == 'remote')
            self.db_setup_strategy = PostgresSetup(is_remote=is_remote_postgres)
        elif self.db_type == 'supabase':
            # Supabase is typically always remote and managed, connection_type might be ignored by the strategy itself
            self.db_setup_strategy = SupabaseSetup()
            if self.connection_type == 'local':
                 logger.warning("Supabase is a cloud service. 'local' connection_type specified, but will be treated as 'remote' by Supabase setup.")
        # -- Future supported types --
        # elif self.db_type == 'oracle': # Uncomment when OracleSetup is fully implemented
        #     self.db_setup_strategy = OracleSetup(connection_type=self.conn_type) # OracleSetup might differentiate local/remote
        # elif self.db_type == 'mongodb': # Uncomment when MongoDBSySetup is fully implemented
        #     self.db_setup_strategy = MongoDBSySetup(connection_type=self.conn_type) # MongoDBSetup might differentiate local/remote
        else: 
            self.is_supported_db = False
            logger.error(f"Automatic set-up not yet supported for the '{self.db_type}' database.")

        if self.db_setup_strategy is None and self.is_supported_db:
            logger.error("Orchestrator: No valid db_setup_strategy could be determined for a supported database type.")
            raise RuntimeError("Failed to initialize database setup strategy for a supported type.")

    
    def run_setup(self, sql_script_name: str = 'postgres_schema.sql') -> bool:
        """
        Deligates the database setup process to the chosen strategy.

        :param sql_script_name: The name of the SQL schema script
        :return: True if setup is successful, False if otherwise.
        """
        if not self.is_supported_db:
            logger.warning(f"Please perform manual set-up for the '{self.db_type}' database as per the '{file_config.schema_file}'.")
            return False # Automated setup is not supported, so return False

        logger.info(f"Orchestrator: Delegating full setup execution to the {self.db_type} strategy.")
        return self.db_setup_strategy.execute_full_setup(sql_script_name=sql_script_name)
    
    def run_data_loading(self, data_loader_strategy: DataLoaderStrategy, dump_file_path: Optional[str] = None) -> bool:
        """
        Delegates the data loading process to the chosen data loader strategy.

        :param data_loader_strategy: An instance of a DataLoaderStrategy (e.g., JSONDataLoader, PostgresDataLoader).
        :type data_loader_strategy: DataLoaderStrategy
        :param dump_file_path: Optional path to a database dump file, passed to the loader.
        :type dump_file_path: Optional[str]
        :return: True if data loading is successful, False otherwise.
        :rtype: bool
        """
        logger.info(f"Orchestrator: Delegating data loading execution to the {type(data_loader_strategy).__name__} strategy.")
        return data_loader_strategy.execute_full_data_load(dump_file_path=dump_file_path)


def main():
    parser = argparse.ArgumentParser(
        description = "Initial DB operation, setting up DB user, DB and respective tables.",
        formatter_class=argparse.RawTextHelpFormatter # For better formatting of help message
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
        help="Other database type (e.g., 'oracle', 'mongodb')."
    )

    # Mutually exclusive group for connection type
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
        nargs='?', # Optional argument
        const='__DEFAULT_JSON_PATH__', # A sentinel value if --load-json-data is present but no path given
        help="Load raw JSON data files from the specified directory.\n"
             "Provide the absolute path to the directory containing JSON files."
    )
    data_load_group.add_argument(
        "--load-db-dump",
        type=str,
        metavar="DUMP_FILE_PATH",
        nargs='?', # Optional argument
        const='__DEFAULT_DUMP_PATH__', # A sentinel value if --load-db-dump is present but no path given
        help="Load data from a database dump file using PostgresDataLoader.\n"
             "Provide the absolute path to the .sql dump file."
    )

    args = parser.parse_args()

     # --- Path Validation for --load-json-data and --load-db-dump ---
    if args.load_db_dump:
        if args.load_db_dump == '__DEFAULT_DUMP_PATH__':
            logger.error("Error: --load-db-dump requires a file path. Please provide the absolute path to the .sql dump file.")
            parser.print_help()
            sys.exit(1)
        elif not os.path.exists(args.load_db_dump):
            logger.error(f"Error: Database dump file not found at '{args.load_db_dump}'. Please provide a valid path.")
            sys.exit(1)
        elif not os.path.isfile(args.load_db_dump):
            logger.error(f"Error: The provided path '{args.load_db_dump}' is not a file. Please provide the path to a .sql dump file.")
            sys.exit(1)
        logger.info(f"Validated database dump file path: {args.load_db_dump}")

    if args.load_json_data:
        if args.load_json_data == '__DEFAULT_JSON_PATH__':
            logger.error("Error: --load-json-data requires a directory path. Please provide the absolute path to the folder containing JSON files.")
            parser.print_help()
            sys.exit(1)
        elif not os.path.exists(args.load_json_data):
            logger.error(f"Error: JSON data directory not found at '{args.load_json_data}'. Please provide a valid path.")
            sys.exit(1)
        elif not os.path.isdir(args.load_json_data):
            logger.error(f"Error: The provided path '{args.load_json_data}' is not a directory. Please provide the path to a folder containing JSON files.")
            sys.exit(1)
        logger.info(f"Validated JSON data directory path: {args.load_json_data}")
    # --- Path Validation ---
    
    # if not args.skip:

    #     # --- Logic to derive db_type and connection_type strings from CLI flags ---
    #     # Determine the database type string
    #     db_type: str
    #     if args.postgres:
    #         db_type = 'postgres'
    #     elif args.supabase:
    #         db_type = 'supabase'
    #     elif args.other:
    #         db_type = args.other.lower()
    #     else:
    #         # Default database type if no specific flag is provided
    #         db_type = 'postgres'
    #         logger.info("No specific database type flag (-P, -S, -O) provided. Defaulting to PostgreSQL.")
    #         logger.info("Preferred local database is postgresql and supabase for cloud.")


    #     # Determine the connection type string
    #     connection_type: str
    #     if args.remote:
    #         connection_type = 'remote'
    #     elif args.local:
    #         connection_type = 'local'
    #     else:
    #         # Default connection type if neither -r nor -l is specified
    #         connection_type = 'local'
    #         logger.info("No specific connection type flag (-l, -r) provided. Defaulting to 'local'.")

    #     # Instantiate the orchestrator with the derived string parameters
    #     try:
    #         orchestrator = Orchestrator(
    #             database_type=db_type,
    #             connection_type=connection_type
    #         )
    #     except (ValueError, NotImplementedError, RuntimeError) as e:
    #         logger.error(f"Error during orchestrator initialization: {e}")
    #         sys.exit(1)

    # else:
    #     # The orchestrator then tells its chosen strategy to run the setup.
    #     setup_successful = False
    #     if orchestrator.run_setup(sql_script_name= file_config.schema_file):
    #         logger.info("Database setup process completed successfully.")
    #         setup_successful = True
    #     else:
    #         logger.error("Database setup process finished with errors. Check logs for details.")
    #         setup_successful = False

    #     # --- Data Loading Phase (only if setup was successful or if not attempting setup) ---
    #     if setup_successful or (not args.postgres and not args.supabase and not args.other): # Allow loading without setup if no setup flag provided
    #         if args.load_json_data:
    #             logger.info("Starting JSON data loading phase...")
    #             # JSONDataLoader needs db_config to pass to DatabaseOperations internally
    #             json_loader = JSONDataLoader(
    #                 files_path=file_config.RAW_WEATHER_DATA_PATH, # Uses RAW_WEATHER_DATA_PATH
    #                 db_config=orchestrator.db_config # Pass the shared DatabaseConfig instance
    #             )
    #             if orchestrator.run_data_loading(json_loader):
    #                 logger.info("JSON data loading process completed successfully.")
    #             else:
    #                 logger.error("JSON data loading process finished with errors.")
    #         elif args.load_db_dump:
    #             logger.info(f"Starting database dump loading phase from: {args.load_db_dump}...")
    #             # Ensure you have PostgresDataLoader if you want to use this path
    #             from deploy.src.database.postgres_data_loader import PostgresDataLoader
    #             db_dump_loader = PostgresDataLoader() # PostgresDataLoader creates its own config internally
    #             if orchestrator.run_data_loading(db_dump_loader, dump_file_path=args.load_db_dump):
    #                 logger.info("Database dump loading process completed successfully.")
    #             else:
    #                 logger.error("Database dump loading process finished with errors.")
    #         else:
    #             logger.info("No data loading flags (--load-json-data, --load-db-dump) provided. Skipping data loading.")
    #     else:
    #         logger.info("Skipping data loading as database setup failed or was not attempted.")

        # Determine if any DB setup flag was explicitly provided
    explicit_db_setup_flag_provided = args.postgres or args.supabase or args.other

    # --- Database Setup Phase ---
    orchestrator_instance = None # Initialize to None
    setup_successful = False

    if args.skip:
        logger.info("Skipping database setup as --skip flag was provided.")
        setup_successful = True # Assume setup is "successful" if we're skipping it
        # We still need a db_config for data loading, so manually create a dummy Orchestrator
        # or just the db_config object. Let's create a minimal Orchestrator instance
        # that doesn't try to setup the DB.
        orchestrator_instance = Orchestrator(database_type="none", connection_type="none")
        orchestrator_instance.is_supported_db = False # Indicate no setup strategy
        orchestrator_instance.db_setup_strategy = None
    elif explicit_db_setup_flag_provided:
        # Only proceed with actual setup if explicit flags are given
        db_type: str
        if args.postgres:
            db_type = 'postgres'
        elif args.supabase:
            db_type = 'supabase'
        else: # args.other must be true
            db_type = args.other.lower()

        connection_type: str
        if args.remote:
            connection_type = 'remote'
        elif args.local:
            connection_type = 'local'
        else:
            connection_type = 'local' # Default connection type if specific setup flag but no conn type given

        try:
            orchestrator_instance = Orchestrator(
                database_type=db_type,
                connection_type=connection_type
            )
            logger.info("Starting database setup phase...")
            if orchestrator_instance.run_setup(sql_script_name=file_config.schema_file):
                logger.info("Database setup process completed successfully.")
                setup_successful = True
            else:
                logger.error("Database setup process finished with errors. Data loading will be skipped.")
                setup_successful = False
        except (ValueError, NotImplementedError, RuntimeError) as e:
            logger.error(f"Error during orchestrator initialization or setup: {e}")
            sys.exit(1)
    else:
        # No explicit setup flags AND no --skip.
        # This implies the user didn't ask for setup, nor explicitly to skip it.
        # We will assume setup is not required for this run, but we will still need an
        # Orchestrator instance to provide a db_config for data loading if requested.
        logger.info("No database setup flags provided. Assuming database is already set up or setup is not required.")
        setup_successful = True # Assume setup is "successful" for data loading purposes
        # Create a minimal Orchestrator instance just to get the db_config
        orchestrator_instance = Orchestrator(database_type="none", connection_type="none")
        orchestrator_instance.is_supported_db = False # Indicate no setup strategy
        orchestrator_instance.db_setup_strategy = None


    # --- Data Loading Phase ---
    # Proceed to data loading ONLY if a loader flag is given AND setup was successful (or skipped)
    if (args.load_json_data or args.load_db_dump) and setup_successful:
        if orchestrator_instance is None:
            logger.error("Orchestrator instance not initialized. Cannot proceed with data loading.")
            sys.exit(1)

        if args.load_json_data:
            logger.info("Starting JSON data loading phase...")
            json_loader = JSONDataLoader(
                files_path=file_config.RAW_WEATHER_DATA_PATH,
                db_config=orchestrator_instance.db_config
            )
            if orchestrator_instance.run_data_loading(json_loader):
                logger.info("JSON data loading process completed successfully.")
            else:
                logger.error("JSON data loading process finished with errors.")
        elif args.load_db_dump:
            logger.info(f"Starting database dump loading phase from: {args.load_db_dump}...")
            from deploy.src.database.postgres_data_loader import PostgresDataLoader
            # db_dump_loader = PostgresDataLoader(db_config=orchestrator_instance.db_config)
            db_dump_loader = PostgresDataLoader() # Assuming it creates its own DatabaseConfig internally
            if orchestrator_instance.run_data_loading(db_dump_loader, dump_file_path=args.load_db_dump):
                logger.info("Database dump loading process completed successfully.")
            else:
                logger.error("Database dump loading process finished with errors.")
    else:
        if not (args.load_json_data or args.load_db_dump):
            logger.info("No data loading flags (--load-json-data, --load-db-dump) provided. Skipping data loading.")
        elif not setup_successful:
            logger.error("Skipping data loading as database setup failed or was not completed successfully.")


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    logger.info(f"Total script execution time: {end_time - start_time:.2f} seconds")
