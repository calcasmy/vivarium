# src/database/database_operations.py
import psycopg2
from psycopg2 import OperationalError as Psycopg2Error
from psycopg2 import sql # Added for clarity in admin operations if needed
from typing import Optional, List, Dict
from utilities.src.config import DatabaseConfig
from utilities.src.logger import LogHelper

logger = LogHelper.get_logger(__name__)

class DatabaseOperations:
    def __init__(self, db_config: Optional[DatabaseConfig] = None):
        """
        Initializes the DatabaseOperations class.

        :param db_config: An optional DatabaseConfig object.
                          If None, it will create its own instance.
        :type db_config: Optional[DatabaseConfig]
        """
        self.conn = None
        self.db_config = db_config if db_config is not None else DatabaseConfig()

    def connect(self, user_type: str = "local", custom_config: Optional[Dict] = None) -> None:
        """
        Connects to the PostgreSQL database based on the user_type or a custom configuration.

        :param user_type: The type of user/config to use.
                          Expected values: "local", "remote", "super", "supabase".
                          Defaults to "local".
        :type user_type: str
        :param custom_config: A dictionary of connection parameters to use
                              instead of the predefined configurations.
                              Useful for ad-hoc connections (e.g., superuser setup).
        :type custom_config: Optional[Dict]
        :raises ValueError: If an invalid user_type is provided or configuration is missing.
        :raises psycopg2.OperationalError: If a connection to the database fails.
        :raises Exception: For any other unexpected errors during connection.
        :returns: None
        :rtype: None
        """
        if self.conn and not self.conn.closed:
            logger.info("Already connected to the database.")
            return

        connection_params = None
        connection_type_name = ""

        if custom_config:
            connection_params = custom_config
            connection_type_name = "Custom"
        else:
            # Select config based on user_type
            if user_type == "local":
                connection_params = self.db_config.postgres
                connection_type_name = "Local User"
            elif user_type == "remote":
                connection_params = self.db_config.postgres_remote
                connection_type_name = "Remote User"
            elif user_type == "super":
                connection_params = self.db_config.postgres_super
                connection_type_name = "Super User"
            # elif user_type == "supabase": # Uncomment when Supabase is fully implemented in config
            #     connection_params = self.db_config.supabase
            #     connection_type_name = "Supabase User"
            else:
                logger.error(f"Invalid user_type '{user_type}' provided. Cannot connect.")
                raise ValueError(f"Invalid user_type: {user_type}")

        if not connection_params:
            logger.error(f"Configuration for '{connection_type_name}' is missing or invalid. Cannot connect.")
            raise ValueError(f"Missing or incomplete configuration for user_type: {user_type}")

        db_name = connection_params.get('dbname', 'N/A')
        db_host = connection_params.get('host', 'N/A')
        db_user = connection_params.get('user', 'N/A')
        db_port = connection_params.get('port', 'N/A')

        try:
            logger.info(f"Attempting to connect as '{db_user}' ({connection_type_name}) to database '{db_name}' at {db_host}:{db_port}.")
            self.conn = psycopg2.connect(**connection_params)
            self.conn.autocommit = True
            logger.info(f"Successfully connected as '{db_user}' ({connection_type_name}) to database '{db_name}'.")
        except Psycopg2Error as e:
            logger.error(f"FATAL: Operational error connecting as '{db_user}' ({connection_type_name}) to database '{db_name}' on host '{db_host}'. "
                         f"Please check connection parameters, database existence, and server status. Error: {e}")
            self.conn = None
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during connection as '{db_user}' ({connection_type_name}) to database '{db_name}' on host '{db_host}'. Error: {e}")
            self.conn = None
            raise

    def close(self) -> None:
        """
        Closes the connection to the PostgreSQL database.

        :returns: None
        :rtype: None
        """
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Successfully closed the connection to the database.")
        else:
            logger.info("No active connection to close or connection was already closed.")
        self.conn = None

    def get_connection(self):
        """
        Returns the database connection. Ensures it is open by calling :py:meth:`~DatabaseOperations.connect`.
        
        This method will attempt to connect as the default "local" user if no prior
        connection was established via :py:meth:`~DatabaseOperations.connect(user_type=...)`.
        
        It is recommended to explicitly call :py:meth:`~DatabaseOperations.connect` with the desired `user_type`
        before relying on this implicit connection attempt, especially in complex scenarios.

        :returns: The active psycopg2 database connection object.
        :rtype: psycopg2.connection
        """
        if not self.conn or self.conn.closed:
            logger.warning("No active connection found. Attempting to connect with default 'local' user.")
            self.connect(user_type="local")
        return self.conn

    def execute_query(
        self, query: str, params: Optional[tuple] = None, fetch: bool = False
    ) -> Optional[List[Dict] | None]:
        """
        Executes a SQL query.

        :param query: The SQL query string.
        :type query: str
        :param params: Optional parameters to pass to the query (for parameterized queries).
        :type params: Optional[tuple]
        :param fetch: Whether to fetch the results.
        :type fetch: bool
        :returns: A list of dictionaries if fetch is True and the query returns results,
                  ``None`` otherwise.
        :rtype: Optional[List[Dict]]
        :raises psycopg2.Error: If a database error occurs during query execution.
        :raises Exception: For any other unexpected errors during query execution.
        """
        conn = self.get_connection()
        if not conn or conn.closed:
            logger.error("Cannot execute query. No active database connection.")
            return None

        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    result = [dict(zip(columns, row)) for row in rows]
                    return result
                else:
                    return None
        except psycopg2.Error as e:
            logger.error(f"Error executing query: {e} | Query: {query} | Params: {params}")
            if not conn.autocommit: # Only rollback if autocommit is False
                conn.rollback()
            return None
        except Exception as e:
            logger.error(
                f"An unexpected error occurred: {e} | Query: {query} | Params: {params}"
            )
            if not conn.autocommit: # Only rollback if autocommit is False
                conn.rollback()
            return None

    def execute_query_with_returning_id(
        self, query: str, params: Optional[tuple] = None
    ) -> Optional[int]:
        """
        Executes a SQL query and returns the ID of the inserted row (if applicable).

        :param query: The SQL query string.
        :type query: str
        :param params: Optional parameters to pass to the query.
        :type params: Optional[tuple]
        :returns: The ID of the inserted row, or ``None`` on error or if no ID is returned.
        :rtype: Optional[int]
        :raises psycopg2.Error: If a database error occurs during query execution.
        :raises Exception: For any other unexpected errors during query execution.
        """
        conn = self.get_connection()
        if not conn or conn.closed:
            logger.error("Cannot execute query. No active database connection.")
            return None
        
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)

                if "RETURNING" in query.upper():
                    result = cur.fetchone()
                    if result:
                        insert_id = result[0]
                        return insert_id
                    else:
                        logger.warning(f"Query returned no result for ID: {query}")
                        return None
                else:
                    logger.warning(f"Query does not contain 'RETURNING' clause. Cannot fetch an ID: {query}")
                    return None
        except psycopg2.Error as e:
            logger.error(f"Error executing query with returning ID: {e} | Query: {query} | Params: {params}")
            if not conn.autocommit:
                conn.rollback()
            return None
        except Exception as e:
            logger.error(
                f"An unexpected error occurred with returning ID: {e} | Query: {query} | Params: {params}"
            )
            if not conn.autocommit:
                conn.rollback()
            return None