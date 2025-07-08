# src/utilities/database_operations.py
import psycopg2
from psycopg2 import sql
from psycopg2 import OperationalError as Psycopg2Error
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field

# Assuming LogHelper is correctly imported from utilities.src.logger
from utilities.src.logger import LogHelper

logger = LogHelper.get_logger(__name__)

@dataclass
class ConnectionDetails:
    """
    A dataclass to hold all parameters required to establish a PostgreSQL connection.

    This class provides a structured and type-hinted way to pass database credentials
    and connection information around the application.

    :param host: The database host address (e.g., 'localhost', 'your.db.server.com').
    :type host: str
    :param port: The database port number (e.g., 5432).
    :type port: int
    :param user: The database username.
    :type user: str
    :param password: The database password.
    :type password: str
    :param dbname: The name of the database to connect to.
    :type dbname: str
    :param sslmode: Optional SSL mode for the connection (e.g., 'require', 'prefer'). Defaults to None.
    :type sslmode: Optional[str]
    :param extra_params: A dictionary for any additional psycopg2 connection parameters. Defaults to an empty dict.
    :type extra_params: Dict[str, Any]
    """
    host: str
    port: int
    user: str
    password: str
    dbname: str
    sslmode: Optional[str] = None
    extra_params: Dict[str, Any] = field(default_factory=dict)

class DBOperations:
    """
    Manages PostgreSQL database connections and common operations.
    """
    def __init__(self):
        """
        Initializes the DatabaseOperations class.
        The connection will be established via the :meth:`connect` method.
        """
        self.conn = None
        self._connection_details: Optional[ConnectionDetails] = None

    def connect(self, connection_details: ConnectionDetails) -> None:
        """
        Establishes a connection to the PostgreSQL database using provided ConnectionDetails.

        If an active connection already exists and is open, this method will do nothing.

        :param connection_details: An object containing all parameters required to establish the connection.
        :type connection_details: ConnectionDetails
        :raises psycopg2.OperationalError: If a connection to the database fails.
        :raises Exception: For any other unexpected errors during connection.
        :returns: None
        :rtype: None
        """

        self._connection_details = connection_details
        if self.conn and not self.conn.closed:
            logger.info("Already connected to the database.")
            return

        db_name = connection_details.dbname
        db_host = connection_details.host
        db_user = connection_details.user
        db_port = connection_details.port

        # Prepare connection parameters for psycopg2.connect
        connect_params = {
            'host': connection_details.host,
            'port': connection_details.port,
            'user': connection_details.user,
            'password': connection_details.password,
            'dbname': connection_details.dbname
        }
        if connection_details.sslmode:
            connect_params['sslmode'] = connection_details.sslmode
        # connect_params.update(connection_details.extra_params)

        try:
            logger.info(f"Attempting to connect as '{db_user}' to database '{db_name}' at {db_host}:{db_port}.")
            self.conn = psycopg2.connect(**connect_params)
            self.conn.autocommit = True
            logger.info(f"Successfully connected as '{db_user}' to database '{db_name}'.")
        except Psycopg2Error as e:
            logger.error(f"FATAL: Operational error connecting as '{db_user}' to database '{db_name}' on host '{db_host}'. "
                         f"Please check connection parameters, database existence, and server status. Error: {e}")
            self.conn = None
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during connection as '{db_user}' to database '{db_name}' on host '{db_host}'. Error: {e}")
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
            self._connection_details = None
            logger.info("Successfully closed the connection to the database.")
        else:
            logger.info("No active connection to close or connection was already closed.")
        self.conn = None

    def get_connection(self):
        """
        Returns the active psycopg2 database connection.

        This method does NOT attempt to establish a new connection if one is not active.
        The caller is responsible for ensuring :meth:`connect` has been called successfully.

        :returns: The active psycopg2 database connection object.
        :rtype: psycopg2.connection
        :raises RuntimeError: If there is no active database connection.
        """
        if not self.conn or self.conn.closed:
            logger.error("No active database connection found. Call connect() first.")
            raise RuntimeError("No active database connection.")
        return self.conn
    
    def execute_query(
        self,
        query: str | sql.SQL | sql.Composed,
        params: Optional[Tuple] = None,
        fetch: bool = False,
        fetch_one: bool = False
    ) -> Optional[Dict] | Optional[List[Dict]] | None:
        """
        Executes a SQL query against the connected PostgreSQL database.

        This method handles various types of queries (DML, DDL, DQL) and provides
        options for fetching single rows or all rows.

        :param query: The SQL query string to be executed.
        :type query: str
        :param params: Optional parameters to pass to the query, used for parameterized
                       queries to prevent SQL injection. Defaults to :obj:`None`.
        :type params: Optional[tuple]
        :param fetch: If :obj:`True`, fetches all available rows as a list of dictionaries.
                      This parameter is primarily for 'fetch all' operations and is
                      superseded by `fetch_one` if both are set to :obj:`True`.
                      Defaults to :obj:`False`.
        :type fetch: bool
        :param fetch_one: If :obj:`True`, fetches only the first row as a dictionary.
                          This parameter takes precedence over `fetch`. Defaults to :obj:`False`.
        :type fetch_one: bool
        :returns:
            - A :py:class:`dict` if `fetch_one` is :obj:`True` and a row is found (e.g., ``{'column_name': value}``).
            - A :py:class:`list` of :py:class:`dict` if `fetch_one` is :obj:`False` and `fetch` is :obj:`True`,
              and rows are found (e.g., ``[{'col1': val1}, {'col1': val2}]``).
            - :obj:`None` if:
                - No rows are found for fetch operations.
                - The query is DDL/DML (no results to fetch).
                - An error occurs during execution.
                - No active connection is available.
        :rtype: Optional[Dict] | Optional[List[Dict]] | None
        :raises RuntimeError: If there is no active database connection to execute the query.
        :raises psycopg2.Error: If a database-specific error occurs during query execution
                                (e.g., syntax error, constraint violation).
        :raises Exception: For any other unexpected errors during the execution process.

        **Example Usage:**

        .. code-block:: python

            # Assuming db_ops is an instance of DatabaseOperations
            # Fetch all users
            all_users = db_ops.execute_query("SELECT id, name FROM users;", fetch=True)
            if all_users:
                for user in all_users:
                    print(f"User: {user['name']}")

            # Fetch a single user by ID
            user_id = 1
            single_user = db_ops.execute_query("SELECT id, name FROM users WHERE id = %s;", (user_id,), fetch_one=True)
            if single_user:
                print(f"Found user: {single_user['name']}")
            else:
                print(f"User with ID {user_id} not found.")

            # Execute an UPDATE query (no fetch needed)
            db_ops.execute_query("UPDATE users SET name = %s WHERE id = %s;", ("New Name", 1))
        """
        try:
            conn = self.get_connection()
            # No need for `if not conn: raise RuntimeError(...)` here, as get_connection already handles it.
        except RuntimeError as e:
            logger.error(f"Cannot execute query. {e}")
            return None
        except Exception as e: # Catch any unexpected errors from get_connection
            logger.error(f"Unexpected error getting database connection: {e}", exc_info=True)
            return None

        try:
            # Using 'with conn.cursor()' ensures the cursor is properly closed
            with conn.cursor() as cur:
                cur.execute(query, params)

                # Check if the query returns a result set (e.g., SELECT statements)
                if cur.description:
                    columns = [desc.name for desc in cur.description] # Use desc.name for column names
                    if fetch_one: # Prioritize fetching a single row
                        row = cur.fetchone()
                        return dict(zip(columns, row)) if row else None
                    elif fetch: # Fallback to fetching all rows if fetch_one is False
                        rows = cur.fetchall()
                        return [dict(zip(columns, row)) for row in rows]
                    else:
                        # No fetch requested, return None as per contract
                        return None
                else:
                    # Query did not return any description (e.g., INSERT, UPDATE, DELETE, DDL).
                    # For such queries, there are no rows to fetch.
                    log_query_str = str(query) if isinstance(query, (sql.SQL, sql.Composed)) else query
                    logger.debug(f"Query returned no description. Query: {log_query_str.strip()[:50]}...")

                    # For DML/DDL, commit if not in autocommit mode
                    if not conn.autocommit:
                        conn.commit()
                    return None
        except Psycopg2Error as e:
            logger.error(f"Database error executing query: {e} | Query: '{str(query).strip()}' | Params: {params}", exc_info=True)
            if conn and not conn.autocommit:
                conn.rollback() # Rollback on database error
            return None
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during query execution: {e} | Query: '{str(query).strip()}' | Params: {params}",
                exc_info=True
            )
            if conn and not conn.autocommit:
                conn.rollback() # Rollback on unexpected error
            return None

    def execute_query_with_returning_id(
        self, query: str, params: Optional[tuple] = None
    ) -> Optional[int]:
        """
        Executes a SQL query and returns the ID of the inserted row (if applicable).

        :param query: The SQL query string. Must include a 'RETURNING id' or similar clause.
        :type query: str
        :param params: Optional parameters to pass to the query.
        :type params: Optional[tuple]
        :returns: The ID of the inserted row, or ``None`` on error or if no ID is returned.
        :rtype: Optional[int]
        :raises psycopg2.Error: If a database error occurs during query execution.
        :raises RuntimeError: If there is no active database connection.
        :raises Exception: For any other unexpected errors during query execution.
        """
        try:
            conn = self.get_connection()
        except RuntimeError as e:
            logger.error(f"Cannot execute query. {e}")
            return None

        try:
            with conn.cursor() as cur:
                cur.execute(query, params)

                # Check for "RETURNING" keyword case-insensitively for better robustness
                if "RETURNING" in query.upper():
                    result = cur.fetchone()
                    if result:
                        insert_id = result[0]
                        return insert_id
                    else:
                        logger.warning(f"Query with 'RETURNING' clause returned no result: {query}")
                        return None
                else:
                    logger.warning(f"Query does not contain 'RETURNING' clause. Cannot fetch an ID: {query}")
                    return None
        except Psycopg2Error as e:
            logger.error(f"Error executing query with returning ID: {e} | Query: {query} | Params: {params}")
            if self.conn and not self.conn.autocommit:
                self.conn.rollback()
            return None
        except Exception as e:
            logger.error(
                f"An unexpected error occurred with returning ID: {e} | Query: {query} | Params: {params}"
            )
            if self.conn and not self.conn.autocommit:
                self.conn.rollback()
            return None
    
    def execute_command(self, query: str | sql.SQL | sql.Composed, params: Optional[Tuple] = None) -> bool: # <-- MODIFIED TYPE HINT
        """
        Executes a SQL command (DML or DDL) that does not return results.
        :param query: The SQL command string or a psycopg2.sql object.
        :type query: str | psycopg2.sql.SQL | psycopg2.sql.Composed
        # ... (rest of the docstring) ...
        """
        try:
            result = self.execute_query(query, params, fetch=False, fetch_one=False)
            return result is None
        except Exception as e:
            logger.error(f"Failed to execute command: '{str(query).strip()}' | Error: {e}", exc_info=True) # Use str(query) for logging
            return False
        
    def get_connection_details(self) -> Optional[ConnectionDetails]:
        """
        Returns the ConnectionDetails object that was last used to establish the connection.

         :returns: The ConnectionDetails object if a connection was established, otherwise None.
        :rtype: Optional[ConnectionDetails]
        """
        return self._connection_details