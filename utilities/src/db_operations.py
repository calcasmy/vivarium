# src/utilities/db_operations.py

import psycopg2
from psycopg2 import sql
from psycopg2 import OperationalError as Psycopg2Error
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from contextlib import contextmanager

from utilities.src.logger import LogHelper

logger = LogHelper.get_logger(__name__)


@dataclass
class ConnectionDetails:
    """
    A dataclass to hold all parameters required to establish a PostgreSQL connection.

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
    Manages PostgreSQL database connections and common operations, including transaction control.
    """

    def __init__(self):
        """
        Initializes the DatabaseOperations class.
        The connection will be established via the :meth:`connect` method.
        """
        self.conn = None
        self._connection_details: Optional[ConnectionDetails] = None
        self._current_autocommit_state: bool = False

    def connect(self, connection_details: ConnectionDetails) -> None:
        """
        Establishes a connection to the PostgreSQL database using provided ConnectionDetails.

        If an active connection already exists and is open, this method will do nothing.
        The connection is set to `autocommit=False` by default, but can be changed
        via :meth:`set_autocommit` or :meth:`autocommit_scope`.

        :param connection_details: An object containing all parameters required to establish the connection.
        :type connection_details: :class:`ConnectionDetails`
        :raises psycopg2.OperationalError: If a connection to the database fails.
        :raises Exception: For any other unexpected errors during connection.
        :returns: None
        :rtype: None
        """
        if self.conn and not self.conn.closed:
            self.close()

        self._connection_details = connection_details
        if self.conn and not self.conn.closed:
            logger.info("Already connected to the database.")
            return

        db_name = connection_details.dbname
        db_host = connection_details.host
        db_user = connection_details.user
        db_port = connection_details.port

        connect_params = {
            'host': connection_details.host,
            'port': connection_details.port,
            'user': connection_details.user,
            'password': connection_details.password,
            'dbname': connection_details.dbname
        }
        if connection_details.sslmode:
            connect_params['sslmode'] = connection_details.sslmode
        connect_params.update(connection_details.extra_params)

        try:
            logger.info(f"Attempting to connect as '{db_user}' to database '{db_name}' at {db_host}:{db_port}.")
            self.conn = psycopg2.connect(**connect_params)
            self.conn.autocommit = self._current_autocommit_state
            logger.info(f"Successfully connected as '{db_user}' to database '{db_name}'. Autocommit: {self.conn.autocommit}")
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

        If the connection is not in autocommit mode, any uncommitted transactions will be rolled back.

        :returns: None
        :rtype: None
        """
        if self.conn and not self.conn.closed:
            if not self._current_autocommit_state:
                try:
                    self.conn.rollback()
                    logger.debug("Rolled back uncommitted transaction before closing connection.")
                except Exception as e:
                    logger.warning(f"Error during rollback before closing: {e}")
            
            self.conn.close()
            self._connection_details = None
            self.conn = None
            logger.info("Successfully closed the connection to the database.")
        else:
            logger.info("No active connection to close or connection was already closed.")

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

    def set_autocommit(self, enabled: bool):
        """
        Sets the autocommit mode for the database connection.
        If a connection is active, applies the change immediately.
        Otherwise, stores the state for the next connection.

        :param enabled: If True, autocommit is enabled. If False, it's disabled.
        :type enabled: bool
        """
        self._current_autocommit_state = enabled
        if self.conn and not self.conn.closed:
            self.conn.autocommit = enabled
            logger.debug(f"Connection autocommit set to: {self.conn.autocommit}")
        else:
            logger.debug(f"Autocommit state ({enabled}) stored for future connection.")

    @contextmanager
    def autocommit_scope(self):
        """
        A context manager to temporarily enable autocommit mode for a block of code.
        Ensures autocommit is reset to its original state after the block.
        Any uncommitted transactions are rolled back when exiting the scope if switching
        back to non-autocommit mode.
        """
        original_autocommit_state = self._current_autocommit_state
        self.set_autocommit(True)
        try:
            yield
        finally:
            self.set_autocommit(original_autocommit_state)
            if not original_autocommit_state and self.conn and not self.conn.closed:
                try:
                    self.conn.rollback()
                    logger.debug("Rolled back connection after autocommit scope.")
                except Exception as e:
                    logger.warning(f"Error during rollback after autocommit scope: {e}")

    def begin_transaction(self) -> None:
        """
        Begins a new database transaction.

        This method will raise a :exc:`RuntimeError` if the connection is currently in autocommit mode.

        :returns: None
        :rtype: None
        :raises RuntimeError: If there is no active database connection or connection is in autocommit mode.
        """
        conn = self.get_connection()
        if conn.autocommit:
            logger.error("Cannot begin transaction: Connection is in autocommit mode.")
            raise RuntimeError("Cannot begin transaction: Connection is in autocommit mode.")
        logger.debug("Beginning database transaction.")

    def commit_transaction(self) -> None:
        """
        Commits the current database transaction.

        All changes made since the last :meth:`begin_transaction` or :meth:`commit_transaction`
        will be permanently saved to the database. This method logs a warning and does nothing
        if the connection is in autocommit mode.

        :returns: None
        :rtype: None
        :raises RuntimeError: If there is no active database connection.
        :raises psycopg2.Error: If a database error occurs during commit.
        """
        conn = self.get_connection()
        if conn.autocommit:
            logger.warning("Attempted to commit transaction while in autocommit mode. No action taken.")
            return
        try:
            conn.commit()
            logger.debug("Database transaction committed successfully.")
        except Psycopg2Error as e:
            logger.error(f"Database error during commit: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during commit: {e}", exc_info=True)
            raise

    def rollback_transaction(self) -> None:
        """
        Rolls back the current database transaction.

        All changes made since the last :meth:`begin_transaction` or :meth:`commit_transaction`
        will be discarded. This method logs a warning and does nothing if the connection is
        in autocommit mode.

        :returns: None
        :rtype: None
        :raises RuntimeError: If there is no active database connection.
        :raises psycopg2.Error: If a database error occurs during rollback.
        """
        conn = self.get_connection()
        if conn.autocommit:
            logger.warning("Attempted to rollback transaction while in autocommit mode. No action taken.")
            return
        try:
            conn.rollback()
            logger.warning("Database transaction rolled back.")
        except Psycopg2Error as e:
            logger.error(f"Database error during rollback: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during rollback: {e}", exc_info=True)
            raise

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
        options for fetching single rows or all rows. When :attr:`conn.autocommit` is
        :obj:`False`, the caller is responsible for managing transactions via
        :meth:`begin_transaction`, :meth:`commit_transaction`, and :meth:`rollback_transaction`.

        :param query: The SQL query string to be executed. Can be a plain string,
                      :class:`psycopg2.sql.SQL`, or :class:`psycopg2.sql.Composed` object.
        :type query: str | psycopg2.sql.SQL | psycopg2.sql.Composed
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
        """
        try:
            conn = self.get_connection()
        except RuntimeError as e:
            logger.error(f"Cannot execute query. {e}")
            return None

        try:
            with conn.cursor() as cur:
                cur.execute(query, params)

                if cur.description:
                    columns = [desc.name for desc in cur.description]
                    if fetch_one:
                        row = cur.fetchone()
                        return dict(zip(columns, row)) if row else None
                    elif fetch:
                        rows = cur.fetchall()
                        return [dict(zip(columns, row)) for row in rows]
                    else:
                        return None
                else:
                    log_query_str = str(query) if isinstance(query, (sql.SQL, sql.Composed)) else query
                    logger.debug(f"Query returned no description. Query: {log_query_str.strip()[:50]}...")
                    return None
        except Psycopg2Error as e:
            logger.error(f"Database error executing query: {e} | Query: '{str(query).strip()}' | Params: {params}", exc_info=True)
            raise
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during query execution: {e} | Query: '{str(query).strip()}' | Params: {params}",
                exc_info=True
            )
            raise

    def execute_query_with_returning_id(
        self, query: str, params: Optional[tuple] = None
    ) -> Optional[int]:
        """
        Executes a SQL query and returns the ID of the inserted row (if applicable).

        This method expects the query to include a 'RETURNING id' or similar clause.
        When :attr:`conn.autocommit` is :obj:`False`, the caller
        is responsible for managing transactions via :meth:`begin_transaction`,
        :meth:`commit_transaction`, and :meth:`rollback_transaction`.

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
            raise
        except Exception as e:
            logger.error(
                f"An unexpected error occurred with returning ID: {e} | Query: {query} | Params: {params}"
            )
            raise

    def execute_command(self, query: str | sql.SQL | sql.Composed, params: Optional[Tuple] = None) -> bool:
        """
        Executes a SQL command (DML or DDL) that does not return results.

        This method relies on :meth:`execute_query` for execution and
        expects the caller or the connection's autocommit mode to handle transactions.

        :param query: The SQL command string or a :class:`psycopg2.sql.SQL` or :class:`psycopg2.sql.Composed` object.
        :type query: str | psycopg2.sql.SQL | psycopg2.sql.Composed
        :param params: Optional parameters to pass to the command.
        :type params: Optional[tuple]
        :returns: ``True`` if the command executes without an immediate error, ``False`` otherwise.
        :rtype: bool
        """
        try:
            self.execute_query(query, params, fetch=False, fetch_one=False)
            return True
        except Exception as e:
            logger.error(f"Failed to execute command: '{str(query).strip()}' | Error: {e}", exc_info=True)
            return False
        
    def get_connection_details(self) -> Optional[ConnectionDetails]:
        """
        Returns the :class:`ConnectionDetails` object that was last used to establish the connection.

        :returns: The :class:`ConnectionDetails` object if a connection was established, otherwise ``None``.
        :rtype: Optional[:class:`ConnectionDetails`]
        """
        return self._connection_details
    
    def test_connection(self) -> bool:
        """
        Tests the current database connection by executing a simple query.
        
        :returns: True if the connection is active and a query can be executed, False otherwise.
        """
        if self.conn is None or self.conn.closed:
            logger.warning("Attempted to test a closed or non-existent database connection.")
            return False
        try:
            # Execute a simple query to test connectivity
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT 1;")
                cursor.fetchone() # Fetch to ensure the query completes successfully
            logger.debug("Database connection test successful.")
            return True
        except Psycopg2Error as e:
            logger.error(f"Database operational error during connection test: {e}", exc_info=True)
            self.close() # Attempt to close the broken connection
            return False
        except Exception as e:
            logger.error(f"Unexpected error during database connection test: {e}", exc_info=True)
            self.close()
            return False