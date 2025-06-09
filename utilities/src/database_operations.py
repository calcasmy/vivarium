# src/database/database_operations.py
import psycopg2
from psycopg2 import OperationalError
from typing import Optional, List, Dict
from utilities.src.config import DatabaseConfig
from utilities.src.logger import LogHelper

logger = LogHelper.get_logger(__name__)
db_config = DatabaseConfig()

class DatabaseOperations:
    def __init__(self):
        """
        Initializes the DatabaseOperations class.
        """
        self.conn = None
        self.postgres_config = db_config.postgres

    def connect(self) -> None:
        """Connects to the PostgreSQL database."""
        if self.conn:
            logger.info("Already connected to the database.")
            return
        
        try:
            self.conn = psycopg2.connect(**self.postgres_config)
            logger.info(f"Successfully connected to database '{self.postgres_config.get('dbname', 'N/A')}'.")
        except OperationalError as e:
            logger.error(f"FATAL: error connecting to database '{self.postgres_config.get('dbname', 'N/A')}'"
                         f"Please check connection parameters, database existance and server status. Error: {e}")
            self.conn = None
            raise
        except psycopg2.Error as e:
            logger.error(f"Error connecting to the database: {e}")
            self.conn = None
            raise

    def close(self) -> None:
        """Closes the connection to the PostgreSQL database."""
        if self.conn:
            self.conn.close()
            logger.info("Successfully closed the connection to the database.")
            self.conn = None

    def get_connection(self):
        """
        Returns the database connection.  Ensures it is open.
        """
        if not self.conn:
            self.connect()  # Establish connection if it doesn't exist
        return self.conn

    def execute_query(
        self, query: str, params: Optional[tuple] = None, fetch: bool = False
    ) -> Optional[List[Dict] | None]:
        """Executes a SQL query.

        Args:
            query: The SQL query string.
            params: Optional parameters to pass to the query (for parameterized queries).
            fetch: Whether to fetch the results.

        Returns:
            A list of dictionaries if fetch is True and the query returns results,
            None otherwise.
        """
        if not self.conn:
            logger.error("Cannot execute query. No database connection.")
            return None

        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    result = [dict(zip(columns, row)) for row in rows]
                    self.conn.commit()  # Explicitly commit *after* fetching
                    cur.close()
                    return result
                else:
                    self.conn.commit()
                    return None
        except psycopg2.Error as e:
            logger.error(f"Error executing query: {e} | Query: {query} | Params: {params}")
            self.conn.rollback()
            return None
        except Exception as e:
            logger.error(
                f"An unexpected error occurred: {e} | Query: {query} | Params: {params}"
            )
            self.conn.rollback()
            return None

    def execute_query_with_returning_id(
        self, query: str, params: Optional[tuple] = None
    ) -> Optional[int]:
        """Executes a SQL query and returns the ID of the inserted row (if applicable).

        Args:
            query: The SQL query string.
            params: Optional parameters to pass to the query.

        Returns:
            The ID of the inserted row, or None on error.
        """
        if not self.conn:
            logger.error("Cannot execute query. No database connection.")
            return None
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                self.conn.commit()

                if "RETURNING" in query.upper():
                    result = cur.fetchone()
                    if result:
                        return result[0]  # Return the ID
                    else:
                        return None #even if no result, return none
                elif cur.description:  # Check if there's a result description for other queries
                    columns = [desc[0] for desc in cur.description]
                    if "id" in columns:
                        return cur.fetchone()[0]
                    else:
                         return None
                else:
                    return None #if no description
        except psycopg2.Error as e:
            logger.error(f"Error executing query: {e} | Query: {query} | Params: {params}")
            self.conn.rollback()
            return None
        except Exception as e:
            logger.error(
                f"An unexpected error occurred: {e} | Query: {query} | Params: {params}"
            )
            self.conn.rollback()
            return None