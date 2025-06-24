# vivarium/deploy/src/database/db_setup_strategy.py
import os
import sys
from abc import ABC, abstractmethod

# Ensure the vivarium root path is in sys.path
if __name__ == "__main__":
    # If run directly, go up two levels from 'database' to 'vivarium' root
    vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
    if vivarium_path not in sys.path:
        sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper

logger = LogHelper.get_logger(__name__)

class DBSetupStrategy(ABC):
    """
    Abstract Base Class for all database setup strategies.
    Defines the common interface for different database types (PostgreSQL, Supabase, etc.).
    Also handles the internal logic for locating the SQL schema file.
    """

    def __init__(self):
        """
        Initializes the abstract strategy.
        Derives the base path for the SQL schema file, which is constant for all strategies.
        """
        # The schema file 'db_schema.sql' is in the same directory as this strategy base class.
        self.sql_schema_base_path = os.path.dirname(os.path.abspath(__file__))
        logger.debug(f"DBSetupStrategy: SQL schema base path set to: {self.sql_schema_base_path}")

    @abstractmethod
    def execute_full_setup(self, sql_script_name: str) -> bool:
        """
        Executes the entire database setup process for a specific database type.
        This method orchestrates other setup steps (e.g., user creation, table creation, data loading).

        :param sql_script_name: The name of the SQL schema script (e.g., 'postgres_schema.sql').
                                The strategy is responsible for locating this file using self.sql_schema_base_path.
        :return: True if the setup is successful, False otherwise.
        """
        pass

    @abstractmethod
    def create_database_and_user(self) -> bool:
        """
        Abstract method to create the necessary database and user (if applicable) for the specific DB type.

        :return: True if creation is successful, False otherwise.
        """
        pass

    @abstractmethod
    def create_tables(self, sql_script_name: str) -> bool:
        """
        Abstract method to create tables in the database using the provided SQL schema script.

        :param sql_script_name: The name of the SQL schema script (e.g., 'postgres_schema.sql').
                                The concrete strategy should use self.sql_schema_base_path to find it.
        :return: True if table creation is successful, False otherwise.
        """
        pass

    @abstractmethod
    def prompt_for_restart(self) -> bool:
        """
        Abstract method to prompt the user for a database service restart if necessary.

        :return: True if the user confirms the restart, False otherwise.
        """
        pass

    # You can add more abstract methods here as common steps emerge, e.g.:
    # @abstractmethod
    # def run_migrations(self) -> bool:
    #     """
    #     Abstract method to run database migrations.
    #     """
    #     pass

    # @abstractmethod
    # def load_initial_data(self) -> bool:
    #     """
    #     Abstract method to load initial reference data into the database.
    #     """
    #     pass