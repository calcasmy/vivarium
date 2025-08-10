# vivarium/database/database_setup_ops/db_setup_strategy.py
"""
Defines the abstract base class for database setup strategies.

Concrete implementations (e.g., PostgresSetup, SupabaseSetup) must
inherit from this class and implement its abstract methods.
"""

from abc import ABC, abstractmethod
from typing import Tuple, Optional

# Assuming DatabaseOperations is correctly imported from utilities.src.database_operations
from utilities.src.db_operations import DBOperations

class DBSetupStrategy(ABC):
    """
    Abstract Base Class defining the interface for database setup strategies.

    Any class implementing a database setup routine (e.g., creating databases,
    users, applying schemas) should inherit from this class.
    """

    @abstractmethod
    def full_setup(self, sql_script_name: str) -> Tuple[bool, Optional[DBOperations]]:
        """
        Executes the full database setup process for a specific database type.

        This method should handle creating the database, setting up users,
        applying schema, and any other necessary initializations.

        :param sql_script_name: The name of the SQL schema script to apply.
        :type sql_script_name: str
        :returns: A tuple containing:
                  - A boolean indicating if the setup was successful.
                  - An active `DatabaseOperations` instance connected to the
                    application database (e.g., as the application user),
                    or ``None`` if the setup failed or no connection is needed
                    for subsequent steps.
        :rtype: Tuple[bool, Optional[DatabaseOperations]]
        """
        pass