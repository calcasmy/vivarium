# vivarium/database/climate_data_ops/base_query_strategy.py

"""
Abstract base class for all database query implementations.
This module defines the common interface that concrete query classes must adhere to,
ensuring they all operate with a provided DBOperations instance.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, List, Union

from utilities.src.db_operations import DBOperations
from utilities.src.logger import LogHelper

logger = LogHelper.get_logger(__name__)


class BaseQuery(ABC):
    """
    Abstract base class defining the interface for database query operations.

    All concrete query classes (e.g., RawDataQueries, LocationQueries)
    must inherit from this class and implement its abstract methods.
    """

    def __init__(self, db_operations: DBOperations):
        """
        Initializes the BaseQuery with a DBOperations instance.

        :param db_operations: An active DBOperations instance for database interaction.
        :type db_operations: DBOperations
        :raises TypeError: If db_operations is not an instance of DBOperations.
        """
        if not isinstance(db_operations, DBOperations):
            logger.error(f"Invalid type for db_operations: {type(db_operations)}. Expected DBOperations.")
            raise TypeError("db_operations must be an instance of DBOperations.")
        self.db_ops = db_operations
        logger.debug(f"BaseQuery initialized with DBOperations for {self.__class__.__name__}.")

    @abstractmethod
    def insert(self, *args: Any, **kwargs: Any) -> Optional[Union[int, str]]:
        """
        Abstract method to insert data into the database.
        Concrete classes must implement this method to handle specific data insertion logic.

        :param args: Positional arguments for the insertion.
        :param kwargs: Keyword arguments for the insertion.
        :returns: The ID or a unique identifier of the inserted record, or None on failure.
        :rtype: Optional[Union[int, str]]
        """
        pass

    @abstractmethod
    def get(self, *args: Any, **kwargs: Any) -> Optional[Union[Dict, List[Dict]]]:
        """
        Abstract method to retrieve data from the database.
        Concrete classes must implement this method to handle specific data retrieval logic.

        :param args: Positional arguments for the retrieval (e.g., ID, date).
        :param kwargs: Keyword arguments for the retrieval.
        :returns: A dictionary for a single record, a list of dictionaries for multiple records, or None if not found.
        :rtype: Optional[Union[Dict, List[Dict]]]
        """
        pass

    @abstractmethod
    def update(self, *args: Any, **kwargs: Any) -> bool:
        """
        Abstract method to update existing data in the database.
        Concrete classes must implement this method to handle specific data update logic.

        :param args: Positional arguments for the update.
        :param kwargs: Keyword arguments for the update.
        :returns: True if the update was successful and affected at least one row, False otherwise.
        :rtype: bool
        """
        pass

    @abstractmethod
    def delete(self, *args: Any, **kwargs: Any) -> bool:
        """
        Abstract method to delete data from the database.
        Concrete classes must implement this method to handle specific data deletion logic.

        :param args: Positional arguments for the deletion.
        :param kwargs: Keyword arguments for the deletion.
        :returns: True if the deletion was successful and affected at least one row, False otherwise.
        :rtype: bool
        """
        pass