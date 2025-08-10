# vivarium/database/data_loader/data_loader_strategy.py
import os
import sys
from abc import ABC, abstractmethod
from typing import Any # Added for more flexible method signatures

# Ensure the vivarium root path is in sys.path
if __name__ == "__main__":
    vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
    if vivarium_path not in sys.path:
        sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper

logger = LogHelper.get_logger(__name__)

class DataLoaderStrategy(ABC):
    """
    Abstract Base Class for all database data loading strategies.
    Defines the common interface for different data sources (e.g., database dumps, JSON files).
    """

    def __init__(self):
        """
        Initializes the abstract data loader strategy.
        """
        logger.debug(f"DataLoaderStrategy: Initialized.")

    @abstractmethod
    def execute_data_load(self, **kwargs: Any) -> bool:
        """
        Abstract method to orchestrate the full data loading process for a specific strategy.
        This method operates on the data source(s) configured during the concrete loader's initialization.
        It can also accept keyword arguments to allow for more flexible data loading scenarios
        (e.g., loading a specific file).

        Concrete implementations will define what constitutes a "full data load" for them,
        which might involve loading from a folder of JSON files, from a SQL dump, or a
        single transaction.

        :param kwargs: Arbitrary keyword arguments that concrete implementations might use
                       (e.g., 'file_path' for a single file, 'date_range', etc.).
        :type kwargs: Any
        :returns: True if all specified data loading steps are successful, False otherwise.
        :rtype: bool
        """
        pass