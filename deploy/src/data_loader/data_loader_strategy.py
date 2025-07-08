# vivarium/deploy/src/data_loader/data_loader_strategy.py
import os
import sys
from abc import ABC, abstractmethod
from typing import Optional

# Ensure the vivarium root path is in sys.path
# Three levels up to vivarium root: database/ -> src/ -> deploy/ -> vivarium/
if __name__ == "__main__":
    vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
    if vivarium_path not in sys.path:
        sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper

logger = LogHelper.get_logger(__name__)

class DataLoaderStrategy(ABC):
    """
    Abstract Base Class for all database data loading strategies.
    Defines the common interface for different data sources (e.g., database dumps, raw files).
    Concrete implementations are expected to be initialized with any necessary paths
    or configurations.
    """

    def __init__(self):
        """
        Initializes the abstract data loader strategy.
        """
        logger.debug(f"DataLoaderStrategy: Initialized.")

    @abstractmethod
    def load_from_dump(self) -> bool:
        """
        Abstract method to load data into the database from a database dump file.
        The path to the dump file should be configured during the concrete loader's initialization.

        Concrete implementations should handle the specifics of connecting to the
        database and executing the dump restoration (e.g., using psql, pg_restore,
        or database-specific client tools).

        :returns: True if the data is successfully loaded from the dump, False otherwise.
        :rtype: bool
        """
        pass

    @abstractmethod
    def load_json_data(self) -> bool:
        """
        Abstract method to load raw climate data from local files into the database.
        The path to the JSON files/folder should be configured during the concrete loader's initialization.

        Concrete implementations should integrate with existing data parsing/loading
        modules (e.g., `weather.rawclimate_dataloader`) and handle any database
        interaction required for the ingestion of this data.

        :returns: True if the raw climate data is successfully loaded, False otherwise.
        :rtype: bool
        """
        pass

    @abstractmethod
    def execute_full_data_load(self) -> bool:
        """
        Abstract method to orchestrate the full data loading process.
        This method operates on the data source(s) configured during the concrete loader's initialization.

        :returns: True if all specified data loading steps are successful, False otherwise.
        :rtype: bool
        """
        pass