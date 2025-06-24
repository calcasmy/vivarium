# vivarium/deploy/src/database/data_loader_strategy.py
import os
import sys
from abc import ABC, abstractmethod
from typing import Optional

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
    Defines the common interface for different data sources (e.g., database dumps, raw files).
    """

    def __init__(self):
        """
        Initializes the abstract data loader strategy.
        """
        logger.debug(f"DataLoaderStrategy: Initialized.")

    @abstractmethod
    def load_from_dump(self, dump_file_path: str) -> bool:
        """
        Abstract method to load data into the database from a database dump file.

        Concrete implementations should handle the specifics of connecting to the
        database and executing the dump restoration (e.g., using psql, pg_restore,
        or database-specific client tools).

        :param dump_file_path: The absolute path to the database dump file.
        :type dump_file_path: str
        :returns: True if the data is successfully loaded from the dump, False otherwise.
        :rtype: bool
        """
        pass

    @abstractmethod
    def load_raw_climate_data(self) -> bool:
        """
        Abstract method to load raw climate data from local files into the database.

        Concrete implementations should integrate with existing data parsing/loading
        modules (e.g., `weather.rawclimate_dataloader`) and handle any database
        interaction required for the ingestion of this data.

        :returns: True if the raw climate data is successfully loaded, False otherwise.
        :rtype: bool
        """
        pass

    @abstractmethod
    def execute_full_data_load(self, dump_file_path: Optional[str] = None) -> bool:
        """
        Abstract method to orchestrate the full data loading process.

        This method should coordinate calls to `load_from_dump` (if `dump_file_path` is provided)
        and `load_raw_climate_data`.

        :param dump_file_path: Optional absolute path to a database dump file. If provided,
                               data will be loaded from this dump.
        :type dump_file_path: Optional[str]
        :returns: True if all specified data loading steps are successful, False otherwise.
        :rtype: bool
        """
        pass