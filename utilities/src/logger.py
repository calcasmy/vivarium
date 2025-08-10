# vivarium/utilities/src/logger.py

import os
import types
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from .path_utils import PathUtils


class LogHelper:
    """
    A utility class for managing logging within the Vivarium project.

    This class provides a centralized way to obtain logger instances,
    ensuring consistent logging configuration across the application.
    It automatically initializes the root logger and sets up file rotation
    and console logging with sensible defaults.
    """

    _loggers = {}
    _initialized = False

    #: Default name for the log folder relative to the project root.
    _DEFAULT_LOG_FOLDER_NAME = "logs"
    #: Default logging level.
    _DEFAULT_LOG_LEVEL = logging.INFO
    #: Default to enable console logging.
    _DEFAULT_CONSOLE_LOG_ENABLED = True
    #: Default maximum size of a log file before rotation (5 MB).
    _DEFAULT_MAX_BYTES = 5 * 1024 * 1024
    #: Default number of backup log files to keep.
    _DEFAULT_BACKUP_COUNT = 5

    @staticmethod
    def _ensure_log_directory_exists(log_file_path: Path) -> None:
        """
        Ensures that the parent directory for the given log file path exists.

        :param log_file_path: The full path to the log file.
        :type log_file_path: Path
        """
        log_file_path.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def get_logger(name: str = 'app') -> logging.Logger:
        """
        Returns a logger instance for a given name.

        If the logging system has not been initialized yet, this method will
        perform a default initialization for the root logger, including
        setting up console and file handlers with rotation. Subsequent calls
        will return the requested named logger which inherits from the
        initialized root logger.

        :param name: The name of the logger to retrieve. Defaults to 'app'.
        :type name: str
        :returns: A configured logging.Logger instance.
        :rtype: logging.Logger
        """
        if not LogHelper._initialized:
            root_logger = logging.getLogger()
            root_logger.setLevel(LogHelper._DEFAULT_LOG_LEVEL)

            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)

            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

            if LogHelper._DEFAULT_CONSOLE_LOG_ENABLED:
                console_handler = logging.StreamHandler()
                console_handler.setFormatter(formatter)
                root_logger.addHandler(console_handler)

            try:
                project_root = PathUtils.get_project_root()
                logs_dir = project_root / LogHelper._DEFAULT_LOG_FOLDER_NAME
                log_file_path = logs_dir / f"{name}.log"

                LogHelper._ensure_log_directory_exists(log_file_path)

                file_handler = RotatingFileHandler(
                    log_file_path,
                    maxBytes=LogHelper._DEFAULT_MAX_BYTES,
                    backupCount=LogHelper._DEFAULT_BACKUP_COUNT
                )
                file_handler.setFormatter(formatter)
                root_logger.addHandler(file_handler)
                root_logger.info(f"Logging initialized to file: {log_file_path}")

            except Exception as e:
                logging.basicConfig(level=LogHelper._DEFAULT_LOG_LEVEL, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                root_logger.error(f"Failed to set up file logging. Falling back to console only. Error: {e}")
            
            LogHelper._initialized = True
            root_logger.info(f"Logging system initialized. Level: {logging.getLevelName(LogHelper._DEFAULT_LOG_LEVEL)}. Console: {LogHelper._DEFAULT_CONSOLE_LOG_ENABLED}")

        if name not in LogHelper._loggers:
            logger = logging.getLogger(name)
            
            if not hasattr(logger, 'newline'):
                def log_newline_method(self, how_many_lines=1):
                    for _ in range(how_many_lines):
                        self.info('')
                logger.newline = types.MethodType(log_newline_method, logger)
            
            LogHelper._loggers[name] = logger
        return LogHelper._loggers[name]


if __name__ == '__main__':
    # Example usage and basic testing of the logger.
    # This block will create 'logs/main_app_test.log' and 'logs/data_processing_test.log'
    # in the project root.
    
    test_logger = LogHelper.get_logger('main_app_test')
    test_logger.info('Starting test sequence.')
    test_logger.debug('This is a debug message (may not be shown based on default level).')
    test_logger.warning('A warning occurred during test.')
    test_logger.newline()
    test_logger.info('Test sequence complete.')

    another_test_logger = LogHelper.get_logger('data_processing_test')
    another_test_logger.info('Data processing started.')
    try:
        raise ValueError("Simulated error in data processing.")
    except ValueError as e:
        another_test_logger.error(f"Error: {e}", exc_info=True)
    another_test_logger.info('Data processing finished.')

    # Verification of log files (optional, for development/testing)
    project_root_for_test = PathUtils.get_project_root()
    log_dir_for_test = project_root_for_test / LogHelper._DEFAULT_LOG_FOLDER_NAME
    main_log_file_for_test = log_dir_for_test / "main_app_test.log"
    data_log_file_for_test = log_dir_for_test / "data_processing_test.log"

    print(f"\n--- Logger Test Summary ---")
    print(f"Log directory: {log_dir_for_test}")
    print(f"Main app log exists: {main_log_file_for_test.exists()}")
    print(f"Data processing log exists: {data_log_file_for_test.exists()}")

    # Clean up test log files
    if main_log_file_for_test.exists():
        print(f"Deleting test log: {main_log_file_for_test}")
        main_log_file_for_test.unlink()
    if data_log_file_for_test.exists():
        print(f"Deleting test log: {data_log_file_for_test}")
        data_log_file_for_test.unlink()
    
    # Optionally, remove the 'logs' directory if it's empty after tests
    try:
        if log_dir_for_test.exists() and not list(log_dir_for_test.iterdir()):
            print(f"Deleting empty log directory: {log_dir_for_test}")
            log_dir_for_test.rmdir()
    except OSError as e:
        print(f"Could not remove log directory {log_dir_for_test}: {e}")