# src/utilities/logger.py

import os
import types
import logging
from logging.handlers import RotatingFileHandler
from .config import LogConfig, FileConfig

# from src.utilities.path_utils import get_project_root
from .path_utils import PathUtils

class LogHelper:
    _logger = {}  # Use a dictionary to store loggers by name

    @staticmethod
    def get_logger(name: str = 'app', level: int = logging.INFO, log_to_file: bool = True, log_to_console: bool = True) -> logging.Logger:
        if name not in LogHelper._logger:
            log_config = LogConfig()
            file_config = FileConfig()
            logs_dir = os.path.join(PathUtils.get_project_root(), file_config.log_folder)
            os.makedirs(logs_dir, exist_ok=True)
            log_file = os.path.join(logs_dir, f"{name}.log")

            logger = logging.getLogger(name)
            logger.setLevel(level)

            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

            # File Handler with Rotation
            file_handler = RotatingFileHandler(log_file, maxBytes=log_config.max_bytes, backupCount=log_config.backup_count)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

            # Console Handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

            # Blank Line Handler (Internal use only, no need to store in logger)
            blank_handler = logging.StreamHandler()
            blank_handler.setLevel(logging.DEBUG)
            blank_handler.setFormatter(logging.Formatter(fmt=''))

            # Add the newline method to the logger instance
            def log_newline(self, how_many_lines=1):
                """Adds blank lines to the log."""
                #  Instead of removing and adding handlers, which can cause issues,
                #  we'll just log empty strings directly.  This is simpler and safer.
                for _ in range(how_many_lines):
                    self.info('')

            # Add the newline method to the logger instance
            # def log_newline(self, how_many_lines=1):
            #     """Adds blank lines to the log."""
            #     self.removeHandler(console_handler)
            #     self.addHandler(blank_handler)
            #     for _ in range(how_many_lines):
            #         self.info('')
            #     self.removeHandler(blank_handler)
            #     self.addHandler(console_handler)

            logger.newline = types.MethodType(log_newline, logger)
            LogHelper._logger[name] = logger

        return LogHelper._logger[name]

    # Risky, hence removing.
    # @staticmethod
    # def log_newline(self, how_many_lines=1):
    #     """
    #     Add blank lines to the log.
    #     :param how_many_lines: Number of blank lines to add.
    #     """
    #     # Remove the console handler temporarily
    #     self.removeHandler(self.console_handler)
    #     self.addHandler(self.blank_handler)

    #     # Add blank lines to the log
    #     for _ in range(how_many_lines):
    #         self.info('')

    #     # Switch back to console handler
    #     self.removeHandler(self.blank_handler)
    #     self.addHandler(self.console_handler)

    @staticmethod
    def log_newline(logger: logging.Logger, how_many_lines=1):
        """
        Add blank lines to the log.

        :param logger: The logger instance to add newlines to.
        :param how_many_lines: Number of blank lines to add.
        """
        for _ in range(how_many_lines):
            logger.info('')

if __name__ == '__main__':
    logger = LogHelper.get_logger('main_app')
    logger.info('Start reading database')
    logger.info('Updating records ...')
    logger.newline()
    logger.info('Finish updating records')

    another_logger = LogHelper.get_logger('data_processing')
    another_logger.warning('Something to be aware of in data processing.')