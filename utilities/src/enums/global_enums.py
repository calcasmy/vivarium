
from enum import Enum


class ErrorCodes(Enum):
    SUCCESS = 0
    CONFIGURATION_ERROR = 1
    DATABASE_ERROR = 2
    FILE_ERROR = 3
    GENERAL_ERROR = 4