import os
import sys

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# Importing Assets
from assets.humidifier.src import vesync
from assets.humidifier.src.vesyncclassic300s import VeSyncHumidClassic300S

# Importing utilities package
from utilities.src.logger import LogHelper
from utilities.src.config import Config, LightConfig, MisterConfig, TempConfig
from utilities.src.database_operations import DatabaseOperations


class HumidiferControllerV2:
    def __init__(self):
        pass

    @staticmethod
    def script_path() -> str:
        '''Returns the Absolute path of the script'''
        return os.path.abspath(__file__)