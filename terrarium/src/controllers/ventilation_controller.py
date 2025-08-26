import os
import sys
from typing import Optional

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# Importing utilities package
from utilities.src.config import ExhaustFanConfig
from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations

# Super class
from terrarium.src.controllers.base_device_controller import BaseDeviceController

# Initialize logger and configuration
logger = LogHelper.get_logger(__name__)
exhaust_fan_config = ExhaustFanConfig()

class VentilationController(BaseDeviceController):
    """
    Controls a single exhaust fan via a GPIO pin.

    This class provides methods to initialize and toggle the state of an
    exhaust fan connected to a Raspberry Pi's GPIO pin.
    """
    def __init__(self, db_operations: DBOperations):
        """
        Initializes the ExhaustFanController object.

        :param db_operations: An instance of DBOperations for database interaction.
        :type db_operations: DBOperations
        """
        self.device_id = exhaust_fan_config.device_id
        self.fan_pin = exhaust_fan_config.gpio_pin
        self.consumer_name = 'exhaust_fan_control'

        super().__init__(
            device_id=self.device_id,
            relay_pin=self.fan_pin,
            consumer_name=self.consumer_name,
            db_operations=db_operations
        )
        logger.info("ExhaustFanController initialized.")