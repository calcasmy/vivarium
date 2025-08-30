# vivarium/terrarium/src/controllers/exhaust_controller.py

import os
import sys
import argparse
from datetime import time
from typing import Optional

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# Importing utilities package
from utilities.src.logger import LogHelper
from utilities.src.config import ExhaustConfig, DatabaseConfig
from utilities.src.db_operations import DBOperations, ConnectionDetails

# Super class
from terrarium.src.controllers.base_device_controller import BaseDeviceController

# Initialize logger and configuration
logger = LogHelper.get_logger(__name__)
exhaust_config = ExhaustConfig()

class ExhaustController(BaseDeviceController):
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
        self.device_id = exhaust_config.device_id
        self.fan_pin = exhaust_config.gpio_pin
        self.consumer_name = 'exhaust_control'

        super().__init__(
            device_id=self.device_id,
            relay_pin=self.fan_pin,
            consumer_name=self.consumer_name,
            db_operations=db_operations
        )
        logger.info("ExhaustController initialized.")

    def control_exhaust(self, action: str, **kwargs):
        try:
            if action == 'on':
                self.toggle_device(action)

            elif action == 'off':
                self.toggle_device(action)

            elif action == 'regulate':
                if 'exhaust_speed' in kwargs and kwargs['exhaust_speed'] is not None:
                    self.regulate_exhaust(speed = kwargs['exhaust_speed'])
                else:
                    self.regulate_exhaust(speed=exhaust_config.low_speed)

            else:
                logger.warning(f"ExhaustController: Invalid action '{action}' provided. Ignoring.")

        except Exception as e:
            logger.exception(f"An unexpected error occurred while controlling the exhaust fan: {e}")
    
    def regulate_exhaust(self, speed_level: int) -> bool:
        """
        Sets the exhaust fan speed using a PWM signal.

        :param speed_level: The desired speed (e.g., an integer from 0 to 100).
        :type speed_level: int
        :returns: True if the speed was set successfully, False otherwise.
        """
        if self.relay_pin == -1:
            logger.error("GPIO pin not configured for fan speed control.")
            return False

        # Placeholder for actual PWM control logic
        try:
            # Code to set the PWM duty cycle for the GPIO pin
            # For example, using a library like pigpio:
            # pigpio.pi.set_PWM_dutycycle(self.relay_pin, speed_level)
            logger.info(f"Exhaust fan speed set to {speed_level}.")
            return True
        except Exception as e:
            logger.error(f"Failed to set fan speed: {e}")
            return False