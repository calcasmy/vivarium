# vivarium/terrarium/src/controllers/aeration_controller.py
import os
import sys
import time
import signal
import json
from typing import Any
from gpiozero import PWMOutputDevice, DigitalInputDevice

# Path Configuration
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# Project Imports
from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations, ConnectionDetails
from utilities.src.config import AerationConfig
from terrarium.src.controllers.fan_controller import FanController
from database.device_data_ops.device_status_queries import DeviceStatusQueries

logger = LogHelper.get_logger(__name__)

class AerationController:
    """
    Manages both the intake and exhaust fans for the vivarium's aeration system.
    This class now acts as a high-level controller using two instances of the
    generic FanController.
    """
    def __init__(self, db_operations: DBOperations = None):
        """
        Initializes the aeration system by creating separate fan controllers
        for intake and exhaust, pulling pin configurations from AerationConfig.
        """
        self.config = AerationConfig()
        self.db_operations = db_operations
        self.device_status_queries = DeviceStatusQueries(db_operations)
        self.intake_fan = FanController(pwm_pin=self.config.intake_pwm_pin, tach_pin=self.config.intake_tach_pin, fan_id = self.config.intake_device_id, aeration_controller=self)
        self.exhaust_fan = FanController(pwm_pin=self.config.exhaust_pwm_pin, tach_pin=self.config.exhaust_tach_pin, fan_id= self.config.exhaust_device_id, aeration_controller=self)
        logger.info("AerationController initialized.")

    def set_intake_speed(self, speed: float) -> None:
        """Sets the intake fan speed (0.0 to 1.0)."""
        self.intake_fan.set_speed(speed)

    def set_exhaust_speed(self, speed: float) -> None:
        """Sets the exhaust fan speed (0.0 to 1.0)."""
        self.exhaust_fan.set_speed(speed)
    
    def set_fans_to_default_speed(self) -> None:
        """Sets both fans to the default speed."""
        logger.info(f"Setting both fans to default speed: {self.config.low_speed}")
        self.set_intake_speed(self.config.low_speed)
        self.set_exhaust_speed(self.config.low_speed)

    def set_fans_to_max_speed(self) -> None:
        """Sets both fans to the max speed."""
        logger.info(f"Setting both fans to max speed: {self.config.max_speed}")
        self.set_intake_speed(self.config.max_speed)
        self.set_exhaust_speed(self.config.max_speed)

    def update_fan_status(self, fan_id: int, speed: float, rpm: float) -> None:
        """
        Updates the fan's state in the database.
        This method is called by the FanController after a speed change.
        """
        is_on = speed > 0
        raw_data = {
            "speed": speed,
            "rpm": rpm,
            "is_on": is_on
        }

        logger.info(f"Updating fan status for ID {fan_id}. Speed: {speed}, RPM: {rpm}")
        self.device_status_queries.insert_device_status(
            device_id=fan_id,
            is_on=is_on,
            raw_data=json.dumps(raw_data)
        )
        self.device_status_queries.update_device_status(
            device_id=fan_id,
            is_on=is_on,
            raw_data=json.dumps(raw_data)
        )

    def get_intake_rpm(self) -> float:
        """Returns the last measured intake fan RPM."""
        return self.intake_fan.get_rpm()

    def get_exhaust_rpm(self) -> float:
        """Returns the last measured exhaust fan RPM."""
        return self.exhaust_fan.get_rpm()
    
    def cleanup(self) -> None:
        """
        Cleans up GPIO resources for both fans.
        """
        self.intake_fan.cleanup()
        self.exhaust_fan.cleanup()
        logger.info("AerationController GPIO cleaned up.")

    # --- Main block for debugging and testing ---
if __name__ == '__main__':
    controller = None
    try:
        controller = AerationController()

        print("Starting fans at medium speed...")
        controller.set_intake_speed(0.5)
        controller.set_exhaust_speed(0.5)
        
        for i in range(5):
            time.sleep(2)
            intake_rpm = controller.get_intake_rpm()
            exhaust_rpm = controller.get_exhaust_rpm()
            print(f"Intake Fan RPM: {intake_rpm:.2f} | Exhaust Fan RPM: {exhaust_rpm:.2f}")

        print("\nSetting fans to full speed...")
        controller.set_intake_speed(1.0)
        controller.set_exhaust_speed(1.0)
        
        for i in range(5):
            time.sleep(2)
            intake_rpm = controller.get_intake_rpm()
            exhaust_rpm = controller.get_exhaust_rpm()
            print(f"Intake Fan RPM: {intake_rpm:.2f} | Exhaust Fan RPM: {exhaust_rpm:.2f}")

    except KeyboardInterrupt:
        print("\nExiting and cleaning up.")
    finally:
        if controller is not None:
            controller.cleanup()