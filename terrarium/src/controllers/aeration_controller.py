# vivarium/terrarium/src/controllers/aeration_controller.py
import os
import sys
import time
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

        self.intake_fan = FanController(
            pwm_pin=self.config.intake_pwm_pin, 
            tach_pin=self.config.intake_tach_pin, 
            fan_id = self.config.intake_device_id, 
            db_operations=self.db_operations)
        self.exhaust_fan = FanController(
            pwm_pin=self.config.exhaust_pwm_pin, 
            tach_pin=self.config.exhaust_tach_pin, 
            fan_id= self.config.exhaust_device_id, 
            db_operations=self.db_operations)
        
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

    # def update_fan_status(self, fan_id: int, speed: float, rpm: float) -> None:
    #     """
    #     Updates the fan's state in the database.
    #     This method is called by the FanController after a speed change.
    #     """
    #     is_on = speed > 0
    #     raw_data = {
    #         "speed": speed,
    #         "rpm": rpm,
    #         "is_on": is_on
    #     }

    #     logger.info(f"Updating fan status for ID {fan_id}. Speed: {speed}, RPM: {rpm}")
    #     self.device_status_queries.insert_device_status(
    #         device_id=fan_id,
    #         is_on=is_on,
    #         raw_data=json.dumps(raw_data)
    #     )

    # def get_intake_rpm(self) -> float:
    #     """Returns the last measured intake fan RPM."""
    #     return self.intake_fan.get_rpm()

    # def get_exhaust_rpm(self) -> float:
    #     """Returns the last measured exhaust fan RPM."""
    #     return self.exhaust_fan.get_rpm()
    
    def cleanup(self) -> None:
        """
        Cleans up GPIO resources for both fans.
        """
        self.intake_fan.cleanup()
        self.exhaust_fan.cleanup()
        logger.info("AerationController GPIO cleaned up.")

# --- Main block for debugging and testing ---
if __name__ == '__main__':
    from utilities.src.db_operations import ConnectionDetails, DBOperations
    from utilities.src.config import DatabaseConfig

    db_config = DatabaseConfig()
    db_operations = DBOperations()
    
    try:
        db_operations.connect(ConnectionDetails(
            host=db_config.postgres_local_connection.host,
            port=db_config.postgres_local_connection.port,
            user=db_config.postgres_local_connection.user,
            password=db_config.postgres_local_connection.password,
            dbname=db_config.postgres_local_connection.dbname
        ))

        controller = AerationController(db_operations=db_operations)
        print("Starting fans at default speed...")
        controller.set_fans_to_default_speed()
        
        time.sleep(5)

        print("\nSetting fans to full speed...")
        controller.set_fans_to_max_speed()
        
        time.sleep(5)
        
        print("\nSetting fans to default speed...")
        controller.set_fans_to_default_speed()

    except KeyboardInterrupt:
        print("\nExiting and cleaning up.")
    finally:
        if controller is not None:
            controller.cleanup()
        if db_operations:
            db_operations.close()