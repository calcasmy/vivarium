# vivarium/terrarium/src/controllers/fan_controller.py
import os
import sys
import time
import json
import signal
from typing import Any
from datetime import time as datetime_time, datetime
from gpiozero import PWMOutputDevice, DigitalInputDevice

# Path Configuration
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# Project Imports
from utilities.src.logger import LogHelper
from utilities.src.config import AerationConfig
from utilities.src.db_operations import DBOperations
from database.device_data_ops.device_status_queries import DeviceStatusQueries

logger = LogHelper.get_logger(__name__)

class FanController:
    """
    A reusable controller for a 4-pin PC fan via PWM and its RPM using gpiozero.
    This class can be used for any fan by providing the specific GPIO pins.
    """
    def __init__(self, pwm_pin: int, tach_pin: int, fan_id:int = 5, db_operations: DBOperations = None):
        """
        Initializes the FanController with specific pins.

        :param pwm_pin: The GPIO pin for PWM speed control.
        :type pwm_pin: int
        :param tach_pin: The GPIO pin for the tachometer (RPM sensing).
        :type tach_pin: int
        :param fan_id: The unique ID for this fan device.
        :type fan_id: int
        :param db_operations: The shared database operations instance.
        :type db_operations: DBOperations
        """
        self.config = AerationConfig()
        self.fan_id = fan_id
        self.db_operations = db_operations
        self.device_status_queries = DeviceStatusQueries(self.db_operations)
        self.fan_pwm = PWMOutputDevice(pin=pwm_pin, frequency=self.config.frequency, initial_value=0)
        
        self.fan_tach = DigitalInputDevice(pin=tach_pin, pull_up=True)
        
        self.rpm = 0
        self.pulse_count = 0
        self.fan_speed_change_delay = 2
        # self.last_update_time = time.time()
        
        self.fan_tach.when_deactivated = self._pulse_counter
        
        # Timer for RPM calculation
        # signal.signal(signal.SIGALRM, self._calculate_rpm)
        # signal.setitimer(signal.ITIMER_REAL, RPM_UPDATE_INTERVAL, RPM_UPDATE_INTERVAL)
        
        logger.info(f"FanController initialized for PWM pin {pwm_pin} and Tach pin {tach_pin}.")

    def _pulse_counter(self) -> None:
        """Increments the pulse counter on each tachometer signal."""
        self.pulse_count += 1
        
    def _calculate_rpm(self) -> float: #, signum: Any, frame: Any) -> None:
        """Calculates RPM based on the pulse count over the update interval."""
        # current_time = time.time()
        # time_elapsed = current_time - self.last_update_time
        
        # if time_elapsed > 0:
        #     self.rpm = (self.pulse_count / PULSES_PER_REVOLUTION) / (time_elapsed / 60)
        
        # self.pulse_count = 0
        # self.last_update_time = current_time

        self.pulse_count = 0  # Reset counter
        time.sleep(1)         # Wait for one second to count pulses
        
        return (self.pulse_count / self.config.pulses_per_revolution) * 60
        
    def set_speed(self, speed: float) -> None:
        """
        Sets the fan speed (0.0 to 1.0) and updates the database with the
        new state and measured RPM after a short delay.
        """
        if not self.config.off_speed <= speed <= self.config.max_speed:
            raise ValueError("Speed must be a value between 0.0 and 1.0")
        
        self.fan_pwm.value = speed
        
        time.sleep(self.fan_speed_change_delay)
        
        current_rpm = self._calculate_rpm()
        self.rpm = current_rpm

        self._update_status(speed, current_rpm)
        
    def get_rpm(self) -> float:
        """Returns the last measured fan RPM."""
        return self.rpm
    
    def cleanup(self) -> None:
        """
        Cleans up GPIO resources and stops the fan.
        """
        self.fan_pwm.value = 0
        self.fan_tach.close()
        logger.info("FanController GPIO cleaned up.")
    
    def _update_status(self, speed: float, rpm: float) -> None:
        """
        Updates the fan's state in the database.
        """
        is_on = speed > 0
        raw_data = {
            "speed": speed,
            "rpm": rpm,
            "is_on": is_on
        }

        try:
            self.db_operations.begin_transaction()
            self.device_status_queries.insert_device_status(
                device_id = self.fan_id,
                timestamp = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),
                is_on = is_on,
                device_data = json.dumps(raw_data)
            )
            self.db_operations.commit_transaction()
            logger.info(f"Updating fan status for ID {self.fan_id}. Speed: {speed}, RPM: {rpm}")
        except Exception as e:
            self.db_operations.rollback_transaction()
            error_message = f"Failed to update status for {self.fan_id} : {e}"
            logger.error(error_message)
            raise