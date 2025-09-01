# vivarium/terrarium/src/controllers/Intake_controller.py

import os
import sys
import json
import time
import signal
import argparse
from typing import Optional, Any
from datetime import time, datetime

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# Importing utilities package
from utilities.src.logger import LogHelper
from utilities.src.config import IntakeConfig
from utilities.src.db_operations import DBOperations

# Import GPIO libraries for fan control and RPM reading
from gpiozero import PWMOutputDevice, DigitalInputDevice

# Initialize logger and configuration
logger = LogHelper.get_logger(__name__)
Intake_config = IntakeConfig()

# Constants
PULSES_PER_REVOLUTION = 2  # Default for many 4-pin fans. Check your fan's datasheet.
RPM_UPDATE_INTERVAL = 1    # Time in seconds to measure pulses for RPM calculation

class IntakeController:
    """
    Controls a single Intake fan via a GPIO pin using PWM.

    This class provides methods to initialize, regulate the speed of,
    and read the RPM of an Intake fan.
    """
    def __init__(self, db_operations: DBOperations):
        """
        Initializes the IntakeController object and GPIO devices.

        :param db_operations: An instance of DBOperations for database interaction.
        :type db_operations: DBOperations
        """
        self.db_operations = db_operations
        self.pwm_pin = Intake_config.pwm_pin
        self.tach_pin = Intake_config.tach_pin
        
        self.fan_pwm = None
        self.fan_tach = None
        self.rpm = 0
        self.pulse_count = 0
        self.last_update_time = time.time()
        
        self._setup_gpio()
        
        logger.info("IntakeController initialized.")

    def _setup_gpio(self) -> None:
        """Sets up the GPIO pins for PWM output and tachometer input."""
        try:
            # PWM setup
            if self.pwm_pin == -1:
                raise ValueError("PWM GPIO pin not configured in config.ini")
            self.fan_pwm = PWMOutputDevice(self.pwm_pin, frequency=25000, initial_value=0)
            
            # Tachometer setup
            if self.tach_pin == -1:
                raise ValueError("Tachometer GPIO pin not configured in config.ini")
            self.fan_tach = DigitalInputDevice(self.tach_pin, pull_up=True)
            
            self.fan_tach.when_deactivated = self._pulse_counter
            
            # Set up a timer to periodically calculate RPM
            signal.signal(signal.SIGALRM, self._calculate_rpm)
            signal.setitimer(signal.ITIMER_REAL, RPM_UPDATE_INTERVAL, RPM_UPDATE_INTERVAL)
            
            logger.info("GPIO pins initialized for Intake fan control.")
        except Exception as e:
            logger.critical(f"Failed to initialize GPIO for IntakeController: {e}")
            self.fan_pwm = None
            self.fan_tach = None
            
    def _pulse_counter(self) -> None:
        """Increments the pulse counter on each tachometer signal."""
        self.pulse_count += 1
        
    def _calculate_rpm(self, signum: Any, frame: Any) -> None:
        """Calculates RPM based on the pulse count over the update interval."""
        current_time = time.time()
        time_elapsed = current_time - self.last_update_time
        
        if time_elapsed > 0:
            self.rpm = (self.pulse_count / PULSES_PER_REVOLUTION) / (time_elapsed / 60)
        
        self.pulse_count = 0
        self.last_update_time = current_time
        
    def control_Intake(self, action: str, **kwargs: Any) -> bool:
        """
        Controls the Intake fan based on the provided action.

        :param action: The action to perform ('on', 'off', or 'regulate').
        :type action: str
        :param kwargs: Optional arguments, e.g., 'Intake_speed' for regulation.
        :type kwargs: Any
        :returns: True if the action was successful, False otherwise.
        :rtype: bool
        """
        if self.fan_pwm is None:
            logger.error("Fan control not initialized. GPIO setup failed.")
            return False

        try:
            if action == 'on':
                self.regulate_Intake(speed = Intake_config.low_speed)
                logger.info("Intake fan turned ON to default low speed.")
                return True
            
            elif action == 'off':
                self.regulate_Intake(speed=0)
                logger.info("Intake fan turned OFF.")
                return True

            elif action == 'regulate':
                speed = kwargs.get('Intake_speed')
                if speed is not None:
                    return self.regulate_Intake(speed)
                else:
                    logger.warning("Regulate action requires 'Intake_speed' parameter. Ignoring.")
                    return False

            else:
                logger.warning(f"IntakeController: Invalid action '{action}' provided. Ignoring.")
                return False

        except Exception as e:
            logger.exception(f"An unexpected error occurred while controlling the Intake fan: {e}")
            return False
    
    def regulate_Intake(self, speed: int) -> bool:
        """
        Sets the Intake fan speed using a PWM signal.

        :param speed: The desired speed as an integer from 0 (off) to 100 (full speed).
        :type speed: int
        :returns: True if the speed was set successfully, False otherwise.
        :rtype: bool
        """
        if self.fan_pwm is None:
            logger.error("Fan control not initialized. GPIO setup failed.")
            return False

        try:
            # Clamp speed between 0 and 100
            clamped_speed = max(0, min(100, speed))
            # Convert to a float between 0.0 and 1.0 for gpiozero
            pwm_value = clamped_speed / 100.0

            self.fan_pwm.value = pwm_value
            logger.info(f"Intake fan speed set to {clamped_speed}%. PWM value: {pwm_value:.2f}")
            return True
        except Exception as e:
            logger.error(f"Failed to set fan speed: {e}")
            return False
            
    def get_rpm(self) -> float:
        """
        Returns the last measured fan RPM.

        :returns: The fan speed in revolutions per minute.
        :rtype: float
        """
        return self.rpm

    def cleanup(self) -> None:
        """Cleans up GPIO resources and stops the fan."""
        if self.fan_pwm:
            self.fan_pwm.off()
            self.fan_pwm.close()
        if self.fan_tach:
            self.fan_tach.close()
        logger.info("IntakeController GPIO cleaned up.")

# --- Usage Example ---
if __name__ == '__main__':
    # This block can be used for testing the class directly.
    parser = argparse.ArgumentParser(description="Intake Controller Script")
    parser.add_argument('--action', type=str, required=True, choices=['on', 'off', 'regulate'], help="Action to perform: on, off, or regulate.")
    parser.add_argument('--speed', type=int, default=50, help="Fan speed for 'regulate' action (0-100).")

    args = parser.parse_args()

    # Mock DBOperations for the purpose of this example
    class MockDBOperations:
        def __init__(self):
            pass
    
    controller = IntakeController(db_operations=MockDBOperations())

    try:
        if args.action == 'regulate':
            controller.control_Intake(action=args.action, Intake_speed=args.speed)
        else:
            controller.control_Intake(action=args.action)

        # In a real application, you would run this in a loop or as a scheduled job.
        # Here, we'll just demonstrate getting RPM.
        print("\nPress Ctrl+C to stop.")
        while True:
            rpm = controller.get_rpm()
            print(f"Current Fan RPM: {rpm:.2f}")
            time.sleep(2)

    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        controller.cleanup()


workingcode 

# vivarium/terrarium/src/controllers/Intake_controller.py

import os
import sys
import time
import signal
import argparse
from typing import Any
from gpiozero import PWMOutputDevice, DigitalInputDevice

# Path Configuration
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# Project Imports
from utilities.src.logger import LogHelper
from utilities.src.config import IntakeConfig

logger = LogHelper.get_logger(__name__)

# Constants
PULSES_PER_REVOLUTION = 2
RPM_UPDATE_INTERVAL = 1

class IntakeController:
    """
    Controls a 4-pin PC fan via PWM and reads its RPM using gpiozero.
    Designed for a Raspberry Pi 5.
    """
    def __init__(self, pwm_pin: int = 13, tach_pin: int = 6):
        self.fan_pwm = PWMOutputDevice(pwm_pin, frequency=250, initial_value=0)
        
        # Using bounce_time to filter some noise
        self.fan_tach = DigitalInputDevice(tach_pin, pull_up=True)
        
        self.rpm = 0
        self.pulse_count = 0
        self.last_update_time = time.time()
        
        self.fan_tach.when_deactivated = self._pulse_counter
        
        # Timer for RPM calculation
        signal.signal(signal.SIGALRM, self._calculate_rpm)
        signal.setitimer(signal.ITIMER_REAL, RPM_UPDATE_INTERVAL, RPM_UPDATE_INTERVAL)
        
        logger.info("IntakeController initialized.")

    def _pulse_counter(self) -> None:
        """Increments the pulse counter on each tachometer signal."""
        self.pulse_count += 1
        
    def _calculate_rpm(self, signum: Any, frame: Any) -> None:
        """Calculates RPM based on the pulse count over the update interval."""
        current_time = time.time()
        time_elapsed = current_time - self.last_update_time
        
        if time_elapsed > 0:
            self.rpm = (self.pulse_count / PULSES_PER_REVOLUTION) / (time_elapsed / 60)
        
        self.pulse_count = 0
        self.last_update_time = current_time
        
    def set_speed(self, speed: float) -> None:
        """Sets the fan speed (0.0 to 1.0)."""
        if not 0.0 <= speed <= 1.0:
            raise ValueError("Speed must be a value between 0.0 and 1.0")
        self.fan_pwm.value = speed
        
    def get_rpm(self) -> float:
        """Returns the last measured fan RPM."""
        return self.rpm
    
    def cleanup(self) -> None:
        """
        Cleans up GPIO resources and stops the fan.
        Avoids the fan restart bug by setting value to 0 and not calling .close().
        """
        self.fan_pwm.value = 0
        self.fan_tach.close()
        logger.info("IntakeController GPIO cleaned up.")

# --- Main block for debugging and testing ---
if __name__ == '__main__':
    controller = None
    try:
        controller = IntakeController(pwm_pin=12, tach_pin=16)

        print("Starting fan at medium speed...")
        controller.set_speed(0.5)
        
        for i in range(10):
            time.sleep(2)
            current_rpm = controller.get_rpm()
            print(f"Current Fan RPM: {current_rpm:.2f}")

        print("\nSetting fan to full speed...")
        controller.set_speed(1.0)
        
        for i in range(10):
            time.sleep(2)
            current_rpm = controller.get_rpm()
            print(f"Current Fan RPM: {current_rpm:.2f}")

    except KeyboardInterrupt:
        print("\nExiting and cleaning up.")
    finally:
        if controller is not None:
            controller.cleanup()