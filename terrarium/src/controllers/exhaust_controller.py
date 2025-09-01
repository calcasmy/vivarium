# vivarium/terrarium/src/controllers/exhaust_controller.py

import os
import sys
import time
import argparse
import signal
from datetime import time as dttime
from typing import Optional
from gpiozero import PWMOutputDevice, DigitalInputDevice

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# Importing utilities package
from utilities.src.logger import LogHelper
from utilities.src.config import coreConfig, ExhaustConfig, DatabaseConfig
from utilities.src.db_operations import DBOperations, ConnectionDetails

# Super class
from terrarium.src.controllers.base_device_controller import BaseDeviceController

#Global Constants
PULSES_PER_REVOLUTION = 2  # Number of pulses per revolution for the fan's tachometer
RPM_UPDATE_INTERVAL = 1  # Interval in seconds to update RPM readings

class ExhaustController:
    """
    A class to control a 4-pin PC fan via PWM and read its RPM.

    The fan is controlled by a PWM signal on GPIO12.
    Its RPM is measured by counting pulses on GPIO16.
    """
    def __init__(self, pwm_pin: int = 12, tach_pin: int = 16):
        """
        Initializes the ExhaustController with the specified GPIO pins.

        :param pwm_pin: The GPIO pin for PWM speed control.
        :type pwm_pin: int
        :param tach_pin: The GPIO pin for the fan's tachometer (RPM).
        :type tach_pin: int
        """
        self.fan_pwm = PWMOutputDevice(pwm_pin, frequency=250, active_high=True, initial_value=0)
        self.fan_tach = DigitalInputDevice(tach_pin, pull_up=True)
        
        self.rpm = 0
        self.pulse_count = 0
        self.last_update_time = time.time()
        
        # Attach the pulse counter to the falling edge of the tachometer signal
        self.fan_tach.when_deactivated = self._pulse_counter
        
        # Set up a timer to periodically calculate RPM
        signal.signal(signal.SIGALRM, self._calculate_rpm)
        signal.setitimer(signal.ITIMER_REAL, RPM_UPDATE_INTERVAL, RPM_UPDATE_INTERVAL)
    
    def get_pi_model() -> str:
        """
        Reads the Raspberry Pi model from the device tree.

        :returns: A string identifying the Pi model, or 'unknown' if not on a Pi.
        """
        try:
            with open('/proc/device-tree/model', 'r') as f:
                model = f.read().strip('\x00')
                return model
        except FileNotFoundError:
            return 'unknown'

        # Get the model and print it
        pi_model = get_pi_model()
        print(f"Detected Raspberry Pi model: {pi_model}")

        # --- Library-specific code based on the model ---

        # Raspberry Pi 5 code block
        if "Raspberry Pi 5" in pi_model:
            print("This is a Raspberry Pi 5. Using the gpiod library for control.")
            
            # Imports for Pi 5
            try:
                import gpiod
                from gpiozero import PWMOutputDevice
                print("gpiod and gpiozero are ready.")
                
                # Place your Pi 5-specific code here, e.g., using gpiod and PWMOutputDevice
                # fan_pwm = PWMOutputDevice(...)
                # chip = gpiod.Chip(...)
                # ... your code ...
                
            except ImportError:
                print("gpiod or gpiozero not installed. Please run 'sudo apt-get install python3-gpiod' or 'pip install gpiozero'")
                sys.exit(1)

        # Raspberry Pi Zero code block
        elif "Raspberry Pi Zero" in pi_model:
            print("This is a Raspberry Pi Zero. Using the RPi.GPIO library for control.")

            # Imports for Pi Zero
            try:
                import RPi.GPIO as GPIO
                print("RPi.GPIO is ready.")
                
                # Place your Pi Zero-specific code here, e.g., using RPi.GPIO
                # GPIO.setmode(GPIO.BCM)
                # GPIO.setup(...)
                # ... your code ...
                
            except ImportError:
                print("RPi.GPIO not installed. Please run 'sudo apt-get install python3-rpi.gpio'")
                sys.exit(1)

        # Handle other or unknown models
        else:
            print("Running on an unsupported or unknown device. Exiting.")
            sys.exit(1)

            def _pulse_counter(self) -> None:
                """Increments the pulse counter on each tachometer signal."""
                self.pulse_count += 1
                
            def _calculate_rpm(self, signum, frame) -> None:
                """Calculates RPM based on the pulse count over the update interval."""
                current_time = time.time()
                time_elapsed = current_time - self.last_update_time
                
                if time_elapsed > 0:
                    # RPM = (pulses / pulses_per_rev) / (time_elapsed / 60 seconds)
                    self.rpm = (self.pulse_count / PULSES_PER_REVOLUTION) / (time_elapsed / 60)
                
                self.pulse_count = 0
                self.last_update_time = current_time

    def set_speed(self, speed: float) -> None:
        """
        Sets the fan speed.

        :param speed: The desired speed as a float between 0.0 (off) and 1.0 (full speed).
        :type speed: float
        """
        if not 0.0 <= speed <= 1.0:
            raise ValueError("Speed must be a value between 0.0 and 1.0")
        self.fan_pwm.value = speed
        
    def get_rpm(self) -> float:
        """Returns the last measured fan RPM."""
        return self.rpm
    
    def cleanup(self) -> None:
        """Cleans up GPIO resources and stops the fan."""
        self.fan_pwm.value = 0
        self.fan_pwm.off()
        self.fan_pwm.close()
        self.fan_tach.close()
        
# --- Usage Example ---
if __name__ == '__main__':
    controller = None
    try:
        controller = ExhaustController()
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


# class ExhaustController(BaseDeviceController):
#     """
#     Controls a single exhaust fan via a GPIO pin.

#     This class provides methods to initialize and toggle the state of an
#     exhaust fan connected to a Raspberry Pi's GPIO pin.
#     """
#     def __init__(self, db_operations: DBOperations):
#         """
#         Initializes the ExhaustFanController object.

#         :param db_operations: An instance of DBOperations for database interaction.
#         :type db_operations: DBOperations
#         """
#         self.device_id = exhaust_config.device_id
#         self.fan_pin = exhaust_config.gpio_pin
#         self.consumer_name = 'exhaust_control'

#         super().__init__(
#             device_id=self.device_id,
#             relay_pin=self.fan_pin,
#             consumer_name=self.consumer_name,
#             db_operations=db_operations
#         )
#         logger.info("ExhaustController initialized.")

#     def control_exhaust(self, action: str, **kwargs):
#         try:
#             if action == 'on':
#                 self.toggle_device(action)

#             elif action == 'off':
#                 self.toggle_device(action)

#             elif action == 'regulate':
#                 if 'exhaust_speed' in kwargs and kwargs['exhaust_speed'] is not None:
#                     self.regulate_exhaust(speed = kwargs['exhaust_speed'])
#                 else:
#                     self.regulate_exhaust(speed=exhaust_config.low_speed)

#             else:
#                 logger.warning(f"ExhaustController: Invalid action '{action}' provided. Ignoring.")

#         except Exception as e:
#             logger.exception(f"An unexpected error occurred while controlling the exhaust fan: {e}")
    
#     def regulate_exhaust(self, speed_level: int) -> bool:
#         """
#         Sets the exhaust fan speed using a PWM signal.

#         :param speed_level: The desired speed (e.g., an integer from 0 to 100).
#         :type speed_level: int
#         :returns: True if the speed was set successfully, False otherwise.
#         """
#         if self.relay_pin == -1:
#             logger.error("GPIO pin not configured for fan speed control.")
#             return False

#         # Placeholder for actual PWM control logic
#         try:
#             # Code to set the PWM duty cycle for the GPIO pin
#             # For example, using a library like pigpio:
#             # pigpio.pi.set_PWM_dutycycle(self.relay_pin, speed_level)
#             logger.info(f"Exhaust fan speed set to {speed_level}.")
#             return True
#         except Exception as e:
#             logger.error(f"Failed to set fan speed: {e}")
#             return False