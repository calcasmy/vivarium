# vivarium/terrarium/src/controllers/exhaust_controller.py

import os
import sys
import time
import argparse
import signal
import RPi.GPIO as GPIO # Import the new library
from datetime import time as dttime
from typing import Optional
from gpiozero import PWMOutputDevice

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# Importing utilities package
from utilities.src.logger import LogHelper
from utilities.src.config import ExhaustConfig
from utilities.src.db_operations import DBOperations

logger = LogHelper.get_logger(__name__)

# Global Constants
PULSES_PER_REVOLUTION = 2
RPM_UPDATE_INTERVAL = 5

class ExhaustController:
    """
    A class to control a 4-pin PC fan via PWM and read its RPM.
    """
    def __init__(self, pwm_pin: int = 12, tach_pin: int = 16):
        self.pwm_pin = pwm_pin
        self.tach_pin = tach_pin
        
        self.fan_pwm = PWMOutputDevice(self.pwm_pin, frequency=250, initial_value=0)
        
        self.rpm = 0
        self.pulse_count = 0
        self.last_update_time = time.time()
        
        # --- RPi.GPIO setup ---
        GPIO.setmode(GPIO.BCM)
        GPIO.setup(self.tach_pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
        GPIO.add_event_detect(self.tach_pin, GPIO.FALLING, callback=self._pulse_counter, bouncetime=10)
        
        # Set up a timer to periodically calculate RPM
        signal.signal(signal.SIGALRM, self._calculate_rpm)
        signal.setitimer(signal.ITIMER_REAL, RPM_UPDATE_INTERVAL, RPM_UPDATE_INTERVAL)
        
        logger.info("ExhaustController initialized.")
        
    def _pulse_counter(self, channel: int) -> None:
        """Increments the pulse counter on each tachometer signal."""
        self.pulse_count += 1
        
    def _calculate_rpm(self, signum, frame) -> None:
        """Calculates RPM based on the pulse count over the update interval."""
        current_time = time.time()
        time_elapsed = current_time - self.last_update_time
        
        if time_elapsed > 0:
            self.rpm = (self.pulse_count / PULSES_PER_REVOLUTION) / (time_elapsed / 60)
        
        self.pulse_count = 0
        self.last_update_time = current_time

    def set_speed(self, speed: float) -> None:
        if not 0.0 <= speed <= 1.0:
            raise ValueError("Speed must be a value between 0.0 and 1.0")
        self.fan_pwm.value = speed
        
    def get_rpm(self) -> float:
        return self.rpm
    
    def cleanup(self) -> None:
        self.fan_pwm.off()
        self.fan_pwm.close()
        GPIO.cleanup(self.tach_pin)
        
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