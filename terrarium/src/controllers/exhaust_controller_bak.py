# vivarium/terrarium/src/controllers/exhaust_controller.py

import os
import sys
import time
import signal
from typing import Any
from gpiozero import PWMOutputDevice, DigitalInputDevice

# Path Configuration
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# Project Imports
from utilities.src.logger import LogHelper
from utilities.src.config import ExhaustConfig

logger = LogHelper.get_logger(__name__)

# Constants
PULSES_PER_REVOLUTION = 2
RPM_UPDATE_INTERVAL = 1

class ExhaustController:
    """
    Controls a 4-pin PC fan via PWM and reads its RPM using gpiozero.
    Designed for a Raspberry Pi 5.
    """
    def __init__(self):
        self.exhaust_config = ExhaustConfig()
        self.fan_pwm = PWMOutputDevice(pin = self.exhaust_config.pwm_controlpin, frequency=self.exhaust_config.frequency, initial_value=0)
        # self.fan_pwm = PWMOutputDevice(pwm_pin, frequency=250, initial_value=0)
        
        # Using bounce_time to filter some noise
        self.fan_tach = DigitalInputDevice(pin = self.exhaust_config.rpm_controlpin, pull_up=True)
        
        self.rpm = 0
        self.pulse_count = 0
        self.last_update_time = time.time()
        
        self.fan_tach.when_deactivated = self._pulse_counter
        
        # Timer for RPM calculation
        signal.signal(signal.SIGALRM, self._calculate_rpm)
        signal.setitimer(signal.ITIMER_REAL, RPM_UPDATE_INTERVAL, RPM_UPDATE_INTERVAL)
        
        logger.info("ExhaustController initialized.")

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
        if not self.exhaust_config.off_speed <= speed <= self.exhaust_config.max_speed:
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
        logger.info("ExhaustController GPIO cleaned up.")

# --- Main block for debugging and testing ---
if __name__ == '__main__':
    controller = None
    try:
        controller = ExhaustController(pwm_pin=12, tach_pin=16)

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