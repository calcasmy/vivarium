""" 
#--------------------------------------------------------------------------------#
# -*- vivarium.py -*-
# Author        : Adithya
# Date          : 03/13/2025
# Description   : The purpose of this program is to control mister motor to run
#                automatically when the humidity in the vivarium falls below a
#                a certain threshold.
# Modififed on  : 03/15/2025
# Modification  : Added logic to restric motor from running frequently with in a
#               specified timeframe defined in ConfigFile.
# Equipment     : 
#                1. Raspberry Pi replay controller
#                2. MistKing mister
#				 
#--------------------------------------------------------------------------------#
"""

import os
import sys
import time 
import gpiod

from pytz import timezone
from datetime import datetime, timezone

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

# Add 'vivarium' to the Python path if it's not already there
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# from vivarium_dao import VivariumDAO
from utilities.src.logger import LogHelper
from utilities.src.config import MisterConfig
from utilities.src.database_operations import DatabaseOperations

from terrarium.src.database.device_status_queries import DeviceStatusQueries
from terrarium.src.database.device_queries import DeviceQueries

# import RPi.GPIO as GPIO
# import time
# import argparse

# from vivarium_dao import VivariumDAO
# from propertyreader import TIMECONFIG, GPIOCONFIG
# from logger_helper import LogHelper
# from datetime import datetime, timezone

# from src.terrarium.humidifier_control import ControlHumidifier

# logger = LogHelper.get_logger('vivarium')

logger = LogHelper.get_logger(__name__)
mister_config = MisterConfig()
# gpio_config = GPIOConfig()

class MisterController:
    def __init__(self,equipment_id = 2, db_operations: DatabaseOperations = None):
        self.relay_pin = int(mister_config.mister_control_pin)
        self.equipment_id = equipment_id
        self.chip = None
        self.line = None
        self._setup_gpio()
        self._devicequeries = DeviceQueries(db_operations = self.db_ops)
        self._devicestatus = DeviceStatusQueries(db_operations= self.db_ops)

    def __del__(self):
        """
        Ensures the GPIO line is released when the object is destroyed.
        """
        if self.line:
            try:
                self.line.release()
                logger.info(f"GPIO line {self.relay_pin} released.")
            except Exception as e:  # Catch all exceptions, including the 'AttributeError' from _update_status
                logger.error(f'An error occurred while controlling the mister or updating database: {e}')

        if self.chip:
            try:
                self.chip.close()
                logger.info(f"GPIO chip closed.")
            except Exception as e:
                logger.error(f"Error closing GPIO chip: {e}")

    @staticmethod
    def script_path() -> str:
        '''Returns the Absolute path of the script'''
        return os.path.abspath(__file__)
    
    def _setup_gpio(self):  # Private method
        """Sets up the GPIO pin for controlling the mister."""

        try:
            # Open the GPIO chip, typically 'gpiochip0' on Raspberry Pi and similar boards
            self.chip = gpiod.Chip('gpiochip0')
            self.line = self.chip.get_line(self.relay_pin)

            # Request the line as output
            # Pass consumer='mister_control' for better debugging with gpiodetect/gpioinfo
            self.line.request(consumer='mister_control', type=gpiod.LINE_REQ_DIR_OUT)
            logger.info(f"GPIO line {self.relay_pin} configured as output.")

        except gpiod.ChipError as e:
            logger.error(f"Failed to open GPIO chip or get line: {e}")
            sys.exit(1)  # Exit if GPIO setup fails, as mister control won't work
        except Exception as e:
            logger.error(f"An unexpected error occurred during GPIO setup: {e}")
            sys.exit(1)

    def _get_status(self): # Private method
        """
        Fetches the current status of the mister from the database.

        Returns:
            bool: The current status (True for on, False for off).

        Raises:
            Exception: If there's an error fetching the status.
        """
        try:
            return self._devicestatus.get_latest_status_by_device_id(device_id=self.equipment_id) # returns dict
        except Exception as e:
            error_message = f"Failed to get status for {self.equipment_id}: {e}"
            logger.error(error_message)
            raise  # Re-raise the exception to be handled in control_mister

    def _update_status(self, status: bool):
        """
        Updates the status of the mister in the database.

        Args:
            status (bool): The new status to set (True for on, False for off).
        Raises:
            Exception: If there's an error updating the status.
        """
        try:
            self._devicestatus.insert_device_status(
                device_id = self.equipment_id, 
                timestamp = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S"), 
                is_on = status
            ) # Call requires refactoring.
        except Exception as e:
            error_message = f"Failed to update status for {self.equipment_id}: {e}"
            logger.error(error_message)
            raise  # Re-raise for handling in mister_control

    def mister_control(self, action:str, duration:int = 5, initiate: str = 'auto'):
        """
        Controls the mister based on the provided action.

        Args:
            action (str): The desired action ('on' or 'off').
        """

        if(initiate == 'auto'):
            mister_status = self._get_status()

            current_is_on = None
            if mister_status is not None and 'is_on' in mister_status:
                current_is_on = mister_status['is_on']
                last_runtime = mister_status['timestamp']
                run_delta = round((datetime.now(timezone('US/Eastern')).replace(tzinfo=None) - last_runtime).total_seconds()/60, 0)
            else:
                last_runtime = datetime.now(timezone('US/Eastern')).replace(tzinfo=None)
                logger.warning("Could not retrieve current mister status from database. Assuming unknown state and proceeding with action.")

            if(run_delta > int(mister_config.mister_interval)):
                self.run_mister(duration)
            else:
                logger.newline()
                logger.info(f'Motor minimum interval duration \'{mister_config.mister_interval} minutes\' not met, running Humidifier\n')
                # ControlHumidifier.control_vivarium_humidifier()
        else:
            self.run_mister(duration)
        
#     def run_motor(self, eqip, duration):
#         try:
#             # Setup the GPIO pin for output
#             GPIO.setwarnings(False)
#             GPIO.setup(self.relay_pin, GPIO.OUT)
#             motor_status = VivariumDAO.getStatus(eqip, 'p')[2]

#             if not motor_status:
#                 start_time = time.time()
#                 # Turn on the relay (Motor on)
#                 GPIO.output(self.relay_pin, GPIO.HIGH)			# Relay module logic is inverted (Turns on the motor)
#                 VivariumDAO.putStatus(eqip, True, 'p')   		# Saves relay status to DB
#                 VivariumDAO.putStatus(eqip, True, 'a')
#                 time.sleep(int(duration))	                    # Keeps the motor running for specified duration.

#                 GPIO.output (self.relay_pin, GPIO.LOW)		   	# Relay module logic is inverted (Turns of the motor)
#                 time.sleep(0.1)                     		    # Small delay
#                 VivariumDAO.putStatus(eqip, False, 'p')  		# Saves relay status to DB
#                 VivariumDAO.putStatus(eqip, False, 'a')
                
#                 GPIO.cleanup                        		    # Clean up GPIO settings to ensure proper cleanup even after error
#                 end_time = time.time()
#                 logger.newline()
#                 logger.info(f'Motor ran for {round((end_time - start_time), 2)} seconds\n')

#             else:
#                 # Turn off the relay (Motor off)
#                 GPIO.output(self.relay_pin, GPIO.LOW)    		# Relay module logic is inverted (Turns of the motor)
#                 VivariumDAO.putStatus(eqip, False, 'p')  		# Saves relay status to DB
#                 VivariumDAO.putStatus(eqip, False, 'a')
#                 GPIO.cleanup()                      		    # Clean up GPIO settings to ensure proper cleanup even after error
#                 logger.error('Motor was turned off\n')
            
#         except Exception as e:
#             logger.newline()
#             logger.error(f"An error occurred while operating the motor: {e}\n")

#         finally:
#             GPIO.cleanup()                                  # Clean up GPIO settings

#     def motor_last_run(self, _eq):
#         try:
#             return VivariumDAO.getStatus(_eq, 'p')
#         except Exception as e:
#             logger.newline()
#             logger.error(f"Failed to fetch motor status: {e} \n")
#             raise
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Control the motor duration.")
#     parser.add_argument("duration", type=int, nargs = '?', default =30, help="Duration (in seconds) to run the motor (default: 30 seconds)")
#     parser.add_argument("initiate", type=str, nargs = '?', default = 'manual', help='If not initiated from a class call, (default: manual)')
#     args = parser.parse_args()

#     controller = MotorControl()
#     controller.motor_control(args.duration, args.initiate)