import os

class MisterController:
    def __init__(self):
        pass

    @staticmethod
    def script_path() -> str:
        '''Returns the Absolute path of the script'''
        return os.path.abspath(__file__)
    
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

# import RPi.GPIO as GPIO
# import time
# import argparse

# from vivarium_dao import VivariumDAO
# from propertyreader import TIMECONFIG, GPIOCONFIG
# from logger_helper import LogHelper
# from datetime import datetime, timezone
# from pytz import timezone

# from src.terrarium.humidifier_control import ControlHumidifier

# logger = LogHelper.get_logger('vivarium')

# class MotorControl:

#     def __init__(self):
#         self.relay_pin = int(GPIOCONFIG.gpiomctl)
#         GPIO.setmode(GPIO.BCM)

#     def motor_control(self, duration = 5, initiate = 'auto'):

#         eqip = 'm'

#         if(initiate == 'auto'):
#             motor_status = self.motor_last_run(eqip)

#             if(motor_status != []):
#                 last_runtime = datetime.combine(motor_status[0], motor_status[1])
#                 run_delta = round((datetime.now(timezone('US/Eastern')).replace(tzinfo=None) - last_runtime).total_seconds()/60, 0)
#             else:
#                 last_runtime = datetime.now(timezone('US/Eastern')).replace(tzinfo=None)

#             if(run_delta > int(TIMECONFIG.motorruntime)):
#                 self.run_motor(eqip, duration)
#             else:
#                 logger.newline()
#                 logger.info(f'Motor minimum interval duration \'{TIMECONFIG.motorruntime} minutes\' not met, running Humidifier\n')
#                 ControlHumidifier.control_vivarium_humidifier()
#         else:
#             self.run_motor(eqip, duration)
        
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