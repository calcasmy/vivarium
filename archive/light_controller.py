import os
import sys
import gpiod
from datetime import date, datetime
# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

# Add 'vivarium' to the Python path if it's not already there
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# from vivarium_dao import VivariumDAO
from utilities.src.logger import LogHelper
from utilities.src.config import GPIOConfig
from utilities.src.config import LightConfig
from utilities.src.database_operations import DatabaseOperations

from terrarium.src.database.device_status_queries import DeviceStatusQueries
from terrarium.src.database.device_queries import DeviceQueries

logger = LogHelper.get_logger(__name__)
# gpio_config = GPIOConfig()
light_config = LightConfig()

class LightController:
    """
    Controls the vivarium lights using GPIO and database interaction.
    """

    def __init__(self, equipment_id=1, db_operations: DatabaseOperations = None):  # Default equipment_id
        """
        Initializes the LightControl object.

        Args:
            equipment_id (str, optional): The equipment ID. Defaults to 'l'.
        """
        if db_operations:
            self.db_ops = db_operations

        self.equipment_id = equipment_id
        # self.relay_pin = int(gpio_config.light_control_pin)  # Get pin from config
        self.relay_pin = int(light_config.lights_control_pin)
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
                logger.error(f'An error occurred while controlling the lights or updating database: {e}')

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
        """Sets up the GPIO pin for controlling the light."""

        try:
            # Open the GPIO chip, typically 'gpiochip0' on Raspberry Pi and similar boards
            self.chip = gpiod.Chip('gpiochip0')
            self.line = self.chip.get_line(self.relay_pin)

            # Request the line as output
            # Pass consumer='light_control' for better debugging with gpiodetect/gpioinfo
            self.line.request(consumer='light_control', type=gpiod.LINE_REQ_DIR_OUT)
            logger.info(f"GPIO line {self.relay_pin} configured as output.")

        except gpiod.ChipError as e:
            logger.error(f"Failed to open GPIO chip or get line: {e}")
            sys.exit(1)  # Exit if GPIO setup fails, as light control won't work
        except Exception as e:
            logger.error(f"An unexpected error occurred during GPIO setup: {e}")
            sys.exit(1)

    def _get_status(self): # Private method
        """
        Fetches the current status of the light from the database.

        Returns:
            bool: The current status (True for on, False for off).

        Raises:
            Exception: If there's an error fetching the status.
        """
        try:
            return self._devicestatus.get_latest_status_by_device_id(device_id=self.equipment_id) # Call requires refactoring
        except Exception as e:
            error_message = f"Failed to get status for {self.equipment_id}: {e}"
            logger.error(error_message)
            raise  # Re-raise the exception to be handled in control_light

    def _update_status(self, status: bool):
        """
        Updates the status of the light in the database.

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
            raise  # Re-raise for handling in control_light
    
    def control_light(self, action: str):
        """
        Controls the light based on the provided action.

        Args:
            action (str): The desired action ('on' or 'off').
        """

        # 1. Validate action and determine target state
        target_state = None
        if action.lower() == 'on':
            target_state = True
        elif action.lower() == 'off':
            target_state = False
        else:
            logger.warning(f"Invalid light control action: '{action}'. Must be 'on' or 'off'.")
            return # Exit if the action is invalid
        
        try:
            # 2. Get current status from the database
            current_status_dict = self._get_status() # This should return {'is_on': True/False, ...} or None
            
            current_is_on = None
            if current_status_dict is not None and 'is_on' in current_status_dict:
                current_is_on = current_status_dict['is_on']
            else:
                logger.warning("Could not retrieve current light status from database. Assuming unknown state and proceeding with action.")

            # 3. Compare current state with target state and act if necessary
            if current_is_on is not None and current_is_on == target_state:
                logger.info(f"Vivarium grow lights are already {'ON' if target_state else 'OFF'}. No action taken.")
            else:
                # State needs to change, or current_is_on was None (unknown)
                gpio_value = 1 if target_state else 0
                log_message = "turning ON" if target_state else "turning OFF"

                self.line.set_value(gpio_value)  # Apply the new state
                self._update_status(target_state) # Update database with the new state
                logger.info(f"Vivarium grow lights successfully {log_message}.")

        except Exception as e:
            # Catch any exceptions from _get_status, _update_status, or gpiod operations
            logger.error(f'An error occurred while controlling the lights or updating database: {e}')


        if not self.line:
            logger.error("GPIO line not initialized. Cannot control light.")
            return
        
        # status = self._get_status()  # Use the private method
        # if status['is_on'] == True:
        #     logger.info(f"Vivarium grow lights are currently {action}. No action taken")
        # else:
        #     try:
        #         if action.lower() == 'on':
        #             self.line.set_value(1)  # Turn on (HIGH)
        #             self._update_status(True)
        #             logger.info("Vivarium grow lights turned ON")
        #         elif action.lower() == 'off':
        #             self.line.set_value(0)  # Turn off (LOW)
        #             self._update_status(False)
        #             logger.info("Vivarium grow lights turned OFF")
        #         else:
        #             logger.warning(f"Invalid light control action: '{action}'. Must be 'on' or 'off'.")

        #     except Exception as e:  # Catch exceptions from database operations
        #         logger.error(f'An error occurred while controlling the lights or updating database: {e}')
        #
        # try:
        #     if action.lower() == 'on':
        #         GPIO.output(self.relay_pin, GPIO.HIGH)  # Turn on (inverted logic)
        #         self._update_status(True, 'p')
        #         logger.info("Vivarium grow lights turned ON")
        #     elif action.lower() == 'off':
        #         GPIO.output(self.relay_pin, GPIO.LOW)  # Turn off (inverted logic)
        #         self._update_status(False, 'p')
        #         logger.info("Vivarium grow lights turned OFF")
        #     else:
        #         logger.warning(f"Invalid light control action: '{action}'. Must be 'on' or 'off'.")
        #
        # except Exception as e:  # Catch exceptions from GPIO and database operations
        #     logger.error(f'An error occurred while controlling the lights: {e}')


    # def control_light(self):
    #     """
    #     Controls the light based on the current status in the database.
    #     This method now handles the core logic.
    #     """
    #     try:
    #         status = self._get_status()  # Use the private method

    #         # Toggle the light state
    #         new_status = not status

    #         # Control the relay (Lights on if new_status is True, off if False)
    #         GPIO.output(self.relay_pin, GPIO.HIGH if new_status else GPIO.LOW)  # Inverted logic
    #         self._update_status(new_status, 'p')  # Use private method
    #         self._update_status(new_status, 'a')  # Use private method

    #         log_message = f"Vivarium grow lights turned {'ON' if new_status else 'OFF'}"
    #         logger.info(log_message)

    #     except Exception as e:  # Catch exceptions from _get_status and _update_status
    #         logger.error(f'An error occurred while controlling the lights: {e}')

def main(action):
    """
    Main function to create and run the LightControl.
    """
    db_operations = DatabaseOperations()
    db_operations.connect()

    try:
        light_controller = LightController(db_operations = db_operations)
        light_controller.control_light(action)
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
    finally:
        db_operations.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        action = sys.argv[1]
        main(action)
    else:
        logger.warning("No action provided. Please specify 'on' or 'off' as a command-line argument.")