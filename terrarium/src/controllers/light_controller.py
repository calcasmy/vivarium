import os
import sys
import RPi.GPIO as GPIO

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

# Add 'vivarium' to the Python path if it's not already there
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# from vivarium_dao import VivariumDAO
from utilities.src.logger import LogHelper
from utilities.src.config import GPIOConfig

from terrarium.src.database.device_queries import DeviceQueries

logger = LogHelper.get_logger(__name__)
gpio_config = GPIOConfig()

class LightControler:
    """
    Controls the vivarium lights using GPIO and database interaction.
    """

    def __init__(self, equipment_id='l'):  # Default equipment_id
        """
        Initializes the LightControl object.

        Args:
            equipment_id (str, optional): The equipment ID. Defaults to 'l'.
        """
        self.equipment_id = equipment_id
        self.relay_pin = int(gpio_config.light_control_pin)  # Get pin from config
        self._setup_gpio()
        self.dao = DeviceQueries() #instanciating DB Queries

    @staticmethod
    def script_path() -> str:
        '''Returns the Absolute path of the script'''
        return os.path.abspath(__file__)

    def _setup_gpio(self):  # Make this method private
        """
        Sets up the GPIO pin for controlling the light.
        This method should only be called internally.
        """
        GPIO.setmode(GPIO.BCM)
        GPIO.setwarnings(False)
        GPIO.setup(self.relay_pin, GPIO.OUT)

    def _get_status(self):
        """
        Fetches the current status of the light from the database.

        Returns:
            bool: The current status (True for on, False for off).

        Raises:
            Exception: If there's an error fetching the status.
        """
        try:
            return self.dao.getStatus(self.equipment_id, 'p')
        except Exception as e:
            error_message = f"Failed to get status for {self.equipment_id}: {e}"
            logger.error(error_message)
            raise  # Re-raise the exception to be handled in control_light

    def _update_status(self, status: bool):
        """
        Updates the status of the light in the database.

        Args:
            status (bool): The new status to set (True for on, False for off).
            db_type (str): The database type ('p' or 'a').

        Raises:
            Exception: If there's an error updating the status.
        """
        try:
            self.dao.putStatus(self.equipment_id, status)
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
        try:
            if action.lower() == 'on':
                GPIO.output(self.relay_pin, GPIO.HIGH)  # Turn on (inverted logic)
                self._update_status(True, 'p')
                logger.info("Vivarium grow lights turned ON")
            elif action.lower() == 'off':
                GPIO.output(self.relay_pin, GPIO.LOW)  # Turn off (inverted logic)
                self._update_status(False, 'p')
                logger.info("Vivarium grow lights turned OFF")
            else:
                logger.warning(f"Invalid light control action: '{action}'. Must be 'on' or 'off'.")

        except Exception as e:  # Catch exceptions from GPIO and database operations
            logger.error(f'An error occurred while controlling the lights: {e}')


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
    light_controller = LightControler()  # No need to pass 'l' again, it is default
    light_controller.control_light(action)

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        action = sys.argv[1]
        main(action)
    else:
        logger.warning("No action provided. Please specify 'on' or 'off' as a command-line argument.")