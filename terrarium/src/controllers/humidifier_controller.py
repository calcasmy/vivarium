import os
import sys
import time
from typing import Optional

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# Importing VeSync Assets
from assets.humidifier.src import vesync
from assets.humidifier.src.vesyncclassic300s import VeSyncHumidClassic300S

# Importing utilities package
from utilities.src.logger import LogHelper
from utilities.src.config import HumidifierConfig, DatabaseConfig
from utilities.src.db_operations import DBOperations, ConnectionDetails

# Super class
from terrarium.src.controllers.base_device_controller import BaseDeviceController

logger = LogHelper.get_logger(__name__)
humid_config = HumidifierConfig()

class HumidifierController(BaseDeviceController):
    """
    Controls a VeSync-compatible humidifier via its API.

    This class manages the connection to the VeSync cloud, discovers a target humidifier,
    and provides methods for manual or automatic control. It overrides the
    GPIO-specific logic of the BaseDeviceController, as it is a network-based device.
    """
    def __init__(self, db_operations: DBOperations):
        """
        Initializes the HumidifierController object, connects to VeSync, and finds the device.

        :param db_operations: An instance of DBOperations for database interaction.
        :type db_operations: DBOperations
        """
        self.device_id = humid_config.device_id
        self.humidifier: Optional[VeSyncHumidClassic300S] = None
        self.username = humid_config.username
        self.password = humid_config.password
        self.consumer_name = 'humidifier_control'
        self.target_humidity = humid_config.target_humidity
        # self.humidity_sensor_id = humid_config.humidity_sensor_id
        # self.hysteresis = humid_config.hysteresis
        
        # Call the base class constructor with a dummy pin as it's API controlled
        super().__init__(
            device_id=self.device_id,
            relay_pin=-1,
            consumer_name=self.consumer_name,
            db_operations=db_operations
        )
        
        # Connect to VeSync and find the humidifier
        self.humidifier = self._get_vesync_humidifier()

        if self.humidifier is None:
            logger.error("Failed to initialize HumidifierController: could not find VeSync humidifier.")
        else:
            logger.info("HumidifierController initialized.")

    def _get_vesync_humidifier(self) -> Optional[VeSyncHumidClassic300S]:
        """
        Connects to the VeSync API, logs in, and retrieves the humidifier object.

        :returns: An instance of VeSyncHumidClassic300S if found, otherwise None.
        :rtype: Optional[VeSyncHumidClassic300S]
        """
        try:
            manager = vesync.VeSync(self.username, self.password)
            login_success = manager.login()
            if not login_success:
                logger.error("Login to VeSync failed.")
                return None

            logger.info("Successfully logged into VeSync. Fetching devices...")
            manager.get_devices()
            
            for device in manager.fans:
                if isinstance(device, VeSyncHumidClassic300S):
                    logger.info(f"Found humidifier: {device.device_name}")
                    return device
            
            logger.warning(f"Could not find a Levoit Classic 300S with name '{humid_config.device_name}'.")
            return None

        except Exception as e:
            logger.exception(f"An error occurred while connecting to VeSync: {e}")
            return None

    def _set_gpio_state(self, state: bool):
        """
        Overrides the BaseDeviceController method to handle network-based control.

        This method is a no-op for this controller, as there are no GPIO pins to toggle.
        The action is performed directly on the VeSync device object.

        :param state: The desired state (True for on, False for off).
        :type state: bool
        """
        if self.humidifier:
            if state:
                if not self.humidifier.is_on:
                    self.humidifier.turn_on()
            else:
                if self.humidifier.is_on:
                    self.humidifier.turn_off()
        else:
            logger.error("Humidifier device not found. Cannot set state.")

    def control_humidifier(self, action: str):
        """
        Performs a manual action on the humidifier.

        :param action: The desired action: 'on', 'off', or 'status'.
        :type action: str
        """
        if not self.humidifier:
            logger.error("Humidifier device not initialized. Cannot perform action.")
            return

        if action == 'status':
            self.humidifier.update()
            status = 'on' if self.humidifier.is_on else 'off'
            logger.info(f"Current HUMIDIFIER status: {status}")
            logger.info(f"Current device humidity reading: {self.humidifier.humidity}%")
        elif action == 'on':
            self.humidifier.turn_on()
            logger.info("Humidifier turned ON via manual command.")
        elif action == 'off':
            self.humidifier.turn_off()
            logger.info("Humidifier turned OFF via manual command.")
        else:
            logger.warning(f"Invalid action '{action}' for humidifier. Ignoring.")
        
        # Update the database after the manual action
        if self.humidifier:
            self.humidifier.update()
            self._update_status(self.humidifier.is_on)

    def control_humidifier_auto(self):
        """
        Controls the humidifier automatically based on the latest humidity sensor reading.

        This method fetches the latest humidity data from the database and
        toggles the humidifier on or off to maintain the target humidity.
        """
        if not self.humidifier:
            logger.error("Humidifier device not initialized. Cannot run auto mode.")
            return

        latest_humidity_data = self.db_operations.get_latest_sensor_reading(
            self.humidity_sensor_id
        )

        if not latest_humidity_data or 'value' not in latest_humidity_data:
            logger.warning("Could not retrieve latest humidity data from database. Skipping auto control.")
            return
        
        current_humidity = latest_humidity_data['value']
        
        # Update device status from VeSync before checking
        self.humidifier.update()
        current_humidifier_status = self.humidifier.is_on

        logger.info(f"Current Terrarium Humidity: {current_humidity}%. Humidifier is currently {'ON' if current_humidifier_status else 'OFF'}.")
        
        # Logic to decide action
        if current_humidity < self.target_humidity:
            if not current_humidifier_status:
                logger.info(f"Humidity {current_humidity}% is below target {self.target_humidity}%. Turning humidifier ON.")
                self.humidifier.turn_on()
            else:
                logger.info("Humidifier is already ON and humidity is below target. No action needed.")

        elif current_humidity > (self.target_humidity + self.hysteresis):
            if current_humidifier_status:
                logger.info(f"Humidity {current_humidity}% is above target {self.target_humidity}%. Turning humidifier OFF.")
                self.humidifier.turn_off()
            else:
                logger.info("Humidifier is already OFF and humidity is above target. No action needed.")
        else:
            logger.info(f"Humidity {current_humidity}% is within acceptable range. No action needed.")

        # Update the database status after the action
        self.humidifier.update()
        self._update_status(self.humidifier.is_on)

def main(action: str):
    """
    Main function to create and run the HumidifierController.

    :param action: Action to perform: 'on', 'off', 'auto', or 'status'.
    :type action: str
    """
    db_config = DatabaseConfig()
    db_operations = DBOperations()
    
    # Establish a database connection before running the controller logic
    db_operations.connect(ConnectionDetails(
        host=db_config.postgres_local_connection.host,
        port=db_config.postgres_local_connection.port,
        user=db_config.postgres_local_connection.user,
        password=db_config.postgres_local_connection.password,
        dbname=db_config.postgres_local_connection.dbname
    ))

    try:
        humid_controller = HumidifierController(db_operations=db_operations)
        
        if action == 'auto':
            humid_controller.control_humidifier_auto()
        elif action in ['on', 'off', 'status']:
            humid_controller.control_humidifier(action)
        else:
            logger.warning(f"Invalid humidifier action: {action}. Use 'auto', 'on', 'off', or 'status'.")

    except Exception as e:
        logger.exception(f"An unexpected error occurred in HumidifierController main: {e}")
    finally:
        # Always close the database connection
        db_operations.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Control the vivarium humidifier.")
    parser.add_argument("action", type=str, help="Action to perform: 'on', 'off', 'auto', or 'status'.")
    args = parser.parse_args()

    main(args.action)