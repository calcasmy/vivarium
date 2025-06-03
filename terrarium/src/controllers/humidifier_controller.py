import os
import sys
import time

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# Importing Assets
from assets.humidifier.src import vesync
from assets.humidifier.src.vesyncclassic300s import VeSyncHumidClassic300S

# Importing utilities package
from utilities.src.logger import LogHelper
from utilities.src.config import HumidifierConfig
from utilities.src.database_operations import DatabaseOperations

#Supre class
from terrarium.src.controllers.base_device_controller import BaseDeviceController

logger = LogHelper.get_logger(__name__)
humid_config = HumidifierConfig()

class HumidiferControllerV2(BaseDeviceController):
    def __init__(self, db_operations: DatabaseOperations,action:str = None, duration:int = 30, level:int = 1):
        self.device_id = humid_config.device_id
        self.manager = None
        self.humidifier: VeSyncHumidClassic300S = None
        self.username = humid_config.username
        self.password = humid_config.password
        self.consumer_name = 'Humidifier'
        self.db_operations = db_operations
        self._humidifier_manager()

        super().__init__(
            device_id=self.humid_config.device_id,
            relay_pin=-1, # Dummy pin as it's API controlled
            consumer_name="Humidifier",
            db_operations=db_operations
        )

        # Humidifier Attributes
        self.action = action
        self.duration = duration
        self.level = level
        

    @staticmethod
    def script_path() -> str:
        '''Returns the Absolute path of the script'''
        return os.path.abspath(__file__)
    
    def _humidifier_manager(self, action:str = None, duration:int = 30, level:int = 1):
        """Connects to VeSync, finds a Classic 300S, and allows control."""
        try:
            self.manager = vesync.VeSync(self.username, self.password)
            login_success = self.manager.login()
            if login_success:
                logger.info("Successfully logged into VeSync.")
                self.manager.get_devices()
                humidifier = None
                for device in self.manager.fans:
                    if isinstance(device, VeSyncHumidClassic300S):
                        self.humidifier = device
                        break

                if self.humidifier:
                    logger.info(f"Found your Levoit Classic 300S: {humidifier.device_name}")
                    self._control_humidifier(humidifier, action, duration, level)
                else:
                    logger.info("Could not find a Levoit Classic 300S humidifier.")

            else:
                logger.info("Login to VeSync failed.")

        except Exception as e:
            logger.error(f"An error occurred: {e}")

    def _control_humidifier(self, current_humidity:float = None ):
        """
        Controls the humidifier based on current and target humidity.
        """
        if not self.gpio_functional or not self.humidifier_device:
            logger.error("Humidifier controller is not functional. Cannot control humidifier.")
            return

        if current_humidity is None:
            logger.warning("Current humidity not available to control humidifier. Doing nothing.")
            return

        # Determine Humidifier state (On/ Off/ Adjust settings)
        if current_humidity:
            if not current_device_status:
                logger.info(f"Terrarium Humidity {current_humidity}%. Turning humidifier ON.")
                self._set_gpio_state(True) # Turn ON
                time.sleep(5)
                self.humidifier.update()

        try:
            # Refresh humidifier status from VeSync before proceeding
            self.humidifier.update()
            current_device_status = self.humidifier.device_status == 'on'
            current_mist_level = self.humidifier.mist_level
            current_target_humidity = self.humidifier.auto_humidity

            logger.info(f"Humidifier status: {current_device_status}, Current Mist Level: {current_mist_level}, Device Target: {current_target_humidity}")
            logger.info(f"Current Vivarium Humidity: {current_humidity}%")


        except Exception as e:
            logger.error(f"An error occurred during humidifier control: {e}", exc_info=True)
            
         # Always ensure the device's auto-target matches our desired target if it's on
                # if self.humidifier_device.device_status == 'on':
                #     if self.humidifier_device.humidity_mode != 'auto' or self.humidifier_device.auto_humidity != target_humidity:
                #         logger.info(f"Setting humidifier mode to 'auto' and target humidity to {target_humidity}%.")
                #         # VeSync's set_humidity also sets mode to auto
                #         if not self.humidifier_device.set_humidity(target_humidity):
                #             logger.error(f"Failed to set humidifier target humidity to {target_humidity}%.")
                #             self.notifier.send_notification(
                #                 title="Humidifier Setting Failed",
                #                 message=f"Failed to set humidifier target to {target_humidity}% (current: {current_humidity}%).",
                #                 priority=0
                #             )
                    # If it's already on and in auto mode with correct target, adjust mist level if needed (optional, if you want manual override on mist)
                    # This part might not be strictly necessary if auto mode is good, but you can add logic to boost mist if humidity falls too far below target
                    # For now, rely on device's auto mode.

            elif current_humidity >= (target_humidity + 2): # Add a hysteresis to prevent rapid on/off cycling
                if current_device_status:
                    logger.info(f"Humidity {current_humidity}% is above target {target_humidity}%. Turning humidifier OFF.")
                    self._set_gpio_state(False) # Turn OFF

            else:
                logger.info(f"Humidity {current_humidity}% is within acceptable range of target {target_humidity}%. No humidifier action needed.")

            # Update database status after action (use BaseDeviceController's update_status)
            self._update_status(self.humidifier_device.device_status == 'on')

        except Exception as e:
            logger.error(f"An error occurred during humidifier control: {e}", exc_info=True)
            self.notifier.send_notification(
                title="Humidifier Automation Error",
                message=f"An error occurred while automating humidifier based on humidity: {e}",
                priority=1
            )

def main(action: str):
    """
    Main function to create and run the HumidiferController.
    """
    db_operations = DatabaseOperations()
    db_operations.connect()

    try:
        humid_controller = HumidiferControllerV2(db_operations = db_operations)
        humid_controller.humidifier_manager()
    except Exception as e:
        logger.exception(f"An unexpected error occurred in LightController main: {e}")
    finally:
        db_operations.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Control the mister duration.")
    parser.add_argument("action", type=str, help="Action to perform: 'run' (manual run) or 'auto' (automatic check).")
    parser.add_argument("--duration", type=int, default=30, help="Duration (in seconds) to run the mister (default: 30 seconds). Only applies to 'run' action.")
    args = parser.parse_args()

    logger.info("Attempting to run humidifer")

    main(args.action, args.duration)