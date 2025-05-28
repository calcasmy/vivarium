import os
import sys
import argparse
import time

# Adjust path to import utilities and controllers
# Assuming this file is in vivarium/manual_control/
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# Import shared utilities
from utilities.src.logger import LogHelper
from utilities.src.database_operations import DatabaseOperations

# Import specific controllers
from terrarium.src.controllers.light_controller_v2 import LightControllerV2
from terrarium.src.controllers.mister_controller_v2 import MisterControllerV2
from terrarium.src.database.device_status_queries import DeviceStatusQueries

logger = LogHelper.get_logger(__name__)

def main():
    parser = argparse.ArgumentParser(
        description="Manually control Vivarium devices (lights, mister)."
    )
    parser.add_argument(
        "--device",
        type=str,
        choices=["light", "mister"],
        required=True,
        help="The device to control (light or mister)."
    )
    parser.add_argument(
        "--action",
        type=str,
        choices=["on", "off", "status", "run_for"],
        required=True,
        help="The action to perform (on, off, status, or run_for for mister)."
    )
    parser.add_argument(
        "--duration",
        type=int,
        help="Duration in seconds for 'run_for' action (only for mister)."
    )

    args = parser.parse_args()

    db_operations = None
    try:
        db_operations = DatabaseOperations()
        db_operations.connect()
        logger.info(f"Manual control script: Database connection established.")

        if args.device == "light":
            if args.action == "on" or args.action == "off":
                controller = LightControllerV2(db_operations=db_operations)
                logger.info(f"Manually turning LIGHT [{str(args.action).upper()}].")
                controller.control_light(args.action)
            elif args.action == "status":
                _get_device_status(consumer_name = args.device, device_id = 1, db_operations = db_operations)
            else:
                logger.error(f"Invalid action '{args.action}' for light device.")

        elif args.device == "mister":
            controller = MisterControllerV2(db_operations=db_operations)
            if args.action == "on" or args.action == "off":
                logger.info(f"Manually turning MISTER [{str(args.action).upper()}].")
                controller.control_mister(args.action)
            elif args.action == "run_for":
                if args.duration is None or args.duration <= 0:
                    logger.error("Error: --duration must be provided and be a positive integer for 'run_for' action.")
                    return
                logger.info(f"Manually running MISTER for {args.duration} seconds.")
                controller.control_mister("on")
                logger.info("Mister ON.")
                time.sleep(args.duration) # This script will block here
                controller.control_mister("off")
                logger.info("Mister OFF after duration.")
            elif args.action == "status":
                _get_device_status(consumer_name = args.device, device_id = 2, db_operations = db_operations)
            else:
                logger.error(f"Invalid action '{args.action}' for mister device.")

    except Exception as e:
        logger.error(f"An error occurred during manual control of {args.device.uppe()}: {e}", exc_info=True)
    finally:
        if db_operations:
            db_operations.close()
            logger.info("Manual control script: Database connection closed.")

def _get_device_status(consumer_name: str, device_id: int, db_operations: DatabaseOperations):
    """
    Fetches the current status of the device from the database.

    Returns:
        dict: A dictionary containing the device status, or None if not found/error.
    """
    try:
        status_queries = DeviceStatusQueries(db_operations=db_operations)
        status_dict = status_queries.get_latest_status_by_device_id(device_id = device_id)
        if status_dict is not None and 'is_on' in status_dict:
            if status_dict["is_on"] == True:
                device_status = "on"
            else: device_status = "off"
            logger.info(f"The {consumer_name.upper()} is currently {device_status.upper()}")
    except Exception as e:
        error_message = f"Failed to get status for {device_id} ({consumer_name}): {e}"
        logger.error(error_message)
        raise # Re-raise for calling method to handle

if __name__ == "__main__":
    main()