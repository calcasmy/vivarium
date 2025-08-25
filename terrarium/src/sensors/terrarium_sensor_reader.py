# /Users/neptune/Development/vivarium/terrarium/src/sensors/terrarium_sensor_reader.py

import os
import sys
import json
import board
import traceback
import multiprocessing

from adafruit_htu21d import HTU21D
from datetime import datetime
from typing import Optional, Dict

# Adjust path to ensure utilities are accessible
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations
from utilities.src.config import TimeConfig

from database.sensor_data_ops.sensor_data_queries import SensorDataQueries
from database.sensor_data_ops.sensor_queries import SensorQueries

logger = LogHelper.get_logger(__name__)

# Constants specific to this module
TEMPERATURE_UNIT = "\u00B0F"

class TerrariumSensorReader:
    """
    A class to read temperature and humidity data from an HTU21D sensor
    using a separate process to handle potential sensor read timeouts.
    It stores the data in the database.
    """
    def __init__(self, db_operations: DBOperations):
        """
        Initializes the sensor reader, database operations, and retrieves timeout setting.

        Args:
            db_operations (DatabaseOperations): An instance of DatabaseOperations for DB interaction.
        """
        self.db_ops = db_operations
        self.sensor_queries = SensorQueries(db_operations=self.db_ops)
        self.sensor_data_queries = SensorDataQueries(db_operations=self.db_ops)
        
        # Get process timeout from config
        self.process_timeout = float(TimeConfig().process_term_span)
        self.sensor_name = 'Adafruit HTU21D-F'

        logger.info(f"TerrariumSensorReader initialized with a process timeout of {self.process_timeout} seconds.")

    @staticmethod
    def _fetch_htu21d_data_process(queue: multiprocessing.Queue):
        """
        Helper function to run in a separate process for fetching HTU21D sensor data.
        Initializes the sensor within this process.
        """
        try:
            # Initialize I2C and sensor within the subprocess
            # This is crucial because I2C objects are generally not pickleable.
            i2c_bus = board.I2C()
            sensor_device = HTU21D(i2c_bus)

            temp_c = sensor_device.temperature
            humidity_p = sensor_device.relative_humidity

            if temp_c is None or humidity_p is None:
                # Raise an error if sensor returns None, indicating a bad read
                raise ValueError("HTU21D sensor returned None values (bad read).")

            temp_f = (temp_c * 9 / 5) + 32

            sensor_data = {
                'temperature_fahrenheit': round(temp_f, 2),
                'temperature_celsius': round(temp_c, 2),
                'humidity_percentage': round(humidity_p, 2)
            }
            queue.put(sensor_data) # Put data into the queue for the parent process
            logger.debug("Sensor data successfully fetched in subprocess.")

        except Exception as e:
            # Log the error within the subprocess for detailed debugging
            logger.error(f"Error in sensor subprocess (_fetch_htu21d_data_process): {e}", exc_info=True)
            # Put an error indicator into the queue so the parent knows something went wrong
            queue.put({"error": str(e), "traceback": traceback.format_exc()})


    def read_and_store_data(self):
        """
        Initiates a subprocess to read sensor data, handles timeouts,
        and stores the data in the database.
        """
        queue = multiprocessing.Queue() # Create a Queue for inter-process communication
        process = multiprocessing.Process(
            target=TerrariumSensorReader._fetch_htu21d_data_process,
            args=(queue,)
        )
        
        logger.info("Starting sensor data retrieval subprocess.")
        process.start()
        process.join(timeout=self.process_timeout) # Wait for process with a timeout

        if process.is_alive():
            # If the process is still alive after the timeout, terminate it
            process.terminate()
            process.join() # Ensure the process has truly terminated
            logger.error(f"Sensor data retrieval timed out after {self.process_timeout} seconds. Process terminated.")
            return False # Exit the function, do not attempt to read from queue

        if not queue.empty():
            # Get the result from the queue
            sensor_data = queue.get()

            if "error" in sensor_data:
                # An error occurred in the subprocess, log it and return
                logger.error(f"Sensor subprocess reported an error: {sensor_data['error']}\n{sensor_data.get('traceback', 'No traceback provided.')}")
                return False

            # Proceed with storing data if no error
            timestamp = datetime.now().isoformat()
            
            # Fetch sensor ID by name (re-using db_ops connection from parent process)
            sensor_details = self.sensor_queries.get_sensor_by_name(self.sensor_name)
            sensor_id = sensor_details.get("sensor_id", 1) # Default to 1 if not found

            raw_data = json.dumps(sensor_data)
            self.sensor_data_queries.insert_sensor_reading(sensor_id, timestamp, raw_data)

            log_message = (
                f"Processed and persisted sensor data: Temperature: {sensor_data['temperature_fahrenheit']:.2f}{TEMPERATURE_UNIT}, "
                f"Humidity: {sensor_data['humidity_percentage']:.2f}%"
            )
            logger.info(log_message)
            return True
        else:
            # This case means process finished, but didn't put anything in queue
            logger.error("Sensor subprocess finished but did not return any data (queue was empty).")
            return False