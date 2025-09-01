# vivarium/terrarium/src/sensors/terrarium_sensor_reader.py

import os
import sys
import json
import board
import traceback
import multiprocessing

from datetime import datetime
from typing import Optional, Dict

# Adjust path to ensure utilities are accessible
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.db_operations import DBOperations
from utilities.src.config import TimeConfig, SensorConfig

from database.sensor_data_ops.sensor_data_queries import SensorDataQueries

logger = LogHelper.get_logger(__name__)

TEMPERATURE_UNIT = "\u00B0F"

class TerrariumSensorReader:
    """
    A class to read temperature and humidity data from a sensor.
    
    This class handles sensor data retrieval in a separate process to prevent
    the main application from freezing due to I/O timeouts. It then persists
    the data to the database.
    """
    def __init__(self, db_operations: DBOperations):
        """
        Initializes the sensor reader and its database dependencies.
        
        :param db_operations: An instance of DBOperations for database interaction.
        :type db_operations: DBOperations
        """
        self.db_ops = db_operations
        self.sensor_data_queries = SensorDataQueries(db_operations=self.db_ops)
        self.process_timeout = float(TimeConfig().process_term_span)
        self.th_sensor_id = int(SensorConfig().THsensorID)
        
        logger.info(f"TerrariumSensorReader initialized with a process timeout of {self.process_timeout} seconds.")

    @staticmethod
    def _fetch_sensor_data_process(queue: multiprocessing.Queue, sensor_id: Optional[int] = 1) -> None:
        """
        Helper function to fetche sensor data within a separate process.
        
        This static method is the target for the multiprocessing process. It
        initializes the I2C sensor to avoid pickling issues.
        
        :param queue: The multiprocessing queue to send data back to the parent process.
        :type queue: multiprocessing.Queue
        :param sensor_id: The ID of the sensor to read from.
        :type sensor_id: Optional[int]
        """
        try:
            i2c_bus = board.I2C()

            if sensor_id == 1:
                from adafruit_htu21d import HTU21D
                sensor_device = HTU21D(i2c_bus)
            elif sensor_id == 2:
                from adafruit_sht4x import SHT4x, Mode
                sensor_device = SHT4x(i2c_bus)
                sensor_device.mode = Mode.NOHEAT_HIGHPRECISION
            elif sensor_id == 3:
                from adafruit_sht31d import SHT31D
                sensor_device = SHT31D(i2c_bus)
                sensor_device.heater = False
            else:
                raise ValueError(f"Unsupported sensor ID: {sensor_id}")

            temp_c = sensor_device.temperature
            humidity_p = sensor_device.relative_humidity

            if temp_c is None or humidity_p is None:
                raise ValueError("Sensor returned None values (bad read).")

            temp_f = (temp_c * 9 / 5) + 32

            sensor_data = {
                'temperature_fahrenheit': round(temp_f, 2),
                'temperature_celsius': round(temp_c, 2),
                'humidity_percentage': round(humidity_p, 2)
            }
            queue.put(sensor_data)
            logger.debug("Sensor data successfully fetched in subprocess.")

        except Exception as e:
            logger.error(f"Error in sensor subprocess: {e}", exc_info=True)
            queue.put({"error": str(e), "traceback": traceback.format_exc()})

    def read_and_store_data(self) -> bool:
        """
        Reads and stores temperature and humidity data.
        
        This method initiates a subprocess to read the sensor. It handles process
        timeouts and persists the collected data to the database.
        
        :returns: True if data was successfully read and stored, False otherwise.
        :rtype: bool
        """
        queue = multiprocessing.Queue()
        process = multiprocessing.Process(
            target=TerrariumSensorReader._fetch_sensor_data_process,
            args=(queue, self.th_sensor_id)
        )
        
        logger.info("Starting sensor data retrieval subprocess.")
        process.start()
        process.join(timeout=self.process_timeout)

        if process.is_alive():
            process.terminate()
            process.join()
            logger.error(f"Sensor data retrieval timed out after {self.process_timeout} seconds. Process terminated.")
            return False

        if not queue.empty():
            sensor_data = queue.get()

            if "error" in sensor_data:
                logger.error(f"Sensor subprocess reported an error: {sensor_data['error']}\n{sensor_data.get('traceback', 'No traceback provided.')}")
                return False

            timestamp = datetime.now().isoformat()
            raw_data = json.dumps(sensor_data)
            self.sensor_data_queries.insert_sensor_reading(self.th_sensor_id, timestamp, raw_data)

            log_message = (
                f"Processed and persisted sensor data: Temperature: {sensor_data['temperature_fahrenheit']:.2f}{TEMPERATURE_UNIT}, "
                f"Humidity: {sensor_data['humidity_percentage']:.2f}%"
            )
            logger.info(log_message)
            return True
        else:
            logger.error("Sensor subprocess finished but did not return any data (queue was empty).")
            return False