import os
import sys
import json
import board
import socket
import multiprocessing
from adafruit_htu21d import HTU21D
from typing import Optional, List, Dict

if __name__ == "__main__":
    vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..' ))

    if vivarium_path not in sys.path:
        sys.path.insert(0, vivarium_path)

from datetime import datetime, timedelta
from utilities.src.config import TimeConfig
from utilities.src.logger import LogHelper
from utilities.src.database_operations import DatabaseOperations

from terrarium.src.database.device_queries import DeviceQueries
from terrarium.src.database.device_status_queries import DeviceStatusQueries
from terrarium.src.database.sensor_queries import SensorsQueries
from terrarium.src.database.sensor_data_queries import SensorDataQueries
from terrarium.src.controllers.light_controller import LightControler
from terrarium.src.controllers.mister_controller import MisterController

# Global class variables (initialize only once)
# logger = LogHelper.get_logger(__name__)
logger = LogHelper.get_logger("Terrarium_Status")

TIMECONFIG = TimeConfig()

# Constants (moved to the top for better organization)
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5001
HUMIDITY_THRESHOLD = 80.0
MOTOR_DURATION = 30  # seconds
TEMPERATURE_UNIT = "\u00B0F"  # Fahrenheit symbol
PROCESS_TIMEOUT = float(TIMECONFIG.process_term_span)


class TerrariumStatus:
    """
    Fetch and log vivarium sensor data (temperature, humidity).

    This script collects data from an HTU21D sensor and saves it to a PostgreSQL database.
    It also controls a motor based on humidity levels and sends temperature data
    to a fan control service.
    """

    def __init__(self):
        """Initializes the sensor and I2C connection."""
        try:
            self.i2c = board.I2C()  # Uses board.SCL and board.SDA
            self.sensor = HTU21D(self.i2c)
            self.sensor_name = 'HTU21D'
        except Exception as e:
            logger.error("Failed to initialize sensor: %s", e, exc_info=True)
            raise  # Re-raise to be caught by the main process

    '''
        Fetches current terrarium status primarily Temperature and Humidity
    '''

    @staticmethod
    def script_path() -> str:
        '''Returns the Absolute path of the script'''
        return os.path.abspath(__file__)

    # def fetch_data(self) -> tuple[float, float]:
    def fetch_data(self) -> Optional[List[Dict]]:
        """
        Fetches temperature and humidity data from the HTU21D sensor.

        Returns:
            tuple[float, float]: Temperature in Fahrenheit and relative humidity in percentage.

        Raises:
            Exception: If there is an error reading from the sensor.
        """
        try:
            temp_c = self.sensor.temperature
            humidity_p = self.sensor.relative_humidity

            temp_f = (temp_c * 9 / 5) + 32

            logger.debug(
                "Raw sensor data: Temperature: %.2f\u00B0C, Humidity: %.2f%%",
                temp_c,
                humidity_p,
            )
            return {
                'temperature_fahrenheit' : round(temp_f, 2), 
                'temperature_celsius' : round(temp_c, 2),
                'humidity_percentage' : round(humidity_p, 2)}
        except Exception as e:
            error_message = "Error fetching data from sensor: %s"
            logger.error(error_message, e, exc_info=True)
            raise  # Re-raise the exception

def send_temperature(temp: float, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> None:
    """
    Sends temperature data to the fan control service via UDP.

    Args:
        temp (float): The temperature to send (in Fahrenheit).
        host (str, optional): The hostname or IP address of the fan control service.
            Defaults to DEFAULT_HOST.
        port (int, optional): The port number of the fan control service.
            Defaults to DEFAULT_PORT.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client_socket:
            client_socket.sendto(str(temp).encode(), (host, port))
        logger.info("Sent temperature: %.2f%s to fan control service", temp, TEMPERATURE_UNIT)
    except Exception as e:
        logger.error("Error sending temperature: %s", e, exc_info=True)


def log_sensor_data(db_operations):
    """
    Fetches sensor data, logs it, saves it to the database, and controls the mister.
    """
    queue = multiprocessing.Queue() # Create a Queue for communication
    sensor_fetcher = TerrariumStatus()
    

    try:
        process = multiprocessing.Process(
            target=fetch_sensor_data_process, args=(queue,)
        )
        process.start()
        process.join(timeout=PROCESS_TIMEOUT) # Wait for spedified time before killing the process.

        if process.is_alive():
            process.terminate() # Forcefully terminate the process
            process.join()  # Ensure termination
            error_message = (
                f"Sensor data retrieval timed out after {PROCESS_TIMEOUT} seconds. "
                "Terminating program."
            )
            logger.error(error_message)
            os._exit(1)  # Forcefully terminate script completely

        # Retrieve data from the queue
        if not queue.empty():
            sensor_data = queue.get() #Fetch data from queue
            #  Create the raw_data dictionary.
            # raw_data = {
            #     "temperature_celsius": temperature,
            #     "temperature_fahrenheit": temperature_fahrenheit,
            #     "humidity": humidity,
            #     #  Add other sensor readings here as needed
            # }
            raw_data = json.dumps(sensor_data)
            # Get the current timestamp in ISO 8601 format
            timestamp = datetime.now().isoformat()
            # Fetch sensor ID by name
            sensor_queries = SensorsQueries(db_operations)
            sensor_data_queries = SensorDataQueries(db_operations)
            # sensor_details = sensor_info.get_sensor_by_name(sensor_name='HTU21D')
            sensor_details = sensor_queries.get_sensor_by_name(sensor_fetcher.sensor_name)

            if isinstance(sensor_details, dict) and "sensor_id" in sensor_details:
                sensor_id =  sensor_details["sensor_id"]
            else:
                sensor_id = 1
            
            sensor_data_queries.insert_sensor_reading(sensor_id, timestamp, raw_data)

            log_message = (
                f"Processed and persisted sensor data: Temperature: {sensor_data['temperature_fahrenheit']:.2f}{TEMPERATURE_UNIT}, "
                f"Humidity: {sensor_data['humidity_percentage']:.2f}%"
            )
            logger.info(log_message)

            # send_temperature(temperature)  # Send the temperature

            # if humidity < HUMIDITY_THRESHOLD:
            #     mister_controller = MisterController()
            #     mister_controller.mist_control(duration=MOTOR_DURATION)

        else:
            logger.error("No data received from sensor process.")

    except Exception as e:
        logger.error("Error in log_sensor_data: %s", e, exc_info=True)
        os._exit(1)  # Terminate on any unhandled exception


def fetch_sensor_data_process(queue: multiprocessing.Queue):
    """
    Helper function to run in a separate process for fetching sensor data.

    Args:
        queue (multiprocessing.Queue): The queue to put the fetched data into.
    """
    try:
        sensor_fetcher = TerrariumStatus()
        sensor_data = sensor_fetcher.fetch_data()
        queue.put(sensor_data)
    except Exception:
        # Log the error within the subprocess.  The parent process is responsible
        # for handling the timeout and deciding whether to terminate.  We log
        # here to ensure the error is captured *within* the subprocess.
        logger.error("Error in fetch_sensor_data_process", exc_info=True)
        # No need to os._exit here.  The parent process handles termination.
        raise  # Re-raise the exception so the parent process knows there was an error.


def main():
    """Main entry point of the script."""
    db_operations = DatabaseOperations()
    db_operations.connect

    log_sensor_data(db_operations)


if __name__ == "__main__":
    main()


'''
Inside the TerrariumStatus Class:

* __init__(self) (Constructor): This special method is called when you create a new instance of the TerrariumStatus class 
    (e.g., sensor_fetcher = TerrariumStatus()). Its purpose is to initialize that specific instance. In this case, it sets up the connection 
    to the sensor using I2C. It defines how a TerrariumStatus object is set up. It's about the object's initial state.

fetch_data(self): This method is responsible for retrieving the temperature and humidity readings from the sensor. It relies on the sensor object 
    (self.sensor) that was initialized in __init__. It's a behavior that a TerrariumStatus object performs. It's about what a TerrariumStatus object does.

Key point: Both __init__ and fetch_data operate on the specific sensor that a TerrariumStatus object is connected to. They are instance-specific.

Outside the TerrariumStatus Class:

send_temperature(temp, host, port): This function takes the temperature data and sends it to another system (the fan controller). 
    It doesn't need to know anything about the TerrariumStatus object itself. It's a general-purpose function for sending data. 
    It's an action that uses the temperature data.

log_sensor_data(): This function is the orchestrator. It uses the TerrariumStatus to get the data, but it also handles other tasks: 
    saving to the database, and controlling the motor. It's at a higher level of logic, managing the overall process. 
    It uses the class to get part of its job done.

fetch_sensor_data_process(queue): This function is a helper for multiprocessing. It's designed to run in a separate process, create a TerrariumStatus, 
get the data, and pass the data back. It's about how to run the data fetching, not the fetching itself.

main(): This function is the main entry point of the script.

Analogy:

Think of a car:

The TerrariumStatus class is like the car's engine.

__init__ is like assembling the engine.

fetch_data is like the engine running and producing power.

The functions outside the class are like:

send_temperature: The car's speedometer sending speed information to a display.

log_sensor_data: A driver who starts the engine, drives the car, and records the journey.

fetch_sensor_data_process: A mechanic who puts the engine on a test bench to measure its output.

main(): Starting the car.

The engine (class) has its own functions, and the driver (other functions) uses the engine to achieve a goal.
'''