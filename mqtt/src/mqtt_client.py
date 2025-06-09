import os
import sys
import ssl
import time
import json
import random
import paho.mqtt.client as mqtt
from datetime import datetime

# Adjust vivarium_path based on the actual file location
# Assuming this file is in vivarium/mqtt/src/
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

# Import your MQTTConfig class
from utilities.src.config import MQTTConfig

class VivariumMqttClient:
    """
    A class to manage the MQTT client connection, publishing, and subscribing
    for the Vivarium project.
    """

    def __init__(self, on_message_callback=None, client_id="VivariumPiClient"):
        """
        Initializes the MQTT client with configuration and sets up callbacks.

        Args:
            on_message_callback (callable, optional): A function to call when a message is received.
                                                    It should accept (client, userdata, msg) as arguments.
                                                    If None, a default print handler is used.
            client_id (str): A unique client ID for the MQTT connection.
        """
        self.config = MQTTConfig() # Instance of your MQTTConfig class

        # Load MQTT details from config
        self.broker = self.config.get('MQTT_BROKER')
        self.port = self.config.get('MQTT_PORT')
        self.username = self.config.get('MQTT_USERNAME')
        self.password = self.config.get('MQTT_PASSWORD')
        self.data_topic = self.config.get('DATA_TOPIC')
        self.command_topic = self.config.get('COMMAND_TOPIC')

        self.client_id = client_id
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, self.client_id)

        # Set up authentication and TLS
        self.client.username_pw_set(self.username, self.password)
        self.client.tls_set(tls_version=ssl.PROTOCOL_TLS)

        # Assign internal callback methods
        self.client.on_connect = self._on_connect
        self.client.on_message = on_message_callback if on_message_callback else self._default_on_message

        print(f"MQTT Client initialized for broker: {self.broker}:{self.port}")

    def _on_connect(self, client, userdata, flags, rc):
        """Internal callback for when the client connects to the broker."""
        if rc == 0:
            print("Connected to MQTT Broker!")
            # Subscribe to the command topic after successful connection
            client.subscribe(self.command_topic)
            print(f"Subscribed to topic: {self.command_topic}")
        else:
            print(f"Failed to connect, return code {rc}\n")

    def _default_on_message(self, client, userdata, msg):
        """Default message handler if no custom callback is provided."""
        print(f"Received message on topic: {msg.topic} - Payload: {msg.payload.decode()}")
        # You can add a basic command interpretation here if you want
        # or rely entirely on the external on_message_callback

    def connect(self):
        """Attempts to connect the MQTT client to the broker."""
        try:
            self.client.connect(self.broker, self.port, 60)
            print(f"Attempting to connect to {self.broker}:{self.port}")
        except Exception as e:
            print(f"Connection failed: {e}")
            sys.exit(1) # Exit if connection fails critically

    def start_loop(self):
        """Starts the MQTT client's network loop in a background thread."""
        self.client.loop_start()
        print("MQTT client loop started.")

    def stop_loop(self):
        """Stops the MQTT client's network loop and disconnects."""
        self.client.loop_stop()
        self.client.disconnect()
        print("MQTT client loop stopped and disconnected.")

    def publish_message(self, topic: str, payload: dict):
        """
        Publishes a JSON message to a specified MQTT topic.
        """
        try:
            json_payload = json.dumps(payload)
            self.client.publish(topic, json_payload)
            print(f"Published to {topic}: {json_payload}")
        except Exception as e:
            print(f"Error publishing message: {e}")

    def publish_sensor_data(self, temperature: float, humidity: float, location_lat: float, location_lon: float):
        """
        Publishes simulated or actual sensor data.
        """
        sensor_data = {
            "timestamp": datetime.now().isoformat(),
            "temperature": temperature,
            "humidity": humidity,
            "location_lat": location_lat,
            "location_lon": location_lon
        }
        self.publish_message(self.data_topic, sensor_data)

# --- Define your specific on_message logic here (outside the class for flexibility) ---
def vivarium_command_handler(client, userdata, msg):
    """
    Handles incoming command messages for the vivarium.
    This function will be passed to the VivariumMqttClient.
    """
    print(f"Handling command: {msg.topic} - {msg.payload.decode()}")
    try:
        command = json.loads(msg.payload.decode())
        if msg.topic == client.command_topic: # Use client's stored topic for verification
            action = command.get("action")
            if action == "light_on":
                print("Executing: Turn vivarium lights ON")
                # Add your GPIO control code here for lights
            elif action == "light_off":
                print("Executing: Turn vivarium lights OFF")
                # Add your GPIO control code here for lights
            elif action == "set_fan_speed":
                speed = command.get("value")
                print(f"Executing: Set fan speed to {speed}%")
                # Add your GPIO/PWM control code here for fan
            else:
                print(f"Unknown command action: {action}")
        else:
            print(f"Message on unexpected topic: {msg.topic}")
    except json.JSONDecodeError:
        print("Received non-JSON command message, skipping.")
    except Exception as e:
        print(f"Error processing command message: {e}")


# --- Main execution block to demonstrate usage ---
if __name__ == "__main__":
    # Ensure your MQTTConfig file is correctly set up with these keys:
    # MQTT_BROKER, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD, DATA_TOPIC, COMMAND_TOPIC
    # For testing, remember to replace placeholders in your config or hardcode them temporarily if testing standalone.

    try:
        # Create an instance of your MQTT client, passing the custom command handler
        vivarium_client = VivariumMqttClient(on_message_callback=vivarium_command_handler)

        # Connect to the MQTT broker
        vivarium_client.connect()

        # Start the background loop for sending/receiving messages
        vivarium_client.start_loop()

        # Simulate continuous operation and data publishing
        print("Simulating sensor data publishing. Press Ctrl+C to exit.")
        while True:
            # Simulate sensor data
            temperature = round(random.uniform(20.0, 30.0), 2)
            humidity = round(random.uniform(50.0, 80.0), 2)

            # Publish the sensor data using the class method
            vivarium_client.publish_sensor_data(temperature, humidity, 5.983, 116.067)

            time.sleep(10) # Publish every 10 seconds

    except KeyboardInterrupt:
        print("\nCtrl+C detected. Shutting down MQTT client.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if 'vivarium_client' in locals() and vivarium_client.client.is_connected():
            vivarium_client.stop_loop()
            print("MQTT client gracefully shut down.")