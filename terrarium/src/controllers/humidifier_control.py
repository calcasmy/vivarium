# /Users/neptune/Development/vivarium/terrarium/src/testhumidi/your_script_name.py

import sys
import os

# Get the absolute path to the 'vivarium' directory
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Add 'vivarium' to the Python path if it's not already there
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

from assets.humidifier.src import vesync
from assets.humidifier.src.vesyncclassic300s import VeSyncHumidClassic300S

# Replace with your VeSync account details
USERNAME = "technoatomic@gmail.com"
PASSWORD = "Cold_Snow#25"

class HumidiferController:
    def __init__(self):
        pass

    @staticmethod
    def script_path() -> str:
        '''Returns the Absolute path of the script'''
        return os.path.abspath(__file__)

def control_vivarium_humidifier():
    """Connects to VeSync, finds a Classic 300S, and allows control."""
    try:
        manager = vesync.VeSync(USERNAME, PASSWORD)
        login_success = manager.login()
        if login_success:
            print("Successfully logged into VeSync.")
            manager.get_devices()
            humidifier = None
            for device in manager.fans:
                if isinstance(device, VeSyncHumidClassic300S):
                    humidifier = device
                    break

            if humidifier:
                print(f"Found your Levoit Classic 300S: {humidifier.device_name}")
                control_menu(humidifier)
            else:
                print("Could not find a Levoit Classic 300S humidifier.")

        else:
            print("Login to VeSync failed.")

    except Exception as e:
        print(f"An error occurred: {e}")

def control_menu(humidifier: VeSyncHumidClassic300S):
    """Presents a menu to control the humidifier."""
    """
    Humidifier Control Menu:
    1. Turn On
    2. Turn Off
    3. Set Mist Level (1-9)
    4. Set Mode (auto, sleep, manual)
    5. Set Target Humidity (30-80%)
    6. Set Night Light Brightness (0-100%)
    7. Get Status
    8. Exit
    """
    while True:
        print("\nHumidifier Control Menu:")
        print("1. Turn On")
        print("2. Turn Off")
        print("3. Set Mist Level (1-9)")
        print("4. Set Mode (auto, sleep, manual)")
        print("5. Set Target Humidity (30-80%)")
        if humidifier.night_light:
            print("6. Set Night Light Brightness (0-100%)")
        print("7. Get Status")
        print("8. Exit")

        choice = input("Enter your choice: ")

        if choice == '1':
            if humidifier.turn_on():
                print("Humidifier turned on.")
            else:
                print("Failed to turn on humidifier.")
        elif choice == '2':
            if humidifier.turn_off():
                print("Humidifier turned off.")
            else:
                print("Failed to turn off humidifier.")
        elif choice == '3':
            level = input("Enter mist level (1-9): ")
            try:
                level = int(level)
                if 1 <= level <= 9:
                    if humidifier.set_mist_level(level):
                        print(f"Mist level set to {level}.")
                    else:
                        print("Failed to set mist level.")
                else:
                    print("Invalid mist level. Please enter a value between 1 and 9.")
            except ValueError:
                print("Invalid input. Please enter a number.")
        elif choice == '4':
            mode = input("Enter mode (auto, sleep, manual): ").lower()
            if mode in humidifier.mist_modes:
                if humidifier.set_humidity_mode(mode):
                    print(f"Mode set to {mode}.")
                else:
                    print("Failed to set mode.")
            else:
                print(f"Invalid mode. Available modes: {humidifier.mist_modes}")
        elif choice == '5':
            humidity = input("Enter target humidity (30-80%): ")
            try:
                humidity = int(humidity)
                if 30 <= humidity <= 80:
                    if humidifier.set_humidity(humidity):
                        print(f"Target humidity set to {humidity}%.")
                    else:
                        print("Failed to set target humidity.")
                else:
                    print("Invalid humidity. Please enter a value between 30 and 80.")
            except ValueError:
                print("Invalid input. Please enter a number.")
        elif choice == '6' and humidifier.night_light:
            brightness = input("Enter night light brightness (0-100%): ")
            try:
                brightness = int(brightness)
                if 0 <= brightness <= 100:
                    if humidifier.set_night_light_brightness(brightness):
                        print(f"Night light brightness set to {brightness}%.")
                    else:
                        print("Failed to set night light brightness.")
                else:
                    print("Invalid brightness. Please enter a value between 0 and 100.")
            except ValueError:
                print("Invalid input. Please enter a number.")
        elif choice == '7':
            humidifier.update()
            humidifier.display()
        elif choice == '8':
            print("Exiting control menu.")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    control_vivarium_humidifier()