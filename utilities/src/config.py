# src/utilities/config.py
import configparser
import os

# from src.utilities.path_utils import get_project_root
from utilities.src.path_utils import PathUtils

class Config:
    """
    A class to manage configuration settings. It loads settings from a config.ini file.
    """
    def __init__(self, config_file: str = 'config.ini'):

        self.config = configparser.ConfigParser()
        # Construct the full path to config.ini
        # project_root = get_project_root()
        # config_path = os.path.join(project_root, config_file)
        config_path = PathUtils.get_config_path()

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found at {config_path}")
        self.config.read(config_path)

    def get_section(self, section):
        """
        Retrieves a section from the configuration.

        Args:
            section (str): The name of the section.

        Returns:
            dict: A dictionary containing the key-value pairs of the section,
                  or an empty dictionary if the section is not found.
        """
        if self.config.has_section(section):
            return dict(self.config.items(section))
        else:
            return {}

    def get(self, section, option, default=None, type=str):
        """
        Retrieves an option from a section, with optional type conversion.

        Args:
            section (str): The name of the section.
            option (str): The name of the option.
            default: The default value to return if the option is not found.
            type: The data type to convert the option value to (e.g., str, int, float, bool).
                  Defaults to str.

        Returns:
            Any: The value of the option, converted to the specified type,
                 or the default value if the option or section is not found.
        """
        try:
            value = self.config.get(section, option)
            if type is bool:
                value = self.config.getboolean(section, option)
            elif type is int:
                value = self.config.getint(section, option)
            elif type is float:
                value = self.config.getfloat(section, option)
            return value
        except (configparser.NoSectionError, configparser.NoOptionError):
            return default
        except ValueError:
            print(f"Warning: Could not convert option '{option}' in section '{section}' to type '{type.__name__}'.  Returning default value.")
            return default

class DatabaseConfig(Config):
    """
    A subclass of Config specifically for database settings.
    """
    def __init__(self, config_file='config.ini'):
        super().__init__(config_file)
        db_section      = 'database'  # Consistent section name
        self.dbname     = self.get(db_section, 'dbname', default='vivarium')
        self.host       = self.get(db_section, 'host', default='localhost')
        self.user       = self.get(db_section, 'user', default='ibis')
        self.password   = self.get(db_section, 'password', default='xxxxxx123456789xxxxxxxxxxxx')
        self.port       = self.get(db_section, 'pport', default=5432, type=int) #added type
        # self.sslmode = self.get(db_section, 'sslmode', default='require') # Removed sslmode

    @property
    def postgres(self):
        """Returns a dictionary of PostgreSQL connection parameters."""
        return {
            'dbname': self.dbname,
            'host': self.host,
            'user': self.user,
            'password': self.password,
            'port': self.port,
            # 'sslmode': self.sslmode, #Removed sslmode
        }

class WeatherAPIConfig(Config):
    """
    A subclass of Config for Weather API settings.
    """
    def __init__(self, config_file='config.ini'):
        super().__init__(config_file)
        api_section = 'api'
        self.url = self.get(api_section, 'weather_api_url', default='https://api.weatherapi.com/v1')
        self.api_key = self.get(api_section, 'weather_api_key', default=None)
        self.lat_long = self.get(api_section, 'weather_api_lat_long', default='5.98,116.07')
        self.location_name = self.get(api_section, 'weather_loc_kinabalu', default='Gunung Kinabalu')


class FileConfig(Config):
    """
    A subclass for file path settings
    """
    def __init__(self, config_file='config.ini'):
        super().__init__(config_file)
        file_section = 'FileSection'
        self.absolute_path = self.get(file_section, 'file.absolute_path', default = '')
        self.log_folder = self.get(file_section, 'file.logsfolder', default = 'logs')
        self.notes_folder = self.get(file_section, 'file.notesfolder', default = 'notes')
        self.raw_folder = self.get(file_section, 'file.rawfielfolder', default = 'rawfiles')

class TimeConfig(Config):
    """
    A subclass for time span settings
    """
    def __init__(self, config_file='config.ini'):
        super().__init__(config_file)
        time_section = 'TimeSecction'
        self.motor_span = self.get(time_section, 'span.motor', default = 360, type=int)
        self.light_span = self.get(time_section, 'span.light', default = 1, type=int)
        self.process_term_span = self.get(time_section, 'span.process_term', default = 10, type=int)

class LogConfig(Config):
    """
    A subclass for log settings
    """
    def __init__(self, config_file='config.ini'):
        super().__init__(config_file)
        log_section = 'LogSection'
        self.max_bytes = self.get(log_section, 'log.max_bytes', default = 5242880, type = int)
        self.backup_count = self.get(log_section, 'log.backup_count', default = 5, type = int)

class ExhaustConfig(Config):
    """
    A subclass for fan settings
    """
    def __init__(self, config_file='config.ini'):
        super().__init__(config_file)
        fan_section = 'exhaust'
        self.off_speed = self.get(fan_section, 'exhaust.off', default = 0, type = int)
        self.low_speed = self.get(fan_section, 'exhaust.low', default = 30, type = int)
        self.medium_speed = self.get(fan_section, 'exhaust.med', default = 60, type = int)
        self.high_speed = self.get(fan_section, 'exhaust.high', default = 85, type = int)
        self.max_speed = self.get(fan_section, 'exhaust.max', default = 100, type = int)
        self.pwm_controlpin = self.get(fan_section, 'exhaust.pwm_controlpin', default = 12, type = int)
        self.rpm_controlpin = self.get(fan_section, 'exhaust.rpm_controlpin', default = 12, type = int)

class TempConfig(Config):
    """
    A subclass for temperature settings
    """
    def __init__(self, config_file='config.ini'):
        super().__init__(config_file)
        temp_section = 'temperature'
        self.low_temp = self.get(temp_section, 'temperature.low', default = 70, type = int)
        self.medium_temp = self.get(temp_section, 'temperature.med', default = 80, type = int)
        self.high_temp = self.get(temp_section, 'temperature.high', default = 90, type = int)
        self.max_temp = self.get(temp_section, 'temperature.max', default = 100, type = int)

class GPIOConfig(Config):
    """
        A subclass for GPIO settings
    """
    def __init__(self, config_file='config.ini'):
        super().__init__(config_file)
        gpio_section = 'GPIOSection'
        self.pwm_pin = self.get(gpio_section, 'gpio.pwm', default = 12, type = int)
        self.rpm_pin = self.get(gpio_section, 'gpio.rpm', default = 16, type = int)
        self.motor_control_pin = self.get(gpio_section, 'gpio.mcontrol', default = 21, type = int)
        self.light_control_pin = self.get(gpio_section, 'gpio.lcontrol', default = 20, type=int)

class MisterConfig(Config):
    '''
        A subclass for Mister Settings
    '''
    def __init__(self, config_file = 'config.ini'):
        super().__init__(config_file)
        mister_section = 'mister'
        self.mister_control_pin = self.get(mister_section, 'mister.controlpin', default = 21, type = int)
        self.humidity_threshold = self.get(mister_section, 'mister.hu_threshold', default = 80, type = int)
        self.mister_duration = self.get(mister_section, 'duration', default = 30, type = int)
        self.mister_interval = self.get(mister_section, 'interval', default = 360, type = int)

class LightConfig(Config):
    '''
        A subclass for Light Settings
    '''
    def __init__(self, config_file = 'config.ini'):
        super().__init__(config_file)
        light_section = 'growlight'
        self.lights_control_pin = self.get(light_section, 'growlight.controlpin', default = 20, type = int)
        self.lights_on = self.get(light_section, 'growlight.on', default = "6:00 AM")
        self.lights_off = self.get(light_section, 'growlight.off', default = "6:00 PM")

class HumidifierConfig(Config):
    '''
    A subclass for Humidifier Settings
    '''
    def __init__(self, config_file = 'config.ini'):
        super().__init__(config_file)
        humidifier_section = 'humidifier'
        self.username = self.get(humidifier_section, 'humidifier.username', default = 'username', type = str)
        self.password = self.get(humidifier_section, 'humidifier.password', default = 'password', type = str)


