# src/utilities/config.py
import os
import sys
import configparser

# from src.utilities.path_utils import get_project_root
from utilities.src.path_utils import PathUtils
# from utilities.src.logger import LogHelper

# logger = LogHelper.get_logger(__name__)

class Config:
    """
    A base class to manage configuration settings from both a general config.ini
    and a secrets-specific config_secrets.ini file.
    """
    def __init__(self, config_file: str = 'config.ini', config_secrets_file: str = 'config_secrets.ini'):

        self.config = configparser.ConfigParser()
        self.config_secrets = configparser.ConfigParser()

        config_path = PathUtils.get_config_path()
        if not os.path.exists(config_path):
            # logger.critical(f"FATAL: Main config file not found at {config_path}")
            raise FileNotFoundError(f"Config file not found at {config_path}")
        self.config.read(config_path)

        config_secrets_path = PathUtils.get_config_secrets_path()
        if not os.path.exists(config_secrets_path):
            # logger.critical(f"FATAL: Main config file not found at {config_path}")
            raise FileNotFoundError(f"secrets config file not found at {config_secrets_path}")
        self.config_secrets.read(config_secrets_path)

    def _get_value_from_parser(self, parser, section: str, option: str, default, target_type):
        """
        Helper method to retrieve and convert an option from a specific parser.
        Handles type conversion and value errors.
        """
        if not parser.has_option(section, option):
            return default

        try:
            if target_type is bool:
                return parser.getboolean(section, option)
            elif target_type is int:
                return parser.getint(section, option)
            elif target_type is float:
                return parser.getfloat(section, option)
            else: # Default to string
                return parser.get(section, option)
        except ValueError as e:
            # logger.error(f"Warning: Failed to convert option '{option}' in section '{section}' to type '{target_type.__name__}'. Error: {e}. Returning default.", file=sys.stderr)
            return default
        except configparser.Error as e:
            # logger.error(f"Warning: Error reading option '{option}' in section '{section}'. Error: {e}. Returning default.", file=sys.stderr)
            return default

    def get(self, section, option, default=None, target_type=str):
        """
        Retrieves an option, prioritizing the secrets file, then the main config file.
        Includes optional type conversion.

        Args:
            section (str): The name of the section.
            option (str): The name of the option.
            default: The default value to return if the option is not found.
            target_type: The data type to convert the option value to (e.g., str, int, float, bool).
                         Defaults to str.

        Returns:
            Any: The value of the option, converted to the specified type,
                 or the default value if the option or section is not found.
        """

        # Prioritize secrets config
        value = self._get_value_from_parser(self.config_secrets, section, option, None, target_type)
        if value is not None: # If found in secrets, return it
            return value

        # Otherwise, check config
        value = self._get_value_from_parser(self.config, section, option, default, target_type)
        return value
    
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
            return {} # Return empty dict if section not found
        
    def get_secret_section(self, section):
        """
        Retrieves a section from the configuration.

        Args:
            section (str): The name of the section.

        Returns:
            dict: A dictionary containing the key-value pairs of the section,
                  or an empty dictionary if the section is not found.
        """
        if self.config_secrets.has_section(section):
            return dict(self.config_secrets.items(section))
        else:
            return {} # Return empty dict if section not found

class DatabaseConfig(Config):
    """
    A subclass of Config specifically for database settings.
    """
    def __init__(self):#, config_file='config.ini', config_secret_file = 'config_secrets.ini'):
        super().__init__()
        db_section      = 'database'  # Consistent section name

        # Main application database user (for connecting to Vivarium DB)
        self.user = self.get(db_section, 'user', default='ibis')
        self.password = self.get(db_section, 'password', default='default_app_password') # Ensure this comes from secrets
        self.dbname = self.get(db_section, 'dbname', default='vivarium')
        self.host = self.get(db_section, 'host', default='localhost')
        self.port = self.get(db_section, 'port', default=5432, target_type=int)

        # Remote user (e.g., 'calcasmy' for general remote access or setup)
        self.remote_user = self.get(db_section, 'remote_user', default='calcasmy')
        self.remote_password = self.get(db_section, 'remote_password', default='') # Remote user password from secrets
        self.remote_host = self.get(db_section, 'remote_host', default='192.168.6872')
        self.remote_dbname = self.get(db_section, 'remote_dbname', default='vivarium')

        # Superuser (e.g., 'postgres' for creating users/databases)
        self.superuser = self.get(db_section, 'super_user', default='postgres')
        self.superuser_password = self.get(db_section, 'super_password', default='') # Superuser password from secrets
        self.superuser_dbname = self.get(db_section, 'super_dbname', default='postgres')

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
    
    @property
    def postgres_super(self):
        """Returns a dictionary of PostgreSQL connection parameters."""
        return {
            'superuser': self.superuser,
            'superhost': self.remote_host,
            'superport': self.port,
            'superpassword': self.superuser_password,
            'superdbname': self.superuser_dbname
        }
    
    @property
    def postgres_remote(self):
        """Returns a dictionary of PostgreSQL connection parameters."""
        return {
            'user': self.remote_user,
            'host': self.remote_host,
            'port': self.port,
            'password': self.remote_password,
            'dbname': self.remote_dbname
        }

class WeatherAPIConfig(Config):
    """
    A subclass of Config for Weather API settings.
    """
    def __init__(self):
        super().__init__()
        api_section = 'weather_api'
        self.url = self.get(api_section, 'weather_api_url', default='https://api.weatherapi.com/v1')
        self.api_key = self.get(api_section, 'weather_api_key', default=None)
        self.lat_long = self.get(api_section, 'weather_api_lat_long', default='5.98,116.07')
        self.location_name = self.get(api_section, 'weather_loc_kinabalu', default='Gunung Kinabalu')
        self.fetch_interval = self.get(api_section, 'weather_fetch_interval', default=3, target_type = int)

class FileConfig(Config):
    """
    A subclass for file path settings
    """
    def __init__(self):
        super().__init__()
        file_section = 'FileSection'
        self.absolute_path = self.get(file_section, 'absolute_path', default = '')
        self.log_folder = self.get(file_section, 'logsfolder', default = 'logs')
        self.notes_folder = self.get(file_section, 'notesfolder', default = 'notes')
        self.raw_folder = self.get(file_section, 'rawfielfolder', default = 'rawfiles')

class TimeConfig(Config):
    """
    A subclass for time span settings
    """
    def __init__(self):
        super().__init__()
        time_section = 'TimeSecction'
        self.motor_span = self.get(time_section, 'motor', default = 360, target_type = int)
        self.light_span = self.get(time_section, 'light', default = 1, target_type = int)
        self.process_term_span = self.get(time_section, 'process_term', default = 10, target_type = int)

class LogConfig(Config):
    """
    A subclass for log settings
    """
    def __init__(self):
        super().__init__()
        log_section = 'LogSection'
        self.max_bytes = self.get(log_section, 'max_bytes', default = 5242880, target_type = int)
        self.backup_count = self.get(log_section, 'backup_count', default = 5, target_type  = int)

class ExhaustConfig(Config):
    """
    A subclass for fan settings
    """
    def __init__(self):
        super().__init__()
        fan_section = 'exhaust'
        self.off_speed = self.get(fan_section, 'off', default = 0, target_type = int)
        self.low_speed = self.get(fan_section, 'low', default = 30, target_type = int)
        self.medium_speed = self.get(fan_section, 'med', default = 60, target_type = int)
        self.high_speed = self.get(fan_section, 'high', default = 85, target_type = int)
        self.max_speed = self.get(fan_section, 'max', default = 100, target_type = int)
        self.pwm_controlpin = self.get(fan_section, 'pwm_controlpin', default = 12, target_type = int)
        self.rpm_controlpin = self.get(fan_section, 'rpm_controlpin', default = 12, target_type = int)

class TempConfig(Config):
    """
    A subclass for temperature settings
    """
    def __init__(self):
        super().__init__()
        temp_section = 'temperature'
        self.low_temp = self.get(temp_section, 'low', default = 70, target_type = int)
        self.medium_temp = self.get(temp_section, 'med', default = 80, target_type = int)
        self.high_temp = self.get(temp_section, 'high', default = 90, target_type = int)
        self.max_temp = self.get(temp_section, 'max', default = 100, target_type = int)

class GPIOConfig(Config):
    """
        A subclass for GPIO settings
    """
    def __init__(self):
        super().__init__()
        gpio_section = 'GPIOSection'
        self.pwm_pin = self.get(gpio_section, 'pwm', default = 12, target_type = int)
        self.rpm_pin = self.get(gpio_section, 'rpm', default = 16, target_type = int)
        self.motor_control_pin = self.get(gpio_section, 'mcontrol', default = 21, target_type = int)
        self.light_control_pin = self.get(gpio_section, 'lcontrol', default = 20, target_type = int)

class MisterConfig(Config):
    '''
        A subclass for Mister Settings
    '''
    def __init__(self, config_file = 'config.ini'):
        super().__init__()
        mister_section = 'mister'
        self.mister_control_pin = self.get(mister_section, 'controlpin', default = 21, target_type = int)
        self.humidity_threshold = self.get(mister_section, 'hu_threshold', default = 80, target_type = int)
        self.mister_duration = self.get(mister_section, 'duration', default = 30, target_type = int)
        self.mister_interval = self.get(mister_section, 'interval', default = 360, target_type = int)
        self.device_id = self.get(mister_section, 'device_id', default = 2, target_type = int)

class LightConfig(Config):
    '''
        A subclass for Light Settings
    '''
    def __init__(self, config_file = 'config.ini'):
        super().__init__()
        light_section = 'growlight'
        self.lights_control_pin = self.get(light_section, 'controlpin', default = 20, target_type = int)
        self.lights_on = self.get(light_section, 'on', default = "6:00 AM")
        self.lights_off = self.get(light_section, 'off', default = "6:00 PM")
        self.device_id = self.get(light_section, 'device_id', default = 1, target_type = int)

class HumidifierConfig(Config):
    '''
    A subclass for Humidifier Settings
    '''
    def __init__(self, config_file = 'config.ini'):
        super().__init__()
        humidifier_section = 'humidifier'
        self.username = self.get(humidifier_section, 'username', default = 'username', target_type = str)
        self.password = self.get(humidifier_section, 'password', default = 'password', target_type = str)
        self.mode = self.get(humidifier_section, 'mode', default = 'manual', target_type = str)
        self.device_id = self.get(humidifier_section, 'device_id', default = 3, target_type = int)
        self.runtime = self.get(humidifier_section, 'runtime', default = 30, target_type = int)
        self.mistlevel_low = self.get(humidifier_section, 'mistlevel_low', default = 1, target_type = int)
        self.mislevel_medium = self.get(humidifier_section, 'mislevel_medium', default = 5, target_type = int)
        self.mistlevel_high = self.get(humidifier_section, 'mistlevel_high', default = 9, target_type = int)
        

class SensorConfig(Config):
    """
    A subclass for Sensor IDs
    """
    def __init__(self, config_file = 'config.ini'):
        super().__init__()
        sensor_section = 'sensor'
        self.HTU21D = self.get(sensor_section, 'HTU21D-F', default = 1, target_type = int)