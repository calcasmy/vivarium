# utilities/src/config.py
import os
import sys
import configparser
from dataclasses import dataclass, field
from typing import Optional, Dict, Any

# Ensure vivarium root path is in sys.path to resolve imports correctly
# This block must be at the very top, before other project-specific imports.
# It allows the script to be run from any directory within the project structure.
vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vivarium_path not in sys.path:
    sys.path.insert(0, vivarium_path)

from utilities.src.path_utils import PathUtils
from utilities.src.db_operations import ConnectionDetails

class Config:
    """
    A base class to manage configuration settings from both a general config.ini
    and a secrets-specific config_secrets.ini file.
    """
    def __init__(self, config_file: str = 'config.ini', config_secrets_file: str = 'config_secrets.ini'):
        """
        Initializes the Config class, loading settings from configuration files.

        :param config_file: The name of the main configuration file (e.g., 'config.ini').
        :type config_file: str
        :param config_secrets_file: The name of the secrets configuration file (e.g., 'config_secrets.ini').
        :type config_secrets_file: str
        """
        self.config = configparser.ConfigParser()
        self.config_secrets = configparser.ConfigParser()

        # PathUtils is expected to return the full path to the config file
        self._load_config_file(self.config, PathUtils.get_config_path())
        self._load_config_file(self.config_secrets, PathUtils.get_config_secrets_path())

    def _load_config_file(self, parser, file_path: str):
        """
        Helper method to load a configuration file into a ConfigParser object.

        :param parser: The ConfigParser instance to load the file into.
        :type parser: configparser.ConfigParser
        :param file_path: The absolute path to the configuration file.
        :type file_path: str
        :raises FileNotFoundError: If the specified configuration file does not exist.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Configuration file '{file_path}' not found.")
        parser.read(file_path)

    def _get_value_from_parser(self, parser, section: str, option: str, default, target_type):
        """
        Helper method to retrieve and convert an option from a specific parser.
        Handles type conversion and value errors.

        :param parser: The ConfigParser instance to retrieve the value from.
        :type parser: configparser.ConfigParser
        :param section: The name of the section.
        :type section: str
        :param option: The name of the option.
        :type option: str
        :param default: The default value to return if the option is not found or conversion fails.
        :type default: Any
        :param target_type: The data type to convert the option value to (e.g., str, int, float, bool).
        :type target_type: type
        :returns: The value of the option, converted to the specified type,
                  or the default value.
        :rtype: Any
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
        except ValueError:
            # Fallback to default if conversion fails
            return default
        except configparser.Error:
            # Fallback to default for general parser errors
            return default
        
    def get(self, section: str, option: str, default=None, target_type=str, is_secret: bool = False):
        """
        Retrieves an option, prioritizing the secrets file if is_secret is True,
        otherwise checking the main config file first.

        :param section: The name of the section.
        :type section: str
        :param option: The name of the option.
        :type option: str
        :param default: The default value to return if the option is not found.
        :type default: Any
        :param target_type: The data type to convert the option value to (e.g., str, int, float, bool).
                            Defaults to str.
        :type target_type: type
        :param is_secret: If True, prioritize looking in the secrets config first.
        :type is_secret: bool
        :returns: The value of the option, converted to the specified type,
                  or the default value if the option or section is not found.
        :rtype: Any
        """
        if is_secret:
            # Check secrets first
            value = self._get_value_from_parser(self.config_secrets, section, option, None, target_type)
            if value is not None:
                return value

        # Check main config (or fallback from secrets check)
        value = self._get_value_from_parser(self.config, section, option, default, target_type)
        return value
    
class DatabaseConfig(Config):
    """
    A subclass of Config specifically for PostgreSQL database settings.
    This class now provides ConnectionDetails objects for different Postgres connection types.
    """
    def __init__(self):
        """
        Initializes the DatabaseConfig, loading PostgreSQL-specific settings.
        """
        super().__init__()
        db_section = 'database'

        # Application user credentials (for connecting to Vivarium DB)
        self._app_user              = self.get(db_section, 'user', default='vivarium')
        self._app_password          = self.get(db_section, 'password', default='default_app_password', is_secret=True)
        self._app_dbname            = self.get(db_section, 'dbname', default='vivarium')
        
        # Local host and Port for local connections
        self._local_host            = self.get(db_section, 'host', default='localhost')
        self._local_port            = self.get(db_section, 'port', default=5432, target_type=int)

        # Remote host and Port for remote connections
        self._remote_host           = self.get(db_section, 'remote_host', default='192.168.68.50')
        self._remote_port           = self.get(db_section, 'remote_port', default=5432, target_type=int)

        # Superuser (e.g., 'postgres' for creating users/databases) - only needed for setup
        self._superuser             = self.get(db_section, 'super_user', default='postgres')
        self._superuser_dbname      = self.get(db_section, 'super_dbname', default='postgres')
        self._superuser_password    = self.get(db_section, 'super_password', default='', is_secret=True)

        # Determine if the target database is remote.
        # This will be used by PostgresSetup to select between local and remote connection details.
        self._is_remote_db = self.get(db_section, 'is_remote', default=False, target_type=bool)

    @property
    def app_user(self) -> str:
        """
        The username for the application's PostgreSQL database user.

        :returns: The application username.
        :rtype: str
        """
        return self._app_user

    @property
    def app_password(self) -> str:
        """
        The password for the application's PostgreSQL database user.

        :returns: The application password.
        :rtype: str
        """
        return self._app_password

    @property
    def app_dbname(self) -> str:
        """
        The name of the application's PostgreSQL database.

        :returns: The application database name.
        :rtype: str
        """
        return self._app_dbname

    @property
    def postgres_local_connection(self) -> ConnectionDetails:
        """
        Returns ConnectionDetails for a local PostgreSQL connection for the application user.

        :returns: Connection details for the local PostgreSQL database.
        :rtype: ConnectionDetails
        """
        return ConnectionDetails(
            user                    = self._app_user,
            password                = self._app_password,
            dbname                  = self._app_dbname,
            host                    = self._local_host,
            port                    = self._local_port
        )
    
    @property
    def postgres_remote_connection(self) -> ConnectionDetails:
        """
        Returns ConnectionDetails for a remote PostgreSQL connection for the application user.

        :returns: Connection details for the remote PostgreSQL database.
        :rtype: ConnectionDetails
        """
        return ConnectionDetails(
            user                    = self._app_user,
            password                = self._app_password,
            dbname                  = self._app_dbname,
            host                    = self._remote_host,
            port                    = self._remote_port
        )

    @property
    def postgres_superuser_connection(self) -> ConnectionDetails:
        """
        Returns ConnectionDetails for a PostgreSQL superuser connection to the superuser_dbname.

        The superuser password is included here if found in config_secrets.ini.
        Otherwise, the setup logic (e.g., PostgresSetup) will prompt for it.

        :returns: Connection details for the PostgreSQL superuser.
        :rtype: ConnectionDetails
        """
        return ConnectionDetails(
            user                    = self._superuser,
            password                = self._superuser_password,
            dbname                  = self._superuser_dbname,
            host                    = self._local_host,
            port                    = self._local_port
        )

class SupabaseConfig(Config):
    """
    A subclass of Config specifically for Supabase settings.
    This class provides a ConnectionDetails object for direct psycopg2 connections.
    It also provides the Supabase URL and keys for the Supabase client library.
    """
    def __init__(self):
        """
        Initializes the SupabaseConfig, loading Supabase-specific settings.
        """
        super().__init__()
        supabase_section = 'supabase'
        self.url                    = self.get(supabase_section, 'supabase_url', is_secret=True)
        self.service_key            = self.get(supabase_section, 'supabase_service_key', is_secret=True)
        self.anon_key               = self.get(supabase_section, 'supabase_anon_key', is_secret=True)
        self._user                  = self.get(supabase_section, 'supabaseipv4_user', is_secret=True)
        self._host                  = self.get(supabase_section, 'supabaseipv4_host', is_secret=True)
        self._port                  = self.get(supabase_section, 'supabaseipv4_port', is_secret=True, target_type=int)
        self._password              = self.get(supabase_section, 'supabaseipv4_password', is_secret=True)
        self._dbname                = self.get(supabase_section, 'supabaseipv4_dbname', is_secret=True)
        self.connstring             = self.get(supabase_section, 'supabaseipv4_connstring', is_secret=True)

        # Ensure critical Supabase credentials are not missing.
        if not self.url or not self.anon_key or not self.service_key:
            print("WARNING: One or more Supabase URL/Keys (anon, service) not found in secrets. Supabase client operations may fail.", file=sys.stderr)

        self.sslmode = self.get(supabase_section, 'sslmode', default='require')

    @property
    def supabase_connection_details(self) -> ConnectionDetails:
        """
        Returns ConnectionDetails for connecting to the Supabase database via psycopg2.

        :returns: Connection details for the Supabase database including URL, Anon Key, service Key and Connection String.
        :rtype: ConnectionDetails
        """
        extra_params = {
            'supabase_url': self.url,
            'supabase_anon_key': self.anon_key,
            'supabase_service_key': self.service_key,
            'supabase_connstring': self.connstring
        }

        return ConnectionDetails(
            user                    = self._user,
            password                = self._password,
            host                    = self._host,
            port                    = self._port,
            dbname                  = self._dbname,
            sslmode                 = self.sslmode,
            extra_params            = extra_params
        )

# All other config classes are retained as per instruction.
class SupabaseConfig_Service(Config):
    """
    A subclass of Config specifically for Supabase service settings.
    """
    def __init__(self):
        """
        Initializes the SupabaseConfig_Service, loading Supabase service key settings.
        """
        super().__init__()
        self.service_key = self.get('supabase', 'supabase_service_key', is_secret=True)
        if not self.service_key:
            print("WARNING: Supabase service_Key not found in secrets. Supabase operations may fail.", file=sys.stderr)

class WeatherAPIConfig(Config):
    """
    A subclass of Config for Weather API settings.
    """
    def __init__(self):
        """
        Initializes the WeatherAPIConfig, loading Weather API settings.
        """
        super().__init__()
        api_section = 'weather_api'
        self.url                    = self.get(api_section, 'weather_api_url', default='https://api.weatherapi.com/v1')
        self.api_key                = self.get(api_section, 'weather_api_key', default=None, is_secret=True)
        self.lat_long               = self.get(api_section, 'weather_api_lat_long', default='5.98,116.07')
        self.location_name          = self.get(api_section, 'weather_loc_kinabalu', default='Gunung Kinabalu')
        self.fetch_interval         = self.get(api_section, 'weather_fetch_interval', default=3, target_type = int)

class FileConfig(Config):
    """
    A subclass for file path settings
    """
    def __init__(self, 
                 schema_file_override: Optional[str] = None, 
                 supabase_schema_override: Optional[str] = None,
                 json_folder_override: Optional[str] = None,
                 processed_json_folder_override: Optional[str] = None,
                 data_file_override: Optional[str] = None):
        """
        Initializes the FileConfig, loading file path settings and applying optional overrides.

        :param schema_file_override: Optional path to override the default PostgreSQL schema file.
        :type schema_file_override: Optional[str]
        :param supabase_schema_override: Optional path to override the default Supabase schema file.
        :type supabase_schema_override: Optional[str]
        """
        super().__init__()
        file_section = 'FileSection'
        self.absolute_path          = self.get(file_section, 'absolute_path', default = '')
        self.log_folder             = self.get(file_section, 'logsfolder', default = 'logs')
        self.notes_folder           = self.get(file_section, 'notesfolder', default = 'notes')
        self.json_folder            = self.get(file_section, 'rawfielfolder', default = 'resources/rawfiles')
        self.data_file              = self.get(file_section, 'data_file', default = 'resources/postgres_sensors_devices_data.sql')
        self.schema_file            = self.get(file_section, 'schema_file', default = 'resources/postgres_schema.sql')
        self.supabase_schema        = self.get(file_section, 'supabase_schema', default = 'resources/supabase_schema.sql')
        self.processed_json_folder  = self.get(file_section, 'processed_json_folder', default = 'resources/processed_api_climatefiles')

        # Apply overrides if provided
        if schema_file_override is not None:
            self.schema_file = schema_file_override
        if supabase_schema_override is not None:
            self.supabase_schema = supabase_schema_override
        if json_folder_override is not None:
            self.json_folder = json_folder_override
        if processed_json_folder_override is not None:
            self.processed_json_folder = processed_json_folder_override
        if data_file_override is not None:
            self.data_file = data_file_override

class TimeConfig(Config):
    """
    A subclass for time span settings
    """
    def __init__(self):
        """
        Initializes the TimeConfig, loading time span settings.
        """
        super().__init__()
        time_section = 'TimeSecction'
        self.motor_span             = self.get(time_section, 'motor', default = 360, target_type = int)
        self.light_span             = self.get(time_section, 'light', default = 1, target_type = int)
        self.process_term_span      = self.get(time_section, 'process_term', default = 10, target_type = int)

class LogConfig(Config):
    """
    A subclass for log settings
    """
    def __init__(self):
        """
        Initializes the LogConfig, loading log settings.
        """
        super().__init__()
        log_section = 'LogSection'
        self.max_bytes              = self.get(log_section, 'max_bytes', default = 5242880, target_type = int)
        self.backup_count           = self.get(log_section, 'backup_count', default = 5, target_type  = int)

class ExhaustConfig(Config):
    """
    A subclass for fan settings
    """
    def __init__(self):
        """
        Initializes the ExhaustConfig, loading fan settings.
        """
        super().__init__()
        fan_section = 'exhaust'
        self.off_speed              = self.get(fan_section, 'off', default = 0, target_type = int)
        self.low_speed              = self.get(fan_section, 'low', default = 30, target_type = int)
        self.medium_speed           = self.get(fan_section, 'med', default = 60, target_type = int)
        self.high_speed             = self.get(fan_section, 'high', default = 85, target_type = int)
        self.max_speed              = self.get(fan_section, 'max', default = 100, target_type = int)
        self.pwm_controlpin         = self.get(fan_section, 'pwm_controlpin', default = 12, target_type = int)
        self.rpm_controlpin         = self.get(fan_section, 'rpm_controlpin', default = 16, target_type = int)
        self.device_id              = self.get(fan_section, 'device_id', default = 5, target_type = int)
        self.enabled                = self.get(fan_section, 'enabled', default = True, target_type = bool)
        self.device_type            = self.get(fan_section, 'device_type', default = 'exhaustfan', target_type = str)

class IntakeConfig(Config):
    """
    A subclass for intake settings
    """
    def __init__(self):
        """
        Initializes the IntakeConfig, loading intake settings.
        """
        super().__init__()
        intake_section = 'intake'
        self.off_speed              = self.get(intake_section, 'off', default = 0, target_type = int)
        self.low_speed              = self.get(intake_section, 'low', default = 30, target_type = int)
        self.medium_speed           = self.get(intake_section, 'med', default = 60, target_type = int)
        self.high_speed             = self.get(intake_section, 'high', default = 85, target_type = int)
        self.max_speed              = self.get(intake_section, 'max', default = 100, target_type = int)
        self.pwm_controlpin         = self.get(intake_section, 'pwm_controlpin', default = 13, target_type = int)
        self.rpm_controlpin         = self.get(intake_section, 'rpm_controlpin', default = 6, target_type = int)
        self.device_id              = self.get(intake_section, 'device_id', default = 6, target_type = int)
        self.enabled                = self.get(intake_section, 'enabled', default = True, target_type = bool)
        self.device_type            = self.get(intake_section, 'device_type', default = 'intakefan', target_type = str)

class TempConfig(Config):
    """
    A subclass for temperature settings
    """
    def __init__(self):
        """
        Initializes the TempConfig, loading temperature settings.
        """
        super().__init__()
        temp_section = 'temperature'
        self.low_temp               = self.get(temp_section, 'low', default = 70, target_type = int)
        self.medium_temp            = self.get(temp_section, 'med', default = 80, target_type = int)
        self.high_temp              = self.get(temp_section, 'high', default = 90, target_type = int)
        self.max_temp               = self.get(temp_section, 'max', default = 100, target_type = int)

class GPIOConfig(Config):
    """
    A subclass for GPIO settings
    """
    def __init__(self):
        """
        Initializes the GPIOConfig, loading GPIO settings.
        """
        super().__init__()
        gpio_section = 'GPIOSection'
        self.pwm_pin                = self.get(gpio_section, 'pwm', default = 12, target_type = int)
        self.rpm_pin                = self.get(gpio_section, 'rpm', default = 16, target_type = int)
        self.motor_control_pin      = self.get(gpio_section, 'mcontrol', default = 21, target_type = int)
        self.light_control_pin      = self.get(gpio_section, 'lcontrol', default = 20, target_type = int)

class MisterConfig(Config):
    '''
    A subclass for Mister Settings
    '''
    def __init__(self):
        """
        Initializes the MisterConfig, loading mister settings.
        """
        super().__init__()
        mister_section = 'mister'
        self.mister_control_pin     = self.get(mister_section, 'controlpin', default = 21, target_type = int)
        self.humidity_threshold     = self.get(mister_section, 'hu_threshold', default = 80, target_type = int)
        self.at_hour                = self.get(mister_section, 'at_hour', default = 7, target_type = int)
        self.at_minute              = self.get(mister_section, 'at_minute', default = 0, target_type = int)
        self.duration        = self.get(mister_section, 'duration', default = 30, target_type = int)
        self.mister_interval        = self.get(mister_section, 'interval', default = 360, target_type = int)
        self.device_id              = self.get(mister_section, 'device_id', default = 2, target_type = int)

class LightConfig(Config):
    '''
    A subclass for Light Settings
    '''
    def __init__(self):
        """
        Initializes the LightConfig, loading grow light settings.
        """
        super().__init__()
        light_section = 'growlight'
        self.lights_control_pin     = self.get(light_section, 'controlpin', default = 20, target_type = int)
        self.lights_on              = self.get(light_section, 'on', default = "6:00 AM")
        self.lights_off             = self.get(light_section, 'off', default = "6:00 PM")
        self.device_id              = self.get(light_section, 'device_id', default = 1, target_type = int)

class HumidifierConfig(Config):
    '''
    A subclass for Humidifier Settings
    '''
    def __init__(self):
        """
        Initializes the HumidifierConfig, loading humidifier settings.
        """
        super().__init__()
        humidifier_section = 'humidifier'
        self.username               = self.get(humidifier_section, 'username', default = 'username', target_type = str)
        self.password               = self.get(humidifier_section, 'password', default = 'password', target_type = str, is_secret=True)
        self.mode                   = self.get(humidifier_section, 'mode', default = 'manual', target_type = str)
        self.device_id              = self.get(humidifier_section, 'device_id', default = 3, target_type = int)
        self.runtime                = self.get(humidifier_section, 'runtime_min', default = 30, target_type = int)
        self.mistlevel_low          = self.get(humidifier_section, 'mistlevel_low', default = 1, target_type = int)
        self.mistlevel_medium        = self.get(humidifier_section, 'mislevel_medium', default = 5, target_type = int)
        self.mistlevel_high         = self.get(humidifier_section, 'mistlevel_high', default = 9, target_type = int)
        self.target_humidity        = self.get(humidifier_section, 'target_humidity', default = 90, target_type = int) 
        self.hysteresis             = self.get(humidifier_section, 'hysteresis', default = 5.0, target_type = float)
        

class SensorConfig(Config):
    """
    A subclass for Sensor IDs
    """
    def __init__(self):
        """
        Initializes the SensorConfig, loading sensor ID settings.
        """
        super().__init__()
        sensor_section = 'sensor'
        self.THsensorID = self.get(sensor_section, 'thsensorid', default = 1, target_type = int)


class MQTTConfig(Config):
    """
    A subclass for MQTT communication
    """
    def __init__(self):
        """
        Initializes the MQTTConfig, loading MQTT settings.
        """
        super().__init__()
        mqtt_section = 'hivemqtt'
        self.MQTT_BROKER            = self.get(mqtt_section, 'MQTT_BROKER', default = None, target_type = str)
        self.MQTT_PORT              = self.get(mqtt_section, 'MQTT_PORT', default = None, target_type = str)
        self.MQTT_USERNAME          = self.get(mqtt_section, 'MQTT_USERNAME', default = None, target_type = str)
        self.MQTT_PASSWORD          = self.get(mqtt_section, 'MQTT_PASSWORD', default = None, target_type = str)
        self.DATA_TOPIC             = self.get(mqtt_section, 'DATA_TOPIC_PUB', default = None, target_type = str)
        self.COMMAND_TOPIC          = self.get(mqtt_section, 'COMMAND_TOPIC_SUB', default = None, target_type = str)

class SchedulerConfig(Config):
    """
    A subclass for managing the enable/disable status of various schedulers.
    """
    def __init__(self):
        """
        Initializes the SchedulerConfig, loading scheduler control settings.
        """
        super().__init__()
        scheduler_section = 'scheduler'
        self.enable_vivarium_control = self.get(scheduler_section, 'enable_vivarium_control', default=True, target_type=bool)
        self.enable_climate_control = self.get(scheduler_section, 'enable_climate_control', default=True, target_type=bool)
        self.enable_grow_light      = self.get(scheduler_section, 'enable_grow_light', default=True, target_type=bool)
        self.enable_data_logging    = self.get(scheduler_section, 'enable_data_logging', default=True, target_type=bool)
        self.enable_weather_fetch   = self.get(scheduler_section, 'enable_weather_fetch', default=True, target_type=bool)
        self.application_db_type    = self.get(scheduler_section, 'application_db_type', default='local', target_type=str)
        self.max_retry_attempts     = self.get(scheduler_section, 'max_retry_attempts', default=3, target_type=int)
        self.retry_interval_minutes = self.get(scheduler_section, 'retry_interval_minutes', default= 5, target_type=int)
        self.schedule_light_hour    = self.get(scheduler_section, 'schedule_light_hour', default = 4, target_type=int)
        self.schedule_light_minute  = self.get(scheduler_section, 'schedule_light_minute', default = 0, target_type=int)
        self.scheule_sensor_read    = self.get(scheduler_section, 'schedule_sensor_read', default = 5, target_type=int)
        self.pi_version             = self.get(scheduler_section, 'pi_version', default = 0, target_type = int)

class coreConfig(Config):
    """
    A subclass for core settings
    """
    def __init__(self):
        """
        Initializes the coreConfig, loading core settings.
        """
        super().__init__()
        core_section = 'core'
        self.pi_version             = self.get(core_section, 'pi_version', default = 0, target_type = int)
        self.thsensor_id            = self.get(core_section, 'thsensorid', default = 1, target_type = int)
        self.application_db_type    = self.get(core_section, 'application_db_type', default='local', target_type=str)
        self.environment            = self.get(core_section, 'environment', default='development', target_type=str)