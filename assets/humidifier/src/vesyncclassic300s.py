"""VeSync API for controling fans and purifiers."""

import json
import logging
from typing import Any, Dict, List, Tuple, Union, Optional
from assets.humidifier.src.vesyncbasedevice import VeSyncBaseDevice
from assets.humidifier.src.helpers import Helpers, Timer

logger = logging.getLogger(__name__)

CLASSIC300S_FEATURES: dict = {
    'module': 'VeSyncHumidClassic300S',  # Renaming the module to be more specific
    'models': ['Classic300S'],
    'features': ['nightlight'],
    'mist_modes': ['auto', 'sleep', 'manual'],
    'mist_levels': list(range(1, 10))
}

def model_dict() -> dict:
    """Build humidifier model dictionary for known Classic300S models."""
    model_modules = {}
    for model in CLASSIC300S_FEATURES['models']:
        model_modules[model] = CLASSIC300S_FEATURES['module']
    return model_modules

def model_features(dev_type: str) -> dict:
    """Get features for the Classic300S device type."""
    if dev_type in CLASSIC300S_FEATURES['models']:
        return CLASSIC300S_FEATURES
    raise ValueError(f'Device type "{dev_type}" is not a supported Classic300S model.')

fan_classes: set = {CLASSIC300S_FEATURES['module']}
fan_modules: dict = model_dict()
__all__: list = list(fan_classes) + ['fan_modules']

class VeSyncHumidClassic300S(VeSyncBaseDevice):
    """300S Humidifier Class."""

    def __init__(self, details, manager):
        """Initialize 300S Humidifier class."""
        super().__init__(details, manager)
        self.enabled = True
        self._config_dict = model_features(self.device_type)
        self.mist_levels = self._config_dict.get('mist_levels')
        self.mist_modes = self._config_dict.get('mist_modes')
        self._features = self._config_dict.get('features')
        self.night_light = 'nightlight' in self._features
        self.details = {
            'humidity': 0,
            'mist_virtual_level': 0,
            'mist_level': 0,
            'mode': 'manual',
            'water_lacks': False,
            'humidity_high': False,
            'water_tank_lifted': False,
            'display': False,
            'automatic_stop_reach_target': False,
        }
        if self.night_light is True:
            self.details['night_light_brightness'] = 0
        self.config = {
            'auto_target_humidity': 0,
            'display': False,
            'automatic_stop': True
        }
        self._api_modes = ['getHumidifierStatus', 'setAutomaticStop',
                           'setSwitch', 'setNightLightBrightness',
                           'setVirtualLevel', 'setTargetHumidity',
                           'setHumidityMode', 'setDisplay', 'setLevel']

    def build_api_dict(self, method: str) -> Tuple[Dict, Dict]:
        """Build humidifier api call header and body.

        Available methods are: 'getHumidifierStatus', 'setAutomaticStop',
        'setSwitch', 'setNightLightBrightness', 'setVirtualLevel',
        'setTargetHumidity', 'setHumidityMode'
        """
        if method not in self._api_modes:
            logger.debug('Invalid mode - %s', method)
            raise ValueError
        head = Helpers.bypass_header()
        body = Helpers.bypass_body_v2(self.manager)
        body['cid'] = self.cid
        body['configModule'] = self.config_module
        body['payload'] = {
            'method': method,
            'source': 'APP'
        }
        return head, body

    def build_humid_dict(self, dev_dict: Dict[str, str]) -> None:
        """Build humidifier status dictionary."""
        self.enabled = dev_dict.get('enabled')
        self.device_status = 'on' if self.enabled else 'off'
        self.mode = dev_dict.get('mode', None)
        self.details['humidity'] = dev_dict.get('humidity', 0)
        self.details['mist_virtual_level'] = dev_dict.get(
            'mist_virtual_level', 0)
        self.details['mist_level'] = dev_dict.get('mist_level', 0)
        self.details['mode'] = dev_dict.get('mode', 'manual')
        self.details['water_lacks'] = dev_dict.get('water_lacks', False)
        self.details['humidity_high'] = dev_dict.get('humidity_high', False)
        self.details['water_tank_lifted'] = dev_dict.get(
            'water_tank_lifted', False)
        self.details['automatic_stop_reach_target'] = dev_dict.get(
            'automatic_stop_reach_target', True
        )
        if self.night_light:
            self.details['night_light_brightness'] = dev_dict.get(
                'night_light_brightness', 0)
        try:
            self.details['display'] = dev_dict['display']
        except KeyError:
            self.details['display'] = dev_dict.get(
                'indicator_light_switch', False)

    def build_config_dict(self, conf_dict):
        """Build configuration dict for 300s humidifier."""
        self.config['auto_target_humidity'] = conf_dict.get(
            'auto_target_humidity', 0)
        self.config['display'] = conf_dict.get('display', False)
        self.config['automatic_stop'] = conf_dict.get('automatic_stop', True)

    def get_details(self) -> None:
        """Build 300S Humidifier details dictionary.
        Fetch and update details for the Classic 300S."""
        head = Helpers.bypass_header()
        body = Helpers.bypass_body_v2(self.manager)
        body['cid'] = self.cid
        body['configModule'] = self.config_module
        body['payload'] = {
            'method': 'getHumidifierStatus',
            'source': 'APP',
            'data': {}
        }

        r, _ = Helpers.call_api(
            '/cloud/v2/deviceManaged/bypassV2',
            method='post',
            headers=head,
            json_object=body,
        )
        if r is None or not isinstance(r, dict):
            logger.debug("Error getting status of %s ", self.device_name)
            return
        outer_result = r.get('result', {})
        inner_result = None

        if outer_result is not None:
            inner_result = r.get('result', {}).get('result')
        if inner_result is not None and Helpers.code_check(r):
            if outer_result.get('code') == 0:
                self.build_humid_dict(inner_result)
            else:
                logger.debug('error in inner result dict from humidifier')
            if inner_result.get('configuration', {}):
                self.build_config_dict(inner_result.get('configuration', {}))
            else:
                logger.debug('No configuration found in humidifier status')
        else:
            logger.debug('Error in humidifier response')

    def update(self):
        """Update 300S Humidifier details."""
        self.get_details()

    def toggle_switch(self, toggle: bool) -> bool:
        """Toggle humidifier on/off."""
        if not isinstance(toggle, bool):
            logger.debug('Invalid toggle value for humidifier switch')
            return False

        head = Helpers.bypass_header()
        body = Helpers.bypass_body_v2(self.manager)
        body['cid'] = self.cid
        body['configModule'] = self.config_module
        body['payload'] = {
            'data': {
                'enabled': toggle,
                'id': 0
            },
            'method': 'setSwitch',
            'source': 'APP'
        }

        r, _ = Helpers.call_api(
            '/cloud/v2/deviceManaged/bypassV2',
            method='post',
            headers=head,
            json_object=body,
        )

        if r is not None and Helpers.code_check(r):
            if toggle:
                self.device_status = 'on'
            else:
                self.device_status = 'off'

            return True
        logger.debug("Error toggling 300S humidifier - %s", self.device_name)
        return False

    def turn_on(self) -> bool:
        """Turn 300S Humidifier on."""
        return self.toggle_switch(True)

    def turn_off(self):
        """Turn 300S Humidifier off."""
        return self.toggle_switch(False)

    def automatic_stop_on(self) -> bool:
        """Turn 300S Humidifier automatic stop on."""
        return self.set_automatic_stop(True)

    def automatic_stop_off(self) -> bool:
        """Turn 200S/300S Humidifier automatic stop on."""
        return self.set_automatic_stop(False)

    def set_automatic_stop(self, mode: bool) -> bool:
        """Set 200S/300S Humidifier to automatic stop."""
        if mode not in (True, False):
            logger.debug(
                'Invalid mode passed to set_automatic_stop - %s', mode)
            return False

        head, body = self.build_api_dict('setAutomaticStop')
        if not head and not body:
            return False

        body['payload']['data'] = {
            'enabled': mode
        }

        r, _ = Helpers.call_api(
            '/cloud/v2/deviceManaged/bypassV2',
            method='post',
            headers=head,
            json_object=body,
        )
#----------------------------------------------------------------------------------
        if r is not None and Helpers.code_check(r):
            return True
        if isinstance(r, dict):
            logger.debug('Error toggling automatic stop')
        else:
            logger.debug('Error in api return json for %s', self.device_name)
        return False
#----------------------------------------------------------------------------------
#suggested code
# if r is not None and Helpers.code_check(r):
#             return True
#         logger.debug('Error toggling automatic stop')
#         return False

    def set_display(self, mode: bool) -> bool:
        """Toggle display on/off."""
        if not isinstance(mode, bool):
            logger.debug("Mode must be True or False")
            return False

        head, body = self.build_api_dict('setDisplay')

        body['payload']['data'] = {
            'state': mode
        }

        r, _ = Helpers.call_api(
            '/cloud/v2/deviceManaged/bypassV2',
            method='post',
            headers=head,
            json_object=body,
        )

        if r is not None and Helpers.code_check(r):
            return True
        logger.debug("Error toggling 300S display - %s", self.device_name)
        return False

    def turn_on_display(self) -> bool:
        """Turn 200S/300S Humidifier on."""
        return self.set_display(True)

    def turn_off_display(self):
        """Turn 200S/300S Humidifier off."""
        return self.set_display(False)

    @property
    def display_state(self) -> bool:
        """Get display state."""
        return bool(self.details['display'])

    def set_humidity(self, humidity: int) -> bool:
        """Set target 200S/300S Humidifier humidity."""
        if humidity < 30 or humidity > 80:
            logger.debug("Humidity value must be set between 30 and 80")
            return False
        head, body = self.build_api_dict('setTargetHumidity')

        if not head and not body:
            return False

        body['payload']['data'] = {
            'target_humidity': humidity
        }

        r, _ = Helpers.call_api(
            '/cloud/v2/deviceManaged/bypassV2',
            method='post',
            headers=head,
            json_object=body,
        )

        if r is not None and Helpers.code_check(r):
            return True
        logger.debug('Error setting humidity')
        return False

    def set_night_light_brightness(self, brightness: int) -> bool:
        """Set target 200S/300S Humidifier night light brightness."""
        if not self.night_light:
            logger.debug('%s is a %s does not have a nightlight',
                         self.device_name, self.device_type)
            return False
        if brightness < 0 or brightness > 100:
            logger.debug("Brightness value must be set between 0 and 100")
            return False
        head, body = self.build_api_dict('setNightLightBrightness')

        if not head and not body:
            return False

        body['payload']['data'] = {
            'night_light_brightness': brightness
        }

        r, _ = Helpers.call_api(
            '/cloud/v2/deviceManaged/bypassV2',
            method='post',
            headers=head,
            json_object=body,
        )

        if r is not None and Helpers.code_check(r):
            return True
        logger.debug('Error setting night light brightness')
        return False

    def set_humidity_mode(self, mode: str) -> bool:
        """Set humidifier mode - sleep or auto."""
        if mode.lower() not in self.mist_modes:
            logger.debug('Invalid humidity mode used - %s',
                         mode)
            logger.debug('Proper modes for this device are - %s',
                         str(self.mist_modes))
            return False
        head, body = self.build_api_dict('setHumidityMode')
        if not head and not body:
            return False
        body['payload']['data'] = {
            'mode': mode.lower()
        }

        r, _ = Helpers.call_api(
            '/cloud/v2/deviceManaged/bypassV2',
            method='post',
            headers=head,
            json_object=body,
        )

        if r is not None and Helpers.code_check(r):
            return True
        logger.debug('Error setting humidity mode')
        return False

    def set_warm_level(self, warm_level) -> bool:
        """Set target 600S Humidifier mist warmth."""
        if not self.warm_mist_feature:
            logger.debug('%s is a %s does not have a mist warmer',
                         self.device_name, self.device_type)
            return False
        if not isinstance(warm_level, int):
            try:
                warm_level = int(warm_level)
            except ValueError:
                logger.debug('Error converting warm mist level to a integer')
        if warm_level not in self.warm_mist_levels:
            logger.debug("warm_level value must be - %s",
                         str(self.warm_mist_levels))
            return False
        head, body = self.build_api_dict('setLevel')

        if not head and not body:
            return False

        body['payload']['data'] = {
            'type': 'warm',
            'level': warm_level,
            'id': 0,
        }

        r, _ = Helpers.call_api(
            '/cloud/v2/deviceManaged/bypassV2',
            method='post',
            headers=head,
            json_object=body,
        )

        if r is not None and Helpers.code_check(r):
            return True
        logger.debug('Error setting warm')
        return False

    def set_auto_mode(self):
        """Set auto mode for humidifiers."""
        if 'auto' in self.mist_modes:
            call_str = 'auto'
        elif 'humidity' in self.mist_modes:
            call_str = 'humidity'
        else:
            logger.debug('Trying auto mode, mode not set for this model, '
                         'please ensure %s model '
                         'is in configuration dictionary', self.device_type)
            call_str = 'auto'
        set_auto = self.set_humidity_mode(call_str)
        return set_auto

    def set_manual_mode(self):
        """Set humifier to manual mode with 1 mist level."""
        return self.set_humidity_mode('manual')

    def set_mist_level(self, level) -> bool:
        """Set humidifier mist level with int between 0 - 9."""
        try:
            level = int(level)
        except ValueError:
            level = str(level)
        if level not in self.mist_levels:
            logger.debug('Humidifier mist level must be between 0 and 9')
            return False

        head, body = self.build_api_dict('setVirtualLevel')
        if not head and not body:
            return False

        body['payload']['data'] = {
            'id': 0,
            'level': level,
            'type': 'mist'
        }

        r, _ = Helpers.call_api(
            '/cloud/v2/deviceManaged/bypassV2',
            method='post',
            headers=head,
            json_object=body,
        )

        if r is not None and Helpers.code_check(r):
            return True
        logger.debug('Error setting mist level')
        return False

    @property
    def humidity(self):
        """Get Humidity level."""
        return self.details['humidity']

    @property
    def mist_level(self):
        """Get current mist level."""
        return self.details['mist_virtual_level']

    @property
    def water_lacks(self):
        """If tank is empty return true."""
        return self.details['water_lacks']

    @property
    def auto_humidity(self):
        """Auto target humidity."""
        return self.config['auto_target_humidity']

    @property
    def auto_enabled(self):
        """Auto mode is enabled."""
        if self.details.get('mode') == 'auto' \
                or self.details.get('mode') == 'humidity':
            return True
        return False

    def display(self) -> None:
        """Return formatted device info to stdout."""
        super().display()
        disp = [
            ('Mode: ', self.details['mode'], ''),
            ('Humidity: ', self.details['humidity'], 'percent'),
            ('Mist Virtual Level: ', self.details['mist_virtual_level'], ''),
            ('Mist Level: ', self.details['mist_level'], ''),
            ('Water Lacks: ', self.details['water_lacks'], ''),
            ('Humidity High: ', self.details['humidity_high'], ''),
            ('Water Tank Lifted: ', self.details['water_tank_lifted'], ''),
            ('Display: ', self.details['display'], ''),
            ('Automatic Stop Reach Target: ',
                self.details['automatic_stop_reach_target'], ''),
            ('Auto Target Humidity: ',
                self.config['auto_target_humidity'], 'percent'),
            ('Automatic Stop: ', self.config['automatic_stop'], ''),
        ]
        if self.night_light:
            disp.append(('Night Light Brightness: ',
                         self.details.get('night_light_brightness', ''), 'percent'))
        if self.warm_mist_feature:
            disp.append(('Warm mist enabled: ',
                         self.details.get('warm_mist_enabled', ''), ''))
            disp.append(('Warm mist level: ',
                         self.details.get('warm_mist_level', ''), ''))
        for line in disp:
            print(f'{line[0]:.<30} {line[1]} {line[2]}')

    def displayJSON(self) -> str:
        """Return air purifier status and properties in JSON output."""
        sup = super().displayJSON()
        sup_val = json.loads(sup)
        sup_val.update(
            {
                'Mode': self.details['mode'],
                'Humidity': str(self.details['humidity']),
                'Mist Virtual Level': str(
                    self.details['mist_virtual_level']),
                'Mist Level': str(self.details['mist_level']),
                'Water Lacks': self.details['water_lacks'],
                'Humidity High': self.details['humidity_high'],
                'Water Tank Lifted': self.details['water_tank_lifted'],
                'Display': self.details['display'],
                'Automatic Stop Reach Target': self.details[
                    'automatic_stop_reach_target'],
                'Auto Target Humidity': str(self.config[
                    'auto_target_humidity']),
                'Automatic Stop': self.config['automatic_stop'],
            }
        )
        if self.night_light:
            sup_val['Night Light Brightness'] = self.details[
                'night_light_brightness']
        if self.warm_mist_feature:
            sup_val['Warm mist enabled'] = self.details['warm_mist_enabled']
            sup_val['Warm mist level'] = self.details['warm_mist_level']
        return json.dumps(sup_val, indent=4)