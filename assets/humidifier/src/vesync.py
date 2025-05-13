"""VeSync API Device Libary."""

import logging
import re
import time
from itertools import chain
from typing import Tuple
from assets.humidifier.src.helpers import Helpers
from assets.humidifier.src.vesyncbasedevice import VeSyncBaseDevice
from assets.humidifier.src.vesyncclassic300s import *
import assets.humidifier.src.vesyncclassic300s as fan_mods
import assets.humidifier.src.helpers as helpermodule

# Attempting to rescue git. Will clean up late

API_RATE_LIMIT: int = 30
DEFAULT_TZ: str = 'America/New_York'

DEFAULT_ENER_UP_INT: int = 21600

def object_factory(dev_type, config, manager) -> Tuple[str, VeSyncBaseDevice]:
    def fans(dev_type, config, manager):
        fan_cls = fan_mods.fan_modules[dev_type]  # noqa: F405
        fan_obj = getattr(fan_mods, fan_cls)
        return 'fans', fan_obj(config, manager)

    if dev_type in fan_mods.fan_modules:  # type: ignore  # noqa: F405
        type_str, dev_obj = fans(dev_type, config, manager)
    else:
        type_str = 'unknown'
        dev_obj = None
    return type_str, dev_obj


class VeSync:  # pylint: disable=function-redefined
    """VeSync Manager Class."""

    def __init__(self, username, password, time_zone=DEFAULT_TZ,
                 debug=False, redact=True):
        self.debug = debug
        self._redact = redact
        if redact:
            self.redact = redact
        self.username = username
        self.password = password
        self.token = None
        self.account_id = None
        self.country_code = None
        self.devices = None
        self.enabled = False
        self.update_interval = API_RATE_LIMIT
        self.last_update_ts = None
        self.in_process = False
        self._energy_update_interval = DEFAULT_ENER_UP_INT
        self._energy_check = True
        self._dev_list = {}
        self.outlets = []
        self.switches = []
        self.fans = []
        self.bulbs = []
        self.scales = []
        self.kitchen = []

        self._dev_list = {
            'fans': self.fans,
            'outlets': self.outlets,
            'switches': self.switches,
            'bulbs': self.bulbs,
            'kitchen': self.kitchen
        }

        if isinstance(time_zone, str) and time_zone:
            reg_test = r'[^a-zA-Z/_]'
            if bool(re.search(reg_test, time_zone)):
                self.time_zone = DEFAULT_TZ
            else:
                self.time_zone = time_zone
        else:
            self.time_zone = DEFAULT_TZ

    @property
    def debug(self) -> bool:
        return self._debug

    @debug.setter
    def debug(self, new_flag: bool) -> None:
        log_modules = [fan_mods,
                       helpermodule]
        self._debug = new_flag

    @property
    def redact(self) -> bool:
        return self._redact

    @redact.setter
    def redact(self, new_flag: bool) -> None:
        if new_flag:
            Helpers.shouldredact = True
        elif new_flag is False:
            Helpers.shouldredact = False
        self._redact = new_flag

    @property
    def energy_update_interval(self) -> int:
        return self._energy_update_interval

    @energy_update_interval.setter
    def energy_update_interval(self, new_energy_update: int) -> None:
        if new_energy_update > 0:
            self._energy_update_interval = new_energy_update

    @staticmethod
    def remove_dev_test(device, new_list: list) -> bool:
        if isinstance(new_list, list) and device.cid:
            for item in new_list:
                device_found = False
                if 'cid' in item:
                    if device.cid == item['cid']:
                        device_found = True
                        break
            if not device_found:
                return False
        return True

    def add_dev_test(self, new_dev: dict) -> bool:
        if 'cid' in new_dev:
            for _, v in self._dev_list.items():
                for dev in v:
                    if (
                        dev.cid == new_dev.get('cid')
                        and new_dev.get('subDeviceNo', 0) == dev.sub_device_no
                    ):
                        return False
        return True

    def remove_old_devices(self, devices: list) -> bool:
        for k, v in self._dev_list.items():
            before = len(v)
            v[:] = [x for x in v if self.remove_dev_test(x, devices)]
            after = len(v)
        return True

    @staticmethod
    def set_dev_id(devices: list) -> list:
        dev_num = 0
        dev_rem = []
        for dev in devices:
            if dev.get('cid') is None:
                if dev.get('macID') is not None:
                    dev['cid'] = dev['macID']
                elif dev.get('uuid') is not None:
                    dev['cid'] = dev['uuid']
                else:
                    dev_rem.append(dev_num)
            dev_num += 1
            if dev_rem:
                devices = [i for j, i in enumerate(
                            devices) if j not in dev_rem]
        return devices

    def process_devices(self, dev_list: list) -> bool:
        devices = VeSync.set_dev_id(dev_list)

        num_devices = 0
        for _, v in self._dev_list.items():
            if isinstance(v, list):
                num_devices += len(v)
            else:
                num_devices += 1

        if not devices:
            return False
        if num_devices != 0:
            self.remove_old_devices(devices)

        devices[:] = [x for x in devices if self.add_dev_test(x)]

        detail_keys = ['deviceType', 'deviceName', 'deviceStatus']
        for dev in devices:
            if not all(k in dev for k in detail_keys):
                continue
            dev_type = dev.get('deviceType')
            try:
                device_str, device_obj = object_factory(dev_type, dev, self)
                device_list = getattr(self, device_str)
                device_list.append(device_obj)
            except AttributeError as err:
                continue

        return True

    def get_devices(self) -> bool:
        if not self.enabled:
            return False

        self.in_process = True
        proc_return = False
        response, _ = Helpers.call_api(
            '/cloud/v1/deviceManaged/devices',
            'post',
            headers=Helpers.req_header_bypass(),
            json_object=Helpers.req_body(self, 'devicelist'),
        )

        if response and Helpers.code_check(response):
            if 'result' in response and 'list' in response['result']:
                device_list = response['result']['list']
                proc_return = self.process_devices(device_list)

        self.in_process = False

        return proc_return

    def login(self) -> bool:
        user_check = isinstance(self.username, str) and len(self.username) > 0
        pass_check = isinstance(self.password, str) and len(self.password) > 0
        if user_check is False:
            return False
        if pass_check is False:
            return False

        response, _ = Helpers.call_api(
            '/cloud/v1/user/login', 'post',
            json_object=Helpers.req_body(self, 'login')
        )

        if Helpers.code_check(response) and 'result' in response:
            self.token = response.get('result').get('token')
            self.account_id = response.get('result').get('accountID')
            self.country_code = response.get('result').get('countryCode')
            self.enabled = True

            return True
        return False

    def device_time_check(self) -> bool:
        return (
            self.last_update_ts is None
            or (time.time() - self.last_update_ts) > self.update_interval
        )

    def update(self) -> None:
        if self.device_time_check():

            if not self.enabled:
                return
            self.get_devices()

            devices = list(self._dev_list.values())

            for device in chain(*devices):
                device.update()

            self.last_update_ts = time.time()

    def update_energy(self, bypass_check=False) -> None:
        if self.outlets:
            for outlet in self.outlets:
                outlet.update_energy(bypass_check)

    def update_all_devices(self) -> None:
        devices = list(self._dev_list.values())
        for dev in chain(*devices):
            dev.update()
