import datetime
import os

class TerrariumStatus:
    '''
        Fetches current terrarium status primarily Temperature and Humidity
    '''

    @staticmethod
    def script_path() -> str:
        return os.path.abspath(__file__)