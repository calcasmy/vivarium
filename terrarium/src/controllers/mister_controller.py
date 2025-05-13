import os

class MisterController:
    def __init__(self):
        pass

    @staticmethod
    def script_path() -> str:
        '''Returns the Absolute path of the script'''
        return os.path.abspath(__file__)