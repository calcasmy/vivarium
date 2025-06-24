# vivarium/deploy/src/database/supabase_setup.py
import os
import re
import sys
import time
import getpass
import argparse
import psycopg2
from typing import Optional, Dict
from psycopg2 import sql, OperationalError, Error as Psycopg2Error

if __name__ == "__main__":
    vivarium_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if vivarium_path not in sys.path:
        sys.path.insert(0, vivarium_path)

from utilities.src.logger import LogHelper
from utilities.src.database_operations import DatabaseOperations
from utilities.src.config import DatabaseConfig, FileConfig, WeatherAPIConfig
from weather.rawclimate_dataloader import main as load_rawfiles_main
from utilities.src.path_utils import PathUtils

logger = LogHelper.get_logger(__name__)

class SupabaseSetup:
    def __init__(self):
        pass