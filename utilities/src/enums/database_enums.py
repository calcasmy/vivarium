
from enum import Enum


class DatabaseType(Enum):
    POSTGRES = "postgres"
    SUPABASE = "supabase"


class ConnectionType(Enum):
    LOCAL = "local"
    REMOTE = "remote"