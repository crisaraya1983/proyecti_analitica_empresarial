import sys
import os

# Agregar path de utils al sistema
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
utils_path = os.path.join(parent_dir, 'utils')

if utils_path not in sys.path:
    sys.path.insert(0, utils_path)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Importar desde el módulo global de conexiones
from utils.db_connection import (
    DatabaseConnection,
    get_oltp_connection,
    get_dw_connection,
    test_connections
)

class DatabaseConfig:

    @staticmethod
    def get_connection_string(database: str, use_secrets: bool = True) -> str:
        return DatabaseConnection.get_connection_string(database, use_secrets)

    @staticmethod
    def _get_default_connection_string(database: str) -> str:
        return DatabaseConnection._get_default_connection_string(database)

    @staticmethod
    def get_oltp_connection(use_secrets: bool = True):
        return DatabaseConnection.get_oltp_connection(use_secrets)

    @staticmethod
    def get_dw_connection(use_secrets: bool = True):
        return DatabaseConnection.get_dw_connection(use_secrets)

    @staticmethod
    def test_connections(use_secrets: bool = True) -> dict:
        results = DatabaseConnection.test_all_connections(use_secrets)
        return {
            "oltp": {
                "success": results["oltp"]["success"],
                "error": results["oltp"].get("error")
            },
            "dw": {
                "success": results["dw"]["success"],
                "error": results["dw"].get("error")
            }
        }


OLTP_DATABASE = DatabaseConnection.OLTP_DATABASE
DW_DATABASE = DatabaseConnection.DW_DATABASE

BATCH_SIZE_DIMENSIONS = 1000
BATCH_SIZE_FACTS = 5000
