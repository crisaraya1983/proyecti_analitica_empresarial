"""
================================================================================
UTILIDADES GLOBALES - STREAMLIT APP
================================================================================
Módulos de utilidad compartidos por toda la aplicación
================================================================================
"""

from .db_connection import (
    DatabaseConnection,
    get_oltp_connection,
    get_dw_connection,
    test_connections
)

__all__ = [
    'DatabaseConnection',
    'get_oltp_connection',
    'get_dw_connection',
    'test_connections'
]
