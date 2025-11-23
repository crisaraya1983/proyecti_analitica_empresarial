"""
================================================================================
MÓDULO DE CONEXIÓN A BASE DE DATOS - GLOBAL
================================================================================
Autor: Sistema de Analítica Empresarial
Fecha: 2025-01-15
Propósito: Proporcionar funciones centralizadas para conectar a SQL Server
          desde cualquier parte de la aplicación Streamlit
================================================================================
"""

import pyodbc
import streamlit as st
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Clase para gestionar conexiones a bases de datos SQL Server"""

    # Nombres de bases de datos
    OLTP_DATABASE = "Ecommerce_OLTP"
    DW_DATABASE = "Ecommerce_DW"

    @staticmethod
    def get_connection_string(database: str, use_secrets: bool = True) -> str:
        """
        Obtiene el connection string para la base de datos especificada

        Args:
            database: Nombre de la base de datos
            use_secrets: Si True, intenta usar Streamlit secrets; si False, usa config local

        Returns:
            Connection string formateado para pyodbc

        Examples:
            >>> conn_str = DatabaseConnection.get_connection_string("Ecommerce_OLTP")
            >>> conn = pyodbc.connect(conn_str)
        """
        if use_secrets:
            try:
                # Intentar usar Streamlit secrets
                server = st.secrets["sqlserver"]["server"]
                driver = st.secrets["sqlserver"]["driver"]
                trusted_connection = st.secrets["sqlserver"]["trusted_connection"]

                conn_str = (
                    f"DRIVER={{{driver}}};"
                    f"SERVER={server};"
                    f"DATABASE={database};"
                    f"Trusted_Connection={trusted_connection};"
                )

                logger.info(f"Usando conexión desde Streamlit secrets para {database}")
                return conn_str

            except (KeyError, FileNotFoundError, AttributeError) as e:
                logger.warning(f"No se pudo leer secrets.toml: {e}. Usando configuración por defecto.")
                return DatabaseConnection._get_default_connection_string(database)
        else:
            return DatabaseConnection._get_default_connection_string(database)

    @staticmethod
    def _get_default_connection_string(database: str) -> str:
        """
        Configuración por defecto para desarrollo local

        Args:
            database: Nombre de la base de datos

        Returns:
            Connection string por defecto
        """
        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER=CRISTIANDELL;"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
        )

    @staticmethod
    def get_connection(database: str, use_secrets: bool = True) -> pyodbc.Connection:
        """
        Obtiene una conexión activa a la base de datos especificada

        Args:
            database: Nombre de la base de datos
            use_secrets: Si True, usa Streamlit secrets

        Returns:
            Conexión pyodbc activa

        Raises:
            pyodbc.Error: Si no se puede establecer la conexión

        Examples:
            >>> conn = DatabaseConnection.get_connection("Ecommerce_OLTP")
            >>> cursor = conn.cursor()
            >>> cursor.execute("SELECT COUNT(*) FROM productos")
            >>> count = cursor.fetchone()[0]
            >>> conn.close()
        """
        conn_str = DatabaseConnection.get_connection_string(database, use_secrets)

        try:
            conn = pyodbc.connect(conn_str)
            logger.info(f"Conexión exitosa a {database}")
            return conn
        except pyodbc.Error as e:
            logger.error(f"Error conectando a {database}: {str(e)}")
            raise

    @staticmethod
    def get_oltp_connection(use_secrets: bool = True) -> pyodbc.Connection:
        """
        Obtiene conexión a la base de datos OLTP (Ecommerce_OLTP)

        Args:
            use_secrets: Si True, usa Streamlit secrets

        Returns:
            Conexión pyodbc activa a Ecommerce_OLTP

        Examples:
            >>> conn = DatabaseConnection.get_oltp_connection()
            >>> # Usar conexión...
            >>> conn.close()
        """
        return DatabaseConnection.get_connection(
            DatabaseConnection.OLTP_DATABASE,
            use_secrets
        )

    @staticmethod
    def get_dw_connection(use_secrets: bool = True) -> pyodbc.Connection:
        """
        Obtiene conexión a la base de datos DW (Ecommerce_DW)

        Args:
            use_secrets: Si True, usa Streamlit secrets

        Returns:
            Conexión pyodbc activa a Ecommerce_DW

        Examples:
            >>> conn = DatabaseConnection.get_dw_connection()
            >>> # Usar conexión...
            >>> conn.close()
        """
        return DatabaseConnection.get_connection(
            DatabaseConnection.DW_DATABASE,
            use_secrets
        )

    @staticmethod
    def test_connection(database: str, use_secrets: bool = True) -> Dict[str, any]:
        """
        Prueba la conexión a una base de datos

        Args:
            database: Nombre de la base de datos
            use_secrets: Si True, usa Streamlit secrets

        Returns:
            Diccionario con resultado de la prueba:
            {
                "success": bool,
                "message": str,
                "error": Optional[str]
            }

        Examples:
            >>> result = DatabaseConnection.test_connection("Ecommerce_OLTP")
            >>> if result["success"]:
            >>>     print("Conexión exitosa!")
        """
        result = {
            "success": False,
            "message": "",
            "error": None
        }

        try:
            conn = DatabaseConnection.get_connection(database, use_secrets)
            cursor = conn.cursor()

            # Verificar que estamos en la base de datos correcta
            cursor.execute("SELECT DB_NAME()")
            db_name = cursor.fetchone()[0]

            if db_name == database:
                result["success"] = True
                result["message"] = f"Conexión exitosa a {database}"
            else:
                result["success"] = False
                result["message"] = f"Conectado a {db_name} en lugar de {database}"

            cursor.close()
            conn.close()

        except Exception as e:
            result["success"] = False
            result["message"] = f"Error al conectar a {database}"
            result["error"] = str(e)
            logger.error(f"Error en test_connection: {str(e)}")

        return result

    @staticmethod
    def test_all_connections(use_secrets: bool = True) -> Dict[str, Dict[str, any]]:
        """
        Prueba las conexiones a todas las bases de datos

        Args:
            use_secrets: Si True, usa Streamlit secrets

        Returns:
            Diccionario con resultados de todas las pruebas:
            {
                "oltp": {"success": bool, "message": str, "error": Optional[str]},
                "dw": {"success": bool, "message": str, "error": Optional[str]}
            }

        Examples:
            >>> results = DatabaseConnection.test_all_connections()
            >>> if results["oltp"]["success"] and results["dw"]["success"]:
            >>>     print("Todas las conexiones funcionan!")
        """
        return {
            "oltp": DatabaseConnection.test_connection(
                DatabaseConnection.OLTP_DATABASE,
                use_secrets
            ),
            "dw": DatabaseConnection.test_connection(
                DatabaseConnection.DW_DATABASE,
                use_secrets
            )
        }

    @staticmethod
    def get_table_count(connection: pyodbc.Connection, table_name: str) -> int:
        """
        Obtiene el conteo de registros de una tabla

        Args:
            connection: Conexión activa a la base de datos
            table_name: Nombre de la tabla

        Returns:
            Número de registros en la tabla

        Examples:
            >>> conn = DatabaseConnection.get_oltp_connection()
            >>> count = DatabaseConnection.get_table_count(conn, "productos")
            >>> print(f"Total productos: {count}")
            >>> conn.close()
        """
        try:
            cursor = connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            logger.error(f"Error obteniendo count de {table_name}: {str(e)}")
            return 0

    @staticmethod
    def execute_query(connection: pyodbc.Connection, query: str, params: tuple = None) -> list:
        """
        Ejecuta una query SELECT y devuelve los resultados

        Args:
            connection: Conexión activa a la base de datos
            query: Query SQL a ejecutar
            params: Parámetros opcionales para la query

        Returns:
            Lista de tuplas con los resultados

        Examples:
            >>> conn = DatabaseConnection.get_dw_connection()
            >>> query = "SELECT * FROM dim_producto WHERE activo = ?"
            >>> results = DatabaseConnection.execute_query(conn, query, (1,))
            >>> conn.close()
        """
        try:
            cursor = connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            logger.error(f"Error ejecutando query: {str(e)}")
            raise


# Funciones de conveniencia para uso directo
def get_oltp_connection(use_secrets: bool = True) -> pyodbc.Connection:
    """
    Función de conveniencia para obtener conexión OLTP

    Examples:
        >>> from utils.db_connection import get_oltp_connection
        >>> conn = get_oltp_connection()
        >>> # usar conexión...
        >>> conn.close()
    """
    return DatabaseConnection.get_oltp_connection(use_secrets)


def get_dw_connection(use_secrets: bool = True) -> pyodbc.Connection:
    """
    Función de conveniencia para obtener conexión DW

    Examples:
        >>> from utils.db_connection import get_dw_connection
        >>> conn = get_dw_connection()
        >>> # usar conexión...
        >>> conn.close()
    """
    return DatabaseConnection.get_dw_connection(use_secrets)


def test_connections(use_secrets: bool = True) -> Dict[str, Dict[str, any]]:
    """
    Función de conveniencia para probar todas las conexiones

    Examples:
        >>> from utils.db_connection import test_connections
        >>> results = test_connections()
        >>> print(results)
    """
    return DatabaseConnection.test_all_connections(use_secrets)
