import pyodbc
import streamlit as st
from typing import Optional, Dict, Union
import logging
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)


class DatabaseConnection:

    # Nombres de bases de datos
    OLTP_DATABASE = "Ecommerce_OLTP"
    DW_DATABASE = "Ecommerce_DW"

    @staticmethod
    def get_connection_string(database: str, use_secrets: bool = True) -> str:

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

        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER=CRISTIANDELL;"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
        )

    @staticmethod
    def get_connection(database: str, use_secrets: bool = True) -> pyodbc.Connection:

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

        return DatabaseConnection.get_connection(
            DatabaseConnection.OLTP_DATABASE,
            use_secrets
        )

    @staticmethod
    def get_dw_connection(use_secrets: bool = True) -> pyodbc.Connection:

        return DatabaseConnection.get_connection(
            DatabaseConnection.DW_DATABASE,
            use_secrets
        )

    @staticmethod
    def get_sqlalchemy_connection_string(database: str, use_secrets: bool = True) -> str:
 
        if use_secrets:
            try:
                server = st.secrets["sqlserver"]["server"]
                driver = st.secrets["sqlserver"]["driver"]
                trusted_connection = st.secrets["sqlserver"]["trusted_connection"]

                # Codificar el driver para URL
                driver_encoded = quote_plus(driver)

                # Formato de connection string para SQLAlchemy con Windows Authentication
                conn_str = (
                    f"mssql+pyodbc://@{server}/{database}?"
                    f"driver={driver_encoded}&"
                    f"Trusted_Connection={trusted_connection}"
                )

                logger.info(f"Usando SQLAlchemy connection string desde secrets para {database}")
                return conn_str

            except (KeyError, FileNotFoundError, AttributeError) as e:
                logger.warning(f"No se pudo leer secrets.toml: {e}. Usando configuración por defecto.")
                return DatabaseConnection._get_default_sqlalchemy_connection_string(database)
        else:
            return DatabaseConnection._get_default_sqlalchemy_connection_string(database)

    @staticmethod
    def _get_default_sqlalchemy_connection_string(database: str) -> str:

        driver_encoded = quote_plus("ODBC Driver 17 for SQL Server")
        return (
            f"mssql+pyodbc://@CRISTIANDELL/{database}?"
            f"driver={driver_encoded}&"
            f"Trusted_Connection=yes"
        )

    @staticmethod
    def get_sqlalchemy_engine(database: str, use_secrets: bool = True, **engine_kwargs) -> Engine:

        conn_str = DatabaseConnection.get_sqlalchemy_connection_string(database, use_secrets)

        default_kwargs = {
            'pool_pre_ping': True,
            'pool_recycle': 3600,
            'echo': False
        }

        default_kwargs.update(engine_kwargs)

        try:
            engine = create_engine(conn_str, **default_kwargs)
            logger.info(f"SQLAlchemy engine creado exitosamente para {database}")
            return engine
        except Exception as e:
            logger.error(f"Error creando SQLAlchemy engine para {database}: {str(e)}")
            raise

    @staticmethod
    def get_oltp_engine(use_secrets: bool = True, **engine_kwargs) -> Engine:

        return DatabaseConnection.get_sqlalchemy_engine(
            DatabaseConnection.OLTP_DATABASE,
            use_secrets,
            **engine_kwargs
        )

    @staticmethod
    def get_dw_engine(use_secrets: bool = True, **engine_kwargs) -> Engine:

        return DatabaseConnection.get_sqlalchemy_engine(
            DatabaseConnection.DW_DATABASE,
            use_secrets,
            **engine_kwargs
        )

    @staticmethod
    def test_connection(database: str, use_secrets: bool = True) -> Dict[str, any]:

        result = {
            "success": False,
            "message": "",
            "error": None
        }

        try:
            conn = DatabaseConnection.get_connection(database, use_secrets)
            cursor = conn.cursor()

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


def get_oltp_connection(use_secrets: bool = True) -> pyodbc.Connection:

    return DatabaseConnection.get_oltp_connection(use_secrets)


def get_dw_connection(use_secrets: bool = True) -> pyodbc.Connection:

    return DatabaseConnection.get_dw_connection(use_secrets)


def test_connections(use_secrets: bool = True) -> Dict[str, Dict[str, any]]:

    return DatabaseConnection.test_all_connections(use_secrets)


def get_oltp_engine(use_secrets: bool = True, **engine_kwargs) -> Engine:

    return DatabaseConnection.get_oltp_engine(use_secrets, **engine_kwargs)


def get_dw_engine(use_secrets: bool = True, **engine_kwargs) -> Engine:

    return DatabaseConnection.get_dw_engine(use_secrets, **engine_kwargs)
