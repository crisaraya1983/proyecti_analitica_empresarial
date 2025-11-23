"""
================================================================================
CARGA DE DIMENSIONES - ETL PIPELINE
================================================================================
Autor: Sistema de Analítica Empresarial
Fecha: 2025-01-15
Propósito: Cargar todas las tablas dimensionales desde OLTP a DW
================================================================================
"""

import pyodbc
import pandas as pd
from typing import Dict, Tuple
from etl_logger import ETLLogger
import logging

logger = logging.getLogger(__name__)


class DimensionLoader:
    """Clase para cargar dimensiones desde OLTP a DW"""

    def __init__(self, conn_oltp: pyodbc.Connection, conn_dw: pyodbc.Connection):
        """
        Inicializa el cargador de dimensiones

        Args:
            conn_oltp: Conexión a Ecommerce_OLTP
            conn_dw: Conexión a Ecommerce_DW
        """
        self.conn_oltp = conn_oltp
        self.conn_dw = conn_dw

    def truncate_all_tables(self):
        """
        Limpia todas las tablas (hechos primero, luego dimensiones) para evitar conflictos de FK
        Usa TRUNCATE cuando sea posible, DELETE cuando TRUNCATE falle por FK constraints
        """
        logger.info("=" * 80)
        logger.info("LIMPIANDO TABLAS (HECHOS Y DIMENSIONES)")
        logger.info("=" * 80)

        cursor_dw = self.conn_dw.cursor()

        try:
            # PASO 1: Limpiar tablas de HECHOS primero (no tienen FK salientes)
            fact_tables = ['fact_ventas', 'fact_comportamiento_web', 'fact_busquedas']

            logger.info("Limpiando tablas de hechos...")
            for table in fact_tables:
                try:
                    cursor_dw.execute(f"TRUNCATE TABLE {table}")
                    self.conn_dw.commit()
                    logger.info(f"  OK - {table} limpiada (TRUNCATE)")
                except Exception as e:
                    # Si TRUNCATE falla, intentar con DELETE
                    try:
                        cursor_dw.execute(f"DELETE FROM {table}")
                        self.conn_dw.commit()
                        logger.info(f"  OK - {table} limpiada (DELETE)")
                    except Exception as e2:
                        logger.error(f"  ERROR - No se pudo limpiar {table}: {str(e2)}")
                        raise

            # PASO 2: Ahora limpiar las DIMENSIONES
            dimension_tables = [
                'dim_tiempo', 'dim_producto', 'dim_cliente', 'dim_geografia',
                'dim_almacen', 'dim_dispositivo', 'dim_navegador',
                'dim_tipo_evento', 'dim_estado_venta', 'dim_metodo_pago'
            ]

            logger.info("\nLimpiando tablas dimensionales...")
            for table in dimension_tables:
                try:
                    cursor_dw.execute(f"TRUNCATE TABLE {table}")
                    self.conn_dw.commit()
                    logger.info(f"  OK - {table} limpiada (TRUNCATE)")
                except Exception as e:
                    # Si TRUNCATE falla por FK, intentar con DELETE
                    try:
                        cursor_dw.execute(f"DELETE FROM {table}")
                        self.conn_dw.commit()
                        logger.info(f"  OK - {table} limpiada (DELETE)")
                    except Exception as e2:
                        logger.error(f"  ERROR - No se pudo limpiar {table}: {str(e2)}")
                        raise

            logger.info("\nOK - Todas las tablas limpiadas exitosamente")

        except Exception as e:
            logger.error(f"Error limpiando tablas: {str(e)}")
            self.conn_dw.rollback()
            raise
        finally:
            cursor_dw.close()

    def load_all_dimensions(self) -> Dict[str, Tuple[int, int]]:
        """
        Carga todas las dimensiones en orden de dependencias

        Returns:
            Diccionario con el resultado de cada dimensión: {nombre: (extraidos, insertados)}
        """
        results = {}

        logger.info("=" * 80)
        logger.info("INICIANDO CARGA DE DIMENSIONES")
        logger.info("=" * 80)

        # PRIMERO: Limpiar todas las tablas (hechos y dimensiones)
        self.truncate_all_tables()

        logger.info("\n" + "=" * 80)
        logger.info("CARGANDO DATOS EN DIMENSIONES")
        logger.info("=" * 80 + "\n")

        # Orden de carga (respetando dependencias)
        results["dim_tiempo"] = self.load_dim_tiempo()
        results["dim_geografia"] = self.load_dim_geografia()
        results["dim_producto"] = self.load_dim_producto()
        results["dim_cliente"] = self.load_dim_cliente()
        results["dim_almacen"] = self.load_dim_almacen()
        results["dim_dispositivo"] = self.load_dim_dispositivo()
        results["dim_navegador"] = self.load_dim_navegador()
        results["dim_tipo_evento"] = self.load_dim_tipo_evento()
        results["dim_estado_venta"] = self.load_dim_estado_venta()
        results["dim_metodo_pago"] = self.load_dim_metodo_pago()

        logger.info("=" * 80)
        logger.info("CARGA DE DIMENSIONES COMPLETADA")
        logger.info("=" * 80)

        return results

    def load_dim_tiempo(self) -> Tuple[int, int]:
        """Carga dimensión tiempo desde tabla tiempo en OLTP"""
        etl_logger = ETLLogger(self.conn_dw)
        etl_logger.iniciar_proceso("LOAD_DIM_TIEMPO", "dim_tiempo")

        try:
            logger.info("Cargando dim_tiempo...")

            cursor_dw = self.conn_dw.cursor()

            # Extraer datos de OLTP
            query = """
                SELECT
                    ID_FECHA,
                    FECHA_CAL,
                    DIA_CAL,
                    DIA_SEM_NUM,
                    UPPER(DIA_SEM_ABRV) AS DIA_SEM_ABRV,
                    UPPER(DIA_SEM_NOMBRE) AS DIA_SEM_NOMBRE,
                    MES_CAL,
                    UPPER(MES_NOMBRE) AS MES_NOMBRE,
                    UPPER(MES_CAL_ABRV) AS MES_CAL_ABRV,
                    MES_CAL_FECHA_INIC,
                    MES_CAL_FECHA_FIN,
                    ANIO_CAL,
                    ANIO_CAL_FECHA_INIC,
                    ANIO_CAL_FECHA_FIN,
                    CAST(REPLACE(ANIO_MES_CAL_NUM, '-', '') AS INT) AS ANIO_MES_CAL_NUM,
                    UPPER(ANIO_MES_CAL_DESCR) AS ANIO_MES_CAL_DESCR,
                    TRIMESTRE,
                    SEM_CAL_NUM,
                    FECHA_INIC_SEM,
                    FECHA_FIN_SEM
                FROM tiempo
            """

            df = pd.read_sql(query, self.conn_oltp)
            registros_extraidos = len(df)

            if registros_extraidos == 0:
                logger.warning("No hay datos en tabla tiempo (OLTP)")
                etl_logger.finalizar_proceso(0, 0)
                return (0, 0)

            # Insertar en DW
            cursor_dw.fast_executemany = True
            insert_sql = """
                INSERT INTO dim_tiempo (
                    ID_FECHA, FECHA_CAL, DIA_CAL, DIA_SEM_NUM, DIA_SEM_ABRV,
                    DIA_SEM_NOMBRE, MES_CAL, MES_NOMBRE, MES_CAL_ABRV,
                    MES_CAL_FECHA_INIC, MES_CAL_FECHA_FIN, ANIO_CAL,
                    ANIO_CAL_FECHA_INIC, ANIO_CAL_FECHA_FIN, ANIO_MES_CAL_NUM,
                    ANIO_MES_CAL_DESCR, TRIMESTRE, SEM_CAL_NUM,
                    FECHA_INIC_SEM, FECHA_FIN_SEM
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            cursor_dw.executemany(insert_sql, df.values.tolist())
            self.conn_dw.commit()
            cursor_dw.close()

            logger.info(f"✓ dim_tiempo: {registros_extraidos} registros cargados")
            etl_logger.finalizar_proceso(registros_extraidos, registros_extraidos)

            return (registros_extraidos, registros_extraidos)

        except Exception as e:
            logger.error(f"Error cargando dim_tiempo: {str(e)}")
            etl_logger.registrar_error(str(e))
            raise

    def load_dim_geografia(self) -> Tuple[int, int]:
        """Carga dimensión geografía desde provincias, cantones y distritos"""
        etl_logger = ETLLogger(self.conn_dw)
        etl_logger.iniciar_proceso("LOAD_DIM_GEOGRAFIA", "dim_geografia")

        try:
            logger.info("Cargando dim_geografia...")

            cursor_dw = self.conn_dw.cursor()

            # Extraer datos de OLTP con JOIN
            query = """
                SELECT DISTINCT
                    d.provincia_id,
                    d.canton_id,
                    d.distrito_id,
                    UPPER(p.nombre_provincia) AS provincia,
                    UPPER(c.nombre_canton) AS canton,
                    UPPER(d.nombre_distrito) AS distrito
                FROM distritos d
                INNER JOIN provincias p ON d.provincia_id = p.provincia_id
                INNER JOIN cantones c ON d.canton_id = c.canton_id
                ORDER BY d.provincia_id, d.canton_id, d.distrito_id
            """

            df = pd.read_sql(query, self.conn_oltp)
            registros_extraidos = len(df)

            # Insertar en DW
            cursor_dw.fast_executemany = True
            insert_sql = """
                INSERT INTO dim_geografia (
                    provincia_id, canton_id, distrito_id,
                    provincia, canton, distrito
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """

            cursor_dw.executemany(insert_sql, df.values.tolist())
            self.conn_dw.commit()
            cursor_dw.close()

            logger.info(f"✓ dim_geografia: {registros_extraidos} registros cargados")
            etl_logger.finalizar_proceso(registros_extraidos, registros_extraidos)

            return (registros_extraidos, registros_extraidos)

        except Exception as e:
            logger.error(f"Error cargando dim_geografia: {str(e)}")
            etl_logger.registrar_error(str(e))
            raise

    def load_dim_producto(self) -> Tuple[int, int]:
        """Carga dimensión producto desde productos y categorias"""
        etl_logger = ETLLogger(self.conn_dw)
        etl_logger.iniciar_proceso("LOAD_DIM_PRODUCTO", "dim_producto")

        try:
            logger.info("Cargando dim_producto...")

            cursor_dw = self.conn_dw.cursor()
            # Extraer datos de OLTP
            query = """
                SELECT
                    p.producto_id,
                    UPPER(p.codigo_producto) AS codigo_producto,
                    UPPER(p.nombre_producto) AS nombre_producto,
                    p.categoria_id,
                    UPPER(c.nombre_categoria) AS categoria,
                    p.descripcion,
                    UPPER(p.marca) AS marca,
                    p.precio_unitario,
                    p.costo_unitario,
                    p.activo,
                    p.fecha_creacion,
                    p.fecha_actualizacion
                FROM productos p
                INNER JOIN categorias c ON p.categoria_id = c.categoria_id
            """

            df = pd.read_sql(query, self.conn_oltp)
            registros_extraidos = len(df)

            # Insertar en DW
            cursor_dw.fast_executemany = True
            insert_sql = """
                INSERT INTO dim_producto (
                    producto_id, codigo_producto, nombre_producto,
                    categoria_id, categoria, descripcion, marca,
                    precio_unitario, costo_unitario, activo,
                    fecha_creacion, fecha_actualizacion
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            cursor_dw.executemany(insert_sql, df.values.tolist())
            self.conn_dw.commit()
            cursor_dw.close()

            logger.info(f"✓ dim_producto: {registros_extraidos} registros cargados")
            etl_logger.finalizar_proceso(registros_extraidos, registros_extraidos)

            return (registros_extraidos, registros_extraidos)

        except Exception as e:
            logger.error(f"Error cargando dim_producto: {str(e)}")
            etl_logger.registrar_error(str(e))
            raise

    def load_dim_cliente(self) -> Tuple[int, int]:
        """Carga dimensión cliente desde clientes y geografía"""
        etl_logger = ETLLogger(self.conn_dw)
        etl_logger.iniciar_proceso("LOAD_DIM_CLIENTE", "dim_cliente")

        try:
            logger.info("Cargando dim_cliente...")

            cursor_dw = self.conn_dw.cursor()
            # Extraer datos de OLTP
            query = """
                SELECT
                    c.cliente_id,
                    UPPER(c.nombre_cliente) AS nombre_cliente,
                    UPPER(c.apellido_cliente) AS apellido_cliente,
                    UPPER(c.correo_electronico) AS correo_electronico,
                    c.telefono,
                    c.numero_cedula,
                    c.provincia_id,
                    c.canton_id,
                    c.distrito_id,
                    UPPER(p.nombre_provincia) AS provincia,
                    UPPER(ca.nombre_canton) AS canton,
                    UPPER(d.nombre_distrito) AS distrito,
                    c.direccion,
                    c.fecha_creacion,
                    CASE
                        WHEN YEAR(c.fecha_primer_compra) < 1753 THEN NULL
                        ELSE c.fecha_primer_compra
                    END AS fecha_primer_compra,
                    CASE
                        WHEN YEAR(c.fecha_ultimo_compra) < 1753 THEN NULL
                        ELSE c.fecha_ultimo_compra
                    END AS fecha_ultimo_compra,
                    c.activo
                FROM clientes c
                INNER JOIN provincias p ON c.provincia_id = p.provincia_id
                INNER JOIN cantones ca ON c.canton_id = ca.canton_id
                INNER JOIN distritos d ON c.distrito_id = d.distrito_id
            """

            df = pd.read_sql(query, self.conn_oltp)
            registros_extraidos = len(df)

            # Reemplazar NaT (Not a Time) con None para campos de fecha
            df['fecha_primer_compra'] = df['fecha_primer_compra'].where(pd.notna(df['fecha_primer_compra']), None)
            df['fecha_ultimo_compra'] = df['fecha_ultimo_compra'].where(pd.notna(df['fecha_ultimo_compra']), None)

            # Insertar en DW usando executemany sin fast_executemany
            # (fast_executemany tiene problemas con tipos de fecha mixtos)
            insert_sql = """
                INSERT INTO dim_cliente (
                    cliente_id, nombre_cliente, apellido_cliente,
                    correo_electronico, telefono, numero_cedula,
                    provincia_id, canton_id, distrito_id,
                    provincia, canton, distrito, direccion,
                    fecha_registro, fecha_primer_compra, fecha_ultimo_compra, activo
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            # Insertar por lotes para mejor rendimiento
            batch_size = 1000
            total_inserted = 0

            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size]
                data_batch = [tuple(row) for row in batch_df.values]
                cursor_dw.executemany(insert_sql, data_batch)
                self.conn_dw.commit()
                total_inserted += len(data_batch)
                logger.info(f"  Insertados {total_inserted}/{len(df)} registros...")
            self.conn_dw.commit()
            cursor_dw.close()

            logger.info(f"✓ dim_cliente: {registros_extraidos} registros cargados")
            etl_logger.finalizar_proceso(registros_extraidos, registros_extraidos)

            return (registros_extraidos, registros_extraidos)

        except Exception as e:
            logger.error(f"Error cargando dim_cliente: {str(e)}")
            etl_logger.registrar_error(str(e))
            raise

    def load_dim_almacen(self) -> Tuple[int, int]:
        """Carga dimensión almacén desde almacenes"""
        etl_logger = ETLLogger(self.conn_dw)
        etl_logger.iniciar_proceso("LOAD_DIM_ALMACEN", "dim_almacen")

        try:
            logger.info("Cargando dim_almacen...")

            cursor_dw = self.conn_dw.cursor()
            # Extraer datos de OLTP
            query = """
                SELECT
                    almacen_id,
                    UPPER(codigo_almacen) AS codigo_almacen,
                    UPPER(nombre_almacen) AS nombre_almacen,
                    UPPER(tipo_almacen) AS tipo_almacen,
                    UPPER(responsable_almacen) AS responsable,
                    provincia_id,
                    canton_id,
                    distrito_id,
                    direccion,
                    telefono,
                    correo_electronico AS correo,
                    latitud,
                    longitud,
                    activo,
                    fecha_apertura
                FROM almacenes
            """

            df = pd.read_sql(query, self.conn_oltp)
            registros_extraidos = len(df)

            # Insertar en DW
            cursor_dw.fast_executemany = True
            insert_sql = """
                INSERT INTO dim_almacen (
                    almacen_id, codigo_almacen, nombre_almacen, tipo_almacen,
                    responsable, provincia_id, canton_id, distrito_id,
                    direccion, telefono, correo, latitud, longitud,
                    activo, fecha_apertura
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            cursor_dw.executemany(insert_sql, df.values.tolist())
            self.conn_dw.commit()
            cursor_dw.close()

            logger.info(f"✓ dim_almacen: {registros_extraidos} registros cargados")
            etl_logger.finalizar_proceso(registros_extraidos, registros_extraidos)

            return (registros_extraidos, registros_extraidos)

        except Exception as e:
            logger.error(f"Error cargando dim_almacen: {str(e)}")
            etl_logger.registrar_error(str(e))
            raise

    def load_dim_dispositivo(self) -> Tuple[int, int]:
        """Carga dimensión dispositivo desde eventos_web (valores únicos)"""
        etl_logger = ETLLogger(self.conn_dw)
        etl_logger.iniciar_proceso("LOAD_DIM_DISPOSITIVO", "dim_dispositivo")

        try:
            logger.info("Cargando dim_dispositivo...")

            # Limpiar tabla destino (debe resetearse el IDENTITY)
            cursor_dw = self.conn_dw.cursor()
            # Extraer valores únicos de dispositivos de OLTP
            query = """
                SELECT DISTINCT
                    UPPER(tipo_dispositivo) AS tipo_dispositivo,
                    UPPER(dispositivo) AS dispositivo,
                    UPPER(sistema_operativo) AS sistema_operativo
                FROM eventos_web
                WHERE tipo_dispositivo IS NOT NULL

                UNION

                SELECT DISTINCT
                    UPPER(tipo_dispositivo) AS tipo_dispositivo,
                    UPPER(dispositivo) AS dispositivo,
                    UPPER(sistema_operativo) AS sistema_operativo
                FROM busquedas_web
                WHERE tipo_dispositivo IS NOT NULL
            """

            df = pd.read_sql(query, self.conn_oltp)
            df = df.drop_duplicates()
            registros_extraidos = len(df)

            # Insertar en DW
            cursor_dw.fast_executemany = True
            insert_sql = """
                INSERT INTO dim_dispositivo (
                    tipo_dispositivo, dispositivo, sistema_operativo
                )
                VALUES (?, ?, ?)
            """

            cursor_dw.executemany(insert_sql, df.values.tolist())
            self.conn_dw.commit()
            cursor_dw.close()

            logger.info(f"✓ dim_dispositivo: {registros_extraidos} registros cargados")
            etl_logger.finalizar_proceso(registros_extraidos, registros_extraidos)

            return (registros_extraidos, registros_extraidos)

        except Exception as e:
            logger.error(f"Error cargando dim_dispositivo: {str(e)}")
            etl_logger.registrar_error(str(e))
            raise

    def load_dim_navegador(self) -> Tuple[int, int]:
        """Carga dimensión navegador desde eventos_web y busquedas_web"""
        etl_logger = ETLLogger(self.conn_dw)
        etl_logger.iniciar_proceso("LOAD_DIM_NAVEGADOR", "dim_navegador")

        try:
            logger.info("Cargando dim_navegador...")

            cursor_dw = self.conn_dw.cursor()
            # Extraer valores únicos de navegadores
            query = """
                SELECT DISTINCT
                    UPPER(navegador) AS navegador
                FROM eventos_web
                WHERE navegador IS NOT NULL

                UNION

                SELECT DISTINCT
                    UPPER(navegador) AS navegador
                FROM busquedas_web
                WHERE navegador IS NOT NULL
            """

            df = pd.read_sql(query, self.conn_oltp)
            df = df.drop_duplicates()
            registros_extraidos = len(df)

            # Clasificar tipo de navegador (simplificado)
            df['tipo_navegador'] = 'Web'

            # Insertar en DW
            cursor_dw.fast_executemany = True
            insert_sql = """
                INSERT INTO dim_navegador (navegador, tipo_navegador)
                VALUES (?, ?)
            """

            cursor_dw.executemany(insert_sql, df.values.tolist())
            self.conn_dw.commit()
            cursor_dw.close()

            logger.info(f"✓ dim_navegador: {registros_extraidos} registros cargados")
            etl_logger.finalizar_proceso(registros_extraidos, registros_extraidos)

            return (registros_extraidos, registros_extraidos)

        except Exception as e:
            logger.error(f"Error cargando dim_navegador: {str(e)}")
            etl_logger.registrar_error(str(e))
            raise

    def load_dim_tipo_evento(self) -> Tuple[int, int]:
        """Carga dimensión tipo evento desde eventos_web"""
        etl_logger = ETLLogger(self.conn_dw)
        etl_logger.iniciar_proceso("LOAD_DIM_TIPO_EVENTO", "dim_tipo_evento")

        try:
            logger.info("Cargando dim_tipo_evento...")

            cursor_dw = self.conn_dw.cursor()
            # Extraer valores únicos de tipo de evento
            query = """
                SELECT DISTINCT
                    UPPER(tipo_evento) AS tipo_evento
                FROM eventos_web
                WHERE tipo_evento IS NOT NULL
            """

            df = pd.read_sql(query, self.conn_oltp)
            df = df.drop_duplicates()
            registros_extraidos = len(df)

            # Clasificar categoría de evento y si es conversión
            df['categoria_evento'] = df['tipo_evento'].apply(
                lambda x: 'Transacción' if 'VENTA' in x or 'COMPRA' in x
                else 'Navegación'
            )
            df['descripcion'] = None
            df['es_conversion'] = df['tipo_evento'].apply(
                lambda x: 1 if 'COMPLETADA' in x else 0
            )

            # Insertar en DW
            cursor_dw.fast_executemany = True
            insert_sql = """
                INSERT INTO dim_tipo_evento (
                    tipo_evento, categoria_evento, descripcion, es_conversion
                )
                VALUES (?, ?, ?, ?)
            """

            cursor_dw.executemany(insert_sql, df.values.tolist())
            self.conn_dw.commit()
            cursor_dw.close()

            logger.info(f"✓ dim_tipo_evento: {registros_extraidos} registros cargados")
            etl_logger.finalizar_proceso(registros_extraidos, registros_extraidos)

            return (registros_extraidos, registros_extraidos)

        except Exception as e:
            logger.error(f"Error cargando dim_tipo_evento: {str(e)}")
            etl_logger.registrar_error(str(e))
            raise

    def load_dim_estado_venta(self) -> Tuple[int, int]:
        """Carga dimensión estado venta desde ventas"""
        etl_logger = ETLLogger(self.conn_dw)
        etl_logger.iniciar_proceso("LOAD_DIM_ESTADO_VENTA", "dim_estado_venta")

        try:
            logger.info("Cargando dim_estado_venta...")

            cursor_dw = self.conn_dw.cursor()
            # Extraer valores únicos de estado de venta
            query = """
                SELECT DISTINCT
                    UPPER(estado_venta) AS estado_venta
                FROM ventas
                WHERE estado_venta IS NOT NULL
            """

            df = pd.read_sql(query, self.conn_oltp)
            df = df.drop_duplicates()
            registros_extraidos = len(df)

            # Clasificar si es exitosa
            df['descripcion'] = None
            df['es_exitosa'] = df['estado_venta'].apply(
                lambda x: 0 if 'CANCELADA' in x or 'ANULADA' in x else 1
            )

            # Insertar en DW
            cursor_dw.fast_executemany = True
            insert_sql = """
                INSERT INTO dim_estado_venta (
                    estado_venta, descripcion, es_exitosa
                )
                VALUES (?, ?, ?)
            """

            cursor_dw.executemany(insert_sql, df.values.tolist())
            self.conn_dw.commit()
            cursor_dw.close()

            logger.info(f"✓ dim_estado_venta: {registros_extraidos} registros cargados")
            etl_logger.finalizar_proceso(registros_extraidos, registros_extraidos)

            return (registros_extraidos, registros_extraidos)

        except Exception as e:
            logger.error(f"Error cargando dim_estado_venta: {str(e)}")
            etl_logger.registrar_error(str(e))
            raise

    def load_dim_metodo_pago(self) -> Tuple[int, int]:
        """Carga dimensión método de pago desde ventas"""
        etl_logger = ETLLogger(self.conn_dw)
        etl_logger.iniciar_proceso("LOAD_DIM_METODO_PAGO", "dim_metodo_pago")

        try:
            logger.info("Cargando dim_metodo_pago...")

            cursor_dw = self.conn_dw.cursor()
            # Extraer valores únicos de método de pago
            query = """
                SELECT DISTINCT
                    UPPER(metodo_pago) AS metodo_pago
                FROM ventas
                WHERE metodo_pago IS NOT NULL
            """

            df = pd.read_sql(query, self.conn_oltp)
            df = df.drop_duplicates()
            registros_extraidos = len(df)

            # Clasificar tipo de pago
            df['descripcion'] = None
            df['tipo_pago'] = df['metodo_pago'].apply(
                lambda x: 'Tarjeta' if 'TARJETA' in x
                else 'Transferencia' if 'SINPE' in x or 'TRANSFERENCIA' in x
                else 'Digital'
            )

            # Insertar en DW
            cursor_dw.fast_executemany = True
            insert_sql = """
                INSERT INTO dim_metodo_pago (
                    metodo_pago, descripcion, tipo_pago
                )
                VALUES (?, ?, ?)
            """

            cursor_dw.executemany(insert_sql, df.values.tolist())
            self.conn_dw.commit()
            cursor_dw.close()

            logger.info(f"✓ dim_metodo_pago: {registros_extraidos} registros cargados")
            etl_logger.finalizar_proceso(registros_extraidos, registros_extraidos)

            return (registros_extraidos, registros_extraidos)

        except Exception as e:
            logger.error(f"Error cargando dim_metodo_pago: {str(e)}")
            etl_logger.registrar_error(str(e))
            raise
