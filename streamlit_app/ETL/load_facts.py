import pyodbc
import pandas as pd
import numpy as np
from typing import Dict, Tuple
from etl_logger import ETLLogger
import logging

logger = logging.getLogger(__name__)


class FactLoader:

    def __init__(self, conn_oltp: pyodbc.Connection, conn_dw: pyodbc.Connection):

        self.conn_oltp = conn_oltp
        self.conn_dw = conn_dw

    def load_all_facts(self) -> Dict[str, Tuple[int, int]]:
        results = {}

        logger.info("=" * 80)
        logger.info("INICIANDO CARGA DE TABLAS DE HECHOS")
        logger.info("=" * 80)

        results["fact_ventas"] = self.load_fact_ventas()
        results["fact_comportamiento_web"] = self.load_fact_comportamiento_web()
        results["fact_busquedas"] = self.load_fact_busquedas()

        logger.info("=" * 80)
        logger.info("CARGA DE TABLAS DE HECHOS COMPLETADA")
        logger.info("=" * 80)

        return results

    def load_fact_ventas(self) -> Tuple[int, int]:

        etl_logger = ETLLogger(self.conn_dw)
        etl_logger.iniciar_proceso("LOAD_FACT_VENTAS", "fact_ventas")

        try:
            logger.info("Cargando fact_ventas...")

            cursor_dw = self.conn_dw.cursor()
            cursor_dw.execute("TRUNCATE TABLE fact_ventas")
            self.conn_dw.commit()

            query = """
                SELECT
                    -- Tiempo (convertir fecha_venta a ID_FECHA formato YYYYMMDD)
                    CONVERT(INT, CONVERT(VARCHAR(8), v.fecha_venta, 112)) AS tiempo_key,

                    -- Producto
                    dv.producto_id,

                    -- Cliente
                    v.cliente_id,

                    -- Geografía del cliente
                    c.provincia_id,
                    c.canton_id,
                    c.distrito_id,

                    -- Almacén
                    v.almacen_id,

                    -- Estado venta y método de pago (necesitan lookup en dimensiones)
                    v.estado_venta,
                    v.metodo_pago,

                    -- IDs degenerados
                    v.venta_id,
                    dv.detalle_venta_id,

                    -- Medidas
                    dv.cantidad,
                    dv.precio_unitario,
                    dv.costo_unitario,
                    dv.descuento_porcentaje,
                    dv.descuento_monto,
                    dv.subtotal,
                    dv.impuesto,
                    dv.monto_total,
                    dv.margen,

                    -- Flags
                    CASE
                        WHEN v.fecha_venta = c.fecha_primer_compra THEN 1
                        ELSE 0
                    END AS es_primera_compra,

                    CASE
                        WHEN v.estado_venta LIKE '%CANCELAD%' OR v.estado_venta LIKE '%ANULAD%' THEN 1
                        ELSE 0
                    END AS venta_cancelada

                FROM detalles_venta dv
                INNER JOIN ventas v ON dv.venta_id = v.venta_id
                INNER JOIN clientes c ON v.cliente_id = c.cliente_id
            """

            df = pd.read_sql(query, self.conn_oltp)
            registros_extraidos = len(df)

            logger.info(f"  Extraídos {registros_extraidos:,} registros de OLTP")

            logger.info("  Obteniendo mappings de dimensiones...")

            cursor_dw.execute("""
                SELECT estado_venta_id, estado_venta
                FROM dim_estado_venta
            """)
            estado_venta_map = {row[1]: row[0] for row in cursor_dw.fetchall()}

            cursor_dw.execute("""
                SELECT metodo_pago_id, metodo_pago
                FROM dim_metodo_pago
            """)
            metodo_pago_map = {row[1]: row[0] for row in cursor_dw.fetchall()}

            df['estado_venta_id'] = df['estado_venta'].str.upper().map(estado_venta_map)
            df['metodo_pago_id'] = df['metodo_pago'].str.upper().map(metodo_pago_map)

            df = df.drop(['estado_venta', 'metodo_pago'], axis=1)

            if df['estado_venta_id'].isna().any() or df['metodo_pago_id'].isna().any():
                logger.warning("  ⚠ Hay valores NULL en estado_venta_id o metodo_pago_id")

            decimal_cols = ['precio_unitario', 'costo_unitario', 'descuento_porcentaje',
                           'descuento_monto', 'subtotal', 'impuesto', 'monto_total', 'margen']
            for col in decimal_cols:
                df[col] = df[col].apply(lambda x: float(round(x, 2)) if pd.notna(x) else 0.0)

            int_cols = ['tiempo_key', 'producto_id', 'cliente_id', 'provincia_id',
                       'canton_id', 'distrito_id', 'almacen_id', 'estado_venta_id',
                       'metodo_pago_id', 'venta_id', 'detalle_venta_id', 'cantidad',
                       'es_primera_compra', 'venta_cancelada']
            for col in int_cols:
                df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) else 0)

            logger.info("  Insertando en fact_ventas...")
            cursor_dw.fast_executemany = False

            insert_sql = """
                INSERT INTO fact_ventas (
                    tiempo_key, producto_id, cliente_id,
                    provincia_id, canton_id, distrito_id,
                    almacen_id, estado_venta_id, metodo_pago_id,
                    venta_id, detalle_venta_id,
                    cantidad, precio_unitario, costo_unitario,
                    descuento_porcentaje, descuento_monto,
                    subtotal, impuesto, monto_total, margen,
                    es_primera_compra, venta_cancelada
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?,
                    CAST(? AS DECIMAL(12,2)),
                    CAST(? AS DECIMAL(12,2)),
                    CAST(? AS DECIMAL(5,2)),
                    CAST(? AS DECIMAL(12,2)),
                    CAST(? AS DECIMAL(12,2)),
                    CAST(? AS DECIMAL(12,2)),
                    CAST(? AS DECIMAL(12,2)),
                    CAST(? AS DECIMAL(12,2)),
                    ?, ?
                )
            """

            column_order = [
                'tiempo_key', 'producto_id', 'cliente_id',
                'provincia_id', 'canton_id', 'distrito_id',
                'almacen_id', 'estado_venta_id', 'metodo_pago_id',
                'venta_id', 'detalle_venta_id',
                'cantidad', 'precio_unitario', 'costo_unitario',
                'descuento_porcentaje', 'descuento_monto',
                'subtotal', 'impuesto', 'monto_total', 'margen',
                'es_primera_compra', 'venta_cancelada'
            ]
            df = df[column_order]

            BATCH_SIZE = 5000
            COMMIT_EVERY = 10000
            CHECKPOINT_EVERY = 20000
            total_insertados = 0

            for i in range(0, len(df), BATCH_SIZE):
                batch = df.iloc[i:i + BATCH_SIZE]
                data_batch = [tuple(row) for row in batch.values]
                cursor_dw.executemany(insert_sql, data_batch)
                total_insertados += len(batch)

                if total_insertados % COMMIT_EVERY == 0:
                    self.conn_dw.commit()
                    logger.info(f"    Insertados: {total_insertados:,} / {len(df):,} (commit)")

                    if total_insertados % CHECKPOINT_EVERY == 0:
                        try:
                            cursor_dw.execute("CHECKPOINT")
                            cursor_dw.execute("DBCC SHRINKFILE('Ecommerce_DW_log', 1)")
                            logger.info(f"    → Log liberado (checkpoint)")
                        except Exception as log_err:
                            logger.warning(f"    ⚠ No se pudo liberar log: {log_err}")

            self.conn_dw.commit()
            cursor_dw.close()

            logger.info(f"✓ fact_ventas: {total_insertados:,} registros cargados")
            etl_logger.finalizar_proceso(registros_extraidos, total_insertados)

            return (registros_extraidos, total_insertados)

        except Exception as e:
            logger.error(f"Error cargando fact_ventas: {str(e)}")
            etl_logger.registrar_error(str(e), registros_extraidos if 'registros_extraidos' in locals() else 0)
            raise

    def load_fact_comportamiento_web(self) -> Tuple[int, int]:
        etl_logger = ETLLogger(self.conn_dw)
        etl_logger.iniciar_proceso("LOAD_FACT_COMPORTAMIENTO_WEB", "fact_comportamiento_web")

        try:
            logger.info("Cargando fact_comportamiento_web...")

            cursor_dw = self.conn_dw.cursor()
            cursor_dw.execute("TRUNCATE TABLE fact_comportamiento_web")
            self.conn_dw.commit()

            query = """
                SELECT
                    -- Tiempo
                    CONVERT(INT, CONVERT(VARCHAR(8), fecha_hora_evento, 112)) AS tiempo_key,

                    -- Cliente (puede ser NULL)
                    cliente_id,

                    -- Producto (puede ser NULL)
                    producto_id,

                    -- Dispositivo, navegador, tipo evento (necesitan lookup)
                    tipo_dispositivo,
                    dispositivo,
                    sistema_operativo,
                    navegador,
                    tipo_evento,

                    -- IDs degenerados
                    evento_id,
                    numero_evento_en_sesion AS numero_evento_sesion,
                    venta_id,

                    -- Medidas
                    ISNULL(tiempo_pagina_segundos, 0) AS tiempo_pagina_segundos,
                    1 AS eventos_sesion,

                    -- Flags
                    cliente_reconocido,
                    genero_venta

                FROM eventos_web
            """

            df = pd.read_sql(query, self.conn_oltp)
            registros_extraidos = len(df)

            logger.info(f"  Extraídos {registros_extraidos:,} registros de OLTP")

            logger.info("  Obteniendo mappings de dimensiones...")

            cursor_dw.execute("""
                SELECT dispositivo_id, tipo_dispositivo, dispositivo, sistema_operativo
                FROM dim_dispositivo
            """)
            dispositivo_map = {}
            for row in cursor_dw.fetchall():
                key = (
                    row[1] if row[1] else '',
                    row[2] if row[2] else '',
                    row[3] if row[3] else ''
                )
                dispositivo_map[key] = row[0]

            cursor_dw.execute("""
                SELECT navegador_id, navegador
                FROM dim_navegador
            """)
            navegador_map = {row[1]: row[0] for row in cursor_dw.fetchall()}

            cursor_dw.execute("""
                SELECT tipo_evento_id, tipo_evento
                FROM dim_tipo_evento
            """)
            tipo_evento_map = {row[1]: row[0] for row in cursor_dw.fetchall()}

            df['dispositivo_key'] = df.apply(
                lambda row: (
                    row['tipo_dispositivo'].upper() if pd.notna(row['tipo_dispositivo']) else '',
                    row['dispositivo'].upper() if pd.notna(row['dispositivo']) else '',
                    row['sistema_operativo'].upper() if pd.notna(row['sistema_operativo']) else ''
                ),
                axis=1
            )
            df['dispositivo_id'] = df['dispositivo_key'].map(dispositivo_map)
            df['navegador_id'] = df['navegador'].str.upper().map(navegador_map)
            df['tipo_evento_id'] = df['tipo_evento'].str.upper().map(tipo_evento_map)

            df = df.drop(['tipo_dispositivo', 'dispositivo', 'sistema_operativo',
                         'navegador', 'tipo_evento', 'dispositivo_key'], axis=1)

            null_count = df[['dispositivo_id', 'navegador_id', 'tipo_evento_id']].isna().sum()
            if null_count.any():
                logger.warning(f"  ⚠ Valores NULL encontrados: {null_count.to_dict()}")

                df = df.dropna(subset=['dispositivo_id', 'navegador_id', 'tipo_evento_id'])
                logger.warning(f"  Registros filtrados. Nuevos total: {len(df):,}")

            column_order = [
                'tiempo_key', 'cliente_id', 'producto_id',
                'dispositivo_id', 'navegador_id', 'tipo_evento_id',
                'evento_id', 'numero_evento_sesion', 'venta_id',
                'tiempo_pagina_segundos', 'eventos_sesion',
                'cliente_reconocido', 'genero_venta'
            ]
            df = df[column_order]

            logger.info("  Insertando en fact_comportamiento_web...")
            cursor_dw.fast_executemany = False

            insert_sql = """
                INSERT INTO fact_comportamiento_web (
                    tiempo_key, cliente_id, producto_id,
                    dispositivo_id, navegador_id, tipo_evento_id,
                    evento_id, numero_evento_sesion, venta_id,
                    tiempo_pagina_segundos, eventos_sesion,
                    cliente_reconocido, genero_venta
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            BATCH_SIZE = 5000
            COMMIT_EVERY = 10000
            CHECKPOINT_EVERY = 20000
            total_insertados = 0

            for i in range(0, len(df), BATCH_SIZE):
                batch = df.iloc[i:i + BATCH_SIZE]

                data_batch = []
                for _, row in batch.iterrows():
                    converted_row = tuple(
                        int(val) if pd.notna(val) and isinstance(val, (np.integer, np.floating, bool, int, float)) else (0 if pd.notna(val) else 0)
                        for val in row
                    )
                    data_batch.append(converted_row)
                cursor_dw.executemany(insert_sql, data_batch)
                total_insertados += len(batch)

                if total_insertados % COMMIT_EVERY == 0:
                    self.conn_dw.commit()
                    logger.info(f"    Insertados: {total_insertados:,} / {len(df):,} (commit)")

                    if total_insertados % CHECKPOINT_EVERY == 0:
                        try:
                            cursor_dw.execute("CHECKPOINT")
                            cursor_dw.execute("DBCC SHRINKFILE('Ecommerce_DW_log', 1)")
                            logger.info(f"    → Log liberado (checkpoint)")
                        except Exception as log_err:
                            logger.warning(f"    ⚠ No se pudo liberar log: {log_err}")

            self.conn_dw.commit()
            cursor_dw.close()

            logger.info(f"✓ fact_comportamiento_web: {total_insertados:,} registros cargados")
            etl_logger.finalizar_proceso(registros_extraidos, total_insertados)

            return (registros_extraidos, total_insertados)

        except Exception as e:
            logger.error(f"Error cargando fact_comportamiento_web: {str(e)}")
            etl_logger.registrar_error(str(e), registros_extraidos if 'registros_extraidos' in locals() else 0)
            raise

    def load_fact_busquedas(self) -> Tuple[int, int]:

        etl_logger = ETLLogger(self.conn_dw)
        etl_logger.iniciar_proceso("LOAD_FACT_BUSQUEDAS", "fact_busquedas")

        try:
            logger.info("Cargando fact_busquedas...")

            cursor_dw = self.conn_dw.cursor()
            cursor_dw.execute("TRUNCATE TABLE fact_busquedas")
            self.conn_dw.commit()

            query = """
                SELECT
                    -- Tiempo
                    CONVERT(INT, CONVERT(VARCHAR(8), fecha_hora_busqueda, 112)) AS tiempo_key,

                    -- Cliente (puede ser NULL)
                    cliente_id,

                    -- Producto visualizado (puede ser NULL)
                    producto_visualizado_id AS producto_id,

                    -- Dispositivo y navegador (necesitan lookup)
                    tipo_dispositivo,
                    dispositivo,
                    sistema_operativo,
                    navegador,

                    -- IDs degenerados
                    busqueda_id,
                    venta_id,

                    -- Medidas
                    ISNULL(cantidad_resultados, 0) AS cantidad_resultados,
                    1 AS total_busquedas,

                    -- Flags
                    cliente_reconocido,
                    genero_venta

                FROM busquedas_web
            """

            df = pd.read_sql(query, self.conn_oltp)
            registros_extraidos = len(df)

            logger.info(f"  Extraídos {registros_extraidos:,} registros de OLTP")

            logger.info("  Obteniendo mappings de dimensiones...")

            cursor_dw.execute("""
                SELECT dispositivo_id, tipo_dispositivo, dispositivo, sistema_operativo
                FROM dim_dispositivo
            """)
            dispositivo_map = {}
            for row in cursor_dw.fetchall():
                key = (
                    row[1] if row[1] else '',
                    row[2] if row[2] else '',
                    row[3] if row[3] else ''
                )
                dispositivo_map[key] = row[0]

            cursor_dw.execute("""
                SELECT navegador_id, navegador
                FROM dim_navegador
            """)
            navegador_map = {row[1]: row[0] for row in cursor_dw.fetchall()}

            df['dispositivo_key'] = df.apply(
                lambda row: (
                    row['tipo_dispositivo'].upper() if pd.notna(row['tipo_dispositivo']) else '',
                    row['dispositivo'].upper() if pd.notna(row['dispositivo']) else '',
                    row['sistema_operativo'].upper() if pd.notna(row['sistema_operativo']) else ''
                ),
                axis=1
            )
            df['dispositivo_id'] = df['dispositivo_key'].map(dispositivo_map)
            df['navegador_id'] = df['navegador'].str.upper().map(navegador_map)

            df = df.drop(['tipo_dispositivo', 'dispositivo', 'sistema_operativo',
                         'navegador', 'dispositivo_key'], axis=1)

            null_count = df[['dispositivo_id', 'navegador_id']].isna().sum()
            if null_count.any():
                logger.warning(f"  ⚠ Valores NULL encontrados: {null_count.to_dict()}")

                df = df.dropna(subset=['dispositivo_id', 'navegador_id'])
                logger.warning(f"  Registros filtrados. Nuevos total: {len(df):,}")

            column_order = [
                'tiempo_key', 'cliente_id', 'producto_id',
                'dispositivo_id', 'navegador_id',
                'busqueda_id', 'venta_id',
                'cantidad_resultados', 'total_busquedas',
                'cliente_reconocido', 'genero_venta'
            ]
            df = df[column_order]

            logger.info("  Insertando en fact_busquedas...")
            cursor_dw.fast_executemany = False

            insert_sql = """
                INSERT INTO fact_busquedas (
                    tiempo_key, cliente_id, producto_id,
                    dispositivo_id, navegador_id,
                    busqueda_id, venta_id,
                    cantidad_resultados, total_busquedas,
                    cliente_reconocido, genero_venta
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            BATCH_SIZE = 5000
            COMMIT_EVERY = 10000
            CHECKPOINT_EVERY = 20000
            total_insertados = 0

            for i in range(0, len(df), BATCH_SIZE):
                batch = df.iloc[i:i + BATCH_SIZE]

                data_batch = []
                for _, row in batch.iterrows():
                    converted_row = tuple(
                        int(val) if pd.notna(val) and isinstance(val, (np.integer, np.floating, bool, int, float)) else (0 if pd.notna(val) else 0)
                        for val in row
                    )
                    data_batch.append(converted_row)
                cursor_dw.executemany(insert_sql, data_batch)
                total_insertados += len(batch)

                if total_insertados % COMMIT_EVERY == 0:
                    self.conn_dw.commit()
                    logger.info(f"    Insertados: {total_insertados:,} / {len(df):,} (commit)")

                    if total_insertados % CHECKPOINT_EVERY == 0:
                        try:
                            cursor_dw.execute("CHECKPOINT")
                            cursor_dw.execute("DBCC SHRINKFILE('Ecommerce_DW_log', 1)")
                            logger.info(f"    → Log liberado (checkpoint)")
                        except Exception as log_err:
                            logger.warning(f"    ⚠ No se pudo liberar log: {log_err}")

            self.conn_dw.commit()
            cursor_dw.close()

            logger.info(f"✓ fact_busquedas: {total_insertados:,} registros cargados")
            etl_logger.finalizar_proceso(registros_extraidos, total_insertados)

            return (registros_extraidos, total_insertados)

        except Exception as e:
            logger.error(f"Error cargando fact_busquedas: {str(e)}")
            etl_logger.registrar_error(str(e), registros_extraidos if 'registros_extraidos' in locals() else 0)
            raise
