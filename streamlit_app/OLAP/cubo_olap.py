"""
================================================================================
MÓDULO CUBO OLAP - OPERACIONES MULTIDIMENSIONALES (CORREGIDO)
================================================================================
Nombres de columnas correctos según estructura real del DW
================================================================================
"""

import pyodbc
import pandas as pd
from typing import Optional, List, Dict, Tuple, Union
import logging
from datetime import datetime
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# MAPEO DE COLUMNAS REALES
COLUMN_MAPPING = {
    # dim_producto
    'nombre': 'nombre_producto',
    'margen_unitario': None,  # NO EXISTE en dim_producto

    # dim_cliente
    'nombre_completo': None,  # NO EXISTE - se debe construir
    'nombre': 'nombre_cliente',
    'apellido': 'apellido_cliente',
    'email': 'correo_electronico',

    # dim_almacen
    'nombre': 'nombre_almacen',

    # dim_tiempo
    'DIA_SEMANA_NOMBRE': 'DIA_SEM_NOMBRE',
    'MES_ABR': 'MES_CAL_ABRV',
    'DIA_SEMANA_ABR': 'DIA_SEM_ABRV',
}


class CuboOLAP:
    """
    Clase para realizar operaciones OLAP sobre el Data Warehouse Ecommerce
    Con nombres de columnas corregidos según estructura real
    """

    def __init__(self, connection: Union[pyodbc.Connection, Engine]):
        """
        Inicializa el cubo OLAP

        Args:
            connection: Puede ser una conexión pyodbc o un SQLAlchemy Engine.
                       Se recomienda usar SQLAlchemy Engine para evitar warnings de pandas.
        """
        self.conn = connection
        self.cache = {}
        logger.info("CuboOLAP inicializado")

    def _execute_query(self, query: str, params: Tuple = None) -> pd.DataFrame:
        """Ejecuta una query y retorna un DataFrame"""
        try:
            if params:
                df = pd.read_sql(query, self.conn, params=params)
            else:
                df = pd.read_sql(query, self.conn)
            return df
        except Exception as e:
            logger.error(f"Error ejecutando query: {str(e)}")
            raise

    # ========================================================================
    # OPERACIONES OLAP BÁSICAS
    # ========================================================================

    def slice(self, dimension: str, value: any, measure: str = "monto_total") -> pd.DataFrame:
        """SLICE: Selecciona un subrectángulo del cubo fijando una dimensión (con agrupación correcta por venta_id)"""
        logger.info(f"SLICE: {dimension} = {value}")

        dimension_queries = {
            'provincia': """
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.provincia_id,
                        fv.canton_id,
                        fv.distrito_id,
                        fv.cliente_id,
                        SUM(fv.cantidad) AS total_unidades,
                        SUM(fv.monto_total) AS monto_venta,
                        SUM(fv.margen) AS margen_venta,
                        SUM(fv.impuesto) AS impuesto_venta
                    FROM fact_ventas fv
                    WHERE fv.venta_cancelada = 0
                    GROUP BY fv.venta_id, fv.provincia_id, fv.canton_id, fv.distrito_id, fv.cliente_id
                )
                SELECT
                    g.provincia AS dimension_value,
                    COUNT(DISTINCT va.venta_id) AS cantidad_ordenes,
                    COUNT(DISTINCT va.cliente_id) AS clientes_unicos,
                    SUM(va.total_unidades) AS total_unidades,
                    SUM(va.monto_venta) AS total_ventas,
                    AVG(va.monto_venta) AS promedio_por_orden,
                    SUM(va.margen_venta) AS total_margen,
                    ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje,
                    SUM(va.impuesto_venta) AS total_impuesto
                FROM VentasAgrupadas va
                INNER JOIN dim_geografia g ON va.provincia_id = g.provincia_id
                    AND va.canton_id = g.canton_id AND va.distrito_id = g.distrito_id
                WHERE g.provincia = ?
                GROUP BY g.provincia
            """,
            'categoria': """
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.producto_id,
                        fv.cliente_id,
                        SUM(fv.cantidad) AS total_unidades,
                        SUM(fv.monto_total) AS monto_venta,
                        SUM(fv.margen) AS margen_venta,
                        SUM(fv.impuesto) AS impuesto_venta
                    FROM fact_ventas fv
                    WHERE fv.venta_cancelada = 0
                    GROUP BY fv.venta_id, fv.producto_id, fv.cliente_id
                )
                SELECT
                    pr.categoria AS dimension_value,
                    COUNT(DISTINCT va.venta_id) AS cantidad_ordenes,
                    COUNT(DISTINCT va.cliente_id) AS clientes_unicos,
                    SUM(va.total_unidades) AS total_unidades,
                    SUM(va.monto_venta) AS total_ventas,
                    AVG(va.monto_venta) AS promedio_por_orden,
                    SUM(va.margen_venta) AS total_margen,
                    ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje,
                    SUM(va.impuesto_venta) AS total_impuesto
                FROM VentasAgrupadas va
                INNER JOIN dim_producto pr ON va.producto_id = pr.producto_id
                WHERE pr.categoria = ?
                GROUP BY pr.categoria
            """,
            'almacen': """
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.almacen_id,
                        fv.cliente_id,
                        SUM(fv.cantidad) AS total_unidades,
                        SUM(fv.monto_total) AS monto_venta,
                        SUM(fv.margen) AS margen_venta,
                        SUM(fv.impuesto) AS impuesto_venta
                    FROM fact_ventas fv
                    WHERE fv.venta_cancelada = 0
                    GROUP BY fv.venta_id, fv.almacen_id, fv.cliente_id
                )
                SELECT
                    a.nombre_almacen AS dimension_value,
                    COUNT(DISTINCT va.venta_id) AS cantidad_ordenes,
                    COUNT(DISTINCT va.cliente_id) AS clientes_unicos,
                    SUM(va.total_unidades) AS total_unidades,
                    SUM(va.monto_venta) AS total_ventas,
                    AVG(va.monto_venta) AS promedio_por_orden,
                    SUM(va.margen_venta) AS total_margen,
                    ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje,
                    SUM(va.impuesto_venta) AS total_impuesto
                FROM VentasAgrupadas va
                INNER JOIN dim_almacen a ON va.almacen_id = a.almacen_id
                WHERE a.nombre_almacen = ?
                GROUP BY a.nombre_almacen
            """,
            'anio': """
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.tiempo_key,
                        fv.cliente_id,
                        SUM(fv.cantidad) AS total_unidades,
                        SUM(fv.monto_total) AS monto_venta,
                        SUM(fv.margen) AS margen_venta,
                        SUM(fv.impuesto) AS impuesto_venta
                    FROM fact_ventas fv
                    WHERE fv.venta_cancelada = 0
                    GROUP BY fv.venta_id, fv.tiempo_key, fv.cliente_id
                )
                SELECT
                    CAST(t.ANIO_CAL AS NVARCHAR) AS dimension_value,
                    COUNT(DISTINCT va.venta_id) AS cantidad_ordenes,
                    COUNT(DISTINCT va.cliente_id) AS clientes_unicos,
                    SUM(va.total_unidades) AS total_unidades,
                    SUM(va.monto_venta) AS total_ventas,
                    AVG(va.monto_venta) AS promedio_por_orden,
                    SUM(va.margen_venta) AS total_margen,
                    ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje,
                    SUM(va.impuesto_venta) AS total_impuesto
                FROM VentasAgrupadas va
                INNER JOIN dim_tiempo t ON va.tiempo_key = t.ID_FECHA
                WHERE t.ANIO_CAL = ?
                GROUP BY t.ANIO_CAL
            """
        }

        if dimension not in dimension_queries:
            raise ValueError(f"Dimensión no soportada: {dimension}")

        return self._execute_query(dimension_queries[dimension], (value,))

    def slice_drill_down(self, dimension: str, value: any) -> pd.DataFrame:
        """Obtiene el desglose detallado después de aplicar SLICE para mostrar en gráficos"""
        logger.info(f"SLICE Drill-Down: {dimension} = {value}")

        drill_down_queries = {
            'provincia': """
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.provincia_id,
                        fv.canton_id,
                        fv.distrito_id,
                        SUM(fv.monto_total) AS monto_venta,
                        SUM(fv.margen) AS margen_venta
                    FROM fact_ventas fv
                    WHERE fv.venta_cancelada = 0
                    GROUP BY fv.venta_id, fv.provincia_id, fv.canton_id, fv.distrito_id
                )
                SELECT TOP 15
                    g.canton,
                    COUNT(DISTINCT va.venta_id) AS cantidad_ordenes,
                    SUM(va.monto_venta) AS total_ventas,
                    SUM(va.margen_venta) AS total_margen,
                    ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje
                FROM VentasAgrupadas va
                INNER JOIN dim_geografia g ON va.provincia_id = g.provincia_id
                    AND va.canton_id = g.canton_id AND va.distrito_id = g.distrito_id
                WHERE g.provincia = ?
                GROUP BY g.canton
                ORDER BY SUM(va.monto_venta) DESC
            """,
            'categoria': """
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.producto_id,
                        SUM(fv.monto_total) AS monto_venta,
                        SUM(fv.margen) AS margen_venta,
                        SUM(fv.cantidad) AS cantidad
                    FROM fact_ventas fv
                    WHERE fv.venta_cancelada = 0
                    GROUP BY fv.venta_id, fv.producto_id
                )
                SELECT TOP 15
                    pr.nombre_producto AS producto,
                    COUNT(DISTINCT va.venta_id) AS cantidad_ordenes,
                    SUM(va.cantidad) AS unidades_vendidas,
                    SUM(va.monto_venta) AS total_ventas,
                    SUM(va.margen_venta) AS total_margen,
                    ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje
                FROM VentasAgrupadas va
                INNER JOIN dim_producto pr ON va.producto_id = pr.producto_id
                WHERE pr.categoria = ?
                GROUP BY pr.nombre_producto
                ORDER BY SUM(va.monto_venta) DESC
            """,
            'almacen': """
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.almacen_id,
                        fv.producto_id,
                        SUM(fv.monto_total) AS monto_venta,
                        SUM(fv.margen) AS margen_venta
                    FROM fact_ventas fv
                    WHERE fv.venta_cancelada = 0
                    GROUP BY fv.venta_id, fv.almacen_id, fv.producto_id
                )
                SELECT TOP 15
                    pr.categoria,
                    COUNT(DISTINCT va.venta_id) AS cantidad_ordenes,
                    SUM(va.monto_venta) AS total_ventas,
                    SUM(va.margen_venta) AS total_margen,
                    ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje
                FROM VentasAgrupadas va
                INNER JOIN dim_almacen a ON va.almacen_id = a.almacen_id
                INNER JOIN dim_producto pr ON va.producto_id = pr.producto_id
                WHERE a.nombre_almacen = ?
                GROUP BY pr.categoria
                ORDER BY SUM(va.monto_venta) DESC
            """,
            'anio': """
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.tiempo_key,
                        SUM(fv.monto_total) AS monto_venta,
                        SUM(fv.margen) AS margen_venta
                    FROM fact_ventas fv
                    WHERE fv.venta_cancelada = 0
                    GROUP BY fv.venta_id, fv.tiempo_key
                )
                SELECT
                    t.MES_NOMBRE AS mes,
                    t.MES_CAL AS mes_numero,
                    COUNT(DISTINCT va.venta_id) AS cantidad_ordenes,
                    SUM(va.monto_venta) AS total_ventas,
                    SUM(va.margen_venta) AS total_margen,
                    ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje
                FROM VentasAgrupadas va
                INNER JOIN dim_tiempo t ON va.tiempo_key = t.ID_FECHA
                WHERE t.ANIO_CAL = ?
                GROUP BY t.MES_NOMBRE, t.MES_CAL
                ORDER BY t.MES_CAL
            """
        }

        if dimension not in drill_down_queries:
            raise ValueError(f"Dimensión no soportada: {dimension}")

        return self._execute_query(drill_down_queries[dimension], (value,))

    def dice(self, filters: Dict[str, any]) -> pd.DataFrame:
        """DICE: Selecciona subrectángulo del cubo con múltiples filtros (con agrupación correcta por venta_id)"""
        logger.info(f"DICE: aplicando {len(filters)} filtros")

        # Construir WHERE clause dinámicamente
        where_conditions = ["fv.venta_cancelada = 0"]
        params = []

        if 'provincia' in filters:
            where_conditions.append("g.provincia = ?")
            params.append(filters['provincia'])
        if 'canton' in filters:
            where_conditions.append("g.canton = ?")
            params.append(filters['canton'])
        if 'categoria' in filters:
            where_conditions.append("pr.categoria = ?")
            params.append(filters['categoria'])
        if 'anio' in filters:
            where_conditions.append("t.ANIO_CAL = ?")
            params.append(filters['anio'])
        if 'mes' in filters:
            where_conditions.append("t.MES_CAL = ?")
            params.append(filters['mes'])

        where_clause = " AND ".join(where_conditions)

        query = f"""
            WITH VentasAgrupadas AS (
                SELECT
                    fv.venta_id,
                    fv.tiempo_key,
                    fv.producto_id,
                    fv.cliente_id,
                    fv.provincia_id,
                    fv.canton_id,
                    fv.distrito_id,
                    fv.almacen_id,
                    SUM(fv.cantidad) AS total_unidades,
                    SUM(fv.monto_total) AS monto_venta,
                    SUM(fv.margen) AS margen_venta,
                    SUM(fv.impuesto) AS impuesto_venta
                FROM fact_ventas fv
                INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
                INNER JOIN dim_producto pr ON fv.producto_id = pr.producto_id
                INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                    AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
                WHERE {where_clause}
                GROUP BY fv.venta_id, fv.tiempo_key, fv.producto_id, fv.cliente_id,
                         fv.provincia_id, fv.canton_id, fv.distrito_id, fv.almacen_id
            )
            SELECT
                t.ANIO_CAL AS anio,
                t.MES_CAL AS mes,
                t.MES_NOMBRE AS mes_nombre,
                pr.categoria,
                g.provincia,
                g.canton,
                g.distrito,
                CONCAT(cl.nombre_cliente, ' ', cl.apellido_cliente) AS cliente,
                a.nombre_almacen AS almacen,
                COUNT(DISTINCT va.venta_id) AS cantidad_ordenes,
                SUM(va.total_unidades) AS total_unidades,
                SUM(va.monto_venta) AS total_ventas,
                AVG(va.monto_venta) AS promedio_por_orden,
                SUM(va.margen_venta) AS total_margen,
                ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje,
                SUM(va.impuesto_venta) AS total_impuesto
            FROM VentasAgrupadas va
            INNER JOIN dim_tiempo t ON va.tiempo_key = t.ID_FECHA
            INNER JOIN dim_producto pr ON va.producto_id = pr.producto_id
            INNER JOIN dim_cliente cl ON va.cliente_id = cl.cliente_id
            INNER JOIN dim_geografia g ON va.provincia_id = g.provincia_id
                AND va.canton_id = g.canton_id AND va.distrito_id = g.distrito_id
            INNER JOIN dim_almacen a ON va.almacen_id = a.almacen_id
            GROUP BY
                t.ANIO_CAL, t.MES_CAL, t.MES_NOMBRE,
                pr.categoria, g.provincia, g.canton, g.distrito,
                cl.nombre_cliente, cl.apellido_cliente, a.nombre_almacen
            ORDER BY t.ANIO_CAL DESC, t.MES_CAL DESC, SUM(va.monto_venta) DESC
        """

        return self._execute_query(query, tuple(params) if params else None)

    def drill_down(self, dimension: str, current_level: int = 0, filters: Dict = None) -> pd.DataFrame:
        """DRILL-DOWN: Desciende en la jerarquía de una dimensión"""
        logger.info(f"DRILL-DOWN: {dimension} nivel {current_level}")

        if dimension == 'tiempo':
            if current_level == 0:
                query = """
                    SELECT
                        t.ANIO_CAL AS anio,
                        t.MES_CAL AS mes,
                        t.MES_NOMBRE AS mes_nombre,
                        COUNT(DISTINCT fv.venta_detalle_key) AS transacciones,
                        SUM(fv.monto_total) AS total_ventas,
                        SUM(fv.margen) AS total_margen
                    FROM fact_ventas fv
                    INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
                    GROUP BY t.ANIO_CAL, t.MES_CAL, t.MES_NOMBRE
                    ORDER BY t.ANIO_CAL DESC, t.MES_CAL DESC
                """
            else:
                query = """
                    SELECT
                        t.FECHA_CAL AS fecha,
                        t.DIA_SEM_NOMBRE AS dia_semana,
                        COUNT(DISTINCT fv.venta_detalle_key) AS transacciones,
                        SUM(fv.monto_total) AS total_ventas,
                        SUM(fv.margen) AS total_margen
                    FROM fact_ventas fv
                    INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
                    GROUP BY t.FECHA_CAL, t.DIA_SEM_NOMBRE
                    ORDER BY t.FECHA_CAL DESC
                """
        elif dimension == 'geografia':
            if current_level == 0:
                query = """
                    SELECT
                        g.provincia,
                        g.canton,
                        COUNT(DISTINCT fv.venta_detalle_key) AS transacciones,
                        SUM(fv.monto_total) AS total_ventas,
                        SUM(fv.margen) AS total_margen,
                        COUNT(DISTINCT fv.cliente_id) AS clientes_unicos
                    FROM fact_ventas fv
                    INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                        AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
                    GROUP BY g.provincia, g.canton
                    ORDER BY total_ventas DESC
                """
            else:
                query = """
                    SELECT
                        g.provincia,
                        g.canton,
                        g.distrito,
                        COUNT(DISTINCT fv.venta_detalle_key) AS transacciones,
                        SUM(fv.monto_total) AS total_ventas,
                        SUM(fv.margen) AS total_margen,
                        COUNT(DISTINCT fv.cliente_id) AS clientes_unicos
                    FROM fact_ventas fv
                    INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                        AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
                    GROUP BY g.provincia, g.canton, g.distrito
                    ORDER BY total_ventas DESC
                """
        elif dimension == 'producto':
            query = """
                SELECT
                    pr.categoria,
                    pr.nombre_producto AS producto,
                    pr.codigo_producto,
                    pr.precio_unitario,
                    pr.costo_unitario,
                    COUNT(DISTINCT fv.venta_detalle_key) AS transacciones,
                    SUM(fv.cantidad) AS total_cantidad,
                    SUM(fv.monto_total) AS total_ventas,
                    SUM(fv.margen) AS total_margen,
                    AVG(fv.precio_unitario) AS precio_promedio
                FROM fact_ventas fv
                INNER JOIN dim_producto pr ON fv.producto_id = pr.producto_id
                GROUP BY pr.categoria, pr.nombre_producto, pr.codigo_producto, pr.precio_unitario, pr.costo_unitario
                ORDER BY total_ventas DESC
            """
        else:
            raise ValueError(f"Dimensión desconocida: {dimension}")

        return self._execute_query(query)

    def roll_up(self, dimension: str, target_level: int = 0) -> pd.DataFrame:
        """ROLL-UP: Asciende en la jerarquía de una dimensión"""
        logger.info(f"ROLL-UP: {dimension} a nivel {target_level}")

        if dimension == 'geografia':
            if target_level == 0:
                query = """
                    SELECT
                        g.provincia,
                        COUNT(DISTINCT fv.venta_detalle_key) AS transacciones,
                        SUM(fv.monto_total) AS total_ventas,
                        SUM(fv.margen) AS total_margen,
                        COUNT(DISTINCT fv.cliente_id) AS clientes_unicos
                    FROM fact_ventas fv
                    INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                        AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
                    GROUP BY g.provincia
                    ORDER BY total_ventas DESC
                """
            elif target_level == 1:
                query = """
                    SELECT
                        g.provincia,
                        g.canton,
                        COUNT(DISTINCT fv.venta_detalle_key) AS transacciones,
                        SUM(fv.monto_total) AS total_ventas,
                        SUM(fv.margen) AS total_margen,
                        COUNT(DISTINCT fv.cliente_id) AS clientes_unicos
                    FROM fact_ventas fv
                    INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                        AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
                    GROUP BY g.provincia, g.canton
                    ORDER BY total_ventas DESC
                """
            else:
                raise ValueError("Nivel inválido para geografía (0-1)")
        elif dimension == 'tiempo':
            if target_level == 0:
                query = """
                    SELECT
                        t.ANIO_CAL AS anio,
                        COUNT(DISTINCT fv.venta_detalle_key) AS transacciones,
                        SUM(fv.monto_total) AS total_ventas,
                        SUM(fv.margen) AS total_margen
                    FROM fact_ventas fv
                    INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
                    GROUP BY t.ANIO_CAL
                    ORDER BY t.ANIO_CAL DESC
                """
            else:
                raise ValueError("Nivel inválido para tiempo (0)")
        else:
            raise ValueError(f"Dimensión desconocida: {dimension}")

        return self._execute_query(query)

    def pivot(self, rows: str, columns: str, values: str = "monto_total") -> pd.DataFrame:
        """PIVOT: Rota dimensiones para diferentes vistas tabulares (con agrupación correcta por venta_id)"""
        logger.info(f"PIVOT: filas={rows}, columnas={columns}, valores={values}")

        query = """
            WITH VentasAgrupadas AS (
                SELECT
                    fv.venta_id,
                    fv.tiempo_key,
                    fv.producto_id,
                    fv.provincia_id,
                    fv.canton_id,
                    fv.distrito_id,
                    SUM(fv.cantidad) AS total_unidades,
                    SUM(fv.monto_total) AS monto_venta,
                    SUM(fv.margen) AS margen_venta
                FROM fact_ventas fv
                WHERE fv.venta_cancelada = 0
                GROUP BY fv.venta_id, fv.tiempo_key, fv.producto_id, fv.provincia_id, fv.canton_id, fv.distrito_id
            )
            SELECT
                g.provincia,
                g.canton,
                g.distrito,
                pr.categoria,
                t.ANIO_CAL AS anio,
                t.MES_CAL AS mes,
                t.MES_NOMBRE AS mes_nombre,
                SUM(va.monto_venta) AS monto_total,
                SUM(va.margen_venta) AS margen,
                COUNT(DISTINCT va.venta_id) AS transacciones,
                SUM(va.total_unidades) AS cantidad
            FROM VentasAgrupadas va
            INNER JOIN dim_tiempo t ON va.tiempo_key = t.ID_FECHA
            INNER JOIN dim_producto pr ON va.producto_id = pr.producto_id
            INNER JOIN dim_geografia g ON va.provincia_id = g.provincia_id
                AND va.canton_id = g.canton_id AND va.distrito_id = g.distrito_id
            GROUP BY g.provincia, g.canton, g.distrito, pr.categoria, t.ANIO_CAL, t.MES_CAL, t.MES_NOMBRE
        """

        df = self._execute_query(query)

        row_col_map = {'provincia': 'provincia', 'canton': 'canton', 'distrito': 'distrito', 'categoria': 'categoria', 'anio': 'anio', 'mes': 'mes_nombre'}
        values_col_map = {'monto_total': 'monto_total', 'margen': 'margen', 'transacciones': 'transacciones', 'cantidad': 'cantidad'}

        if rows not in row_col_map or columns not in row_col_map or values not in values_col_map:
            raise ValueError(f"Dimensión o métrica no válida")

        pivoted = df.pivot_table(index=row_col_map[rows], columns=row_col_map[columns], values=values_col_map[values], aggfunc='sum', fill_value=0)
        return pivoted

    # ========================================================================
    # CONSULTAS OLAP PREDEFINIDAS (CORREGIDAS)
    # ========================================================================

    def get_ventas_por_tiempo(self, granularidad: str = 'mes') -> pd.DataFrame:
        """Ventas agregadas por período de tiempo (con agrupación correcta por venta_id)"""
        logger.info(f"Ventas por tiempo (granularidad: {granularidad})")

        if granularidad == 'anio':
            query = """
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.tiempo_key,
                        SUM(fv.cantidad) AS total_unidades,
                        SUM(fv.monto_total) AS monto_venta,
                        SUM(fv.margen) AS margen_venta
                    FROM fact_ventas fv
                    WHERE fv.venta_cancelada = 0
                    GROUP BY fv.venta_id, fv.tiempo_key
                )
                SELECT
                    t.ANIO_CAL AS periodo,
                    COUNT(DISTINCT va.venta_id) AS transacciones,
                    SUM(va.monto_venta) AS total_ventas,
                    AVG(va.monto_venta) AS promedio_venta,
                    SUM(va.margen_venta) AS total_margen,
                    SUM(va.total_unidades) AS total_unidades,
                    ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje
                FROM VentasAgrupadas va
                INNER JOIN dim_tiempo t ON va.tiempo_key = t.ID_FECHA
                GROUP BY t.ANIO_CAL
                ORDER BY t.ANIO_CAL DESC
            """
        elif granularidad == 'trimestre':
            query = """
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.tiempo_key,
                        SUM(fv.cantidad) AS total_unidades,
                        SUM(fv.monto_total) AS monto_venta,
                        SUM(fv.margen) AS margen_venta
                    FROM fact_ventas fv
                    WHERE fv.venta_cancelada = 0
                    GROUP BY fv.venta_id, fv.tiempo_key
                )
                SELECT
                    t.ANIO_CAL AS anio,
                    t.TRIMESTRE AS trimestre,
                    CAST(CONCAT(t.ANIO_CAL, '-T', t.TRIMESTRE) AS NVARCHAR(10)) AS periodo,
                    COUNT(DISTINCT va.venta_id) AS transacciones,
                    SUM(va.monto_venta) AS total_ventas,
                    AVG(va.monto_venta) AS promedio_venta,
                    SUM(va.margen_venta) AS total_margen,
                    SUM(va.total_unidades) AS total_unidades,
                    ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje
                FROM VentasAgrupadas va
                INNER JOIN dim_tiempo t ON va.tiempo_key = t.ID_FECHA
                GROUP BY t.ANIO_CAL, t.TRIMESTRE
                ORDER BY t.ANIO_CAL DESC, t.TRIMESTRE DESC
            """
        elif granularidad == 'mes':
            query = """
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.tiempo_key,
                        SUM(fv.cantidad) AS total_unidades,
                        SUM(fv.monto_total) AS monto_venta,
                        SUM(fv.margen) AS margen_venta
                    FROM fact_ventas fv
                    WHERE fv.venta_cancelada = 0
                    GROUP BY fv.venta_id, fv.tiempo_key
                )
                SELECT
                    t.ANIO_CAL AS anio,
                    t.MES_CAL AS mes,
                    t.MES_NOMBRE AS mes_nombre,
                    CAST(CONCAT(t.ANIO_CAL, '-', RIGHT(CONCAT('0', t.MES_CAL), 2)) AS NVARCHAR(7)) AS periodo,
                    COUNT(DISTINCT va.venta_id) AS transacciones,
                    SUM(va.monto_venta) AS total_ventas,
                    AVG(va.monto_venta) AS promedio_venta,
                    SUM(va.margen_venta) AS total_margen,
                    SUM(va.total_unidades) AS total_unidades,
                    ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje
                FROM VentasAgrupadas va
                INNER JOIN dim_tiempo t ON va.tiempo_key = t.ID_FECHA
                GROUP BY t.ANIO_CAL, t.MES_CAL, t.MES_NOMBRE
                ORDER BY t.ANIO_CAL DESC, t.MES_CAL DESC
            """
        elif granularidad == 'dia':
            query = """
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.tiempo_key,
                        SUM(fv.cantidad) AS total_unidades,
                        SUM(fv.monto_total) AS monto_venta,
                        SUM(fv.margen) AS margen_venta
                    FROM fact_ventas fv
                    WHERE fv.venta_cancelada = 0
                    GROUP BY fv.venta_id, fv.tiempo_key
                )
                SELECT
                    t.FECHA_CAL AS fecha,
                    t.DIA_SEM_NOMBRE AS dia_semana,
                    COUNT(DISTINCT va.venta_id) AS transacciones,
                    SUM(va.monto_venta) AS total_ventas,
                    AVG(va.monto_venta) AS promedio_venta,
                    SUM(va.margen_venta) AS total_margen,
                    SUM(va.total_unidades) AS total_unidades,
                    ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje
                FROM VentasAgrupadas va
                INNER JOIN dim_tiempo t ON va.tiempo_key = t.ID_FECHA
                GROUP BY t.FECHA_CAL, t.DIA_SEM_NOMBRE
                ORDER BY t.FECHA_CAL DESC
            """
        else:
            raise ValueError(f"Granularidad no soportada: {granularidad}")

        return self._execute_query(query)

    def get_ventas_por_categoria(self) -> pd.DataFrame:
        """Ventas por categoría de producto (con agrupación correcta por venta_id)"""
        logger.info("Ventas por categoría")

        query = """
            WITH VentasAgrupadas AS (
                SELECT
                    fv.venta_id,
                    fv.producto_id,
                    fv.cliente_id,
                    SUM(fv.cantidad) AS total_unidades,
                    SUM(fv.monto_total) AS monto_venta,
                    SUM(fv.margen) AS margen_venta
                FROM fact_ventas fv
                WHERE fv.venta_cancelada = 0
                GROUP BY fv.venta_id, fv.producto_id, fv.cliente_id
            )
            SELECT
                pr.categoria,
                COUNT(DISTINCT va.venta_id) AS transacciones,
                SUM(va.total_unidades) AS total_unidades,
                SUM(va.monto_venta) AS total_ventas,
                AVG(va.monto_venta) AS promedio_venta,
                MIN(va.monto_venta) AS venta_minima,
                MAX(va.monto_venta) AS venta_maxima,
                SUM(va.margen_venta) AS total_margen,
                ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje,
                COUNT(DISTINCT va.cliente_id) AS clientes_unicos
            FROM VentasAgrupadas va
            INNER JOIN dim_producto pr ON va.producto_id = pr.producto_id
            GROUP BY pr.categoria
            ORDER BY total_ventas DESC
        """

        return self._execute_query(query)

    def get_ventas_por_region(self, nivel: str = 'provincia') -> pd.DataFrame:
        """Ventas por región geográfica (con agrupación correcta por venta_id)"""
        logger.info(f"Ventas por región (nivel: {nivel})")

        if nivel == 'provincia':
            query = """
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.provincia_id,
                        fv.canton_id,
                        fv.distrito_id,
                        fv.cliente_id,
                        SUM(fv.cantidad) AS total_unidades,
                        SUM(fv.monto_total) AS monto_venta,
                        SUM(fv.margen) AS margen_venta
                    FROM fact_ventas fv
                    WHERE fv.venta_cancelada = 0
                    GROUP BY fv.venta_id, fv.provincia_id, fv.canton_id, fv.distrito_id, fv.cliente_id
                )
                SELECT
                    g.provincia,
                    COUNT(DISTINCT va.venta_id) AS transacciones,
                    SUM(va.total_unidades) AS total_unidades,
                    SUM(va.monto_venta) AS total_ventas,
                    AVG(va.monto_venta) AS promedio_venta,
                    SUM(va.margen_venta) AS total_margen,
                    ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje,
                    COUNT(DISTINCT va.cliente_id) AS clientes_unicos
                FROM VentasAgrupadas va
                INNER JOIN dim_geografia g ON va.provincia_id = g.provincia_id
                    AND va.canton_id = g.canton_id AND va.distrito_id = g.distrito_id
                GROUP BY g.provincia
                ORDER BY total_ventas DESC
            """
        elif nivel == 'canton':
            query = """
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.provincia_id,
                        fv.canton_id,
                        fv.distrito_id,
                        fv.cliente_id,
                        SUM(fv.cantidad) AS total_unidades,
                        SUM(fv.monto_total) AS monto_venta,
                        SUM(fv.margen) AS margen_venta
                    FROM fact_ventas fv
                    WHERE fv.venta_cancelada = 0
                    GROUP BY fv.venta_id, fv.provincia_id, fv.canton_id, fv.distrito_id, fv.cliente_id
                )
                SELECT
                    g.provincia,
                    g.canton,
                    COUNT(DISTINCT va.venta_id) AS transacciones,
                    SUM(va.total_unidades) AS total_unidades,
                    SUM(va.monto_venta) AS total_ventas,
                    AVG(va.monto_venta) AS promedio_venta,
                    SUM(va.margen_venta) AS total_margen,
                    ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje,
                    COUNT(DISTINCT va.cliente_id) AS clientes_unicos
                FROM VentasAgrupadas va
                INNER JOIN dim_geografia g ON va.provincia_id = g.provincia_id
                    AND va.canton_id = g.canton_id AND va.distrito_id = g.distrito_id
                GROUP BY g.provincia, g.canton
                ORDER BY total_ventas DESC
            """
        elif nivel == 'distrito':
            query = """
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.provincia_id,
                        fv.canton_id,
                        fv.distrito_id,
                        fv.cliente_id,
                        SUM(fv.cantidad) AS total_unidades,
                        SUM(fv.monto_total) AS monto_venta,
                        SUM(fv.margen) AS margen_venta
                    FROM fact_ventas fv
                    WHERE fv.venta_cancelada = 0
                    GROUP BY fv.venta_id, fv.provincia_id, fv.canton_id, fv.distrito_id, fv.cliente_id
                )
                SELECT
                    g.provincia,
                    g.canton,
                    g.distrito,
                    COUNT(DISTINCT va.venta_id) AS transacciones,
                    SUM(va.total_unidades) AS total_unidades,
                    SUM(va.monto_venta) AS total_ventas,
                    AVG(va.monto_venta) AS promedio_venta,
                    SUM(va.margen_venta) AS total_margen,
                    ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje,
                    COUNT(DISTINCT va.cliente_id) AS clientes_unicos
                FROM VentasAgrupadas va
                INNER JOIN dim_geografia g ON va.provincia_id = g.provincia_id
                    AND va.canton_id = g.canton_id AND va.distrito_id = g.distrito_id
                GROUP BY g.provincia, g.canton, g.distrito
                ORDER BY total_ventas DESC
            """
        else:
            raise ValueError(f"Nivel geográfico no soportado: {nivel}")

        return self._execute_query(query)

    def get_ventas_por_cliente(self, top_n: int = 20, segmento: str = None) -> pd.DataFrame:
        """Top clientes (con agrupación correcta por venta_id)"""
        logger.info(f"Top {top_n} clientes")

        query = f"""
            WITH VentasAgrupadas AS (
                SELECT
                    fv.venta_id,
                    fv.cliente_id,
                    SUM(fv.cantidad) AS total_unidades,
                    SUM(fv.monto_total) AS monto_venta,
                    SUM(fv.margen) AS margen_venta
                FROM fact_ventas fv
                WHERE fv.venta_cancelada = 0
                GROUP BY fv.venta_id, fv.cliente_id
            )
            SELECT TOP {top_n}
                cl.cliente_id,
                CONCAT(cl.nombre_cliente, ' ', cl.apellido_cliente) AS cliente,
                cl.correo_electronico AS email,
                COUNT(DISTINCT va.venta_id) AS transacciones,
                SUM(va.total_unidades) AS total_unidades,
                SUM(va.monto_venta) AS total_gasto,
                AVG(va.monto_venta) AS promedio_compra,
                MAX(va.monto_venta) AS compra_maxima,
                SUM(va.margen_venta) AS margen_generado,
                ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje
            FROM VentasAgrupadas va
            INNER JOIN dim_cliente cl ON va.cliente_id = cl.cliente_id
            GROUP BY cl.cliente_id, cl.nombre_cliente, cl.apellido_cliente, cl.correo_electronico
            ORDER BY total_gasto DESC
        """

        return self._execute_query(query)

    def top_productos(self, top_n: int = 10) -> pd.DataFrame:
        """Obtiene los N productos más vendidos (con agrupación correcta por venta_id)"""
        logger.info(f"Top {top_n} productos")

        query = f"""
            WITH VentasAgrupadas AS (
                SELECT
                    fv.venta_id,
                    fv.producto_id,
                    SUM(fv.cantidad) AS total_unidades,
                    SUM(fv.monto_total) AS monto_venta,
                    SUM(fv.margen) AS margen_venta
                FROM fact_ventas fv
                WHERE fv.venta_cancelada = 0
                GROUP BY fv.venta_id, fv.producto_id
            )
            SELECT TOP {top_n}
                pr.producto_id,
                pr.nombre_producto AS producto,
                pr.categoria,
                pr.precio_unitario,
                pr.costo_unitario,
                COUNT(DISTINCT va.venta_id) AS transacciones,
                SUM(va.total_unidades) AS total_unidades,
                SUM(va.monto_venta) AS total_ventas,
                ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje,
                SUM(va.margen_venta) AS total_margen
            FROM VentasAgrupadas va
            INNER JOIN dim_producto pr ON va.producto_id = pr.producto_id
            GROUP BY pr.producto_id, pr.nombre_producto, pr.categoria, pr.precio_unitario, pr.costo_unitario
            ORDER BY total_ventas DESC
        """

        return self._execute_query(query)

    def resumen_negocio(self) -> Dict:
        """Obtiene un resumen de KPIs principales del negocio"""
        logger.info("Generando resumen de negocio")

        query = """
            SELECT
                -- Ventas completadas (agrupadas por venta_id)
                (SELECT COUNT(DISTINCT venta_id)
                 FROM (
                     SELECT venta_id
                     FROM fact_ventas
                     WHERE venta_cancelada = 0
                     GROUP BY venta_id
                 ) AS vc
                ) AS total_transacciones,

                (SELECT SUM(total_unidades)
                 FROM (
                     SELECT venta_id, SUM(cantidad) AS total_unidades
                     FROM fact_ventas
                     WHERE venta_cancelada = 0
                     GROUP BY venta_id
                 ) AS vc
                ) AS total_unidades,

                (SELECT SUM(MontoFactura)
                 FROM (
                     SELECT venta_id, SUM(monto_total) AS MontoFactura
                     FROM fact_ventas
                     WHERE venta_cancelada = 0
                     GROUP BY venta_id
                 ) AS vc
                ) AS total_ventas_sin_canceladas,

                (SELECT SUM(MargenFactura)
                 FROM (
                     SELECT venta_id, SUM(margen) AS MargenFactura
                     FROM fact_ventas
                     WHERE venta_cancelada = 0
                     GROUP BY venta_id
                 ) AS vc
                ) AS total_ganancia,

                (SELECT AVG(MontoFactura)
                 FROM (
                     SELECT venta_id, SUM(monto_total) AS MontoFactura
                     FROM fact_ventas
                     WHERE venta_cancelada = 0
                     GROUP BY venta_id
                 ) AS vc
                ) AS promedio_venta,

                (SELECT MIN(MontoFactura)
                 FROM (
                     SELECT venta_id, SUM(monto_total) AS MontoFactura
                     FROM fact_ventas
                     WHERE venta_cancelada = 0
                     GROUP BY venta_id
                 ) AS vc
                ) AS venta_minima,

                (SELECT MAX(MontoFactura)
                 FROM (
                     SELECT venta_id, SUM(monto_total) AS MontoFactura
                     FROM fact_ventas
                     WHERE venta_cancelada = 0
                     GROUP BY venta_id
                 ) AS vc
                ) AS venta_maxima,

                (SELECT SUM(MargenFactura)
                 FROM (
                     SELECT venta_id, SUM(margen) AS MargenFactura
                     FROM fact_ventas
                     WHERE venta_cancelada = 0
                     GROUP BY venta_id
                 ) AS vc
                ) AS total_margen,

                ROUND(100.0 *
                    (SELECT SUM(MargenFactura)
                     FROM (
                         SELECT venta_id, SUM(margen) AS MargenFactura
                         FROM fact_ventas
                         WHERE venta_cancelada = 0
                         GROUP BY venta_id
                     ) AS vc
                    ) /
                    NULLIF((SELECT SUM(MontoFactura)
                     FROM (
                         SELECT venta_id, SUM(monto_total) AS MontoFactura
                         FROM fact_ventas
                         WHERE venta_cancelada = 0
                         GROUP BY venta_id
                     ) AS vc
                    ), 0), 2) AS margen_porcentaje,

                (SELECT COUNT(DISTINCT cliente_id)
                 FROM fact_ventas
                 WHERE venta_cancelada = 0
                ) AS clientes_unicos,

                (SELECT COUNT(DISTINCT producto_id)
                 FROM fact_ventas
                 WHERE venta_cancelada = 0
                ) AS productos_vendidos,

                (SELECT COUNT(DISTINCT almacen_id)
                 FROM fact_ventas
                 WHERE venta_cancelada = 0
                ) AS almacenes_activos,

                -- Ventas canceladas
                (SELECT COUNT(DISTINCT venta_id)
                 FROM (
                     SELECT venta_id
                     FROM fact_ventas
                     WHERE venta_cancelada = 1
                     GROUP BY venta_id
                 ) AS vcn
                ) AS cantidad_canceladas,

                ISNULL((SELECT SUM(MontoFacturaCancelada)
                 FROM (
                     SELECT venta_id, SUM(monto_total) AS MontoFacturaCancelada
                     FROM fact_ventas
                     WHERE venta_cancelada = 1
                     GROUP BY venta_id
                 ) AS vcn
                ), 0) AS monto_canceladas,

                -- Total general (todas las ventas agrupadas por venta_id)
                (SELECT SUM(MontoTotal)
                 FROM (
                     SELECT venta_id, SUM(monto_total) AS MontoTotal
                     FROM fact_ventas
                     GROUP BY venta_id
                 ) AS tv
                ) AS total_ventas_con_canceladas
        """

        result = self._execute_query(query)
        if result.empty:
            return {}
        return result.iloc[0].to_dict()

    def analisis_comportamiento_web(self) -> Dict:
        """Analiza el comportamiento web de usuarios usando fact_comportamiento_web"""
        logger.info("Analizando comportamiento web")

        # Eventos por tipo
        query_eventos_tipo = """
            SELECT
                te.tipo_evento,
                te.categoria_evento,
                COUNT(*) AS total_eventos,
                COUNT(DISTINCT fcw.cliente_id) AS usuarios_unicos,
                AVG(fcw.tiempo_pagina_segundos) AS tiempo_promedio_segundos,
                SUM(CAST(fcw.genero_venta AS INT)) AS conversiones,
                ROUND(100.0 * SUM(CAST(fcw.genero_venta AS INT)) / COUNT(*), 2) AS tasa_conversion
            FROM fact_comportamiento_web fcw
            INNER JOIN dim_tipo_evento te ON fcw.tipo_evento_id = te.tipo_evento_id
            GROUP BY te.tipo_evento, te.categoria_evento
            ORDER BY total_eventos DESC
        """

        # Eventos por dispositivo
        query_dispositivos = """
            SELECT
                d.tipo_dispositivo,
                d.dispositivo,
                d.sistema_operativo,
                COUNT(*) AS total_eventos,
                COUNT(DISTINCT fcw.cliente_id) AS usuarios_unicos,
                SUM(CAST(fcw.genero_venta AS INT)) AS conversiones,
                ROUND(100.0 * SUM(CAST(fcw.genero_venta AS INT)) / COUNT(*), 2) AS tasa_conversion
            FROM fact_comportamiento_web fcw
            INNER JOIN dim_dispositivo d ON fcw.dispositivo_id = d.dispositivo_id
            GROUP BY d.tipo_dispositivo, d.dispositivo, d.sistema_operativo
            ORDER BY total_eventos DESC
        """

        # Navegadores más usados
        query_navegadores = """
            SELECT
                n.navegador,
                n.tipo_navegador,
                COUNT(*) AS total_eventos,
                COUNT(DISTINCT fcw.cliente_id) AS usuarios_unicos,
                SUM(CAST(fcw.genero_venta AS INT)) AS conversiones,
                ROUND(100.0 * SUM(CAST(fcw.genero_venta AS INT)) / COUNT(*), 2) AS tasa_conversion
            FROM fact_comportamiento_web fcw
            INNER JOIN dim_navegador n ON fcw.navegador_id = n.navegador_id
            GROUP BY n.navegador, n.tipo_navegador
            ORDER BY total_eventos DESC
        """

        # Productos más vistos (eventos relacionados con productos)
        query_productos_vistos = """
            SELECT TOP 20
                p.nombre_producto AS producto,
                p.categoria,
                p.precio_unitario,
                COUNT(*) AS total_visualizaciones,
                COUNT(DISTINCT fcw.cliente_id) AS usuarios_unicos,
                SUM(CAST(fcw.genero_venta AS INT)) AS veces_comprado,
                ROUND(100.0 * SUM(CAST(fcw.genero_venta AS INT)) / COUNT(*), 2) AS tasa_conversion
            FROM fact_comportamiento_web fcw
            INNER JOIN dim_producto p ON fcw.producto_id = p.producto_id
            WHERE fcw.producto_id IS NOT NULL
            GROUP BY p.nombre_producto, p.categoria, p.precio_unitario
            ORDER BY total_visualizaciones DESC
        """

        # Eventos por tiempo (tendencia)
        query_eventos_tiempo = """
            SELECT
                t.FECHA_CAL AS fecha,
                t.DIA_SEM_NOMBRE AS dia_semana,
                COUNT(*) AS total_eventos,
                COUNT(DISTINCT fcw.cliente_id) AS usuarios_unicos,
                SUM(CAST(fcw.genero_venta AS INT)) AS conversiones,
                AVG(fcw.tiempo_pagina_segundos) AS tiempo_promedio_segundos
            FROM fact_comportamiento_web fcw
            INNER JOIN dim_tiempo t ON fcw.tiempo_key = t.ID_FECHA
            GROUP BY t.FECHA_CAL, t.DIA_SEM_NOMBRE
            ORDER BY t.FECHA_CAL DESC
        """

        return {
            'eventos_por_tipo': self._execute_query(query_eventos_tipo),
            'dispositivos': self._execute_query(query_dispositivos),
            'navegadores': self._execute_query(query_navegadores),
            'productos_vistos': self._execute_query(query_productos_vistos),
            'eventos_tiempo': self._execute_query(query_eventos_tiempo)
        }

    def analisis_busquedas(self) -> Dict:
        """Analiza las búsquedas de usuarios usando fact_busquedas"""
        logger.info("Analizando búsquedas")

        # Búsquedas por dispositivo
        query_busquedas_dispositivo = """
            SELECT
                d.tipo_dispositivo,
                d.dispositivo,
                COUNT(*) AS total_busquedas,
                COUNT(DISTINCT fb.cliente_id) AS usuarios_unicos,
                AVG(fb.cantidad_resultados) AS promedio_resultados,
                SUM(CAST(fb.genero_venta AS INT)) AS conversiones,
                ROUND(100.0 * SUM(CAST(fb.genero_venta AS INT)) / COUNT(*), 2) AS tasa_conversion
            FROM fact_busquedas fb
            INNER JOIN dim_dispositivo d ON fb.dispositivo_id = d.dispositivo_id
            GROUP BY d.tipo_dispositivo, d.dispositivo
            ORDER BY total_busquedas DESC
        """

        # Búsquedas por navegador
        query_busquedas_navegador = """
            SELECT
                n.navegador,
                COUNT(*) AS total_busquedas,
                COUNT(DISTINCT fb.cliente_id) AS usuarios_unicos,
                AVG(fb.cantidad_resultados) AS promedio_resultados,
                SUM(CAST(fb.genero_venta AS INT)) AS conversiones,
                ROUND(100.0 * SUM(CAST(fb.genero_venta AS INT)) / COUNT(*), 2) AS tasa_conversion
            FROM fact_busquedas fb
            INNER JOIN dim_navegador n ON fb.navegador_id = n.navegador_id
            GROUP BY n.navegador
            ORDER BY total_busquedas DESC
        """

        # Productos más buscados (que generaron resultados)
        query_productos_buscados = """
            SELECT TOP 20
                p.nombre_producto AS producto,
                p.categoria,
                p.precio_unitario,
                COUNT(*) AS total_busquedas,
                COUNT(DISTINCT fb.cliente_id) AS usuarios_unicos,
                AVG(fb.cantidad_resultados) AS promedio_resultados,
                SUM(CAST(fb.genero_venta AS INT)) AS veces_comprado,
                ROUND(100.0 * SUM(CAST(fb.genero_venta AS INT)) / COUNT(*), 2) AS tasa_conversion
            FROM fact_busquedas fb
            INNER JOIN dim_producto p ON fb.producto_id = p.producto_id
            WHERE fb.producto_id != 0
            GROUP BY p.nombre_producto, p.categoria, p.precio_unitario
            ORDER BY total_busquedas DESC
        """

        # Búsquedas por tiempo
        query_busquedas_tiempo = """
            SELECT
                t.FECHA_CAL AS fecha,
                t.DIA_SEM_NOMBRE AS dia_semana,
                COUNT(*) AS total_busquedas,
                COUNT(DISTINCT fb.cliente_id) AS usuarios_unicos,
                AVG(fb.cantidad_resultados) AS promedio_resultados,
                SUM(CAST(fb.genero_venta AS INT)) AS conversiones
            FROM fact_busquedas fb
            INNER JOIN dim_tiempo t ON fb.tiempo_key = t.ID_FECHA
            GROUP BY t.FECHA_CAL, t.DIA_SEM_NOMBRE
            ORDER BY t.FECHA_CAL DESC
        """

        # Resumen general de búsquedas
        query_resumen = """
            SELECT
                COUNT(*) AS total_busquedas,
                COUNT(DISTINCT cliente_id) AS usuarios_unicos,
                AVG(cantidad_resultados) AS promedio_resultados,
                SUM(CAST(genero_venta AS INT)) AS conversiones_totales,
                ROUND(100.0 * SUM(CAST(genero_venta AS INT)) / COUNT(*), 2) AS tasa_conversion_global,
                COUNT(CASE WHEN cantidad_resultados = 0 THEN 1 END) AS busquedas_sin_resultado,
                ROUND(100.0 * COUNT(CASE WHEN cantidad_resultados = 0 THEN 1 END) / COUNT(*), 2) AS porcentaje_sin_resultado
            FROM fact_busquedas
        """

        return {
            'busquedas_dispositivo': self._execute_query(query_busquedas_dispositivo),
            'busquedas_navegador': self._execute_query(query_busquedas_navegador),
            'productos_buscados': self._execute_query(query_productos_buscados),
            'busquedas_tiempo': self._execute_query(query_busquedas_tiempo),
            'resumen': self._execute_query(query_resumen)
        }

    def get_funnel_conversion(self) -> pd.DataFrame:
        """
        Crea un funnel de conversión basado en los eventos web
        desde eventos iniciales hasta conversión en venta
        """
        logger.info("Generando funnel de conversión")

        query = """
            WITH EventoCounts AS (
                SELECT
                    'Total Eventos' AS etapa,
                    1 AS orden,
                    COUNT(*) AS cantidad
                FROM fact_comportamiento_web

                UNION ALL

                SELECT
                    'Eventos con Cliente Identificado' AS etapa,
                    2 AS orden,
                    COUNT(*) AS cantidad
                FROM fact_comportamiento_web
                WHERE cliente_reconocido = 1

                UNION ALL

                SELECT
                    'Eventos con Producto' AS etapa,
                    3 AS orden,
                    COUNT(*) AS cantidad
                FROM fact_comportamiento_web
                WHERE producto_id IS NOT NULL

                UNION ALL

                SELECT
                    'Eventos que Generaron Venta' AS etapa,
                    4 AS orden,
                    COUNT(*) AS cantidad
                FROM fact_comportamiento_web
                WHERE genero_venta = 1

                UNION ALL

                SELECT
                    'Ventas Completadas' AS etapa,
                    5 AS orden,
                    COUNT(DISTINCT venta_id) AS cantidad
                FROM fact_comportamiento_web
                WHERE genero_venta = 1 AND venta_id IS NOT NULL
            )
            SELECT
                etapa,
                cantidad,
                ROUND(100.0 * cantidad / FIRST_VALUE(cantidad) OVER (ORDER BY orden), 2) AS porcentaje_del_total,
                ROUND(100.0 * cantidad / LAG(cantidad) OVER (ORDER BY orden), 2) AS porcentaje_anterior
            FROM EventoCounts
            ORDER BY orden
        """

        return self._execute_query(query)

    # Alias para compatibilidad
    ventas_por_tiempo = get_ventas_por_tiempo
    ventas_por_categoria = get_ventas_por_categoria
    ventas_por_geografia = get_ventas_por_region
    top_clientes = get_ventas_por_cliente
