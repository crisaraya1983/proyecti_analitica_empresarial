"""
================================================================================
MÓDULO DE CÁLCULO DE KPIs - BALANCED SCORECARD
================================================================================
Implementa funciones para calcular todos los KPIs del negocio
Organizado según las 4 perspectivas del Balanced Scorecard:
- Financiera
- Clientes
- Procesos Internos
- Aprendizaje y Crecimiento
================================================================================
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, Union
import logging
import pyodbc
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class KPICalculator:
    """
    Clase para calcular KPIs del negocio según Balanced Scorecard
    """

    def __init__(self, conn: Union[pyodbc.Connection, Engine]):
        """
        Inicializa el calculador de KPIs

        Args:
            conn: Conexión pyodbc o SQLAlchemy Engine a la base de datos DW.
                  Se recomienda usar SQLAlchemy Engine para evitar warnings de pandas.
        """
        self.conn = conn

    def _convertir_tipos_arrow_compatibles(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convierte tipos nullable de pandas (Int64, Float64, etc.) a tipos estándar numpy
        para evitar errores de serialización con PyArrow/Streamlit.

        Args:
            df: DataFrame a convertir

        Returns:
            DataFrame con tipos compatibles con Arrow
        """
        for col in df.columns:
            if hasattr(df[col].dtype, 'numpy_dtype'):  # Es un tipo nullable de pandas
                df[col] = df[col].astype(df[col].dtype.numpy_dtype)
        return df

    # ========================================================================
    # PERSPECTIVA FINANCIERA
    # ========================================================================

    def calcular_ventas_totales(self,
                                fecha_inicio: Optional[str] = None,
                                fecha_fin: Optional[str] = None,
                                filtros: Dict = None) -> Dict:
        """
        Calcula ventas totales y comparación con periodo anterior

        Args:
            fecha_inicio: Fecha inicio (YYYY-MM-DD)
            fecha_fin: Fecha fin (YYYY-MM-DD)
            filtros: Dict con filtros opcionales (categoria, provincia, etc.)

        Returns:
            Dict con ventas actuales, anteriores y variación
        """
        logger.info("Calculando ventas totales...")

        condiciones = self._construir_condiciones_filtro(filtros)
        filtro_where = " AND ".join(condiciones) if condiciones else "1=1"

        # Ventas periodo actual
        query_actual = f"""
            SELECT
                SUM(fv.monto_total) AS ventas_totales,
                COUNT(DISTINCT fv.venta_id) AS num_transacciones,
                AVG(fv.monto_total) AS ticket_promedio
            FROM fact_ventas fv
            INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
            INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
            INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
            WHERE fv.venta_cancelada = 0
              AND {filtro_where}
              {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio, fecha_fin)}
        """

        df_actual = pd.read_sql(query_actual, self.conn)

        # Calcular periodo anterior (mismo rango de tiempo anterior)
        if fecha_inicio and fecha_fin:
            inicio = pd.to_datetime(fecha_inicio)
            fin = pd.to_datetime(fecha_fin)
            dias_diff = (fin - inicio).days + 1

            fecha_inicio_anterior = (inicio - timedelta(days=dias_diff)).strftime('%Y-%m-%d')
            fecha_fin_anterior = (inicio - timedelta(days=1)).strftime('%Y-%m-%d')

            query_anterior = f"""
                SELECT
                    SUM(fv.monto_total) AS ventas_totales
                FROM fact_ventas fv
                INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
                INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
                INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                    AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
                WHERE fv.venta_cancelada = 0
                  AND {filtro_where}
                  {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio_anterior, fecha_fin_anterior)}
            """

            df_anterior = pd.read_sql(query_anterior, self.conn)
            ventas_anterior = df_anterior['ventas_totales'].iloc[0] if not df_anterior.empty else 0
        else:
            ventas_anterior = 0

        ventas_actual = df_actual['ventas_totales'].iloc[0] if not df_actual.empty else 0
        variacion = ((ventas_actual - ventas_anterior) / ventas_anterior * 100) if ventas_anterior > 0 else 0

        return {
            'ventas_totales': float(ventas_actual) if ventas_actual else 0,
            'ventas_anterior': float(ventas_anterior) if ventas_anterior else 0,
            'variacion_porcentaje': float(variacion),
            'num_transacciones': int(df_actual['num_transacciones'].iloc[0]) if not df_actual.empty else 0,
            'ticket_promedio': float(df_actual['ticket_promedio'].iloc[0]) if not df_actual.empty else 0
        }

    def calcular_margen_ganancia(self,
                                 fecha_inicio: Optional[str] = None,
                                 fecha_fin: Optional[str] = None,
                                 filtros: Dict = None) -> Dict:
        """
        Calcula margen de ganancia promedio

        Returns:
            Dict con margen actual, anterior y variación
        """
        logger.info("Calculando margen de ganancia...")

        condiciones = self._construir_condiciones_filtro(filtros)
        filtro_where = " AND ".join(condiciones) if condiciones else "1=1"

        query = f"""
            SELECT
                SUM(fv.margen) AS margen_total,
                SUM(fv.monto_total) AS ventas_totales,
                ROUND(100.0 * SUM(fv.margen) / NULLIF(SUM(fv.monto_total), 0), 2) AS margen_porcentaje
            FROM fact_ventas fv
            INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
            INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
            INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
            WHERE fv.venta_cancelada = 0
              AND {filtro_where}
              {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio, fecha_fin)}
        """

        df = pd.read_sql(query, self.conn)

        return {
            'margen_total': float(df['margen_total'].iloc[0]) if not df.empty else 0,
            'margen_porcentaje': float(df['margen_porcentaje'].iloc[0]) if not df.empty else 0
        }

    def calcular_crecimiento_ventas(self, periodo: str = 'mes') -> pd.DataFrame:
        """
        Calcula crecimiento de ventas periodo a periodo

        Args:
            periodo: 'mes', 'trimestre', 'anio'

        Returns:
            DataFrame con crecimiento por periodo
        """
        logger.info(f"Calculando crecimiento de ventas por {periodo}...")

        if periodo == 'mes':
            grupo = "t.ANIO_CAL, t.MES_CAL"
            orden = "t.ANIO_CAL, t.MES_CAL"
        elif periodo == 'trimestre':
            grupo = "t.ANIO_CAL, t.TRIMESTRE"
            orden = "t.ANIO_CAL, t.TRIMESTRE"
        else:
            grupo = "t.ANIO_CAL"
            orden = "t.ANIO_CAL"

        query = f"""
            SELECT
                {grupo},
                SUM(fv.monto_total) AS ventas
            FROM fact_ventas fv
            INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
            WHERE fv.venta_cancelada = 0
            GROUP BY {grupo}
            ORDER BY {orden}
        """

        df = pd.read_sql(query, self.conn)

        # Calcular crecimiento periodo a periodo
        df['ventas_anterior'] = df['ventas'].shift(1)
        df['crecimiento_porcentaje'] = ((df['ventas'] - df['ventas_anterior']) / df['ventas_anterior'] * 100)

        return df

    # ========================================================================
    # PERSPECTIVA DE CLIENTES
    # ========================================================================

    def calcular_clientes_activos(self,
                                   fecha_inicio: Optional[str] = None,
                                   fecha_fin: Optional[str] = None,
                                   filtros: Dict = None) -> Dict:
        """
        Calcula número de clientes activos (con al menos 1 compra en el periodo)

        Returns:
            Dict con clientes activos actual, anterior y variación
        """
        logger.info("Calculando clientes activos...")

        condiciones = self._construir_condiciones_filtro(filtros)
        filtro_where = " AND ".join(condiciones) if condiciones else "1=1"

        query_actual = f"""
            SELECT
                COUNT(DISTINCT fv.cliente_id) AS clientes_activos,
                COUNT(DISTINCT CASE WHEN fv.es_primera_compra = 1 THEN fv.cliente_id END) AS clientes_nuevos
            FROM fact_ventas fv
            INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
            INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
            INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
            WHERE fv.venta_cancelada = 0
              AND {filtro_where}
              {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio, fecha_fin)}
        """

        df_actual = pd.read_sql(query_actual, self.conn)

        # Periodo anterior
        if fecha_inicio and fecha_fin:
            inicio = pd.to_datetime(fecha_inicio)
            fin = pd.to_datetime(fecha_fin)
            dias_diff = (fin - inicio).days + 1

            fecha_inicio_anterior = (inicio - timedelta(days=dias_diff)).strftime('%Y-%m-%d')
            fecha_fin_anterior = (inicio - timedelta(days=1)).strftime('%Y-%m-%d')

            query_anterior = f"""
                SELECT COUNT(DISTINCT fv.cliente_id) AS clientes_activos
                FROM fact_ventas fv
                INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
                INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
                INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                    AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
                WHERE fv.venta_cancelada = 0
                  AND {filtro_where}
                  {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio_anterior, fecha_fin_anterior)}
            """

            df_anterior = pd.read_sql(query_anterior, self.conn)
            clientes_anterior = df_anterior['clientes_activos'].iloc[0] if not df_anterior.empty else 0
        else:
            clientes_anterior = 0

        clientes_actual = df_actual['clientes_activos'].iloc[0] if not df_actual.empty else 0
        variacion = ((clientes_actual - clientes_anterior) / clientes_anterior * 100) if clientes_anterior > 0 else 0

        return {
            'clientes_activos': int(clientes_actual),
            'clientes_anterior': int(clientes_anterior),
            'variacion_porcentaje': float(variacion),
            'clientes_nuevos': int(df_actual['clientes_nuevos'].iloc[0]) if not df_actual.empty else 0
        }

    def calcular_tasa_retencion(self, meses: int = 3) -> Dict:
        """
        Calcula tasa de retención de clientes

        Args:
            meses: Número de meses para calcular retención

        Returns:
            Dict con tasa de retención
        """
        logger.info(f"Calculando tasa de retención ({meses} meses)...")

        fecha_inicio = (datetime.now() - timedelta(days=meses * 30)).strftime('%Y-%m-%d')
        fecha_fin = datetime.now().strftime('%Y-%m-%d')

        query = f"""
            WITH ClientesPeriodo AS (
                SELECT DISTINCT
                    fv.cliente_id,
                    MIN(t.FECHA_CAL) AS primera_compra,
                    MAX(t.FECHA_CAL) AS ultima_compra,
                    COUNT(DISTINCT fv.venta_id) AS num_compras
                FROM fact_ventas fv
                INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
                WHERE fv.venta_cancelada = 0
                  AND t.FECHA_CAL >= '{fecha_inicio}'
                  AND t.FECHA_CAL <= '{fecha_fin}'
                GROUP BY fv.cliente_id
            )
            SELECT
                COUNT(*) AS total_clientes,
                SUM(CASE WHEN num_compras > 1 THEN 1 ELSE 0 END) AS clientes_retenidos,
                ROUND(100.0 * SUM(CASE WHEN num_compras > 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS tasa_retencion
            FROM ClientesPeriodo
        """

        df = pd.read_sql(query, self.conn)

        return {
            'total_clientes': int(df['total_clientes'].iloc[0]) if not df.empty else 0,
            'clientes_retenidos': int(df['clientes_retenidos'].iloc[0]) if not df.empty else 0,
            'tasa_retencion': float(df['tasa_retencion'].iloc[0]) if not df.empty else 0
        }

    def calcular_customer_lifetime_value(self, top_n: int = 100) -> pd.DataFrame:
        """
        Calcula Customer Lifetime Value

        Args:
            top_n: Número de clientes top a retornar

        Returns:
            DataFrame con CLV por cliente
        """
        logger.info("Calculando Customer Lifetime Value...")

        query = f"""
            SELECT TOP {top_n}
                fv.cliente_id,
                cl.nombre_cliente + ' ' + cl.apellido_cliente AS nombre_completo,
                COUNT(DISTINCT fv.venta_id) AS num_compras,
                SUM(fv.monto_total) AS valor_total,
                AVG(fv.monto_total) AS ticket_promedio,
                DATEDIFF(DAY, MIN(t.FECHA_CAL), MAX(t.FECHA_CAL)) AS dias_cliente,
                SUM(fv.margen) AS margen_total
            FROM fact_ventas fv
            INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
            INNER JOIN dim_cliente cl ON fv.cliente_id = cl.cliente_id
            WHERE fv.venta_cancelada = 0
            GROUP BY fv.cliente_id, cl.nombre_cliente, cl.apellido_cliente
            ORDER BY valor_total DESC
        """

        df = pd.read_sql(query, self.conn)
        return self._convertir_tipos_arrow_compatibles(df)

    def calcular_frecuencia_compra(self,
                                   fecha_inicio: Optional[str] = None,
                                   fecha_fin: Optional[str] = None) -> Dict:
        """
        Calcula frecuencia de compra promedio

        Returns:
            Dict con frecuencia promedio y distribución
        """
        logger.info("Calculando frecuencia de compra...")

        query = f"""
            SELECT
                AVG(CAST(num_compras AS FLOAT)) AS frecuencia_promedio,
                MAX(num_compras) AS max_compras,
                MIN(num_compras) AS min_compras
            FROM (
                SELECT
                    fv.cliente_id,
                    COUNT(DISTINCT fv.venta_id) AS num_compras
                FROM fact_ventas fv
                INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
                WHERE fv.venta_cancelada = 0
                  {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio, fecha_fin)}
                GROUP BY fv.cliente_id
            ) AS frecuencias
        """

        df = pd.read_sql(query, self.conn)

        return {
            'frecuencia_promedio': float(df['frecuencia_promedio'].iloc[0]) if not df.empty else 0,
            'max_compras': int(df['max_compras'].iloc[0]) if not df.empty else 0,
            'min_compras': int(df['min_compras'].iloc[0]) if not df.empty else 0
        }

    # ========================================================================
    # PERSPECTIVA DE PRODUCTOS
    # ========================================================================

    def calcular_productos_mas_vendidos(self,
                                        top_n: int = 10,
                                        fecha_inicio: Optional[str] = None,
                                        fecha_fin: Optional[str] = None,
                                        filtros: Dict = None) -> pd.DataFrame:
        """
        Calcula productos más vendidos por unidades y valor

        Returns:
            DataFrame con productos y métricas
        """
        logger.info(f"Calculando top {top_n} productos más vendidos...")

        condiciones = self._construir_condiciones_filtro(filtros)
        filtro_where = " AND ".join(condiciones) if condiciones else "1=1"

        query = f"""
            SELECT TOP {top_n}
                p.producto_id,
                p.nombre_producto,
                p.categoria,
                SUM(fv.cantidad) AS unidades_vendidas,
                SUM(fv.monto_total) AS valor_total,
                SUM(fv.margen) AS margen_total,
                COUNT(DISTINCT fv.venta_id) AS num_transacciones
            FROM fact_ventas fv
            INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
            INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
            INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
            WHERE fv.venta_cancelada = 0
              AND {filtro_where}
              {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio, fecha_fin)}
            GROUP BY p.producto_id, p.nombre_producto, p.categoria
            ORDER BY valor_total DESC
        """

        df = pd.read_sql(query, self.conn)
        return self._convertir_tipos_arrow_compatibles(df)

    def calcular_categorias_mayor_margen(self,
                                         fecha_inicio: Optional[str] = None,
                                         fecha_fin: Optional[str] = None,
                                         filtros: Dict = None) -> pd.DataFrame:
        """
        Calcula categorías con mayor margen

        Returns:
            DataFrame con categorías y márgenes
        """
        logger.info("Calculando categorías con mayor margen...")

        condiciones = self._construir_condiciones_filtro(filtros)
        filtro_where = " AND ".join(condiciones) if condiciones else "1=1"

        query = f"""
            SELECT
                p.categoria,
                SUM(fv.margen) AS margen_total,
                SUM(fv.monto_total) AS ventas_totales,
                ROUND(100.0 * SUM(fv.margen) / NULLIF(SUM(fv.monto_total), 0), 2) AS margen_porcentaje,
                SUM(fv.cantidad) AS unidades_vendidas
            FROM fact_ventas fv
            INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
            INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
            INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
            WHERE fv.venta_cancelada = 0
              AND {filtro_where}
              {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio, fecha_fin)}
            GROUP BY p.categoria
            ORDER BY margen_porcentaje DESC
        """

        df = pd.read_sql(query, self.conn)
        return self._convertir_tipos_arrow_compatibles(df)

    # ========================================================================
    # PERSPECTIVA DE PROCESOS INTERNOS (WEB)
    # ========================================================================

    def calcular_tasa_conversion(self,
                                  fecha_inicio: Optional[str] = None,
                                  fecha_fin: Optional[str] = None) -> Dict:
        """
        Calcula tasa de conversión (ventas / visitas a productos)

        Returns:
            Dict con métricas de conversión
        """
        logger.info("Calculando tasa de conversión...")

        query = f"""
            SELECT
                COUNT(DISTINCT fcw.evento_id) AS total_eventos,
                COUNT(DISTINCT CASE WHEN fcw.producto_id IS NOT NULL THEN fcw.evento_id END) AS visitas_productos,
                SUM(CAST(fcw.genero_venta AS INT)) AS conversiones,
                ROUND(100.0 * SUM(CAST(fcw.genero_venta AS INT)) / COUNT(DISTINCT fcw.evento_id), 2) AS tasa_conversion,
                COUNT(DISTINCT fcw.cliente_id) AS usuarios_unicos
            FROM fact_comportamiento_web fcw
            INNER JOIN dim_tiempo t ON fcw.tiempo_key = t.ID_FECHA
            WHERE 1=1
              {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio, fecha_fin)}
        """

        df = pd.read_sql(query, self.conn)

        return {
            'total_eventos': int(df['total_eventos'].iloc[0]) if not df.empty else 0,
            'visitas_productos': int(df['visitas_productos'].iloc[0]) if not df.empty else 0,
            'conversiones': int(df['conversiones'].iloc[0]) if not df.empty else 0,
            'tasa_conversion': float(df['tasa_conversion'].iloc[0]) if not df.empty else 0,
            'usuarios_unicos': int(df['usuarios_unicos'].iloc[0]) if not df.empty else 0
        }

    def calcular_productos_mas_buscados_vs_vendidos(self,
                                                     top_n: int = 20,
                                                     fecha_inicio: Optional[str] = None,
                                                     fecha_fin: Optional[str] = None) -> pd.DataFrame:
        """
        Compara productos más buscados vs más vendidos

        Returns:
            DataFrame con comparación
        """
        logger.info("Calculando productos más buscados vs vendidos...")

        query = f"""
            WITH ProductosBusquedas AS (
                SELECT
                    p.producto_id,
                    p.nombre_producto,
                    p.categoria,
                    COUNT(*) AS num_busquedas
                FROM fact_busquedas fb
                INNER JOIN dim_producto p ON fb.producto_id = p.producto_id
                INNER JOIN dim_tiempo t ON fb.tiempo_key = t.ID_FECHA
                WHERE fb.producto_id IS NOT NULL
                  {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio, fecha_fin)}
                GROUP BY p.producto_id, p.nombre_producto, p.categoria
            ),
            ProductosVentas AS (
                SELECT
                    p.producto_id,
                    COUNT(DISTINCT fv.venta_id) AS num_ventas,
                    SUM(fv.cantidad) AS unidades_vendidas
                FROM fact_ventas fv
                INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
                INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
                WHERE fv.venta_cancelada = 0
                  {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio, fecha_fin)}
                GROUP BY p.producto_id
            )
            SELECT TOP {top_n}
                COALESCE(pb.producto_id, pv.producto_id) AS producto_id,
                COALESCE(pb.nombre_producto, '') AS nombre_producto,
                COALESCE(pb.categoria, '') AS categoria,
                COALESCE(pb.num_busquedas, 0) AS num_busquedas,
                COALESCE(pv.num_ventas, 0) AS num_ventas,
                COALESCE(pv.unidades_vendidas, 0) AS unidades_vendidas,
                CASE
                    WHEN pb.num_busquedas > 0 THEN ROUND(100.0 * pv.num_ventas / pb.num_busquedas, 2)
                    ELSE 0
                END AS tasa_conversion_busqueda
            FROM ProductosBusquedas pb
            FULL OUTER JOIN ProductosVentas pv ON pb.producto_id = pv.producto_id
            ORDER BY COALESCE(pb.num_busquedas, 0) DESC
        """

        df = pd.read_sql(query, self.conn)
        return self._convertir_tipos_arrow_compatibles(df)

    def calcular_metricas_dispositivos(self,
                                       fecha_inicio: Optional[str] = None,
                                       fecha_fin: Optional[str] = None) -> pd.DataFrame:
        """
        Calcula métricas por tipo de dispositivo

        Returns:
            DataFrame con métricas por dispositivo
        """
        logger.info("Calculando métricas por dispositivo...")

        query = f"""
            SELECT
                d.tipo_dispositivo,
                COUNT(*) AS total_eventos,
                COUNT(DISTINCT fcw.cliente_id) AS usuarios_unicos,
                SUM(CAST(fcw.genero_venta AS INT)) AS conversiones,
                ROUND(100.0 * SUM(CAST(fcw.genero_venta AS INT)) / COUNT(*), 2) AS tasa_conversion,
                AVG(fcw.tiempo_pagina_segundos) AS tiempo_promedio_segundos
            FROM fact_comportamiento_web fcw
            INNER JOIN dim_dispositivo d ON fcw.dispositivo_id = d.dispositivo_id
            INNER JOIN dim_tiempo t ON fcw.tiempo_key = t.ID_FECHA
            WHERE 1=1
              {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio, fecha_fin)}
            GROUP BY d.tipo_dispositivo
            ORDER BY total_eventos DESC
        """

        df = pd.read_sql(query, self.conn)
        return self._convertir_tipos_arrow_compatibles(df)

    def calcular_funnel_conversion(self,
                                    fecha_inicio: Optional[str] = None,
                                    fecha_fin: Optional[str] = None) -> pd.DataFrame:
        """
        Calcula el funnel de conversión completo

        Returns:
            DataFrame con etapas del funnel
        """
        logger.info("Calculando funnel de conversión...")

        query = f"""
            WITH EventoCounts AS (
                SELECT
                    'Total Eventos' AS etapa,
                    1 AS orden,
                    COUNT(*) AS cantidad
                FROM fact_comportamiento_web fcw
                INNER JOIN dim_tiempo t ON fcw.tiempo_key = t.ID_FECHA
                WHERE 1=1
                  {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio, fecha_fin)}

                UNION ALL

                SELECT
                    'Clientes Identificados' AS etapa,
                    2 AS orden,
                    COUNT(*) AS cantidad
                FROM fact_comportamiento_web fcw
                INNER JOIN dim_tiempo t ON fcw.tiempo_key = t.ID_FECHA
                WHERE fcw.cliente_reconocido = 1
                  {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio, fecha_fin)}

                UNION ALL

                SELECT
                    'Visualización de Productos' AS etapa,
                    3 AS orden,
                    COUNT(*) AS cantidad
                FROM fact_comportamiento_web fcw
                INNER JOIN dim_tiempo t ON fcw.tiempo_key = t.ID_FECHA
                WHERE fcw.producto_id IS NOT NULL
                  {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio, fecha_fin)}

                UNION ALL

                SELECT
                    'Búsquedas Realizadas' AS etapa,
                    4 AS orden,
                    COUNT(*) AS cantidad
                FROM fact_busquedas fb
                INNER JOIN dim_tiempo t ON fb.tiempo_key = t.ID_FECHA
                WHERE 1=1
                  {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio, fecha_fin)}

                UNION ALL

                SELECT
                    'Conversiones (Ventas)' AS etapa,
                    5 AS orden,
                    SUM(CAST(fcw.genero_venta AS INT)) AS cantidad
                FROM fact_comportamiento_web fcw
                INNER JOIN dim_tiempo t ON fcw.tiempo_key = t.ID_FECHA
                WHERE 1=1
                  {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio, fecha_fin)}
            )
            SELECT
                etapa,
                cantidad,
                ROUND(100.0 * cantidad / FIRST_VALUE(cantidad) OVER (ORDER BY orden), 2) AS porcentaje_total
            FROM EventoCounts
            ORDER BY orden
        """

        df = pd.read_sql(query, self.conn)
        return self._convertir_tipos_arrow_compatibles(df)

    # ========================================================================
    # FUNCIONES AUXILIARES
    # ========================================================================

    def _construir_condiciones_filtro(self, filtros: Optional[Dict]) -> list:
        """Construye lista de condiciones WHERE basado en filtros"""
        condiciones = []

        if not filtros:
            return condiciones

        if 'categoria' in filtros and filtros['categoria']:
            condiciones.append(f"p.categoria = '{filtros['categoria']}'")

        if 'provincia' in filtros and filtros['provincia']:
            condiciones.append(f"g.provincia = '{filtros['provincia']}'")

        if 'almacen' in filtros and filtros['almacen']:
            condiciones.append(f"a.nombre_almacen = '{filtros['almacen']}'")

        return condiciones

    def _construir_filtro_fecha(self, campo_fecha: str,
                                fecha_inicio: Optional[str],
                                fecha_fin: Optional[str]) -> str:
        """Construye filtro de fecha para queries"""
        if not fecha_inicio and not fecha_fin:
            return ""

        filtros = []

        if fecha_inicio:
            filtros.append(f"AND {campo_fecha} >= '{fecha_inicio}'")

        if fecha_fin:
            filtros.append(f"AND {campo_fecha} <= '{fecha_fin}'")

        return " ".join(filtros)

    # ========================================================================
    # BALANCED SCORECARD COMPLETO
    # ========================================================================

    def obtener_balanced_scorecard(self,
                                    fecha_inicio: Optional[str] = None,
                                    fecha_fin: Optional[str] = None,
                                    filtros: Dict = None) -> Dict:
        """
        Obtiene todos los KPIs organizados según Balanced Scorecard

        Returns:
            Dict con 4 perspectivas y sus KPIs
        """
        logger.info("Generando Balanced Scorecard completo...")

        return {
            'financiera': {
                'ventas': self.calcular_ventas_totales(fecha_inicio, fecha_fin, filtros),
                'margen': self.calcular_margen_ganancia(fecha_inicio, fecha_fin, filtros)
            },
            'clientes': {
                'activos': self.calcular_clientes_activos(fecha_inicio, fecha_fin, filtros),
                'retencion': self.calcular_tasa_retencion(),
                'frecuencia': self.calcular_frecuencia_compra(fecha_inicio, fecha_fin)
            },
            'procesos': {
                'conversion': self.calcular_tasa_conversion(fecha_inicio, fecha_fin),
                'dispositivos': self.calcular_metricas_dispositivos(fecha_inicio, fecha_fin)
            },
            'productos': {
                'top_productos': self.calcular_productos_mas_vendidos(10, fecha_inicio, fecha_fin, filtros),
                'categorias': self.calcular_categorias_mayor_margen(fecha_inicio, fecha_fin, filtros)
            }
        }
