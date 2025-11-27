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
        Calcula ventas totales correctamente considerando que fact_ventas
        tiene múltiples registros por venta_id (detalle por línea de producto)
        """
        logger.info("Calculando ventas totales...")

        condiciones = self._construir_condiciones_filtro(filtros)
        filtro_where = " AND ".join(condiciones) if condiciones else "1=1"

        # Ventas periodo actual - Agrupar por venta_id primero
        query_actual = f"""
            SELECT
                SUM(MontoFactura) AS ventas_totales,
                COUNT(DISTINCT venta_id) AS num_transacciones,
                AVG(MontoFactura) AS ticket_promedio
            FROM (
                SELECT
                    fv.venta_id,
                    SUM(fv.monto_total) AS MontoFactura
                FROM fact_ventas fv
                INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
                INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
                INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                    AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
                WHERE fv.venta_cancelada = 0
                  AND {filtro_where}
                  {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio, fecha_fin)}
                GROUP BY fv.venta_id
            ) AS Facturas
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
                    SUM(MontoFactura) AS ventas_totales
                FROM (
                    SELECT
                        fv.venta_id,
                        SUM(fv.monto_total) AS MontoFactura
                    FROM fact_ventas fv
                    INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
                    INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
                    INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                        AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
                    WHERE fv.venta_cancelada = 0
                      AND {filtro_where}
                      {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio_anterior, fecha_fin_anterior)}
                    GROUP BY fv.venta_id
                ) AS Facturas
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
        Calcula margen de ganancia promedio correctamente considerando que fact_ventas
        tiene múltiples registros por venta_id (detalle por línea de producto)

        Returns:
            Dict con margen actual, anterior y variación
        """
        logger.info("Calculando margen de ganancia...")

        condiciones = self._construir_condiciones_filtro(filtros)
        filtro_where = " AND ".join(condiciones) if condiciones else "1=1"

        # Agrupar por venta_id primero para obtener totales correctos
        query = f"""
            SELECT
                SUM(MargenFactura) AS margen_total,
                SUM(MontoFactura) AS ventas_totales,
                ROUND(100.0 * SUM(MargenFactura) / NULLIF(SUM(MontoFactura), 0), 2) AS margen_porcentaje
            FROM (
                SELECT
                    fv.venta_id,
                    SUM(fv.margen) AS MargenFactura,
                    SUM(fv.monto_total) AS MontoFactura
                FROM fact_ventas fv
                INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
                INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
                INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                    AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
                WHERE fv.venta_cancelada = 0
                  AND {filtro_where}
                  {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio, fecha_fin)}
                GROUP BY fv.venta_id
            ) AS Facturas
        """

        df = pd.read_sql(query, self.conn)

        return {
            'margen_total': float(df['margen_total'].iloc[0]) if not df.empty else 0,
            'margen_porcentaje': float(df['margen_porcentaje'].iloc[0]) if not df.empty else 0
        }

    def calcular_crecimiento_ventas(self, periodo: str = 'mes') -> pd.DataFrame:
        """
        Calcula crecimiento de ventas periodo a periodo correctamente considerando que fact_ventas
        tiene múltiples registros por venta_id (detalle por línea de producto)

        Args:
            periodo: 'mes', 'trimestre', 'anio'

        Returns:
            DataFrame con crecimiento por periodo
        """
        logger.info(f"Calculando crecimiento de ventas por {periodo}...")

        if periodo == 'mes':
            grupo_subquery = "t.ANIO_CAL, t.MES_CAL, fv.venta_id"
            grupo_outer = "ANIO_CAL, MES_CAL"
            orden = "ANIO_CAL, MES_CAL"
            select_grupo = "ANIO_CAL, MES_CAL"
        elif periodo == 'trimestre':
            grupo_subquery = "t.ANIO_CAL, t.TRIMESTRE, fv.venta_id"
            grupo_outer = "ANIO_CAL, TRIMESTRE"
            orden = "ANIO_CAL, TRIMESTRE"
            select_grupo = "ANIO_CAL, TRIMESTRE"
        else:
            grupo_subquery = "t.ANIO_CAL, fv.venta_id"
            grupo_outer = "ANIO_CAL"
            orden = "ANIO_CAL"
            select_grupo = "ANIO_CAL"

        # Agrupar por venta_id primero, luego por periodo
        # Excluir noviembre 2025 (202511) para mantener solo hasta octubre 2025
        query = f"""
            SELECT
                {select_grupo},
                SUM(MontoFactura) AS ventas,
                SUM(MargenFactura) AS margen
            FROM (
                SELECT
                    {grupo_subquery},
                    SUM(fv.monto_total) AS MontoFactura,
                    SUM(fv.margen) AS MargenFactura
                FROM fact_ventas fv
                INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
                WHERE fv.venta_cancelada = 0
                  AND (t.ANIO_CAL < 2025 OR (t.ANIO_CAL = 2025 AND t.MES_CAL <= 10))
                GROUP BY {grupo_subquery}
            ) AS Facturas
            GROUP BY {grupo_outer}
            ORDER BY {orden}
        """

        df = pd.read_sql(query, self.conn)

        # Crear etiqueta de periodo
        if periodo == 'mes' and 'MES_CAL' in df.columns:
            df['periodo'] = df['ANIO_CAL'].astype(str) + '-' + df['MES_CAL'].astype(str).str.zfill(2)
        elif periodo == 'trimestre' and 'TRIMESTRE' in df.columns:
            df['periodo'] = df['ANIO_CAL'].astype(str) + '-T' + df['TRIMESTRE'].astype(str)
        else:
            df['periodo'] = df['ANIO_CAL'].astype(str)

        # Calcular crecimiento periodo a periodo
        df['ventas_anterior'] = df['ventas'].shift(1)
        df['crecimiento_porcentaje'] = ((df['ventas'] - df['ventas_anterior']) / df['ventas_anterior'] * 100)

        # Calcular margen porcentaje
        df['margen_porcentaje'] = (df['margen'] / df['ventas'] * 100).fillna(0)

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

    def calcular_ventas_por_categoria_tiempo(self) -> pd.DataFrame:
        """
        Calcula ventas por categoría a través del tiempo (mensual)
        Excluye noviembre 2025, mantiene desde 2023 hasta octubre 2025

        Returns:
            DataFrame con año, mes, categoría y ventas
        """
        logger.info("Calculando ventas por categoría a través del tiempo...")

        query = """
            SELECT
                t.ANIO_CAL,
                t.MES_CAL,
                p.categoria,
                SUM(MontoFactura) AS ventas
            FROM (
                SELECT
                    fv.venta_id,
                    fv.tiempo_key,
                    fv.producto_id,
                    SUM(fv.monto_total) AS MontoFactura
                FROM fact_ventas fv
                INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
                WHERE fv.venta_cancelada = 0
                  AND (t.ANIO_CAL < 2025 OR (t.ANIO_CAL = 2025 AND t.MES_CAL <= 10))
                GROUP BY fv.venta_id, fv.tiempo_key, fv.producto_id
            ) AS Facturas
            INNER JOIN dim_tiempo t ON Facturas.tiempo_key = t.ID_FECHA
            INNER JOIN dim_producto p ON Facturas.producto_id = p.producto_id
            GROUP BY t.ANIO_CAL, t.MES_CAL, p.categoria
            ORDER BY t.ANIO_CAL, t.MES_CAL, p.categoria
        """

        df = pd.read_sql(query, self.conn)
        # Crear etiqueta de periodo
        df['periodo'] = df['ANIO_CAL'].astype(str) + '-' + df['MES_CAL'].astype(str).str.zfill(2)
        return self._convertir_tipos_arrow_compatibles(df)

    def calcular_dias_promedio_entre_compras(self) -> Dict:
        """
        Calcula el promedio de días que tardan los clientes en volver a comprar
        Solo considera clientes que han comprado más de una vez

        Returns:
            Dict con promedio de días entre compras
        """
        logger.info("Calculando días promedio entre compras...")

        query = """
            WITH ComprasPorCliente AS (
                SELECT
                    fv.cliente_id,
                    t.FECHA_CAL,
                    ROW_NUMBER() OVER (PARTITION BY fv.cliente_id ORDER BY t.FECHA_CAL) AS num_compra
                FROM (
                    SELECT DISTINCT venta_id, cliente_id, tiempo_key
                    FROM fact_ventas
                    WHERE venta_cancelada = 0
                ) fv
                INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
            ),
            DiasEntreCompras AS (
                SELECT
                    c1.cliente_id,
                    DATEDIFF(DAY, c1.FECHA_CAL, c2.FECHA_CAL) AS dias_diferencia
                FROM ComprasPorCliente c1
                INNER JOIN ComprasPorCliente c2
                    ON c1.cliente_id = c2.cliente_id
                    AND c2.num_compra = c1.num_compra + 1
            )
            SELECT
                AVG(CAST(dias_diferencia AS FLOAT)) AS dias_promedio_entre_compras,
                COUNT(*) AS total_intervalos,
                COUNT(DISTINCT cliente_id) AS clientes_recurrentes
            FROM DiasEntreCompras
        """

        df = pd.read_sql(query, self.conn)

        return {
            'dias_promedio': float(df['dias_promedio_entre_compras'].iloc[0]) if not df.empty else 0,
            'total_intervalos': int(df['total_intervalos'].iloc[0]) if not df.empty else 0,
            'clientes_recurrentes': int(df['clientes_recurrentes'].iloc[0]) if not df.empty else 0
        }

    def calcular_clientes_activos_por_mes(self) -> pd.DataFrame:
        """
        Calcula cantidad de clientes únicos que compraron por mes
        Desde 2023 hasta octubre 2025 (excluye noviembre 2025)

        Returns:
            DataFrame con año, mes y cantidad de clientes
        """
        logger.info("Calculando clientes activos por mes...")

        query = """
            SELECT
                t.ANIO_CAL,
                t.MES_CAL,
                COUNT(DISTINCT fv.cliente_id) AS clientes_activos
            FROM fact_ventas fv
            INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
            WHERE fv.venta_cancelada = 0
              AND (t.ANIO_CAL < 2025 OR (t.ANIO_CAL = 2025 AND t.MES_CAL <= 10))
            GROUP BY t.ANIO_CAL, t.MES_CAL
            ORDER BY t.ANIO_CAL, t.MES_CAL
        """

        df = pd.read_sql(query, self.conn)
        # Crear etiqueta de periodo
        df['periodo'] = df['ANIO_CAL'].astype(str) + '-' + df['MES_CAL'].astype(str).str.zfill(2)
        return self._convertir_tipos_arrow_compatibles(df)

    def calcular_producto_mas_vendido(self) -> Dict:
        """
        Calcula el producto más vendido (por cantidad de unidades)
        Excluye noviembre 2025, mantiene desde 2023 hasta octubre 2025

        Returns:
            Dict con nombre del producto y cantidad vendida
        """
        logger.info("Calculando producto más vendido...")

        query = """
            SELECT TOP 1
                p.nombre_producto AS producto_nombre,
                SUM(fv.cantidad) AS cantidad_vendida
            FROM fact_ventas fv
            INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
            INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
            WHERE fv.venta_cancelada = 0
              AND (t.ANIO_CAL < 2025 OR (t.ANIO_CAL = 2025 AND t.MES_CAL <= 10))
            GROUP BY p.nombre_producto
            ORDER BY SUM(fv.cantidad) DESC
        """

        df = pd.read_sql(query, self.conn)

        if df.empty:
            return {'producto_nombre': 'N/A', 'cantidad_vendida': 0}

        return {
            'producto_nombre': df['producto_nombre'].iloc[0],
            'cantidad_vendida': int(df['cantidad_vendida'].iloc[0])
        }

    def calcular_producto_mayor_margen(self) -> Dict:
        """
        Calcula el producto con mayor margen de ganancia total
        Excluye noviembre 2025, mantiene desde 2023 hasta octubre 2025

        Returns:
            Dict con nombre del producto y margen total
        """
        logger.info("Calculando producto con mayor margen...")

        query = """
            SELECT TOP 1
                p.nombre_producto AS producto_nombre,
                SUM(fv.margen) AS margen_total
            FROM fact_ventas fv
            INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
            INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
            WHERE fv.venta_cancelada = 0
              AND (t.ANIO_CAL < 2025 OR (t.ANIO_CAL = 2025 AND t.MES_CAL <= 10))
            GROUP BY p.nombre_producto
            ORDER BY SUM(fv.margen) DESC
        """

        df = pd.read_sql(query, self.conn)

        if df.empty:
            return {'producto_nombre': 'N/A', 'margen_total': 0}

        return {
            'producto_nombre': df['producto_nombre'].iloc[0],
            'margen_total': float(df['margen_total'].iloc[0])
        }

    # ========================================================================
    # PERSPECTIVA GEOGRÁFICA
    # ========================================================================

    def calcular_ventas_por_provincia(self) -> pd.DataFrame:
        """
        Calcula cantidad de órdenes únicas por provincia
        Excluye noviembre 2025, mantiene desde 2023 hasta octubre 2025

        Returns:
            DataFrame con provincia y cantidad de ventas (venta_id únicos)
        """
        logger.info("Calculando ventas por provincia...")

        query = """
            SELECT
                g.provincia,
                COUNT(DISTINCT fv.venta_id) AS num_ventas
            FROM fact_ventas fv
            INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
            INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
            WHERE fv.venta_cancelada = 0
              AND (t.ANIO_CAL < 2025 OR (t.ANIO_CAL = 2025 AND t.MES_CAL <= 10))
            GROUP BY g.provincia
            ORDER BY num_ventas DESC
        """

        df = pd.read_sql(query, self.conn)
        return self._convertir_tipos_arrow_compatibles(df)

    def calcular_ventas_por_almacen(self) -> pd.DataFrame:
        """
        Calcula cantidad de órdenes únicas despachadas por cada almacén/bodega
        Excluye noviembre 2025, mantiene desde 2023 hasta octubre 2025

        Returns:
            DataFrame con nombre_almacen, tipo_almacen y cantidad de ventas
        """
        logger.info("Calculando ventas por almacén...")

        query = """
            SELECT
                a.nombre_almacen,
                a.tipo_almacen,
                COUNT(DISTINCT fv.venta_id) AS num_ventas
            FROM fact_ventas fv
            INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
            INNER JOIN dim_almacen a ON fv.almacen_id = a.almacen_id
            WHERE fv.venta_cancelada = 0
              AND (t.ANIO_CAL < 2025 OR (t.ANIO_CAL = 2025 AND t.MES_CAL <= 10))
            GROUP BY a.nombre_almacen, a.tipo_almacen
            ORDER BY num_ventas DESC
        """

        df = pd.read_sql(query, self.conn)
        return self._convertir_tipos_arrow_compatibles(df)

    def calcular_canton_top(self) -> Dict:
        """
        Calcula el cantón con más compras (venta_id únicos)
        Excluye noviembre 2025, mantiene desde 2023 hasta octubre 2025

        Returns:
            Dict con nombre del cantón y cantidad de ventas
        """
        logger.info("Calculando cantón top...")

        query = """
            SELECT TOP 1
                g.canton,
                COUNT(DISTINCT fv.venta_id) AS num_ventas
            FROM fact_ventas fv
            INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
            INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
            WHERE fv.venta_cancelada = 0
              AND (t.ANIO_CAL < 2025 OR (t.ANIO_CAL = 2025 AND t.MES_CAL <= 10))
            GROUP BY g.canton
            ORDER BY COUNT(DISTINCT fv.venta_id) DESC
        """

        df = pd.read_sql(query, self.conn)

        if df.empty:
            return {'canton': 'N/A', 'num_ventas': 0}

        return {
            'canton': df['canton'].iloc[0],
            'num_ventas': int(df['num_ventas'].iloc[0])
        }

    def calcular_distrito_top(self) -> Dict:
        """
        Calcula el distrito con más compras (venta_id únicos)
        Excluye noviembre 2025, mantiene desde 2023 hasta octubre 2025

        Returns:
            Dict con nombre del distrito y cantidad de ventas
        """
        logger.info("Calculando distrito top...")

        query = """
            SELECT TOP 1
                g.distrito,
                g.canton,
                g.provincia,
                COUNT(DISTINCT fv.venta_id) AS num_ventas
            FROM fact_ventas fv
            INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
            INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
            WHERE fv.venta_cancelada = 0
              AND (t.ANIO_CAL < 2025 OR (t.ANIO_CAL = 2025 AND t.MES_CAL <= 10))
            GROUP BY g.distrito, g.canton, g.provincia
            ORDER BY COUNT(DISTINCT fv.venta_id) DESC
        """

        df = pd.read_sql(query, self.conn)

        if df.empty:
            return {'distrito': 'N/A', 'canton': 'N/A', 'provincia': 'N/A', 'num_ventas': 0}

        return {
            'distrito': df['distrito'].iloc[0],
            'canton': df['canton'].iloc[0],
            'provincia': df['provincia'].iloc[0],
            'num_ventas': int(df['num_ventas'].iloc[0])
        }

    def calcular_clientes_por_provincia(self) -> pd.DataFrame:
        """
        Calcula cantidad de clientes únicos por provincia
        Excluye noviembre 2025, mantiene desde 2023 hasta octubre 2025

        Returns:
            DataFrame con provincia y cantidad de clientes únicos
        """
        logger.info("Calculando clientes por provincia...")

        query = """
            SELECT
                g.provincia,
                COUNT(DISTINCT fv.cliente_id) AS num_clientes
            FROM fact_ventas fv
            INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
            INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
            WHERE fv.venta_cancelada = 0
              AND (t.ANIO_CAL < 2025 OR (t.ANIO_CAL = 2025 AND t.MES_CAL <= 10))
            GROUP BY g.provincia
            ORDER BY num_clientes DESC
        """

        df = pd.read_sql(query, self.conn)
        return self._convertir_tipos_arrow_compatibles(df)

    # ========================================================================
    # PERSPECTIVA DE COMPORTAMIENTO WEB
    # ========================================================================

    def calcular_funnel_comportamiento_web(self) -> pd.DataFrame:
        """
        Calcula el funnel de comportamiento web con eventos clave
        Agrupa por session_id únicos para cada etapa
        Excluye noviembre 2025, mantiene desde 2023 hasta octubre 2025

        Returns:
            DataFrame con etapas del funnel y cantidad de sesiones únicas
        """
        logger.info("Calculando funnel de comportamiento web...")

        query = """
            SELECT
                COUNT(DISTINCT fcw.sesion_id) AS total_sesiones,
                COUNT(DISTINCT CASE WHEN fcw.cliente_reconocido = 1 THEN fcw.sesion_id END) AS clientes_identificados,
                COUNT(DISTINCT CASE WHEN fcw.evento_id = 122 THEN fcw.sesion_id END) AS anadir_carrito,
                COUNT(DISTINCT CASE WHEN fcw.evento_id = 131 THEN fcw.sesion_id END) AS vista_ofertas,
                COUNT(DISTINCT CASE WHEN fcw.evento_id = 128 THEN fcw.sesion_id END) AS iniciar_checkout,
                COUNT(DISTINCT CASE WHEN fcw.genero_venta = 1 THEN fcw.sesion_id END) AS conversion_ventas
            FROM fact_comportamiento_web fcw
            INNER JOIN dim_tiempo t ON fcw.tiempo_key = t.ID_FECHA
            WHERE (t.ANIO_CAL < 2025 OR (t.ANIO_CAL = 2025 AND t.MES_CAL <= 10))
        """

        df = pd.read_sql(query, self.conn)

        if df.empty:
            return pd.DataFrame()

        # Convertir a formato de funnel
        funnel_data = {
            'etapa': [
                'Total Eventos',
                'Clientes Identificados',
                'Vista Ofertas',
                'Añadir Carrito',
                'Iniciar Checkout',
                'Conversión Ventas'
            ],
            'cantidad': [
                int(df['total_sesiones'].iloc[0]),
                int(df['clientes_identificados'].iloc[0]),
                int(df['vista_ofertas'].iloc[0]),
                int(df['anadir_carrito'].iloc[0]),
                int(df['iniciar_checkout'].iloc[0]),
                int(df['conversion_ventas'].iloc[0])
            ]
        }

        return pd.DataFrame(funnel_data)

    def calcular_metricas_comportamiento_web(self) -> Dict:
        """
        Calcula métricas clave de comportamiento web
        Excluye noviembre 2025, mantiene desde 2023 hasta octubre 2025

        Returns:
            Dict con tasa de conversión, usuarios únicos y navegadores diferentes
        """
        logger.info("Calculando métricas de comportamiento web...")

        query = """
            SELECT
                COUNT(DISTINCT fcw.codigo_sesion) AS sesiones_unicas,
                COUNT(DISTINCT CASE WHEN fcw.genero_venta = 1 THEN fcw.codigo_sesion END) AS sesiones_con_venta,
                COUNT(DISTINCT d.navegador) AS navegadores_diferentes,
                COUNT(*) AS total_eventos
            FROM fact_comportamiento_web fcw
            INNER JOIN dim_tiempo t ON fcw.tiempo_key = t.ID_FECHA
            INNER JOIN dim_dispositivo d ON fcw.dispositivo_id = d.dispositivo_id
            WHERE (t.ANIO_CAL < 2025 OR (t.ANIO_CAL = 2025 AND t.MES_CAL <= 10))
        """

        df = pd.read_sql(query, self.conn)

        if df.empty:
            return {
                'tasa_conversion': 0,
                'usuarios_unicos': 0,
                'navegadores_diferentes': 0,
                'total_eventos': 0
            }

        sesiones_unicas = int(df['sesiones_unicas'].iloc[0])
        sesiones_con_venta = int(df['sesiones_con_venta'].iloc[0])
        tasa_conversion = (sesiones_con_venta / sesiones_unicas * 100) if sesiones_unicas > 0 else 0

        return {
            'tasa_conversion': round(tasa_conversion, 2),
            'usuarios_unicos': sesiones_unicas,
            'navegadores_diferentes': int(df['navegadores_diferentes'].iloc[0]),
            'total_eventos': int(df['total_eventos'].iloc[0])
        }

    def calcular_tasa_conversion(self,
                                  fecha_inicio: Optional[str] = None,
                                  fecha_fin: Optional[str] = None) -> Dict:
        """
        Calcula tasa de conversión (ventas / sesiones)
        DEPRECATED: Usar calcular_metricas_comportamiento_web() en su lugar

        Returns:
            Dict con métricas de conversión
        """
        logger.info("Calculando tasa de conversión...")

        query = f"""
            SELECT
                COUNT(DISTINCT fcw.codigo_sesion) AS total_sesiones,
                COUNT(DISTINCT CASE WHEN fcw.genero_venta = 1 THEN fcw.codigo_sesion END) AS conversiones,
                ROUND(100.0 * COUNT(DISTINCT CASE WHEN fcw.genero_venta = 1 THEN fcw.codigo_sesion END) /
                      NULLIF(COUNT(DISTINCT fcw.codigo_sesion), 0), 2) AS tasa_conversion,
                COUNT(*) AS total_eventos,
                COUNT(DISTINCT fcw.codigo_sesion) AS usuarios_unicos
            FROM fact_comportamiento_web fcw
            INNER JOIN dim_tiempo t ON fcw.tiempo_key = t.ID_FECHA
            WHERE 1=1
              {self._construir_filtro_fecha('t.FECHA_CAL', fecha_inicio, fecha_fin)}
        """

        df = pd.read_sql(query, self.conn)

        return {
            'total_eventos': int(df['total_eventos'].iloc[0]) if not df.empty else 0,
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

    # ========================================================================
    # KPIs ANUALES 2025 vs 2024 (NO AFECTADOS POR FILTROS)
    # ========================================================================

    def calcular_kpis_principales_2025(self, mes_hasta: int = 10) -> Dict:
        """
        Calcula los KPIs principales para 2025 vs 2024 hasta un mes específico.
        Estos KPIs NO se ven afectados por los filtros de sidebar.

        Args:
            mes_hasta: Mes hasta el cual comparar (por defecto 10 = octubre)

        Returns:
            Dict con métricas 2025, 2024 y variaciones porcentuales
        """
        logger.info(f"Calculando KPIs principales 2025 vs 2024 (hasta mes {mes_hasta})...")

        # Query para ventas totales 2025
        query_ventas_2025 = f"""
            SELECT
                SUM(MontoFactura) AS ventas_totales,
                COUNT(DISTINCT venta_id) AS num_ventas,
                AVG(MontoFactura) AS ticket_promedio,
                SUM(MargenFactura) AS margen_total
            FROM (
                SELECT
                    venta_id,
                    SUM(monto_total) AS MontoFactura,
                    SUM(margen) AS MargenFactura,
                    MAX(CONVERT(date, CAST(tiempo_key AS varchar(8)))) AS FechaFactura
                FROM fact_ventas
                WHERE venta_cancelada = 0
                  AND tiempo_key >= 20250101
                  AND tiempo_key <= 2025{str(mes_hasta).zfill(2)}31
                GROUP BY venta_id
            ) AS Facturas
        """

        # Query para ventas totales 2024
        query_ventas_2024 = f"""
            SELECT
                SUM(MontoFactura) AS ventas_totales,
                COUNT(DISTINCT venta_id) AS num_ventas,
                AVG(MontoFactura) AS ticket_promedio,
                SUM(MargenFactura) AS margen_total
            FROM (
                SELECT
                    venta_id,
                    SUM(monto_total) AS MontoFactura,
                    SUM(margen) AS MargenFactura,
                    MAX(CONVERT(date, CAST(tiempo_key AS varchar(8)))) AS FechaFactura
                FROM fact_ventas
                WHERE venta_cancelada = 0
                  AND tiempo_key >= 20240101
                  AND tiempo_key <= 2024{str(mes_hasta).zfill(2)}31
                GROUP BY venta_id
            ) AS Facturas
        """

        # Query para ventas canceladas 2025
        query_canceladas_2025 = f"""
            SELECT
                COUNT(DISTINCT venta_id) AS ventas_canceladas
            FROM (
                SELECT DISTINCT venta_id
                FROM fact_ventas
                WHERE venta_cancelada = 1
                  AND tiempo_key >= 20250101
                  AND tiempo_key <= 2025{str(mes_hasta).zfill(2)}31
            ) AS VentasCanceladas
        """

        # Query para ventas canceladas 2024
        query_canceladas_2024 = f"""
            SELECT
                COUNT(DISTINCT venta_id) AS ventas_canceladas
            FROM (
                SELECT DISTINCT venta_id
                FROM fact_ventas
                WHERE venta_cancelada = 1
                  AND tiempo_key >= 20240101
                  AND tiempo_key <= 2024{str(mes_hasta).zfill(2)}31
            ) AS VentasCanceladas
        """

        # Query para total productos vendidos 2025
        query_productos_2025 = f"""
            SELECT
                SUM(cantidad) AS total_productos_vendidos
            FROM fact_ventas
            WHERE venta_cancelada = 0
              AND tiempo_key >= 20250101
              AND tiempo_key <= 2025{str(mes_hasta).zfill(2)}31
        """

        # Query para total productos vendidos 2024
        query_productos_2024 = f"""
            SELECT
                SUM(cantidad) AS total_productos_vendidos
            FROM fact_ventas
            WHERE venta_cancelada = 0
              AND tiempo_key >= 20240101
              AND tiempo_key <= 2024{str(mes_hasta).zfill(2)}31
        """

        # Query para clientes únicos 2025
        query_clientes_2025 = f"""
            SELECT
                COUNT(DISTINCT cliente_id) AS clientes_activos
            FROM fact_ventas
            WHERE venta_cancelada = 0
              AND tiempo_key >= 20250101
              AND tiempo_key <= 2025{str(mes_hasta).zfill(2)}31
        """

        # Query para clientes únicos 2024
        query_clientes_2024 = f"""
            SELECT
                COUNT(DISTINCT cliente_id) AS clientes_activos
            FROM fact_ventas
            WHERE venta_cancelada = 0
              AND tiempo_key >= 20240101
              AND tiempo_key <= 2024{str(mes_hasta).zfill(2)}31
        """

        # Query para promedio de productos por venta 2025
        query_promedio_productos_2025 = f"""
            SELECT
                AVG(CAST(CantidadProductos AS FLOAT)) AS promedio_productos_por_venta
            FROM (
                SELECT
                    venta_id,
                    SUM(cantidad) AS CantidadProductos
                FROM fact_ventas
                WHERE venta_cancelada = 0
                  AND tiempo_key >= 20250101
                  AND tiempo_key <= 2025{str(mes_hasta).zfill(2)}31
                GROUP BY venta_id
            ) AS ProductosPorVenta
        """

        # Query para promedio de productos por venta 2024
        query_promedio_productos_2024 = f"""
            SELECT
                AVG(CAST(CantidadProductos AS FLOAT)) AS promedio_productos_por_venta
            FROM (
                SELECT
                    venta_id,
                    SUM(cantidad) AS CantidadProductos
                FROM fact_ventas
                WHERE venta_cancelada = 0
                  AND tiempo_key >= 20240101
                  AND tiempo_key <= 2024{str(mes_hasta).zfill(2)}31
                GROUP BY venta_id
            ) AS ProductosPorVenta
        """

        # Ejecutar todas las queries
        df_ventas_2025 = pd.read_sql(query_ventas_2025, self.conn)
        df_ventas_2024 = pd.read_sql(query_ventas_2024, self.conn)
        df_canceladas_2025 = pd.read_sql(query_canceladas_2025, self.conn)
        df_canceladas_2024 = pd.read_sql(query_canceladas_2024, self.conn)
        df_productos_2025 = pd.read_sql(query_productos_2025, self.conn)
        df_productos_2024 = pd.read_sql(query_productos_2024, self.conn)
        df_clientes_2025 = pd.read_sql(query_clientes_2025, self.conn)
        df_clientes_2024 = pd.read_sql(query_clientes_2024, self.conn)
        df_promedio_productos_2025 = pd.read_sql(query_promedio_productos_2025, self.conn)
        df_promedio_productos_2024 = pd.read_sql(query_promedio_productos_2024, self.conn)

        # Extraer valores
        ventas_2025 = float(df_ventas_2025['ventas_totales'].iloc[0] or 0)
        ventas_2024 = float(df_ventas_2024['ventas_totales'].iloc[0] or 0)
        num_ventas_2025 = int(df_ventas_2025['num_ventas'].iloc[0] or 0)
        num_ventas_2024 = int(df_ventas_2024['num_ventas'].iloc[0] or 0)
        ticket_2025 = float(df_ventas_2025['ticket_promedio'].iloc[0] or 0)
        ticket_2024 = float(df_ventas_2024['ticket_promedio'].iloc[0] or 0)
        margen_2025 = float(df_ventas_2025['margen_total'].iloc[0] or 0)
        margen_2024 = float(df_ventas_2024['margen_total'].iloc[0] or 0)

        canceladas_2025 = int(df_canceladas_2025['ventas_canceladas'].iloc[0] or 0)
        canceladas_2024 = int(df_canceladas_2024['ventas_canceladas'].iloc[0] or 0)

        productos_2025 = int(df_productos_2025['total_productos_vendidos'].iloc[0] or 0)
        productos_2024 = int(df_productos_2024['total_productos_vendidos'].iloc[0] or 0)

        clientes_2025 = int(df_clientes_2025['clientes_activos'].iloc[0] or 0)
        clientes_2024 = int(df_clientes_2024['clientes_activos'].iloc[0] or 0)

        promedio_productos_2025 = float(df_promedio_productos_2025['promedio_productos_por_venta'].iloc[0] or 0)
        promedio_productos_2024 = float(df_promedio_productos_2024['promedio_productos_por_venta'].iloc[0] or 0)

        # Calcular totales de ventas (completadas + canceladas)
        total_ventas_2025 = num_ventas_2025 + canceladas_2025
        total_ventas_2024 = num_ventas_2024 + canceladas_2024

        # Calcular tasas de completación y cancelación
        tasa_completadas_2025 = (num_ventas_2025 / total_ventas_2025 * 100) if total_ventas_2025 > 0 else 0
        tasa_completadas_2024 = (num_ventas_2024 / total_ventas_2024 * 100) if total_ventas_2024 > 0 else 0

        tasa_canceladas_2025 = (canceladas_2025 / total_ventas_2025 * 100) if total_ventas_2025 > 0 else 0
        tasa_canceladas_2024 = (canceladas_2024 / total_ventas_2024 * 100) if total_ventas_2024 > 0 else 0

        # Calcular variaciones porcentuales
        def calcular_variacion(actual, anterior):
            if anterior > 0:
                return ((actual - anterior) / anterior) * 100
            return 0 if actual == 0 else 100

        # Calcular variación en puntos porcentuales para tasas
        def calcular_variacion_puntos(tasa_actual, tasa_anterior):
            return tasa_actual - tasa_anterior

        # Calcular margen porcentual
        margen_porcentaje_2025 = (margen_2025 / ventas_2025 * 100) if ventas_2025 > 0 else 0
        margen_porcentaje_2024 = (margen_2024 / ventas_2024 * 100) if ventas_2024 > 0 else 0

        return {
            # Ventas Totales
            'ventas_totales_2025': ventas_2025,
            'ventas_totales_2024': ventas_2024,
            'ventas_variacion': calcular_variacion(ventas_2025, ventas_2024),

            # Margen de Ganancia
            'margen_total_2025': margen_2025,
            'margen_porcentaje_2025': margen_porcentaje_2025,
            'margen_total_2024': margen_2024,
            'margen_porcentaje_2024': margen_porcentaje_2024,
            'margen_variacion': calcular_variacion(margen_porcentaje_2025, margen_porcentaje_2024),

            # Ticket Promedio
            'ticket_promedio_2025': ticket_2025,
            'ticket_promedio_2024': ticket_2024,
            'ticket_variacion': calcular_variacion(ticket_2025, ticket_2024),

            # Clientes Activos
            'clientes_activos_2025': clientes_2025,
            'clientes_activos_2024': clientes_2024,
            'clientes_variacion': calcular_variacion(clientes_2025, clientes_2024),

            # Ventas Completadas y Canceladas (CANTIDADES Y TASAS)
            'ventas_completadas_2025': num_ventas_2025,
            'ventas_completadas_2024': num_ventas_2024,
            'tasa_completadas_2025': tasa_completadas_2025,
            'tasa_completadas_2024': tasa_completadas_2024,
            'ventas_completadas_variacion': calcular_variacion_puntos(tasa_completadas_2025, tasa_completadas_2024),

            'ventas_canceladas_2025': canceladas_2025,
            'ventas_canceladas_2024': canceladas_2024,
            'tasa_canceladas_2025': tasa_canceladas_2025,
            'tasa_canceladas_2024': tasa_canceladas_2024,
            'ventas_canceladas_variacion': calcular_variacion_puntos(tasa_canceladas_2025, tasa_canceladas_2024),

            # Total Productos Vendidos
            'productos_vendidos_2025': productos_2025,
            'productos_vendidos_2024': productos_2024,
            'productos_variacion': calcular_variacion(productos_2025, productos_2024),

            # Promedio Productos por Venta
            'promedio_productos_2025': promedio_productos_2025,
            'promedio_productos_2024': promedio_productos_2024,
            'promedio_productos_variacion': calcular_variacion(promedio_productos_2025, promedio_productos_2024)
        }

    def calcular_funnel_comportamiento_web(self) -> pd.DataFrame:
        """
        Calcula el funnel de comportamiento web agrupado por sesión
        - Total sesiones únicas (distinct codigo_sesion)
        - Clientes identificados (sesiones con cliente reconocido)
        - Añadir al carrito (tipo_evento = 'ANADIR_CARRITO')
        - Vista de ofertas (tipo_evento = 'VISTA_OFERTAS')
        - Iniciar checkout (tipo_evento = 'INICIAR_CHECKOUT')
        - Conversión de ventas (sesiones que generaron venta)

        Returns:
            DataFrame con etapas del funnel
        """
        logger.info("Calculando funnel de comportamiento web...")

        query = """
            SELECT
                COUNT(DISTINCT s.codigo_sesion) AS total_sesiones,
                COUNT(DISTINCT CASE WHEN fcw.cliente_reconocido = 1 THEN s.codigo_sesion END) AS clientes_identificados,
                COUNT(DISTINCT CASE WHEN te.tipo_evento = 'ANADIR_CARRITO' THEN s.codigo_sesion END) AS anadir_carrito,
                COUNT(DISTINCT CASE WHEN te.tipo_evento = 'VISTA_OFERTAS' THEN s.codigo_sesion END) AS vista_ofertas,
                COUNT(DISTINCT CASE WHEN te.tipo_evento = 'INICIAR_CHECKOUT' THEN s.codigo_sesion END) AS iniciar_checkout,
                COUNT(DISTINCT CASE WHEN fcw.genero_venta = 1 THEN s.codigo_sesion END) AS conversion_ventas
            FROM fact_comportamiento_web fcw
            INNER JOIN dim_sesion s ON fcw.evento_id = s.evento_id
            INNER JOIN dim_tiempo t ON fcw.tiempo_key = t.ID_FECHA
            INNER JOIN dim_tipo_evento te ON fcw.tipo_evento_id = te.tipo_evento_id
            WHERE (t.ANIO_CAL < 2025 OR (t.ANIO_CAL = 2025 AND t.MES_CAL <= 10))
        """

        df = pd.read_sql(query, self.conn)

        # Crear DataFrame del funnel
        funnel_data = {
            'etapa': [
                'Total Sesiones',
                'Clientes Identificados',
                'Añadir al Carrito',
                'Vista de Ofertas',
                'Iniciar Checkout',
                'Conversión de Ventas'
            ],
            'cantidad': [
                int(df['total_sesiones'].iloc[0]),
                int(df['clientes_identificados'].iloc[0]),
                int(df['anadir_carrito'].iloc[0]),
                int(df['vista_ofertas'].iloc[0]),
                int(df['iniciar_checkout'].iloc[0]),
                int(df['conversion_ventas'].iloc[0])
            ]
        }

        return pd.DataFrame(funnel_data)

    def calcular_metricas_comportamiento_web(self) -> Dict:
        """
        Calcula métricas clave de comportamiento web
        - Tasa de conversión (sesiones con venta / sesiones totales)
        - Usuarios únicos (sesiones únicas)
        - Navegadores diferentes usados

        Returns:
            Dict con métricas web
        """
        logger.info("Calculando métricas de comportamiento web...")

        query = """
            SELECT
                COUNT(DISTINCT s.codigo_sesion) AS sesiones_unicas,
                COUNT(DISTINCT CASE WHEN fcw.genero_venta = 1 THEN s.codigo_sesion END) AS sesiones_con_venta,
                COUNT(DISTINCT n.navegador) AS navegadores_diferentes,
                COUNT(*) AS total_eventos
            FROM fact_comportamiento_web fcw
            INNER JOIN dim_sesion s ON fcw.evento_id = s.evento_id
            INNER JOIN dim_tiempo t ON fcw.tiempo_key = t.ID_FECHA
            INNER JOIN dim_navegador n ON fcw.navegador_id = n.navegador_id
            WHERE (t.ANIO_CAL < 2025 OR (t.ANIO_CAL = 2025 AND t.MES_CAL <= 10))
        """

        df = pd.read_sql(query, self.conn)

        sesiones_unicas = int(df['sesiones_unicas'].iloc[0])
        sesiones_con_venta = int(df['sesiones_con_venta'].iloc[0])
        tasa_conversion = (sesiones_con_venta / sesiones_unicas * 100) if sesiones_unicas > 0 else 0

        return {
            'tasa_conversion': round(tasa_conversion, 2),
            'usuarios_unicos': sesiones_unicas,
            'navegadores_diferentes': int(df['navegadores_diferentes'].iloc[0]),
            'total_eventos': int(df['total_eventos'].iloc[0])
        }
