"""
================================================================================
MÓDULO DE PROYECCIONES - SERIES TEMPORALES
================================================================================
Implementa modelos de series temporales para predicción de ventas futuras
Soporta: ARIMA, Exponential Smoothing, y modelos simples
================================================================================
"""

import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.stattools import adfuller, acf, pacf
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from sklearn.metrics import mean_squared_error, mean_absolute_error, mean_absolute_percentage_error
import joblib
import logging
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional, List, Union
import warnings
import pyodbc
from sqlalchemy.engine import Engine
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)


class ModeloProyeccionVentas:
    """
    Clase para proyección de ventas usando series temporales
    Implementa ARIMA y Exponential Smoothing
    """

    def __init__(self, conn: Union[pyodbc.Connection, Engine]):
        """
        Inicializa el modelo de proyección

        Args:
            conn: Conexión pyodbc o SQLAlchemy Engine a la base de datos DW.
                  Se recomienda usar SQLAlchemy Engine para evitar warnings de pandas.
        """
        self.conn = conn
        self.modelo = None
        self.tipo_modelo = None
        self.serie_temporal = None
        self.datos_entrenamiento = None
        self.datos_prueba = None
        self.predicciones = None
        self.intervalos_confianza = None
        self.metricas = None

    def extraer_serie_temporal(self,
                               granularidad: str = 'mes',
                               agregacion: str = 'total',
                               filtros: Dict = None) -> pd.DataFrame:
        """
        Extrae serie temporal de ventas

        Args:
            granularidad: 'dia', 'semana', 'mes', 'trimestre'
            agregacion: 'total', 'promedio', 'cantidad'
            filtros: Diccionario con filtros opcionales (categoria, provincia, etc.)

        Returns:
            DataFrame con serie temporal
        """
        logger.info(f"Extrayendo serie temporal (granularidad: {granularidad}, agregación: {agregacion})...")

        # Construir query según granularidad
        if granularidad == 'dia':
            grupo_temporal = "t.FECHA_CAL"
            orden_temporal = "t.FECHA_CAL"
        elif granularidad == 'semana':
            grupo_temporal = "t.ANIO_CAL, t.SEM_CAL_NUM"
            orden_temporal = "t.ANIO_CAL, t.SEM_CAL_NUM"
        elif granularidad == 'mes':
            grupo_temporal = "t.ANIO_CAL, t.MES_CAL"
            orden_temporal = "t.ANIO_CAL, t.MES_CAL"
        elif granularidad == 'trimestre':
            grupo_temporal = "t.ANIO_CAL, t.TRIMESTRE"
            orden_temporal = "t.ANIO_CAL, t.TRIMESTRE"
        else:
            raise ValueError(f"Granularidad no soportada: {granularidad}")

        # Seleccionar métrica de agregación
        if agregacion == 'total':
            metrica = "SUM(fv.monto_total) AS valor"
        elif agregacion == 'promedio':
            metrica = "AVG(fv.monto_total) AS valor"
        elif agregacion == 'cantidad':
            metrica = "SUM(fv.cantidad) AS valor"
        else:
            raise ValueError(f"Agregación no soportada: {agregacion}")

        # Construir filtros
        condiciones_filtro = ["fv.venta_cancelada = 0"]

        if filtros:
            if 'categoria' in filtros:
                condiciones_filtro.append(f"p.categoria = '{filtros['categoria']}'")
            if 'provincia' in filtros:
                condiciones_filtro.append(f"g.provincia = '{filtros['provincia']}'")
            if 'almacen' in filtros:
                condiciones_filtro.append(f"a.nombre_almacen = '{filtros['almacen']}'")

        filtro_where = " AND ".join(condiciones_filtro)

        # Query con CTE para agrupación correcta por venta_id
        if granularidad == 'dia':
            query = f"""
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.tiempo_key,
                        SUM(fv.cantidad) AS total_unidades,
                        SUM(fv.monto_total) AS monto_venta
                    FROM fact_ventas fv
                    INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
                    INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                        AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
                    INNER JOIN dim_almacen a ON fv.almacen_id = a.almacen_id
                    WHERE {filtro_where}
                    GROUP BY fv.venta_id, fv.tiempo_key
                )
                SELECT
                    t.FECHA_CAL AS fecha,
                    {metrica.replace('fv.', 'va.').replace('monto_total', 'monto_venta').replace('cantidad', 'total_unidades')}
                FROM VentasAgrupadas va
                INNER JOIN dim_tiempo t ON va.tiempo_key = t.ID_FECHA
                GROUP BY t.FECHA_CAL
                ORDER BY t.FECHA_CAL
            """
        elif granularidad == 'semana':
            query = f"""
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.tiempo_key,
                        SUM(fv.cantidad) AS total_unidades,
                        SUM(fv.monto_total) AS monto_venta
                    FROM fact_ventas fv
                    INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
                    INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                        AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
                    INNER JOIN dim_almacen a ON fv.almacen_id = a.almacen_id
                    WHERE {filtro_where}
                    GROUP BY fv.venta_id, fv.tiempo_key
                )
                SELECT
                    t.ANIO_CAL AS anio,
                    t.SEM_CAL_NUM AS semana,
                    MIN(t.FECHA_CAL) AS fecha,
                    {metrica.replace('fv.', 'va.').replace('monto_total', 'monto_venta').replace('cantidad', 'total_unidades')}
                FROM VentasAgrupadas va
                INNER JOIN dim_tiempo t ON va.tiempo_key = t.ID_FECHA
                GROUP BY t.ANIO_CAL, t.SEM_CAL_NUM
                ORDER BY t.ANIO_CAL, t.SEM_CAL_NUM
            """
        elif granularidad == 'mes':
            query = f"""
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.tiempo_key,
                        SUM(fv.cantidad) AS total_unidades,
                        SUM(fv.monto_total) AS monto_venta
                    FROM fact_ventas fv
                    INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
                    INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                        AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
                    INNER JOIN dim_almacen a ON fv.almacen_id = a.almacen_id
                    WHERE {filtro_where}
                    GROUP BY fv.venta_id, fv.tiempo_key
                )
                SELECT
                    t.ANIO_CAL AS anio,
                    t.MES_CAL AS mes,
                    DATEFROMPARTS(t.ANIO_CAL, t.MES_CAL, 1) AS fecha,
                    {metrica.replace('fv.', 'va.').replace('monto_total', 'monto_venta').replace('cantidad', 'total_unidades')}
                FROM VentasAgrupadas va
                INNER JOIN dim_tiempo t ON va.tiempo_key = t.ID_FECHA
                GROUP BY t.ANIO_CAL, t.MES_CAL
                ORDER BY t.ANIO_CAL, t.MES_CAL
            """
        else:  # trimestre
            query = f"""
                WITH VentasAgrupadas AS (
                    SELECT
                        fv.venta_id,
                        fv.tiempo_key,
                        SUM(fv.cantidad) AS total_unidades,
                        SUM(fv.monto_total) AS monto_venta
                    FROM fact_ventas fv
                    INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
                    INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                        AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
                    INNER JOIN dim_almacen a ON fv.almacen_id = a.almacen_id
                    WHERE {filtro_where}
                    GROUP BY fv.venta_id, fv.tiempo_key
                )
                SELECT
                    t.ANIO_CAL AS anio,
                    t.TRIMESTRE AS trimestre,
                    DATEFROMPARTS(t.ANIO_CAL, (t.TRIMESTRE - 1) * 3 + 1, 1) AS fecha,
                    {metrica.replace('fv.', 'va.').replace('monto_total', 'monto_venta').replace('cantidad', 'total_unidades')}
                FROM VentasAgrupadas va
                INNER JOIN dim_tiempo t ON va.tiempo_key = t.ID_FECHA
                GROUP BY t.ANIO_CAL, t.TRIMESTRE
                ORDER BY t.ANIO_CAL, t.TRIMESTRE
            """

        df = pd.read_sql(query, self.conn)

        # Convertir a serie temporal
        df['fecha'] = pd.to_datetime(df['fecha'])
        df = df.set_index('fecha')
        df = df.sort_index()

        self.serie_temporal = df

        logger.info(f"Serie temporal extraída: {len(df)} períodos desde {df.index.min()} hasta {df.index.max()}")

        return df

    def analizar_estacionariedad(self) -> Dict:
        """
        Analiza si la serie es estacionaria usando prueba Augmented Dickey-Fuller

        Returns:
            Dict con resultados de la prueba
        """
        if self.serie_temporal is None:
            raise ValueError("Debe extraer la serie temporal primero")

        logger.info("Analizando estacionariedad de la serie...")

        resultado_adf = adfuller(self.serie_temporal['valor'].dropna())

        resultado = {
            'estadistico_adf': resultado_adf[0],
            'p_valor': resultado_adf[1],
            'valores_criticos': resultado_adf[4],
            'es_estacionaria': resultado_adf[1] < 0.05
        }

        logger.info(f"ADF Statistic: {resultado['estadistico_adf']:.4f}")
        logger.info(f"p-value: {resultado['p_valor']:.4f}")
        logger.info(f"¿Es estacionaria?: {resultado['es_estacionaria']}")

        return resultado

    def dividir_serie(self, test_size: int = 6) -> Tuple[pd.Series, pd.Series]:
        """
        Divide la serie en entrenamiento y prueba

        Args:
            test_size: Número de períodos para prueba

        Returns:
            Tuple (train, test)
        """
        if self.serie_temporal is None:
            raise ValueError("Debe extraer la serie temporal primero")

        logger.info(f"Dividiendo serie (test_size={test_size} períodos)...")

        serie = self.serie_temporal['valor']

        train = serie[:-test_size]
        test = serie[-test_size:]

        self.datos_entrenamiento = train
        self.datos_prueba = test

        logger.info(f"Train: {len(train)} períodos | Test: {len(test)} períodos")

        return train, test

    def entrenar_arima(self,
                       order: Tuple[int, int, int] = None,
                       buscar_orden: bool = True) -> ARIMA:
        """
        Entrena modelo ARIMA

        Args:
            order: Orden del modelo (p, d, q)
            buscar_orden: Si True, busca el mejor orden automáticamente

        Returns:
            Modelo ARIMA entrenado
        """
        if self.datos_entrenamiento is None:
            raise ValueError("Debe dividir la serie primero")

        logger.info("Entrenando modelo ARIMA...")

        if buscar_orden or order is None:
            # Búsqueda simple de mejores parámetros
            logger.info("Buscando mejores parámetros ARIMA...")
            order = self._buscar_mejor_arima()

        logger.info(f"Entrenando ARIMA{order}...")

        self.modelo = ARIMA(self.datos_entrenamiento, order=order)
        self.modelo_fit = self.modelo.fit()

        self.tipo_modelo = f'ARIMA{order}'

        # Evaluar en test
        self._evaluar_modelo()

        logger.info(f"Modelo entrenado - AIC: {self.modelo_fit.aic:.2f}")

        return self.modelo_fit

    def _buscar_mejor_arima(self) -> Tuple[int, int, int]:
        """
        Busca el mejor orden ARIMA usando grid search simple

        Returns:
            Mejor orden (p, d, q)
        """
        mejor_aic = np.inf
        mejor_orden = None

        # Grid search simplificado
        for p in range(0, 3):
            for d in range(0, 2):
                for q in range(0, 3):
                    try:
                        modelo_temp = ARIMA(self.datos_entrenamiento, order=(p, d, q))
                        modelo_fit = modelo_temp.fit()

                        if modelo_fit.aic < mejor_aic:
                            mejor_aic = modelo_fit.aic
                            mejor_orden = (p, d, q)

                    except:
                        continue

        logger.info(f"Mejor orden encontrado: {mejor_orden} con AIC={mejor_aic:.2f}")

        return mejor_orden if mejor_orden else (1, 1, 1)

    def entrenar_exponential_smoothing(self,
                                       seasonal: str = 'add',
                                       seasonal_periods: int = 12) -> ExponentialSmoothing:
        """
        Entrena modelo de Exponential Smoothing (Holt-Winters)

        Args:
            seasonal: Tipo de estacionalidad ('add', 'mul', o None)
            seasonal_periods: Número de períodos estacionales

        Returns:
            Modelo entrenado
        """
        if self.datos_entrenamiento is None:
            raise ValueError("Debe dividir la serie primero")

        logger.info(f"Entrenando Exponential Smoothing (seasonal={seasonal})...")

        self.modelo = ExponentialSmoothing(
            self.datos_entrenamiento,
            seasonal=seasonal,
            seasonal_periods=seasonal_periods if seasonal else None,
            trend='add'
        )

        self.modelo_fit = self.modelo.fit()

        self.tipo_modelo = f'ExpSmoothing_{seasonal}'

        # Evaluar en test
        self._evaluar_modelo()

        logger.info("Modelo entrenado")

        return self.modelo_fit

    def _evaluar_modelo(self):
        """Evalúa el modelo en el conjunto de prueba"""
        if self.datos_prueba is None:
            return

        # Predecir en test
        predicciones = self.modelo_fit.forecast(steps=len(self.datos_prueba))

        # Calcular métricas
        rmse = np.sqrt(mean_squared_error(self.datos_prueba, predicciones))
        mae = mean_absolute_error(self.datos_prueba, predicciones)

        # MAPE
        mask = self.datos_prueba != 0
        mape = mean_absolute_percentage_error(
            self.datos_prueba[mask],
            predicciones[mask]
        ) * 100 if mask.sum() > 0 else 0

        self.metricas = {
            'rmse': rmse,
            'mae': mae,
            'mape': mape
        }

        logger.info(f"Métricas Test - RMSE: {rmse:.2f}, MAE: {mae:.2f}, MAPE: {mape:.2f}%")

    def proyectar(self,
                  periodos: int = 12,
                  intervalo_confianza: float = 0.95) -> pd.DataFrame:
        """
        Genera proyecciones futuras

        Args:
            periodos: Número de períodos a proyectar
            intervalo_confianza: Nivel de confianza para intervalos

        Returns:
            DataFrame con proyecciones e intervalos
        """
        if self.modelo_fit is None:
            raise ValueError("Debe entrenar el modelo primero")

        logger.info(f"Generando proyecciones para {periodos} períodos...")

        # Obtener forecast
        if hasattr(self.modelo_fit, 'get_forecast'):
            # Para ARIMA
            forecast = self.modelo_fit.get_forecast(steps=periodos)
            predicciones = forecast.predicted_mean
            intervalos = forecast.conf_int(alpha=1 - intervalo_confianza)

        else:
            # Para Exponential Smoothing
            predicciones = self.modelo_fit.forecast(steps=periodos)
            # Calcular intervalos aproximados
            std_error = np.std(self.datos_entrenamiento - self.modelo_fit.fittedvalues)
            z_score = 1.96 if intervalo_confianza == 0.95 else 2.576
            intervalos = pd.DataFrame({
                'lower': predicciones - z_score * std_error,
                'upper': predicciones + z_score * std_error
            })

        # Crear fechas futuras
        ultima_fecha = self.serie_temporal.index[-1]
        frecuencia = pd.infer_freq(self.serie_temporal.index)

        if frecuencia is None:
            # Inferir frecuencia manualmente
            diff = (self.serie_temporal.index[-1] - self.serie_temporal.index[-2]).days
            if diff <= 1:
                frecuencia = 'D'
            elif diff <= 7:
                frecuencia = 'W'
            elif diff <= 31:
                frecuencia = 'MS'
            else:
                frecuencia = 'QS'

        fechas_futuras = pd.date_range(
            start=ultima_fecha,
            periods=periodos + 1,
            freq=frecuencia
        )[1:]

        # Crear DataFrame de proyecciones
        df_proyecciones = pd.DataFrame({
            'fecha': fechas_futuras,
            'proyeccion': predicciones.values,
            'limite_inferior': intervalos.iloc[:, 0].values,
            'limite_superior': intervalos.iloc[:, 1].values,
            'intervalo_confianza': intervalo_confianza
        })

        df_proyecciones = df_proyecciones.set_index('fecha')

        self.predicciones = df_proyecciones

        logger.info(f"Proyecciones generadas desde {fechas_futuras[0]} hasta {fechas_futuras[-1]}")

        return df_proyecciones

    def obtener_resumen_completo(self) -> Dict:
        """
        Obtiene resumen completo del análisis

        Returns:
            Dict con datos históricos, predicciones test, y proyecciones
        """
        if self.modelo_fit is None:
            raise ValueError("Debe entrenar el modelo primero")

        # Datos históricos
        historico = self.serie_temporal.copy()
        historico['tipo'] = 'Histórico'

        # Predicciones en test (si existen)
        if self.datos_prueba is not None:
            pred_test = self.modelo_fit.forecast(steps=len(self.datos_prueba))

            df_test = pd.DataFrame({
                'valor': self.datos_prueba.values,
                'prediccion': pred_test.values
            }, index=self.datos_prueba.index)
        else:
            df_test = None

        return {
            'historico': historico,
            'predicciones_test': df_test,
            'proyecciones': self.predicciones,
            'metricas': self.metricas,
            'tipo_modelo': self.tipo_modelo
        }

    def guardar_modelo(self, ruta: str) -> str:
        """
        Guarda el modelo entrenado

        Args:
            ruta: Ruta base donde guardar

        Returns:
            Ruta del archivo guardado
        """
        if self.modelo_fit is None:
            raise ValueError("Debe entrenar el modelo primero")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        ruta_modelo = f"{ruta}/proyecciones_{self.tipo_modelo}_{timestamp}.pkl"
        joblib.dump({
            'modelo': self.modelo_fit,
            'tipo_modelo': self.tipo_modelo,
            'metricas': self.metricas
        }, ruta_modelo)

        logger.info(f"Modelo guardado en {ruta_modelo}")

        return ruta_modelo

    def cargar_modelo(self, ruta_modelo: str):
        """
        Carga un modelo previamente entrenado

        Args:
            ruta_modelo: Ruta al modelo
        """
        datos = joblib.load(ruta_modelo)

        self.modelo_fit = datos['modelo']
        self.tipo_modelo = datos['tipo_modelo']
        self.metricas = datos.get('metricas')

        logger.info("Modelo cargado exitosamente")
