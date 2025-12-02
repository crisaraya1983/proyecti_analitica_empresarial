"""
================================================================================
MÓDULO DE REGRESIÓN - PREDICCIÓN DE VENTAS
================================================================================
Implementa regresión lineal múltiple para predecir ventas
Variables: mes, categoría, provincia, almacén
================================================================================
"""

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error, mean_absolute_percentage_error
import joblib
import logging
from datetime import datetime
from typing import Dict, Tuple, Optional, List, Union
import warnings
import pyodbc
from sqlalchemy.engine import Engine
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)


class ModeloRegresionVentas:
    """
    Clase para predicción de ventas usando regresión múltiple
    Soporta múltiples algoritmos: Linear, Ridge, Lasso, Random Forest, Gradient Boosting
    """

    def __init__(self, conn: Union[pyodbc.Connection, Engine]):
        """
        Inicializa el modelo de regresión

        Args:
            conn: Conexión pyodbc o SQLAlchemy Engine a la base de datos DW.
                  Se recomienda usar SQLAlchemy Engine para evitar warnings de pandas.
        """
        self.conn = conn
        self.scaler = StandardScaler()
        self.label_encoders = {}
        self.modelo = None
        self.tipo_modelo = None
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        self.feature_names = None
        self.metricas = None

    def extraer_datos_ventas(self, limite: Optional[int] = None) -> pd.DataFrame:
        """
        Extrae datos agregados de ventas para entrenamiento
        Usa CTE con agrupación por venta_id para evitar duplicados en cálculos

        Args:
            limite: Límite de registros (None para todos)

        Returns:
            DataFrame con datos agregados de ventas
        """
        logger.info("Extrayendo datos de ventas para regresión (con agrupación correcta por venta_id)...")

        # Query optimizada con CTE por venta_id para obtener datos agregados correctos
        query = """
            WITH VentasAgrupadas AS (
                -- Agrupar primero por venta_id para evitar duplicados
                SELECT
                    fv.venta_id,
                    fv.tiempo_key,
                    fv.producto_id,
                    fv.provincia_id,
                    fv.almacen_id,
                    fv.estado_venta_id,
                    fv.metodo_pago_id,
                    fv.es_primera_compra,
                    SUM(fv.cantidad) AS total_unidades,
                    SUM(fv.monto_total) AS monto_venta,
                    SUM(fv.margen) AS margen_venta,
                    SUM(fv.descuento_monto) AS descuento_venta,
                    AVG(fv.precio_unitario) AS precio_promedio_venta
                FROM fact_ventas fv
                WHERE fv.venta_cancelada = 0
                GROUP BY fv.venta_id, fv.tiempo_key, fv.producto_id,
                         fv.provincia_id, fv.almacen_id, fv.estado_venta_id,
                         fv.metodo_pago_id, fv.es_primera_compra
            )
            SELECT {limit_clause}
                -- Dimensiones temporales
                t.ANIO_CAL AS anio,
                t.MES_CAL AS mes,
                t.TRIMESTRE AS trimestre,
                t.DIA_SEM_NUM AS dia_semana,

                -- Dimensiones de producto
                p.categoria,
                p.marca,

                -- Dimensiones geográficas
                g.provincia,
                g.canton,

                -- Dimensión almacén
                a.nombre_almacen AS almacen,
                a.tipo_almacen,

                -- Dimensiones de venta
                ev.estado_venta,
                mp.metodo_pago,

                -- Variables objetivo y features agregadas (usando ventas únicas)
                COUNT(DISTINCT va.venta_id) AS num_transacciones,
                SUM(va.total_unidades) AS cantidad_total,
                SUM(va.monto_venta) AS monto_ventas,
                AVG(va.monto_venta) AS monto_promedio,
                SUM(va.margen_venta) AS margen_total,
                SUM(va.descuento_venta) AS descuento_total,
                AVG(va.precio_promedio_venta) AS precio_promedio,

                -- Indicadores
                CAST(SUM(CAST(va.es_primera_compra AS INT)) * 1.0 / COUNT(DISTINCT va.venta_id) AS FLOAT) AS proporcion_nuevos_clientes

            FROM VentasAgrupadas va
            INNER JOIN dim_tiempo t ON va.tiempo_key = t.ID_FECHA
            INNER JOIN dim_producto p ON va.producto_id = p.producto_id
            INNER JOIN dim_geografia g ON va.provincia_id = g.provincia_id
            INNER JOIN dim_almacen a ON va.almacen_id = a.almacen_id
            INNER JOIN dim_estado_venta ev ON va.estado_venta_id = ev.estado_venta_id
                AND ev.es_exitosa = 1
            INNER JOIN dim_metodo_pago mp ON va.metodo_pago_id = mp.metodo_pago_id

            GROUP BY
                t.ANIO_CAL, t.MES_CAL, t.TRIMESTRE, t.DIA_SEM_NUM,
                p.categoria, p.marca,
                g.provincia, g.canton,
                a.nombre_almacen, a.tipo_almacen,
                ev.estado_venta, mp.metodo_pago

            HAVING COUNT(DISTINCT va.venta_id) >= 1

            ORDER BY t.ANIO_CAL DESC, t.MES_CAL DESC
        """

        limit_clause = f"TOP {limite}" if limite else ""
        query = query.format(limit_clause=limit_clause)

        df = pd.read_sql(query, self.conn)

        logger.info(f"Datos extraídos: {len(df)} registros agregados")
        return df

    def preparar_features(self,
                          df: pd.DataFrame,
                          variable_objetivo: str = 'monto_ventas',
                          variables_categoricas: List[str] = None,
                          variables_numericas: List[str] = None) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Prepara features para entrenamiento

        Args:
            df: DataFrame con datos
            variable_objetivo: Nombre de la variable objetivo
            variables_categoricas: Lista de variables categóricas a incluir
            variables_numericas: Lista de variables numéricas a incluir

        Returns:
            Tuple (X, y) con features y variable objetivo
        """
        logger.info("Preparando features para regresión...")

        df = df.copy()

        # Variables categóricas por defecto
        if variables_categoricas is None:
            variables_categoricas = [
                'mes', 'trimestre', 'dia_semana',
                'categoria', 'provincia', 'almacen',
                'estado_venta', 'metodo_pago'
            ]

        # Variables numéricas por defecto (excluyendo la objetivo)
        if variables_numericas is None:
            variables_numericas = [
                'anio',
                'num_transacciones',
                'cantidad_total',
                'monto_promedio',
                'margen_total',
                'descuento_total',
                'precio_promedio',
                'proporcion_nuevos_clientes'
            ]

            # Remover la variable objetivo si está en numéricas
            if variable_objetivo in variables_numericas:
                variables_numericas.remove(variable_objetivo)

        # Codificar variables categóricas
        df_encoded = df.copy()

        for col in variables_categoricas:
            if col in df.columns:
                le = LabelEncoder()
                df_encoded[f'{col}_encoded'] = le.fit_transform(df[col].astype(str))
                self.label_encoders[col] = le

        # Crear lista de features
        features_finales = []

        # Agregar variables numéricas
        features_finales.extend([col for col in variables_numericas if col in df.columns])

        # Agregar variables categóricas codificadas
        features_finales.extend([f'{col}_encoded' for col in variables_categoricas if col in df.columns])

        # Crear X e y
        X = df_encoded[features_finales]
        y = df[variable_objetivo]

        # Guardar nombres de features
        self.feature_names = features_finales

        logger.info(f"Features preparados: {X.shape[1]} features, {len(y)} muestras")
        logger.info(f"Features: {', '.join(features_finales)}")

        return X, y

    def dividir_datos(self,
                      X: pd.DataFrame,
                      y: pd.Series,
                      test_size: float = 0.2,
                      random_state: int = 42) -> Tuple:
        """
        Divide datos en entrenamiento y prueba

        Args:
            X: Features
            y: Variable objetivo
            test_size: Proporción de datos para prueba
            random_state: Semilla aleatoria

        Returns:
            Tuple (X_train, X_test, y_train, y_test)
        """
        logger.info(f"Dividiendo datos (test_size={test_size})...")

        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )

        # Escalar features
        self.X_train_scaled = pd.DataFrame(
            self.scaler.fit_transform(self.X_train),
            columns=self.X_train.columns,
            index=self.X_train.index
        )

        self.X_test_scaled = pd.DataFrame(
            self.scaler.transform(self.X_test),
            columns=self.X_test.columns,
            index=self.X_test.index
        )

        logger.info(f"Train: {len(self.X_train)} | Test: {len(self.X_test)}")

        return self.X_train_scaled, self.X_test_scaled, self.y_train, self.y_test

    def entrenar_modelo(self,
                        tipo_modelo: str = 'linear',
                        **kwargs) -> object:
        """
        Entrena el modelo de regresión

        Args:
            tipo_modelo: Tipo de modelo ('linear', 'ridge', 'lasso', 'random_forest', 'gradient_boosting')
            **kwargs: Parámetros adicionales del modelo

        Returns:
            Modelo entrenado
        """
        logger.info(f"Entrenando modelo: {tipo_modelo}...")

        self.tipo_modelo = tipo_modelo

        # Seleccionar modelo
        modelos = {
            'linear': LinearRegression(),
            'ridge': Ridge(alpha=kwargs.get('alpha', 1.0)),
            'lasso': Lasso(alpha=kwargs.get('alpha', 1.0)),
            'random_forest': RandomForestRegressor(
                n_estimators=kwargs.get('n_estimators', 100),
                max_depth=kwargs.get('max_depth', 10),
                random_state=42
            ),
            'gradient_boosting': GradientBoostingRegressor(
                n_estimators=kwargs.get('n_estimators', 100),
                max_depth=kwargs.get('max_depth', 5),
                learning_rate=kwargs.get('learning_rate', 0.1),
                random_state=42
            )
        }

        if tipo_modelo not in modelos:
            raise ValueError(f"Tipo de modelo no soportado: {tipo_modelo}")

        self.modelo = modelos[tipo_modelo]

        # Entrenar
        self.modelo.fit(self.X_train_scaled, self.y_train)

        # Predecir
        y_train_pred = self.modelo.predict(self.X_train_scaled)
        y_test_pred = self.modelo.predict(self.X_test_scaled)

        # Calcular métricas
        self.metricas = {
            'train': self._calcular_metricas(self.y_train, y_train_pred),
            'test': self._calcular_metricas(self.y_test, y_test_pred)
        }

        logger.info(f"Modelo entrenado - R² Test: {self.metricas['test']['r2']:.4f}")

        return self.modelo

    def _calcular_metricas(self, y_real: pd.Series, y_pred: np.ndarray) -> Dict:
        """
        Calcula métricas de evaluación

        Args:
            y_real: Valores reales
            y_pred: Valores predichos

        Returns:
            Dict con métricas
        """
        r2 = r2_score(y_real, y_pred)
        rmse = np.sqrt(mean_squared_error(y_real, y_pred))
        mae = mean_absolute_error(y_real, y_pred)

        # MAPE (evitar división por cero)
        mask = y_real != 0
        mape = mean_absolute_percentage_error(y_real[mask], y_pred[mask]) * 100 if mask.sum() > 0 else 0

        return {
            'r2': r2,
            'rmse': rmse,
            'mae': mae,
            'mape': mape
        }

    def cross_validation(self, cv: int = 5) -> Dict:
        """
        Realiza validación cruzada

        Args:
            cv: Número de folds

        Returns:
            Dict con scores de CV
        """
        logger.info(f"Realizando validación cruzada ({cv} folds)...")

        if self.modelo is None:
            raise ValueError("Debe entrenar el modelo primero")

        # Usar todos los datos (train + test)
        X_all = pd.concat([self.X_train_scaled, self.X_test_scaled])
        y_all = pd.concat([self.y_train, self.y_test])

        scores = cross_val_score(
            self.modelo,
            X_all,
            y_all,
            cv=cv,
            scoring='r2'
        )

        resultado = {
            'scores': scores,
            'media': scores.mean(),
            'std': scores.std()
        }

        logger.info(f"CV R² medio: {resultado['media']:.4f} (+/- {resultado['std']:.4f})")

        return resultado

    def obtener_importancia_features(self, top_n: int = 10) -> pd.DataFrame:
        """
        Obtiene la importancia de features

        Args:
            top_n: Número de features más importantes a retornar

        Returns:
            DataFrame con importancia de features
        """
        if self.modelo is None:
            raise ValueError("Debe entrenar el modelo primero")

        # Para modelos basados en árboles
        if hasattr(self.modelo, 'feature_importances_'):
            importancias = self.modelo.feature_importances_

        # Para modelos lineales
        elif hasattr(self.modelo, 'coef_'):
            importancias = np.abs(self.modelo.coef_)

        else:
            logger.warning("El modelo no soporta importancia de features")
            return pd.DataFrame()

        df_importancia = pd.DataFrame({
            'feature': self.feature_names,
            'importancia': importancias
        })

        df_importancia = df_importancia.sort_values('importancia', ascending=False)

        return df_importancia.head(top_n)

    def predecir(self, X_nuevo: pd.DataFrame) -> np.ndarray:
        """
        Realiza predicciones con nuevos datos

        Args:
            X_nuevo: DataFrame con features

        Returns:
            Array con predicciones
        """
        if self.modelo is None:
            raise ValueError("Debe entrenar el modelo primero")

        X_nuevo_scaled = self.scaler.transform(X_nuevo)
        predicciones = self.modelo.predict(X_nuevo_scaled)

        return predicciones

    def analizar_residuos(self) -> pd.DataFrame:
        """
        Analiza residuos del modelo

        Returns:
            DataFrame con análisis de residuos
        """
        if self.modelo is None:
            raise ValueError("Debe entrenar el modelo primero")

        y_pred = self.modelo.predict(self.X_test_scaled)
        residuos = self.y_test - y_pred

        df_residuos = pd.DataFrame({
            'y_real': self.y_test,
            'y_pred': y_pred,
            'residuo': residuos,
            'residuo_abs': np.abs(residuos),
            'residuo_porcentual': (residuos / self.y_test) * 100
        })

        return df_residuos

    def guardar_modelo(self, ruta: str) -> Dict[str, str]:
        """
        Guarda el modelo entrenado

        Args:
            ruta: Ruta base donde guardar

        Returns:
            Dict con rutas de archivos guardados
        """
        if self.modelo is None:
            raise ValueError("Debe entrenar el modelo primero")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Guardar modelo
        ruta_modelo = f"{ruta}/regresion_{self.tipo_modelo}_{timestamp}.pkl"
        joblib.dump(self.modelo, ruta_modelo)

        # Guardar scaler
        ruta_scaler = f"{ruta}/scaler_regresion_{timestamp}.pkl"
        joblib.dump(self.scaler, ruta_scaler)

        # Guardar label encoders
        ruta_encoders = f"{ruta}/encoders_regresion_{timestamp}.pkl"
        joblib.dump(self.label_encoders, ruta_encoders)

        logger.info(f"Modelo guardado en {ruta}")

        return {
            'modelo': ruta_modelo,
            'scaler': ruta_scaler,
            'encoders': ruta_encoders
        }

    def cargar_modelo(self, ruta_modelo: str, ruta_scaler: str, ruta_encoders: str):
        """
        Carga un modelo previamente entrenado

        Args:
            ruta_modelo: Ruta al modelo
            ruta_scaler: Ruta al scaler
            ruta_encoders: Ruta a los encoders
        """
        self.modelo = joblib.load(ruta_modelo)
        self.scaler = joblib.load(ruta_scaler)
        self.label_encoders = joblib.load(ruta_encoders)

        logger.info("Modelo cargado exitosamente")
