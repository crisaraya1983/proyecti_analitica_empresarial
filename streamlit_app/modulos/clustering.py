"""
================================================================================
MÓDULO DE CLUSTERING - SEGMENTACIÓN DE CLIENTES
================================================================================
Implementa K-Means para segmentar clientes basado en comportamiento de compra
Características: RFM (Recencia, Frecuencia, Monto) + Categorías preferidas
================================================================================
"""

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from sklearn.metrics import silhouette_score, davies_bouldin_score
import joblib
import logging
from datetime import datetime
from typing import Dict, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)


class SegmentacionClientes:
    """
    Clase para segmentación de clientes usando K-Means
    Implementa análisis RFM y clustering multidimensional
    """

    def __init__(self, conn):
        """
        Inicializa el segmentador de clientes

        Args:
            conn: Conexión a la base de datos DW
        """
        self.conn = conn
        self.scaler = StandardScaler()
        self.modelo_kmeans = None
        self.pca_model = None
        self.datos_originales = None
        self.datos_escalados = None
        self.labels = None
        self.n_clusters_optimo = None

    def extraer_datos_clientes(self, limite: Optional[int] = None) -> pd.DataFrame:
        """
        Extrae características RFM y comportamiento de clientes del DW

        Args:
            limite: Límite de clientes a procesar (None para todos)

        Returns:
            DataFrame con características de clientes
        """
        logger.info("Extrayendo datos de clientes para clustering...")

        # Query optimizada para obtener métricas RFM
        query = """
            WITH ClienteMetricas AS (
                SELECT
                    fv.cliente_id,
                    -- Recencia (días desde última compra)
                    DATEDIFF(DAY, MAX(t.FECHA_CAL), GETDATE()) AS recencia_dias,

                    -- Frecuencia (número de transacciones)
                    COUNT(DISTINCT fv.venta_id) AS frecuencia_compras,

                    -- Monto (total gastado)
                    SUM(fv.monto_total) AS monto_total,
                    AVG(fv.monto_total) AS monto_promedio,

                    -- Cantidad de productos diferentes
                    COUNT(DISTINCT fv.producto_id) AS productos_diferentes,

                    -- Cantidad total de unidades
                    SUM(fv.cantidad) AS unidades_totales,

                    -- Margen generado
                    SUM(fv.margen) AS margen_total

                FROM fact_ventas fv
                INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
                WHERE fv.venta_cancelada = 0
                GROUP BY fv.cliente_id
            ),
            ClienteCategorias AS (
                SELECT
                    fv.cliente_id,
                    -- Categoría más comprada
                    (SELECT TOP 1 p.categoria
                     FROM fact_ventas fv2
                     INNER JOIN dim_producto p ON fv2.producto_id = p.producto_id
                     WHERE fv2.cliente_id = fv.cliente_id
                     GROUP BY p.categoria
                     ORDER BY COUNT(*) DESC) AS categoria_preferida,

                    -- Número de categorías diferentes
                    COUNT(DISTINCT p.categoria) AS categorias_diferentes

                FROM fact_ventas fv
                INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
                WHERE fv.venta_cancelada = 0
                GROUP BY fv.cliente_id
            ),
            ClienteUbicacion AS (
                SELECT DISTINCT
                    fv.cliente_id,
                    g.provincia
                FROM fact_ventas fv
                INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                    AND fv.canton_id = g.canton_id
                    AND fv.distrito_id = g.distrito_id
            )
            SELECT {limit_clause}
                cm.cliente_id,
                cl.nombre_cliente + ' ' + cl.apellido_cliente AS nombre_completo,
                cm.recencia_dias,
                cm.frecuencia_compras,
                cm.monto_total,
                cm.monto_promedio,
                cm.productos_diferentes,
                cm.unidades_totales,
                cm.margen_total,
                cc.categoria_preferida,
                cc.categorias_diferentes,
                cu.provincia
            FROM ClienteMetricas cm
            INNER JOIN ClienteCategorias cc ON cm.cliente_id = cc.cliente_id
            INNER JOIN ClienteUbicacion cu ON cm.cliente_id = cu.cliente_id
            INNER JOIN dim_cliente cl ON cm.cliente_id = cl.cliente_id
            WHERE cm.frecuencia_compras >= 2  -- Al menos 2 compras
            ORDER BY cm.monto_total DESC
        """

        # Aplicar límite si se especifica
        limit_clause = f"TOP {limite}" if limite else ""
        query = query.format(limit_clause=limit_clause)

        df = pd.read_sql(query, self.conn)

        logger.info(f"Datos extraídos: {len(df)} clientes")
        return df

    def preparar_features(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Prepara las características para clustering

        Args:
            df: DataFrame con datos de clientes

        Returns:
            Tuple (df_original, df_features_escalado)
        """
        logger.info("Preparando features para clustering...")

        # Seleccionar features numéricas para clustering
        features_numericas = [
            'recencia_dias',
            'frecuencia_compras',
            'monto_total',
            'monto_promedio',
            'productos_diferentes',
            'unidades_totales',
            'margen_total',
            'categorias_diferentes'
        ]

        # Crear dataframe de features
        df_features = df[features_numericas].copy()

        # Escalar features
        df_features_escalado = pd.DataFrame(
            self.scaler.fit_transform(df_features),
            columns=features_numericas,
            index=df.index
        )

        self.datos_originales = df
        self.datos_escalados = df_features_escalado

        logger.info(f"Features preparados: {df_features_escalado.shape}")
        return df, df_features_escalado

    def encontrar_numero_clusters_optimo(self,
                                         df_features: pd.DataFrame,
                                         k_min: int = 2,
                                         k_max: int = 10) -> Dict:
        """
        Determina el número óptimo de clusters usando método del codo y silhouette

        Args:
            df_features: DataFrame con features escaladas
            k_min: Número mínimo de clusters a probar
            k_max: Número máximo de clusters a probar

        Returns:
            Dict con métricas por cada k
        """
        logger.info(f"Buscando número óptimo de clusters (k={k_min} a {k_max})...")

        resultados = {
            'k_values': [],
            'inercia': [],
            'silhouette': [],
            'davies_bouldin': []
        }

        for k in range(k_min, k_max + 1):
            # Entrenar K-Means
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = kmeans.fit_predict(df_features)

            # Calcular métricas
            inercia = kmeans.inertia_
            silhouette = silhouette_score(df_features, labels)
            davies_bouldin = davies_bouldin_score(df_features, labels)

            resultados['k_values'].append(k)
            resultados['inercia'].append(inercia)
            resultados['silhouette'].append(silhouette)
            resultados['davies_bouldin'].append(davies_bouldin)

            logger.info(f"k={k}: inercia={inercia:.2f}, silhouette={silhouette:.3f}, DB={davies_bouldin:.3f}")

        # Determinar k óptimo (mayor silhouette score)
        idx_mejor = np.argmax(resultados['silhouette'])
        self.n_clusters_optimo = resultados['k_values'][idx_mejor]

        logger.info(f"Número óptimo de clusters: {self.n_clusters_optimo}")

        return resultados

    def entrenar_modelo(self, df_features: pd.DataFrame, n_clusters: int) -> KMeans:
        """
        Entrena el modelo K-Means con el número de clusters especificado

        Args:
            df_features: DataFrame con features escaladas
            n_clusters: Número de clusters

        Returns:
            Modelo K-Means entrenado
        """
        logger.info(f"Entrenando modelo K-Means con {n_clusters} clusters...")

        self.modelo_kmeans = KMeans(
            n_clusters=n_clusters,
            random_state=42,
            n_init=20,
            max_iter=300
        )

        self.labels = self.modelo_kmeans.fit_predict(df_features)

        # Calcular métricas del modelo final
        silhouette = silhouette_score(df_features, self.labels)
        davies_bouldin = davies_bouldin_score(df_features, self.labels)

        logger.info(f"Modelo entrenado - Silhouette: {silhouette:.3f}, Davies-Bouldin: {davies_bouldin:.3f}")

        return self.modelo_kmeans

    def reducir_dimensionalidad_pca(self, df_features: pd.DataFrame) -> pd.DataFrame:
        """
        Reduce dimensionalidad usando PCA a 2 componentes para visualización

        Args:
            df_features: DataFrame con features escaladas

        Returns:
            DataFrame con 2 componentes principales
        """
        logger.info("Reduciendo dimensionalidad con PCA...")

        self.pca_model = PCA(n_components=2, random_state=42)
        componentes = self.pca_model.fit_transform(df_features)

        df_pca = pd.DataFrame(
            componentes,
            columns=['PC1', 'PC2'],
            index=df_features.index
        )

        varianza_explicada = self.pca_model.explained_variance_ratio_
        logger.info(f"Varianza explicada: PC1={varianza_explicada[0]:.2%}, PC2={varianza_explicada[1]:.2%}")

        return df_pca

    def reducir_dimensionalidad_tsne(self, df_features: pd.DataFrame,
                                      perplexity: int = 30) -> pd.DataFrame:
        """
        Reduce dimensionalidad usando t-SNE a 2 componentes

        Args:
            df_features: DataFrame con features escaladas
            perplexity: Parámetro de t-SNE

        Returns:
            DataFrame con 2 componentes t-SNE
        """
        logger.info("Reduciendo dimensionalidad con t-SNE...")

        tsne = TSNE(n_components=2, random_state=42, perplexity=perplexity)
        componentes = tsne.fit_transform(df_features)

        df_tsne = pd.DataFrame(
            componentes,
            columns=['TSNE1', 'TSNE2'],
            index=df_features.index
        )

        return df_tsne

    def interpretar_clusters(self, df_original: pd.DataFrame, labels: np.ndarray) -> pd.DataFrame:
        """
        Interpreta y caracteriza cada cluster

        Args:
            df_original: DataFrame original con datos de clientes
            labels: Etiquetas de cluster asignadas

        Returns:
            DataFrame con estadísticas por cluster
        """
        logger.info("Interpretando clusters...")

        df_con_clusters = df_original.copy()
        df_con_clusters['cluster'] = labels

        # Calcular estadísticas por cluster
        metricas = []

        for cluster_id in sorted(df_con_clusters['cluster'].unique()):
            df_cluster = df_con_clusters[df_con_clusters['cluster'] == cluster_id]

            metrica = {
                'cluster': cluster_id,
                'num_clientes': len(df_cluster),
                'porcentaje': len(df_cluster) / len(df_con_clusters) * 100,
                'recencia_promedio': df_cluster['recencia_dias'].mean(),
                'frecuencia_promedio': df_cluster['frecuencia_compras'].mean(),
                'monto_total_promedio': df_cluster['monto_total'].mean(),
                'monto_promedio': df_cluster['monto_promedio'].mean(),
                'productos_diferentes_promedio': df_cluster['productos_diferentes'].mean(),
                'margen_total_promedio': df_cluster['margen_total'].mean(),
                'categoria_mas_comun': df_cluster['categoria_preferida'].mode()[0] if len(df_cluster['categoria_preferida'].mode()) > 0 else 'N/A'
            }

            metricas.append(metrica)

        df_interpretacion = pd.DataFrame(metricas)

        # Asignar nombres descriptivos a los clusters
        df_interpretacion['nombre_segmento'] = df_interpretacion.apply(
            self._asignar_nombre_segmento, axis=1
        )

        return df_interpretacion

    def _asignar_nombre_segmento(self, row: pd.Series) -> str:
        """
        Asigna un nombre descriptivo al cluster basado en sus características

        Args:
            row: Fila con estadísticas del cluster

        Returns:
            Nombre descriptivo del segmento
        """
        # Criterios basados en RFM
        recencia = row['recencia_promedio']
        frecuencia = row['frecuencia_promedio']
        monto = row['monto_total_promedio']

        # Clientes VIP: alta frecuencia, alto monto, recencia baja
        if frecuencia > 15 and monto > 500000 and recencia < 90:
            return "🌟 VIP - Clientes Premium"

        # Clientes leales: alta frecuencia, recencia baja
        elif frecuencia > 10 and recencia < 120:
            return "💎 Leales - Alta Frecuencia"

        # Clientes de alto valor: alto monto pero menor frecuencia
        elif monto > 300000 and frecuencia < 10:
            return "💰 Alto Valor - Compras Grandes"

        # Clientes regulares: frecuencia y monto medio
        elif frecuencia >= 5 and monto > 100000:
            return "👥 Regulares - Activos"

        # Clientes en riesgo: baja frecuencia, alta recencia
        elif frecuencia < 5 and recencia > 180:
            return "⚠️ En Riesgo - Baja Actividad"

        # Clientes nuevos: pocas compras, recencia baja
        elif frecuencia < 5 and recencia < 90:
            return "🆕 Nuevos - Potencial Desarrollo"

        # Clientes ocasionales
        else:
            return "🔵 Ocasionales - Engagement Medio"

    def obtener_clientes_por_cluster(self, cluster_id: int, top_n: int = 10) -> pd.DataFrame:
        """
        Obtiene los clientes de un cluster específico

        Args:
            cluster_id: ID del cluster
            top_n: Número de clientes a retornar

        Returns:
            DataFrame con clientes del cluster
        """
        if self.datos_originales is None or self.labels is None:
            raise ValueError("Debe entrenar el modelo primero")

        df_con_clusters = self.datos_originales.copy()
        df_con_clusters['cluster'] = self.labels

        df_cluster = df_con_clusters[df_con_clusters['cluster'] == cluster_id]

        # Ordenar por monto total descendente
        df_cluster = df_cluster.sort_values('monto_total', ascending=False)

        return df_cluster.head(top_n)

    def guardar_modelo(self, ruta: str):
        """
        Guarda el modelo entrenado y el scaler

        Args:
            ruta: Ruta base donde guardar los archivos
        """
        if self.modelo_kmeans is None:
            raise ValueError("Debe entrenar el modelo primero")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Guardar modelo K-Means
        ruta_modelo = f"{ruta}/kmeans_clientes_{timestamp}.pkl"
        joblib.dump(self.modelo_kmeans, ruta_modelo)

        # Guardar scaler
        ruta_scaler = f"{ruta}/scaler_clientes_{timestamp}.pkl"
        joblib.dump(self.scaler, ruta_scaler)

        # Guardar PCA si existe
        if self.pca_model is not None:
            ruta_pca = f"{ruta}/pca_clientes_{timestamp}.pkl"
            joblib.dump(self.pca_model, ruta_pca)

        logger.info(f"Modelo guardado en {ruta}")

        return {
            'modelo': ruta_modelo,
            'scaler': ruta_scaler,
            'pca': ruta_pca if self.pca_model else None
        }

    def cargar_modelo(self, ruta_modelo: str, ruta_scaler: str, ruta_pca: str = None):
        """
        Carga un modelo previamente entrenado

        Args:
            ruta_modelo: Ruta al modelo K-Means
            ruta_scaler: Ruta al scaler
            ruta_pca: Ruta al modelo PCA (opcional)
        """
        self.modelo_kmeans = joblib.load(ruta_modelo)
        self.scaler = joblib.load(ruta_scaler)

        if ruta_pca:
            self.pca_model = joblib.load(ruta_pca)

        logger.info("Modelo cargado exitosamente")
