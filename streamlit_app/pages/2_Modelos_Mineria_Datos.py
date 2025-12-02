import sys
import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

project_root = os.path.dirname(os.path.dirname(__file__))
for path in [os.path.join(project_root, 'utils'),
             os.path.join(project_root, 'modulos')]:
    if path not in sys.path:
        sys.path.insert(0, path)

from utils.db_connection import DatabaseConnection
from modulos.clustering import SegmentacionClientes
from modulos.regression import ModeloRegresionVentas
from modulos.proyecciones import ModeloProyeccionVentas
from modulos.componentes import inicializar_componentes, crear_seccion_encabezado, COLORES

st.set_page_config(
    page_title="Modelos de Minería de Datos",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

inicializar_componentes()

st.title("Ecommerce Cenfotec")


# ============================================================================
# FUNCIONES DE CACHÉ
# ============================================================================

@st.cache_resource
def get_dw_engine():
    """Obtiene SQLAlchemy engine del DW (cached)"""
    try:
        return DatabaseConnection.get_dw_engine(use_secrets=True)
    except Exception as e:
        st.error(f"Error conectando al DW: {str(e)}")
        st.stop()

# ============================================================================
# SIDEBAR - SELECCIÓN DE MODELO
# ============================================================================

st.sidebar.title("Configuración")

modelo_seleccionado = st.sidebar.selectbox(
    "Seleccionar Modelo",
    ["Clustering - Segmentación de Clientes",
     "Regresión - Predicción de Ventas",
     "Proyecciones - Series Temporales"]
)

engine = get_dw_engine()

# ============================================================================
# TAB 1: CLUSTERING - SEGMENTACIÓN DE CLIENTES
# ============================================================================

if modelo_seleccionado == "Clustering - Segmentación de Clientes":

    crear_seccion_encabezado(
        "Clustering - Segmentación de Clientes",
        "Agrupación de clientes según comportamiento de compra (RFM)",
    )

    # Parámetros
    with st.expander("⚙️ Configuración del Modelo", expanded=True):
        col1, col2, col3 = st.columns(3)

        with col1:
            limite_clientes = st.number_input(
                "Límite de clientes",
                min_value=100,
                max_value=10000,
                value=2000,
                step=100,
                help="Número máximo de clientes a analizar"
            )

        with col2:
            k_min = st.number_input("K mínimo", min_value=2, max_value=5, value=2)
            k_max = st.number_input("K máximo", min_value=3, max_value=15, value=8)

        with col3:
            metodo_viz = st.selectbox(
                "Método de visualización",
                ["PCA", "t-SNE"],
                help="Reducción de dimensionalidad para visualizar clusters"
            )

    if st.button("Entrenar Modelo de Clustering", use_container_width=True, type="primary"):

        with st.spinner("Entrenando modelo de clustering..."):

            try:
                segmentador = SegmentacionClientes(engine)

                st.write("### Paso 1: Extrayendo datos de clientes...")
                df_clientes = segmentador.extraer_datos_clientes(limite=limite_clientes)
                st.success(f"{len(df_clientes)} clientes extraídos")

                with st.expander("Ver muestra de datos"):
                    st.dataframe(df_clientes.head(10), use_container_width=True)

                st.write("### Paso 2: Preparando características...")
                df_original, df_features = segmentador.preparar_features(df_clientes)
                st.success(f"{df_features.shape[1]} características preparadas")

                st.write("### Paso 3: Buscando número óptimo de clusters...")

                resultados_k = segmentador.encontrar_numero_clusters_optimo(
                    df_features,
                    k_min=k_min,
                    k_max=k_max
                )

                col1, col2, col3 = st.columns(3)

                with col1:
                    fig_inercia = go.Figure()
                    fig_inercia.add_trace(go.Scatter(
                        x=resultados_k['k_values'],
                        y=resultados_k['inercia'],
                        mode='lines+markers',
                        name='Inercia',
                        line=dict(color=COLORES[0], width=3)
                    ))
                    fig_inercia.update_layout(
                        title="Método del Codo - Inercia",
                        xaxis_title="Número de Clusters (k)",
                        yaxis_title="Inercia",
                        height=350
                    )
                    st.plotly_chart(fig_inercia, use_container_width=True)

                with col2:
                    fig_silhouette = go.Figure()
                    fig_silhouette.add_trace(go.Scatter(
                        x=resultados_k['k_values'],
                        y=resultados_k['silhouette'],
                        mode='lines+markers',
                        name='Silhouette',
                        line=dict(color=COLORES[2], width=3)
                    ))
                    fig_silhouette.update_layout(
                        title="Silhouette Score",
                        xaxis_title="Número de Clusters (k)",
                        yaxis_title="Score",
                        height=350
                    )
                    st.plotly_chart(fig_silhouette, use_container_width=True)

                with col3:
                    fig_db = go.Figure()
                    fig_db.add_trace(go.Scatter(
                        x=resultados_k['k_values'],
                        y=resultados_k['davies_bouldin'],
                        mode='lines+markers',
                        name='Davies-Bouldin',
                        line=dict(color=COLORES[4], width=3)
                    ))
                    fig_db.update_layout(
                        title="Davies-Bouldin Index",
                        xaxis_title="Número de Clusters (k)",
                        yaxis_title="Índice",
                        height=350
                    )
                    st.plotly_chart(fig_db, use_container_width=True)

                k_optimo = segmentador.n_clusters_optimo
                st.info(f"**Número óptimo de clusters sugerido: {k_optimo}**")

                st.write(f"### Paso 4: Entrenando K-Means...")
                modelo = segmentador.entrenar_modelo(df_features, k_optimo)
                st.success("Modelo entrenado exitosamente")

                st.write("### Paso 5: Reduciendo dimensionalidad...")

                if metodo_viz == "PCA":
                    df_viz = segmentador.reducir_dimensionalidad_pca(df_features)
                    x_col, y_col = 'PC1', 'PC2'
                else:
                    df_viz = segmentador.reducir_dimensionalidad_tsne(df_features)
                    x_col, y_col = 'TSNE1', 'TSNE2'

                df_viz['cluster'] = segmentador.labels
                df_viz['cliente'] = df_original['nombre_completo'].values
                df_viz['monto_total'] = df_original['monto_total'].values

                st.write("### Visualización de Clusters")

                fig_clusters = px.scatter(
                    df_viz,
                    x=x_col,
                    y=y_col,
                    color='cluster',
                    size='monto_total',
                    hover_data=['cliente', 'monto_total'],
                    title=f'Clusters de Clientes - {metodo_viz}',
                    color_continuous_scale='Viridis',
                    height=600
                )

                fig_clusters.update_traces(marker=dict(opacity=0.7))
                st.plotly_chart(fig_clusters, use_container_width=True)

                st.write("### Interpretación de Segmentos")

                df_interpretacion = segmentador.interpretar_clusters(df_original, segmentador.labels)

                df_tabla = df_interpretacion.copy()
                df_tabla['monto_total_promedio'] = df_tabla['monto_total_promedio'].apply(lambda x: f"₡{x:,.0f}")
                df_tabla['monto_promedio'] = df_tabla['monto_promedio'].apply(lambda x: f"₡{x:,.0f}")
                df_tabla['margen_total_promedio'] = df_tabla['margen_total_promedio'].apply(lambda x: f"₡{x:,.0f}")
                df_tabla['porcentaje'] = df_tabla['porcentaje'].apply(lambda x: f"{x:.1f}%")

                st.dataframe(df_tabla, use_container_width=True)

                col1, col2 = st.columns(2)

                with col1:
                    fig_dist = px.pie(
                        df_interpretacion,
                        values='num_clientes',
                        names='nombre_segmento',
                        title='Distribución de Clientes por Segmento',
                        color_discrete_sequence=COLORES
                    )
                    st.plotly_chart(fig_dist, use_container_width=True)

                with col2:
                    fig_freq = px.bar(
                        df_interpretacion.sort_values('frecuencia_promedio', ascending=False),
                        x='nombre_segmento',
                        y='frecuencia_promedio',
                        title='Frecuencia Promedio de Compra por Segmento',
                        color='frecuencia_promedio',
                        color_continuous_scale='Greens',
                        labels={'frecuencia_promedio': 'Número de Compras', 'nombre_segmento': 'Segmento'}
                    )
                    fig_freq.update_xaxes(tickangle=45)
                    st.plotly_chart(fig_freq, use_container_width=True)


            except Exception as e:
                st.error(f"Error: {str(e)}")
                import traceback
                st.code(traceback.format_exc())

# ============================================================================
# TAB 2: REGRESIÓN - PREDICCIÓN DE VENTAS
# ============================================================================

elif modelo_seleccionado == "Regresión - Predicción de Ventas":

    crear_seccion_encabezado(
        "Regresión - Predicción de Ventas",
        "Predicción de ventas basada en variables del negocio",
    )

    # Parámetros
    with st.expander("⚙️ Configuración del Modelo", expanded=True):
        col1, col2, col3 = st.columns(3)

        with col1:
            limite_datos = st.number_input(
                "Límite de registros",
                min_value=1000,
                max_value=50000,
                value=10000,
                step=1000
            )

            variable_objetivo = st.selectbox(
                "Variable objetivo",
                ["monto_ventas", "cantidad_total", "margen_total"]
            )

        with col2:
            tipo_modelo = st.selectbox(
                "Tipo de modelo",
                ["linear", "ridge", "lasso", "random_forest", "gradient_boosting"],
                format_func=lambda x: {
                    'linear': 'Regresión Lineal',
                    'ridge': 'Ridge Regression',
                    'lasso': 'Lasso Regression',
                    'random_forest': 'Random Forest',
                    'gradient_boosting': 'Gradient Boosting'
                }[x]
            )

            test_size = st.slider("Tamaño del conjunto de prueba", 0.1, 0.4, 0.2, 0.05)

        with col3:
            if tipo_modelo in ['ridge', 'lasso']:
                alpha = st.number_input("Alpha (regularización)", 0.01, 10.0, 1.0, 0.1)
            elif tipo_modelo in ['random_forest', 'gradient_boosting']:
                n_estimators = st.number_input("Número de árboles", 50, 300, 100, 50)
                max_depth = st.number_input("Profundidad máxima", 3, 20, 10, 1)

    if st.button("Entrenar Modelo de Regresión", use_container_width=True, type="primary"):

        with st.spinner("Entrenando modelo de regresión..."):

            try:
                modelo_reg = ModeloRegresionVentas(engine)

                st.write("### Paso 1: Extrayendo datos de ventas...")
                df_ventas = modelo_reg.extraer_datos_ventas(limite=limite_datos)
                st.success(f"{len(df_ventas)} registros extraídos")

                with st.expander("Ver muestra de datos"):
                    st.dataframe(df_ventas.head(10), use_container_width=True)

                st.write("### Paso 2: Preparando características...")
                X, y = modelo_reg.preparar_features(df_ventas, variable_objetivo=variable_objetivo)
                st.success(f"{X.shape[1]} características preparadas")

                st.write("### Paso 3: Dividiendo datos...")
                X_train, X_test, y_train, y_test = modelo_reg.dividir_datos(X, y, test_size=test_size)
                st.success(f"Train: {len(X_train)} | Test: {len(X_test)}")

                st.write(f"### Paso 4: Entrenando modelo {tipo_modelo}...")

                kwargs = {}
                if tipo_modelo in ['ridge', 'lasso']:
                    kwargs['alpha'] = alpha
                elif tipo_modelo in ['random_forest', 'gradient_boosting']:
                    kwargs['n_estimators'] = n_estimators
                    kwargs['max_depth'] = max_depth

                modelo = modelo_reg.entrenar_modelo(tipo_modelo=tipo_modelo, **kwargs)
                st.success("Modelo entrenado exitosamente")

                st.write("### Evaluación del Modelo")

                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("#### Métricas de Entrenamiento")
                    metricas_train = modelo_reg.metricas['train']
                    st.metric("R² Score", f"{metricas_train['r2']:.4f}")
                    st.metric("RMSE", f"{metricas_train['rmse']:,.2f}")
                    st.metric("MAE", f"{metricas_train['mae']:,.2f}")
                    st.metric("MAPE", f"{metricas_train['mape']:.2f}%")

                with col2:
                    st.markdown("#### Métricas de Prueba")
                    metricas_test = modelo_reg.metricas['test']
                    st.metric("R² Score", f"{metricas_test['r2']:.4f}")
                    st.metric("RMSE", f"{metricas_test['rmse']:,.2f}")
                    st.metric("MAE", f"{metricas_test['mae']:,.2f}")
                    st.metric("MAPE", f"{metricas_test['mape']:.2f}%")

                st.write("### Validación Cruzada")
                cv_results = modelo_reg.cross_validation(cv=5)

                st.info(f"**R² Score CV**: {cv_results['media']:.4f} ± {cv_results['std']:.4f}")

                st.write("### Importancia de Características")

                df_importancia = modelo_reg.obtener_importancia_features(top_n=15)

                if not df_importancia.empty:
                    fig_importancia = px.bar(
                        df_importancia,
                        x='importancia',
                        y='feature',
                        orientation='h',
                        title='Top 15 Características Más Importantes',
                        color='importancia',
                        color_continuous_scale='Blues'
                    )
                    st.plotly_chart(fig_importancia, use_container_width=True)

                st.write("### Análisis de Residuos")

                df_residuos = modelo_reg.analizar_residuos()

                col1, col2 = st.columns(2)

                with col1:
                    fig_scatter = px.scatter(
                        df_residuos,
                        x='y_pred',
                        y='y_real',
                        title='Valores Reales vs Predichos',
                        labels={'y_pred': 'Predicción', 'y_real': 'Valor Real'},
                        trendline='ols',
                        color_discrete_sequence=[COLORES[0]]
                    )
                    fig_scatter.add_trace(
                        go.Scatter(
                            x=[df_residuos['y_pred'].min(), df_residuos['y_pred'].max()],
                            y=[df_residuos['y_pred'].min(), df_residuos['y_pred'].max()],
                            mode='lines',
                            name='Ideal',
                            line=dict(dash='dash', color='red')
                        )
                    )
                    st.plotly_chart(fig_scatter, use_container_width=True)

                with col2:
                    fig_residuos = px.scatter(
                        df_residuos,
                        x='y_pred',
                        y='residuo',
                        title='Gráfico de Residuos',
                        labels={'y_pred': 'Predicción', 'residuo': 'Residuo'},
                        color_discrete_sequence=[COLORES[2]]
                    )
                    fig_residuos.add_hline(y=0, line_dash='dash', line_color='red')
                    st.plotly_chart(fig_residuos, use_container_width=True)

                fig_dist_residuos = px.histogram(
                    df_residuos,
                    x='residuo',
                    title='Distribución de Residuos',
                    nbins=30,
                    color_discrete_sequence=[COLORES[1]]
                )
                st.plotly_chart(fig_dist_residuos, use_container_width=True)

            except Exception as e:
                st.error(f"Error: {str(e)}")
                import traceback
                st.code(traceback.format_exc())

# ============================================================================
# TAB 3: PROYECCIONES - SERIES TEMPORALES
# ============================================================================

elif modelo_seleccionado == "Proyecciones - Series Temporales":

    crear_seccion_encabezado(
        "Proyecciones - Series Temporales",
        "Predicción de ventas futuras con modelos de series temporales",
    )

    with st.expander("⚙️ Configuración del Modelo", expanded=True):
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            granularidad = st.selectbox(
                "Granularidad",
                ["mes", "semana", "dia"],
                help="Nivel de agregación temporal"
            )

            agregacion = st.selectbox(
                "Métrica",
                ["total", "promedio", "cantidad"],
                format_func=lambda x: {
                    'total': 'Monto Total',
                    'promedio': 'Monto Promedio',
                    'cantidad': 'Cantidad Vendida'
                }[x]
            )

        with col2:
            tipo_modelo_ts = st.selectbox(
                "Tipo de modelo",
                ["ARIMA", "Exponential Smoothing"],
                help="Modelo de series temporales"
            )

            test_periods = st.number_input(
                "Períodos de prueba",
                min_value=3,
                max_value=24,
                value=6,
                help="Períodos para validación"
            )

        with col3:
            periodos_proyectar = st.number_input(
                "Períodos a proyectar",
                min_value=1,
                max_value=24,
                value=12,
                help="Períodos futuros a predecir"
            )

            intervalo_confianza = st.slider(
                "Intervalo de confianza",
                0.80, 0.99, 0.95, 0.05
            )

        with col4:
            st.markdown("**Filtros Opcionales**")
            aplicar_filtros = st.checkbox("Aplicar filtros")

            filtros = {}
            if aplicar_filtros:
                query_cat = "SELECT DISTINCT categoria FROM dim_producto ORDER BY categoria"
                categorias = pd.read_sql(query_cat, engine)['categoria'].tolist()

                filtro_cat = st.selectbox("Categoría", ["Todas"] + categorias)
                if filtro_cat != "Todas":
                    filtros['categoria'] = filtro_cat

    if st.button("Generar Proyecciones", use_container_width=True, type="primary"):

        with st.spinner("Generando proyecciones de series temporales..."):

            try:
                modelo_proyeccion = ModeloProyeccionVentas(engine)

                st.write("### Paso 1: Extrayendo serie temporal...")
                df_serie = modelo_proyeccion.extraer_serie_temporal(
                    granularidad=granularidad,
                    agregacion=agregacion,
                    filtros=filtros if filtros else None
                )
                st.success(f"{len(df_serie)} períodos extraídos")

                st.write("### Serie Temporal Histórica")

                fig_serie = px.line(
                    df_serie.reset_index(),
                    x='fecha',
                    y='valor',
                    title='Evolución de Ventas en el Tiempo',
                    labels={'fecha': 'Fecha', 'valor': 'Valor'},
                    color_discrete_sequence=[COLORES[0]]
                )
                fig_serie.update_traces(line=dict(width=2))
                st.plotly_chart(fig_serie, use_container_width=True)

                st.write("### Análisis de Estacionariedad")

                resultado_adf = modelo_proyeccion.analizar_estacionariedad()

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("ADF Statistic", f"{resultado_adf['estadistico_adf']:.4f}")
                with col2:
                    st.metric("p-valor", f"{resultado_adf['p_valor']:.4f}")
                with col3:
                    if resultado_adf['es_estacionaria']:
                        st.success("Serie es estacionaria")
                    else:
                        st.warning("Serie no es estacionaria")

                st.write("### Paso 2: Dividiendo serie...")
                train, test = modelo_proyeccion.dividir_serie(test_size=test_periods)
                st.success(f"Train: {len(train)} | Test: {len(test)}")

                st.write(f"### Paso 3: Entrenando modelo {tipo_modelo_ts}...")

                if tipo_modelo_ts == "ARIMA":
                    modelo = modelo_proyeccion.entrenar_arima(buscar_orden=True)
                    st.success(f"Modelo {modelo_proyeccion.tipo_modelo} entrenado")
                else:
                    seasonal_periods = 12 if granularidad == 'mes' else (52 if granularidad == 'semana' else 7)
                    modelo = modelo_proyeccion.entrenar_exponential_smoothing(
                        seasonal='add',
                        seasonal_periods=seasonal_periods
                    )
                    st.success("Modelo entrenado")

                if modelo_proyeccion.metricas:
                    st.write("### Métricas de Evaluación (Test)")

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("RMSE", f"{modelo_proyeccion.metricas['rmse']:,.2f}")
                    with col2:
                        st.metric("MAE", f"{modelo_proyeccion.metricas['mae']:,.2f}")
                    with col3:
                        st.metric("MAPE", f"{modelo_proyeccion.metricas['mape']:.2f}%")

                st.write(f"### Paso 4: Generando proyecciones para {periodos_proyectar} períodos...")

                df_proyecciones = modelo_proyeccion.proyectar(
                    periodos=periodos_proyectar,
                    intervalo_confianza=intervalo_confianza
                )
                st.success(f"Proyecciones generadas hasta {df_proyecciones.index[-1].strftime('%Y-%m-%d')}")

                st.write("### Visualización de Proyecciones")

                resumen = modelo_proyeccion.obtener_resumen_completo()

                fig_proyeccion = go.Figure()

                # Serie histórica
                fig_proyeccion.add_trace(go.Scatter(
                    x=resumen['historico'].index,
                    y=resumen['historico']['valor'],
                    mode='lines',
                    name='Histórico',
                    line=dict(color=COLORES[0], width=2)
                ))

                # Proyecciones
                fig_proyeccion.add_trace(go.Scatter(
                    x=df_proyecciones.index,
                    y=df_proyecciones['proyeccion'],
                    mode='lines',
                    name='Proyección',
                    line=dict(color=COLORES[4], width=3, dash='dash')
                ))

                # Intervalo de confianza
                fig_proyeccion.add_trace(go.Scatter(
                    x=df_proyecciones.index,
                    y=df_proyecciones['limite_superior'],
                    mode='lines',
                    name=f'IC {intervalo_confianza*100:.0f}% Superior',
                    line=dict(width=0),
                    showlegend=False
                ))

                fig_proyeccion.add_trace(go.Scatter(
                    x=df_proyecciones.index,
                    y=df_proyecciones['limite_inferior'],
                    mode='lines',
                    name=f'IC {intervalo_confianza*100:.0f}%',
                    fill='tonexty',
                    fillcolor='rgba(68, 68, 68, 0.2)',
                    line=dict(width=0)
                ))

                fig_proyeccion.update_layout(
                    title='Proyección de Ventas con Intervalos de Confianza',
                    xaxis_title='Fecha',
                    yaxis_title='Valor',
                    hovermode='x unified',
                    height=600
                )

                st.plotly_chart(fig_proyeccion, use_container_width=True)

                st.write("### Tabla de Proyecciones")

                df_tabla = df_proyecciones.reset_index()
                df_tabla['fecha'] = df_tabla['fecha'].dt.strftime('%Y-%m-%d')
                df_tabla['proyeccion'] = df_tabla['proyeccion'].round(2)
                df_tabla['limite_inferior'] = df_tabla['limite_inferior'].round(2)
                df_tabla['limite_superior'] = df_tabla['limite_superior'].round(2)

                st.dataframe(df_tabla, use_container_width=True)

                st.write("### Estadísticas de Proyecciones")

                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Promedio Proyectado", f"{df_proyecciones['proyeccion'].mean():,.2f}")
                with col2:
                    st.metric("Mínimo", f"{df_proyecciones['proyeccion'].min():,.2f}")
                with col3:
                    st.metric("Máximo", f"{df_proyecciones['proyeccion'].max():,.2f}")
                with col4:
                    tendencia = "Creciente" if df_proyecciones['proyeccion'].iloc[-1] > df_proyecciones['proyeccion'].iloc[0] else "Decreciente"
                    st.metric("Tendencia", tendencia)

            except Exception as e:
                st.error(f"Error: {str(e)}")
                import traceback
                st.code(traceback.format_exc())

st.markdown("---")
st.caption("Sistema de Analítica Empresarial - Modelos de Minería de Datos")
