import streamlit as st
import sys
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

project_root = os.path.dirname(os.path.dirname(__file__))
for path in [os.path.join(project_root, 'utils'), os.path.join(project_root, 'OLAP'), os.path.join(project_root, 'modulos')]:
    if path not in sys.path:
        sys.path.insert(0, path)

from utils.db_connection import DatabaseConnection
from OLAP.cubo_olap import CuboOLAP
from modulos.componentes import inicializar_componentes, crear_seccion_encabezado

st.set_page_config(
    page_title="Cubo OLAP",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

inicializar_componentes()

st.title("Ecommerce Cenfotec")

crear_seccion_encabezado(
    "Cubo OLAP - Análisis Multidimensional",
    badge_color="primary"
)

# ============================================================================
# FUNCIONES CON CACHÉ
# ============================================================================

@st.cache_resource
def get_olap_cube():
    """Obtiene instancia del cubo OLAP (cached)"""
    try:
        engine = DatabaseConnection.get_dw_engine(use_secrets=True)
        return CuboOLAP(engine)
    except Exception as e:
        st.error(f"Error conectando al DW: {str(e)}")
        st.stop()

@st.cache_data(ttl=600)
def get_resumen_negocio(_cubo):
    """Obtiene resumen de negocio (cached 10min)"""
    return _cubo.resumen_negocio()

@st.cache_data(ttl=600)
def get_ventas_tiempo(_cubo, granularidad):
    """Obtiene ventas por tiempo (cached 10min)"""
    return _cubo.get_ventas_por_tiempo(granularidad)

@st.cache_data(ttl=600)
def get_ventas_categoria(_cubo):
    """Obtiene ventas por categoría (cached 10min)"""
    return _cubo.get_ventas_por_categoria()

@st.cache_data(ttl=600)
def get_ventas_region(_cubo, nivel):
    """Obtiene ventas por región (cached 10min)"""
    return _cubo.get_ventas_por_region(nivel)

@st.cache_data(ttl=600)
def get_top_productos(_cubo, n):
    """Obtiene top productos (cached 10min)"""
    return _cubo.top_productos(n)

@st.cache_data(ttl=600)
def get_top_clientes(_cubo, n):
    """Obtiene top clientes (cached 10min)"""
    return _cubo.get_ventas_por_cliente(n)

@st.cache_data(ttl=600)
def get_comportamiento_web(_cubo):
    """Obtiene análisis web (cached 10min)"""
    return _cubo.analisis_comportamiento_web()

@st.cache_data(ttl=600)
def get_analisis_busquedas(_cubo):
    """Obtiene análisis de búsquedas (cached 10min)"""
    return _cubo.analisis_busquedas()

@st.cache_data(ttl=300)
def ejecutar_slice(_cubo, dimension, value):
    """Ejecuta operación SLICE (cached 5min)"""
    return _cubo.slice(dimension, value)

@st.cache_data(ttl=300)
def ejecutar_dice(_cubo, filters_tuple):
    """Ejecuta operación DICE (cached 5min)"""
    filters = dict(filters_tuple)
    return _cubo.dice(filters)

@st.cache_data(ttl=300)
def ejecutar_slice_drill_down(_cubo, dimension, value):
    """Ejecuta drill-down para SLICE (cached 5min)"""
    return _cubo.slice_drill_down(dimension, value)

# Obtener cubo OLAP
cubo = get_olap_cube()

# ============================================================================
# MÉTRICAS GLOBALES (RESUMEN EJECUTIVO)
# ============================================================================

with st.spinner("Cargando métricas globales..."):
    try:
        resumen = get_resumen_negocio(cubo)

        if resumen:
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(
                    "Total Ventas (incluye canceladas)",
                    f"₡{resumen.get('total_ventas_con_canceladas', 0):,.2f}",
                    help="Monto total de todas las ventas incluyendo canceladas"
                )

            with col2:
                st.metric(
                    "Ganancia Total",
                    f"₡{resumen.get('total_ganancia', 0):,.2f}",
                    help=f"{resumen.get('margen_porcentaje', 0):.2f}% margen (sin canceladas)"
                )

            with col3:
                st.metric(
                    "Cantidad Ventas Totales",
                    f"{resumen.get('total_transacciones', 0):,}",
                    help="Número total de ventas completadas"
                )

            with col4:
                st.metric(
                    "Total Ventas (sin canceladas)",
                    f"₡{resumen.get('total_ventas_sin_canceladas', 0):,.2f}",
                    help="Monto de ventas excluyendo cancelaciones"
                )

            col5, col6, col7, col8 = st.columns(4)

            with col5:
                st.metric(
                    "Total Ordenes Canceladas",
                    f"{resumen.get('cantidad_canceladas', 0):,}",
                    help="Número de órdenes canceladas"
                )

            with col6:
                st.metric(
                    "Monto Cancelaciones",
                    f"₡{resumen.get('monto_canceladas', 0):,.2f}",
                    help="Monto total de ventas canceladas"
                )

            with col7:
                st.metric(
                    "Promedio por Venta",
                    f"₡{resumen.get('promedio_venta', 0):,.2f}",
                    help=f"₡{resumen.get('venta_maxima', 0):,.2f} máx"
                )

            with col8:
                st.metric(
                    "Clientes Únicos",
                    f"{resumen.get('clientes_unicos', 0):,}",
                    help=f"{resumen.get('productos_vendidos', 0):,} productos"
                )

            st.markdown("---")

        else:
            st.warning("No hay datos disponibles")

    except Exception as e:
        st.error(f"Error cargando métricas: {str(e)}")

# ============================================================================
# TABS PRINCIPALES
# ============================================================================

tab1, tab2, tab3, tab4 = st.tabs([
    "Enfoque de Negocio",
    "Análisis Multidimensional",
    "Exploración Jerárquica",
    "Comportamiento Web"
])

# ============================================================================
# TAB 1: ENFOQUE DE NEGOCIO (antes SLICE)
# ============================================================================

with tab1:
    crear_seccion_encabezado(
        "Enfoque de Negocio",
        "Análisis focalizado en una dimensión específica del negocio"
    )

    try:
        col1, col2 = st.columns([1, 1])

        dimension_labels = {
            'provincia': 'Región Geográfica',
            'categoria': 'Categoría de Producto',
            'almacen': 'Almacén/Tienda',
            'anio': 'Año Fiscal'
        }

        with col1:
            dimension = st.selectbox(
                "Dimensión de Análisis",
                list(dimension_labels.keys()),
                format_func=lambda x: dimension_labels[x],
                key="slice_dim"
            )

        with col2:
            if dimension == "provincia":
                query = "SELECT DISTINCT provincia FROM dim_geografia ORDER BY provincia"
            elif dimension == "categoria":
                query = "SELECT DISTINCT categoria FROM dim_producto ORDER BY categoria"
            elif dimension == "almacen":
                query = "SELECT DISTINCT nombre_almacen FROM dim_almacen ORDER BY nombre_almacen"
            elif dimension == "anio":
                query = "SELECT DISTINCT ANIO_CAL FROM dim_tiempo ORDER BY ANIO_CAL DESC"

            df_valores = pd.read_sql(query, cubo.conn)
            valores = [str(row) for row in df_valores.iloc[:, 0].tolist()]

            valor_seleccionado = st.selectbox(
                f"Seleccionar {dimension_labels[dimension]}",
                valores,
                key="slice_val"
            )

        if st.button("Analizar", use_container_width=True, type="primary"):
            with st.spinner(f"Analizando {dimension_labels[dimension]}: {valor_seleccionado}..."):
                try:
                    df_resumen = ejecutar_slice(cubo, dimension, valor_seleccionado)

                    if not df_resumen.empty:
                        row = df_resumen.iloc[0]

                        st.markdown("### Indicadores Clave")
                        col_m1, col_m2, col_m3, col_m4 = st.columns(4)

                        with col_m1:
                            st.metric(
                                "Órdenes Completadas",
                                f"{row.get('cantidad_ordenes', 0):,}",
                                help="Número total de órdenes completadas"
                            )

                        with col_m2:
                            st.metric(
                                "Ingresos Totales",
                                f"₡{row.get('total_ventas', 0):,.2f}",
                                help="Monto total de ventas"
                            )

                        with col_m3:
                            st.metric(
                                "Margen",
                                f"{row.get('margen_porcentaje', 0):.2f}%",
                                help="Porcentaje de margen de ganancia"
                            )

                        with col_m4:
                            st.metric(
                                "Clientes Únicos",
                                f"{row.get('clientes_unicos', 0):,}",
                                help="Número de clientes distintos"
                            )

                        st.markdown("---")

                        df_drill = ejecutar_slice_drill_down(cubo, dimension, valor_seleccionado)

                        if not df_drill.empty:
                            st.markdown("### Desglose Detallado")

                            col_chart, col_table = st.columns([3, 2])

                            with col_chart:
                                if dimension == "provincia":
                                    x_col, title = 'canton', f'Top Cantones en {valor_seleccionado}'
                                elif dimension == "categoria":
                                    x_col, title = 'producto', f'Top Productos en {valor_seleccionado}'
                                elif dimension == "almacen":
                                    x_col, title = 'categoria', f'Categorías en {valor_seleccionado}'
                                elif dimension == "anio":
                                    x_col, title = 'mes', f'Evolución Mensual - {valor_seleccionado}'

                                fig = go.Figure()
                                fig.add_trace(go.Bar(
                                    x=df_drill[x_col],
                                    y=df_drill['total_ventas'],
                                    name='Ventas',
                                    marker_color='#3498db',
                                    hovertemplate='<b>%{x}</b><br>Ventas: ₡%{y:,.2f}<extra></extra>'
                                ))

                                fig.update_layout(
                                    title=title,
                                    xaxis_title='',
                                    yaxis_title='Ingresos (₡)',
                                    height=400,
                                    showlegend=False
                                )

                                if dimension != 'anio':
                                    fig.update_xaxes(tickangle=-45)

                                st.plotly_chart(fig, use_container_width=True)

                            with col_table:
                                st.markdown("**Resumen Numérico**")
                                df_display = df_drill.copy()
                                if 'total_ventas' in df_display.columns:
                                    df_display['total_ventas'] = df_display['total_ventas'].apply(lambda x: f"₡{x:,.2f}")
                                if 'total_margen' in df_display.columns:
                                    df_display['total_margen'] = df_display['total_margen'].apply(lambda x: f"₡{x:,.2f}")
                                if 'margen_porcentaje' in df_display.columns:
                                    df_display['margen_porcentaje'] = df_display['margen_porcentaje'].apply(lambda x: f"{x:.2f}%")

                                st.dataframe(df_display, use_container_width=True, height=370)

                        st.markdown("---")

                        with st.expander("Ver Datos Detallados"):
                            df_resumen_display = df_resumen.copy()
                            for col in df_resumen_display.columns:
                                if col == 'dimension_value':
                                    continue 
                                elif 'porcentaje' in col.lower() or 'margen_porcentaje' == col:
                                    df_resumen_display[col] = df_resumen_display[col].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "")
                                elif df_resumen_display[col].dtype in ['float64', 'int64']:
                                    df_resumen_display[col] = df_resumen_display[col].apply(
                                        lambda x: f"{x:,.2f}" if isinstance(x, float) else f"{x:,}" if pd.notna(x) else ""
                                    )
                            st.dataframe(df_resumen_display, use_container_width=True)

                    else:
                        st.warning("No hay datos disponibles para esta selección")

                except Exception as e:
                    st.error(f"Error durante el análisis: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())

    except Exception as e:
        st.error(f"Error: {str(e)}")

# ============================================================================
# TAB 2: ANÁLISIS MULTIDIMENSIONAL
# ============================================================================

with tab2:
    crear_seccion_encabezado(
        "Análisis Multidimensional",
        "Filtros avanzados para análisis combinado de múltiples dimensiones"
    )

    try:
        st.subheader("Selecciona los filtros")

        col1, col2, col3 = st.columns(3)

        filters = {}

        with col1:
            query = "SELECT DISTINCT provincia FROM dim_geografia ORDER BY provincia"
            df_prov = pd.read_sql(query, cubo.conn)
            provincias = ['TODAS'] + df_prov['provincia'].tolist()

            prov_sel = st.selectbox("Provincia", provincias, key="dice_prov")
            if prov_sel != 'TODAS':
                filters['provincia'] = prov_sel

        with col2:
            query = "SELECT DISTINCT categoria FROM dim_producto ORDER BY categoria"
            df_cat = pd.read_sql(query, cubo.conn)
            categorias = ['TODAS'] + df_cat['categoria'].tolist()

            cat_sel = st.selectbox("Categoría", categorias, key="dice_cat")
            if cat_sel != 'TODAS':
                filters['categoria'] = cat_sel

        with col3:
            query = "SELECT DISTINCT ANIO_CAL FROM dim_tiempo ORDER BY ANIO_CAL DESC"
            df_anio = pd.read_sql(query, cubo.conn)
            anios = ['TODOS'] + [str(int(a)) for a in df_anio['ANIO_CAL'].tolist()]

            anio_sel = st.selectbox("Año", anios, key="dice_anio")
            if anio_sel != 'TODOS':
                filters['anio'] = int(anio_sel)

        col1, col2 = st.columns(2)

        with col1:
            query = "SELECT DISTINCT MES_CAL, MES_NOMBRE FROM dim_tiempo ORDER BY MES_CAL"
            df_meses = pd.read_sql(query, cubo.conn)
            meses = ['TODOS'] + [f"{row['MES_NOMBRE']} ({int(row['MES_CAL'])})" for _, row in df_meses.iterrows()]

            mes_sel = st.selectbox("Mes", meses, key="dice_mes")
            if mes_sel != 'TODOS':
                mes_num = int(mes_sel.split('(')[1].split(')')[0])
                filters['mes'] = mes_num

        if st.button("Aplicar Filtros y Analizar", use_container_width=True, type="primary"):
            with st.spinner("Procesando análisis multidimensional..."):
                try:
                    filters_tuple = tuple(sorted(filters.items()))
                    df = ejecutar_dice(cubo, filters_tuple)

                    if not df.empty:

                        st.markdown("### Resultados del Análisis")

                        col_k1, col_k2, col_k3, col_k4 = st.columns(4)

                        with col_k1:
                            ordenes_col = 'cantidad_ordenes' if 'cantidad_ordenes' in df.columns else 'cantidad_transacciones'
                            st.metric(
                                "Órdenes Totales",
                                f"{df[ordenes_col].sum():,}",
                                help="Número total de órdenes que cumplen los filtros"
                            )

                        with col_k2:
                            st.metric(
                                "Ingresos Totales",
                                f"₡{df['total_ventas'].sum():,.2f}",
                                help="Monto total de ventas filtradas"
                            )

                        with col_k3:
                            total_margen = df['total_margen'].sum()
                            total_ventas_sum = df['total_ventas'].sum()
                            margen_pct = (total_margen / total_ventas_sum * 100) if total_ventas_sum > 0 else 0
                            st.metric(
                                "Margen Promedio",
                                f"{margen_pct:.2f}%",
                                help=f"Ganancia: ₡{total_margen:,.2f}"
                            )

                        with col_k4:
                            st.metric(
                                "Registros Encontrados",
                                f"{len(df):,}",
                                help="Número de combinaciones únicas en resultados"
                            )

                        st.markdown("---")

                        if 'mes_nombre' in df.columns and 'total_ventas' in df.columns:
                            st.markdown("### Evolución Temporal")

                            ordenes_col = 'cantidad_ordenes' if 'cantidad_ordenes' in df.columns else 'cantidad_transacciones'

                            df_mes = df.groupby(['anio', 'mes', 'mes_nombre'], as_index=False).agg({
                                'total_ventas': 'sum',
                                'total_margen': 'sum',
                                ordenes_col: 'sum'
                            }).sort_values(['anio', 'mes'])

                            años_unicos = df_mes['anio'].unique()

                            fig = go.Figure()

                            if len(años_unicos) == 1:
                                fig.add_trace(go.Bar(
                                    x=df_mes['mes_nombre'],
                                    y=df_mes['total_ventas'],
                                    name='Ventas',
                                    marker_color='#3498db',
                                    hovertemplate='<b>%{x}</b><br>Ventas: ₡%{y:,.2f}<br>Órdenes: %{customdata:,}<extra></extra>',
                                    customdata=df_mes[ordenes_col]
                                ))

                                fig.add_trace(go.Scatter(
                                    x=df_mes['mes_nombre'],
                                    y=df_mes['total_margen'],
                                    name='Ganancia',
                                    mode='lines+markers',
                                    marker=dict(
                                        color='#2ecc71',
                                        size=10,
                                        line=dict(width=2, color='white')
                                    ),
                                    line=dict(width=3, color='#2ecc71'),
                                    hovertemplate='<b>%{x}</b><br>Ganancia: ₡%{y:,.2f}<extra></extra>'
                                ))

                                title = f'Ventas y Ganancia por Mes - {años_unicos[0]}'

                            else:
                                colores_años = ['#3498db', '#e74c3c', '#9b59b6', '#f39c12', '#1abc9c']
                                colores_ganancia = ['#2ecc71', '#27ae60', '#16a085', '#d4ac0d', '#117a65']

                                df_mes['mes_label'] = df_mes['mes_nombre'] + ' (' + df_mes['mes'].astype(str) + ')'

                                df_pivot = df_mes.pivot(index='mes', columns='anio', values='total_ventas').fillna(0)
                                df_pivot_margen = df_mes.pivot(index='mes', columns='anio', values='total_margen').fillna(0)

                                mes_map = df_mes.groupby('mes')['mes_nombre'].first().to_dict()

                                df_pivot = df_pivot.sort_index()
                                df_pivot_margen = df_pivot_margen.sort_index()

                                df_pivot.index = df_pivot.index.map(lambda x: mes_map.get(x, str(x)))
                                df_pivot_margen.index = df_pivot_margen.index.map(lambda x: mes_map.get(x, str(x)))

                                for idx, año in enumerate(sorted(años_unicos)):
                                    if año in df_pivot.columns:
                                        color_bar = colores_años[idx % len(colores_años)]

                                        fig.add_trace(go.Bar(
                                            x=df_pivot.index,
                                            y=df_pivot[año],
                                            name=f'Ventas {int(año)}',
                                            marker_color=color_bar,
                                            hovertemplate=f'<b>%{{x}} {int(año)}</b><br>Ventas: ₡%{{y:,.2f}}<extra></extra>'
                                        ))

                                for idx, año in enumerate(sorted(años_unicos)):
                                    if año in df_pivot_margen.columns and año in df_pivot.columns:
                                        color_line = colores_ganancia[idx % len(colores_ganancia)]

                                        años_anteriores = sorted(años_unicos)[:idx]
                                        if años_anteriores:
                                            y_base = df_pivot[[a for a in años_anteriores if a in df_pivot.columns]].sum(axis=1)
                                        else:
                                            y_base = 0

                                        y_position = y_base + (df_pivot[año] / 2)

                                        fig.add_trace(go.Scatter(
                                            x=df_pivot_margen.index,
                                            y=y_position,
                                            name=f'Ganancia {int(año)}',
                                            mode='lines+markers',
                                            marker=dict(
                                                color=color_line,
                                                size=8,
                                                line=dict(width=2, color='white')
                                            ),
                                            line=dict(width=2, color=color_line),
                                            hovertemplate=f'<b>%{{x}} {int(año)}</b><br>Ganancia: ₡%{{customdata:,.2f}}<extra></extra>',
                                            customdata=df_pivot_margen[año]
                                        ))

                                title = 'Ventas y Ganancia por Mes (Comparativa por Año)'
                                fig.update_layout(barmode='stack') 

                            fig.update_layout(
                                title=title,
                                xaxis_title='Mes',
                                yaxis_title='Monto (₡)',
                                hovermode='x unified',
                                height=500,
                                showlegend=True,
                                legend=dict(
                                    orientation="v",
                                    yanchor="top",
                                    y=1,
                                    xanchor="left",
                                    x=1.02
                                )
                            )

                            st.plotly_chart(fig, use_container_width=True)

                        st.markdown("---")

                        st.markdown("### Tabla de Resultados Detallados")
                        st.caption(f"Mostrando los primeros 100 de {len(df):,} registros totales")

                        df_display = df.head(100).copy()

                        for col in df_display.columns:
                            if 'porcentaje' in col.lower():
                                df_display[col] = df_display[col].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "")
                            elif col in ['total_ventas', 'promedio_por_orden', 'total_margen', 'total_impuesto']:
                                df_display[col] = df_display[col].apply(lambda x: f"₡{x:,.2f}" if pd.notna(x) else "")
                            elif col in ['cantidad_ordenes', 'cantidad_transacciones', 'total_unidades']:
                                df_display[col] = df_display[col].apply(lambda x: f"{x:,}" if pd.notna(x) else "")

                        st.dataframe(df_display, use_container_width=True, height=400)

                        with st.expander("📥 Descargar Datos Completos"):
                            st.download_button(
                                label="Descargar CSV con todos los registros",
                                data=df.to_csv(index=False).encode('utf-8'),
                                file_name=f"analisis_multidimensional_{len(filters)}_filtros.csv",
                                mime="text/csv",
                                use_container_width=True
                            )
                    else:
                        st.warning("No hay datos disponibles para la combinación de filtros seleccionada")

                except Exception as e:
                    st.error(f"Error durante el análisis: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())

    except Exception as e:
        st.error(f"Error: {str(e)}")

# ============================================================================
# TAB 3: EXPLORACIÓN JERÁRQUICA
# ============================================================================

with tab3:
    crear_seccion_encabezado(
        "Exploración Jerárquica de Datos",
        "Navegación por dimensiones de tiempo, geografía, categorías y rankings",
        #badge="DRILL"
    )

    subtab1, subtab2, subtab3 = st.tabs([
        "Ventas por Tiempo",
        "Ventas por Geografía",
        "Top N"
    ])

    with subtab1:
        st.subheader("Análisis de Ventas por Período")

        granularidad = st.radio(
            "Selecciona la granularidad temporal",
            ["Anual", "Trimestral", "Mensual", "Diaria"],
            horizontal=True
        )

        if st.button("Cargar Ventas por Tiempo", use_container_width=True, type="primary"):
            with st.spinner("Cargando datos..."):
                try:
                    gran_map = {
                        "Anual": "anio",
                        "Trimestral": "trimestre",
                        "Mensual": "mes",
                        "Diaria": "dia"
                    }
                    df = get_ventas_tiempo(cubo, gran_map[granularidad])

                    if not df.empty:
                        col1, col2 = st.columns(2)

                        with col1:
                            if 'total_ventas' in df.columns:
                                x_col = 'periodo' if 'periodo' in df.columns else df.columns[0]
                                fig = px.bar(
                                    df,
                                    x=x_col,
                                    y='total_ventas',
                                    title='Total de Ventas',
                                    labels={'total_ventas': 'Ventas (₡)'},
                                    color_discrete_sequence=['#3498db']
                                )
                                st.plotly_chart(fig, use_container_width=True)

                        with col2:
                            if 'total_margen' in df.columns:
                                x_col = 'periodo' if 'periodo' in df.columns else df.columns[0]
                                fig = px.bar(
                                    df,
                                    x=x_col,
                                    y='total_margen',
                                    title='Margen Total',
                                    labels={'total_margen': 'Margen (₡)'},
                                    color_discrete_sequence=['#2ecc71']
                                )
                                st.plotly_chart(fig, use_container_width=True)

                        st.markdown("---")

                        st.markdown("### Datos Detallados")
                        df_display = df.copy()

                        for col in df_display.columns:
                            if col in ['total_ventas', 'promedio_venta', 'total_margen']:
                                df_display[col] = df_display[col].apply(lambda x: f"₡{x:,.2f}" if pd.notna(x) else "")
                            elif col in ['transacciones', 'total_unidades']:
                                df_display[col] = df_display[col].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "")
                            elif 'porcentaje' in col.lower():
                                df_display[col] = df_display[col].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "")

                        st.dataframe(df_display, use_container_width=True, height=400)
                    else:
                        st.warning("No hay datos")

                except Exception as e:
                    st.error(f"Error: {str(e)}")

    with subtab2:
        st.subheader("Análisis de Ventas por Ubicación")

        nivel_geo = st.radio(
            "Nivel geográfico",
            ["Provincia", "Cantón", "Distrito"],
            horizontal=True
        )

        if st.button("Cargar Ventas por Geografía", use_container_width=True, type="primary"):
            with st.spinner("Cargando datos..."):
                try:
                    nivel_map = {"Provincia": "provincia", "Cantón": "canton", "Distrito": "distrito"}
                    df = get_ventas_region(cubo, nivel_map[nivel_geo])

                    if not df.empty:
                        col1, col2 = st.columns(2)

                        if nivel_geo == "Provincia":
                            y_col = 'provincia'
                        elif nivel_geo == "Cantón":
                            y_col = 'canton'
                        else:
                            y_col = 'distrito'

                        with col1:
                            fig = px.bar(
                                df.head(15),
                                x='total_ventas',
                                y=y_col,
                                orientation='h',
                                title=f'Top 15 Ventas por {nivel_geo}',
                                labels={'total_ventas': 'Ventas (₡)', y_col: nivel_geo},
                                color_discrete_sequence=['#3498db']
                            )
                            st.plotly_chart(fig, use_container_width=True)

                        with col2:
                            fig = px.pie(
                                df.head(10),
                                values='total_ventas',
                                names=y_col,
                                title=f'Distribución Top 10 {nivel_geo}'
                            )
                            st.plotly_chart(fig, use_container_width=True)

                        st.markdown("---")
                        st.markdown("### Datos Detallados")
                        df_display = df.copy()

                        for col in df_display.columns:
                            if col in ['total_ventas', 'promedio_venta', 'total_margen']:
                                df_display[col] = df_display[col].apply(lambda x: f"₡{x:,.2f}" if pd.notna(x) else "")
                            elif col in ['transacciones', 'total_unidades', 'clientes_unicos']:
                                df_display[col] = df_display[col].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "")
                            elif 'porcentaje' in col.lower():
                                df_display[col] = df_display[col].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "")

                        st.dataframe(df_display, use_container_width=True, height=400)
                    else:
                        st.warning("No hay datos")

                except Exception as e:
                    st.error(f"Error: {str(e)}")

    with subtab3:
        st.subheader("Top N Análisis")

        col1, col2 = st.columns(2)

        with col1:
            top_type = st.radio("Selecciona", ["Productos", "Clientes"], horizontal=True)

        with col2:
            top_n = st.slider("Cantidad", 5, 50, 10)

        if st.button("Cargar TOP N", use_container_width=True, type="primary"):
            with st.spinner(f"Cargando TOP {top_n}..."):
                try:
                    if top_type == "Productos":
                        df = get_top_productos(cubo, top_n)
                        col_sort = 'total_ventas'
                        y_col = 'producto'
                    else:
                        df = get_top_clientes(cubo, top_n)
                        col_sort = 'total_gasto'
                        y_col = 'cliente'

                    if not df.empty:
                        fig = px.bar(
                            df,
                            x=col_sort,
                            y=y_col,
                            orientation='h',
                            title=f'Top {top_n} {top_type}',
                            labels={col_sort: 'Monto (₡)'},
                            color_discrete_sequence=['#e74c3c']
                        )
                        st.plotly_chart(fig, use_container_width=True)

                        st.markdown("---")

                        st.markdown("### Datos Detallados")
                        df_display = df.copy()

                        for col in df_display.columns:
                            if col in ['total_ventas', 'total_gasto', 'promedio_compra', 'compra_maxima', 'total_margen', 'margen_generado', 'precio_unitario', 'costo_unitario']:
                                df_display[col] = df_display[col].apply(lambda x: f"₡{x:,.2f}" if pd.notna(x) else "")
                            elif col in ['transacciones', 'total_unidades']:
                                df_display[col] = df_display[col].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "")
                            elif 'porcentaje' in col.lower():
                                df_display[col] = df_display[col].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "")

                        st.dataframe(df_display, use_container_width=True, height=400)
                    else:
                        st.warning("No hay datos")

                except Exception as e:
                    st.error(f"Error: {str(e)}")

# ============================================================================
# TAB 4: COMPORTAMIENTO WEB (SIMPLIFICADO)
# ============================================================================

with tab4:
    crear_seccion_encabezado(
        "Análisis de Interacción Digital",
        "Navegación, eventos web y patrones de búsqueda de usuarios",
        #badge="WEB"
    )

    subtab1, subtab2 = st.tabs([
        "Eventos de Navegación",
        "Análisis de Búsquedas"
    ])

    with subtab1:
        st.subheader("Interacciones y Eventos de Usuarios")

        if st.button("Cargar Eventos Web", use_container_width=True, type="primary"):
            with st.spinner("Cargando análisis de eventos..."):
                try:
                    comportamiento = get_comportamiento_web(cubo)

                    if 'eventos_por_tipo' in comportamiento and not comportamiento['eventos_por_tipo'].empty:
                        df_eventos = comportamiento['eventos_por_tipo']

                        total_eventos = df_eventos['total_eventos'].sum()
                        total_usuarios = df_eventos['usuarios_unicos'].max()
                        total_conversiones = df_eventos['conversiones'].sum()
                        tasa_global = (total_conversiones / total_eventos * 100) if total_eventos > 0 else 0

                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Total Eventos", f"{total_eventos:,}")
                        with col2:
                            st.metric("Usuarios Únicos", f"{total_usuarios:,}")
                        with col3:
                            st.metric("Conversiones", f"{total_conversiones:,}")
                        with col4:
                            st.metric("Tasa Conversión", f"{tasa_global:.2f}%")

                        st.markdown("---")

                        col1, col2 = st.columns(2)

                        with col1:
                            st.markdown("### Eventos por Tipo")
                            fig = px.bar(
                                df_eventos.head(10),
                                x='tipo_evento',
                                y='total_eventos',
                                labels={'total_eventos': 'Cantidad', 'tipo_evento': 'Tipo de Evento'},
                                color='total_eventos',
                                color_continuous_scale='Blues'
                            )
                            fig.update_xaxes(tickangle=-45)
                            st.plotly_chart(fig, use_container_width=True)

                        with col2:
                            st.markdown("### Tasa de Conversión por Evento")
                            fig = px.bar(
                                df_eventos.head(10),
                                x='tipo_evento',
                                y='tasa_conversion',
                                labels={'tasa_conversion': 'Conversión (%)', 'tipo_evento': 'Tipo'},
                                color='tasa_conversion',
                                color_continuous_scale='Greens'
                            )
                            fig.update_xaxes(tickangle=-45)
                            st.plotly_chart(fig, use_container_width=True)

                    st.markdown("---")
                    st.markdown("### Análisis de Plataformas")

                    col1, col2 = st.columns(2)

                    with col1:
                        if 'dispositivos' in comportamiento and not comportamiento['dispositivos'].empty:
                            df_dispositivos = comportamiento['dispositivos']
                            fig = px.pie(
                                df_dispositivos,
                                values='total_eventos',
                                names='tipo_dispositivo',
                                title='Distribución por Tipo de Dispositivo'
                            )
                            st.plotly_chart(fig, use_container_width=True)

                    with col2:
                        if 'navegadores' in comportamiento and not comportamiento['navegadores'].empty:
                            df_navegadores = comportamiento['navegadores']
                            fig = px.bar(
                                df_navegadores.head(5),
                                x='navegador',
                                y='total_eventos',
                                title='Top 5 Navegadores',
                                labels={'total_eventos': 'Eventos', 'navegador': 'Navegador'},
                                color_discrete_sequence=['#3498db']
                            )
                            st.plotly_chart(fig, use_container_width=True)

                    if 'productos_vistos' in comportamiento and not comportamiento['productos_vistos'].empty:
                        st.markdown("---")
                        st.markdown("### Top 10 Productos Más Vistos")
                        df_productos = comportamiento['productos_vistos']
                        df_productos = df_productos[df_productos['producto'] != 'SIN PRODUCTO']

                        fig = px.bar(
                            df_productos.head(10),
                            x='total_visualizaciones',
                            y='producto',
                            orientation='h',
                            labels={'total_visualizaciones': 'Visualizaciones', 'producto': 'Producto'},
                            color='tasa_conversion',
                            color_continuous_scale='RdYlGn'
                        )
                        st.plotly_chart(fig, use_container_width=True)

                except Exception as e:
                    st.error(f"Error: {str(e)}")

    with subtab2:
        st.subheader("Patrones de Búsqueda y Productos")

        if st.button("Cargar Análisis de Búsquedas", use_container_width=True, type="primary"):
            with st.spinner("Cargando análisis de búsquedas..."):
                try:
                    busquedas = get_analisis_busquedas(cubo)

                    if 'resumen' in busquedas and not busquedas['resumen'].empty:
                        resumen = busquedas['resumen'].iloc[0]

                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Total Búsquedas", f"{resumen['total_busquedas']:,.0f}")
                        with col2:
                            st.metric("Usuarios Únicos", f"{resumen['usuarios_unicos']:,.0f}")
                        with col3:
                            st.metric("Conversiones", f"{resumen['conversiones_totales']:,.0f}")
                        with col4:
                            st.metric("Tasa Conversión", f"{resumen['tasa_conversion_global']:.2f}%")

                        st.markdown("---")

                    col1, col2 = st.columns(2)

                    with col1:
                        if 'busquedas_dispositivo' in busquedas and not busquedas['busquedas_dispositivo'].empty:
                            st.markdown("### Distribución por Dispositivo")
                            df_dispositivo = busquedas['busquedas_dispositivo']
                            fig = px.pie(
                                df_dispositivo,
                                values='total_busquedas',
                                names='tipo_dispositivo',
                                color_discrete_sequence=px.colors.qualitative.Pastel
                            )
                            st.plotly_chart(fig, use_container_width=True)

                    with col2:
                        if 'busquedas_navegador' in busquedas and not busquedas['busquedas_navegador'].empty:
                            st.markdown("### Top 5 Navegadores")
                            df_navegador = busquedas['busquedas_navegador']
                            fig = px.bar(
                                df_navegador.head(5),
                                x='navegador',
                                y='total_busquedas',
                                labels={'total_busquedas': 'Búsquedas', 'navegador': 'Navegador'},
                                color_discrete_sequence=['#e74c3c']
                            )
                            st.plotly_chart(fig, use_container_width=True)

                    if 'productos_buscados' in busquedas and not busquedas['productos_buscados'].empty:
                        st.markdown("---")
                        st.markdown("### Top 10 Productos Más Buscados")
                        df_productos = busquedas['productos_buscados']

                        fig = px.bar(
                            df_productos.head(10),
                            x='total_busquedas',
                            y='producto',
                            orientation='h',
                            labels={'total_busquedas': 'Búsquedas', 'producto': 'Producto'},
                            color='tasa_conversion',
                            color_continuous_scale='Purples'
                        )
                        st.plotly_chart(fig, use_container_width=True)

                except Exception as e:
                    st.error(f"Error: {str(e)}")

st.markdown("---")
st.caption("Sistema de Analítica Empresarial - Cubo OLAP")
