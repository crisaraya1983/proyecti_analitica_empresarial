"""
================================================================================
DASHBOARD EJECUTIVO - BALANCED SCORECARD
================================================================================
Dashboard principal con KPIs y visualizaciones clave para toma de decisiones
Organizado según Balanced Scorecard (4 perspectivas)
================================================================================
"""

import sys
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Configurar paths
project_root = os.path.dirname(__file__)
for path in [os.path.join(project_root, 'utils'),
             os.path.join(project_root, 'modulos')]:
    if path not in sys.path:
        sys.path.insert(0, path)

from utils.db_connection import DatabaseConnection
from modulos.kpis_calculator import KPICalculator
from modulos.componentes import inicializar_componentes, crear_seccion_encabezado, COLORES

# ============================================================================
# CONFIGURACIÓN DE PÁGINA
# ============================================================================

st.set_page_config(
    page_title="Dashboard Ejecutivo - E-commerce Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Inicializar componentes y estilos
inicializar_componentes()

st.title("Ecommerce")

# Estilos adicionales para el dashboard
st.markdown("""
<style>
    /* Métricas destacadas */
    [data-testid="stMetricValue"] {
        font-size: 2.2em;
        font-weight: 700;
        color: #1a365d;
    }

    [data-testid="stMetricDelta"] {
        font-size: 1em;
    }

    /* Tarjetas de KPI */
    .kpi-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }

    /* Secciones del BSC */
    .bsc-section {
        background: #f8f9fa;
        padding: 15px;
        border-radius: 8px;
        border-left: 5px solid #2c5aa0;
        margin-bottom: 20px;
    }

    /* Filtros */
    .filter-container {
        background: white;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        margin-bottom: 20px;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# FUNCIONES DE CACHÉ
# ============================================================================

@st.cache_resource
def get_dw_engine():
    """Obtiene SQLAlchemy engine del DW (cached)"""
    try:
        # Usar SQLAlchemy engine en lugar de pyodbc para evitar warnings de pandas
        return DatabaseConnection.get_dw_engine(use_secrets=True)
    except Exception as e:
        st.error(f"Error conectando al DW: {str(e)}")
        st.stop()

@st.cache_data(ttl=600)
def obtener_opciones_filtros(_conn):
    """Obtiene opciones para filtros (cached 10min)"""
    categorias = pd.read_sql("SELECT DISTINCT categoria FROM dim_producto ORDER BY categoria", _conn)
    provincias = pd.read_sql("SELECT DISTINCT provincia FROM dim_geografia ORDER BY provincia", _conn)

    return {
        'categorias': ['Todas'] + categorias['categoria'].tolist(),
        'provincias': ['Todas'] + provincias['provincia'].tolist()
    }

# ============================================================================
# INICIALIZACIÓN
# ============================================================================

# Inicializar session_state para filtros
if 'fecha_inicio' not in st.session_state:
    # Por defecto: últimos 3 meses
    st.session_state.fecha_inicio = (datetime.now() - timedelta(days=90)).date()
    st.session_state.fecha_fin = datetime.now().date()
    st.session_state.categoria_filtro = 'Todas'
    st.session_state.provincia_filtro = 'Todas'

# Obtener engine
engine = get_dw_engine()
kpi_calc = KPICalculator(engine)

# Obtener opciones para filtros
opciones_filtros = obtener_opciones_filtros(engine)

# ============================================================================
# HEADER Y FILTROS
# ============================================================================

# Header principal
crear_seccion_encabezado(
    "Dashboard Ejecutivo",
    "Balanced Scorecard y KPIs Principales del Negocio",
    #badge="EJECUTIVO",
    badge_color="primary"
)

# ============================================================================
# SIDEBAR - FILTROS GLOBALES
# ============================================================================

st.sidebar.title("🔍 Filtros Globales")

with st.sidebar.expander("📅 Rango de Fechas", expanded=True):
    fecha_inicio = st.date_input(
        "Fecha inicio",
        value=st.session_state.fecha_inicio,
        key="date_inicio_input"
    )

    fecha_fin = st.date_input(
        "Fecha fin",
        value=st.session_state.fecha_fin,
        key="date_fin_input"
    )

with st.sidebar.expander("🏷️ Filtros Dimensionales", expanded=True):
    categoria_filtro = st.selectbox(
        "Categoría",
        opciones_filtros['categorias'],
        index=opciones_filtros['categorias'].index(st.session_state.categoria_filtro)
    )

    provincia_filtro = st.selectbox(
        "Provincia",
        opciones_filtros['provincias'],
        index=opciones_filtros['provincias'].index(st.session_state.provincia_filtro)
    )

# Botón para aplicar filtros
if st.sidebar.button("🔄 Aplicar Filtros", use_container_width=True, type="primary"):
    st.session_state.fecha_inicio = fecha_inicio
    st.session_state.fecha_fin = fecha_fin
    st.session_state.categoria_filtro = categoria_filtro
    st.session_state.provincia_filtro = provincia_filtro
    st.cache_data.clear()
    st.rerun()

# Mostrar filtros activos
st.sidebar.markdown("---")
st.sidebar.markdown("### Filtros Activos")
st.sidebar.info(f"""
**Periodo:** {st.session_state.fecha_inicio.strftime('%Y-%m-%d')} a {st.session_state.fecha_fin.strftime('%Y-%m-%d')}
**Categoría:** {st.session_state.categoria_filtro}
**Provincia:** {st.session_state.provincia_filtro}
""")

# Preparar filtros para queries
filtros = {}
if st.session_state.categoria_filtro != 'Todas':
    filtros['categoria'] = st.session_state.categoria_filtro
if st.session_state.provincia_filtro != 'Todas':
    filtros['provincia'] = st.session_state.provincia_filtro

fecha_inicio_str = st.session_state.fecha_inicio.strftime('%Y-%m-%d')
fecha_fin_str = st.session_state.fecha_fin.strftime('%Y-%m-%d')

# ============================================================================
# SECCIÓN 1: KPIs PRINCIPALES (MÉTRICAS DESTACADAS)
# ============================================================================

st.markdown("## 📊 KPIs Principales")

with st.spinner("Cargando KPIs principales..."):
    # Calcular KPIs principales
    ventas_data = kpi_calc.calcular_ventas_totales(fecha_inicio_str, fecha_fin_str, filtros)
    margen_data = kpi_calc.calcular_margen_ganancia(fecha_inicio_str, fecha_fin_str, filtros)
    clientes_data = kpi_calc.calcular_clientes_activos(fecha_inicio_str, fecha_fin_str, filtros)

    # Mostrar métricas en 4 columnas
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "💰 Ventas Totales",
            f"₡{ventas_data['ventas_totales']:,.0f}",
            f"{ventas_data['variacion_porcentaje']:.1f}% vs periodo anterior",
            delta_color="normal"
        )

    with col2:
        st.metric(
            "📈 Margen de Ganancia",
            f"{margen_data['margen_porcentaje']:.1f}%",
            f"₡{margen_data['margen_total']:,.0f}"
        )

    with col3:
        st.metric(
            "🛒 Ticket Promedio",
            f"₡{ventas_data['ticket_promedio']:,.0f}",
            f"{ventas_data['num_transacciones']:,} transacciones"
        )

    with col4:
        st.metric(
            "👥 Clientes Activos",
            f"{clientes_data['clientes_activos']:,}",
            f"{clientes_data['variacion_porcentaje']:.1f}% vs periodo anterior",
            delta_color="normal"
        )

st.markdown("---")

# ============================================================================
# SECCIÓN 2: BALANCED SCORECARD
# ============================================================================

st.markdown("## 🎯 Balanced Scorecard")

# Crear 2x2 grid para las 4 perspectivas
col_left, col_right = st.columns(2)

# ====== PERSPECTIVA FINANCIERA ======
with col_left:
    st.markdown("""
    <div class="bsc-section">
        <h3 style="color: #2c5aa0; margin-top: 0;">💎 Perspectiva Financiera</h3>
        <p style="color: #718096; font-size: 0.9em;">Crecimiento y rentabilidad del negocio</p>
    </div>
    """, unsafe_allow_html=True)

    # Gráfico de crecimiento de ventas
    df_crecimiento = kpi_calc.calcular_crecimiento_ventas('mes')

    if not df_crecimiento.empty:
        # Crear etiqueta de periodo
        if 'MES_CAL' in df_crecimiento.columns:
            df_crecimiento['periodo'] = df_crecimiento['ANIO_CAL'].astype(str) + '-' + df_crecimiento['MES_CAL'].astype(str).str.zfill(2)

        fig_crecimiento = go.Figure()

        fig_crecimiento.add_trace(go.Scatter(
            x=df_crecimiento['periodo'],
            y=df_crecimiento['ventas'],
            mode='lines+markers',
            name='Ventas',
            line=dict(color=COLORES[0], width=3),
            fill='tozeroy',
            fillcolor=f'rgba({int(COLORES[0][1:3], 16)}, {int(COLORES[0][3:5], 16)}, {int(COLORES[0][5:7], 16)}, 0.1)'
        ))

        fig_crecimiento.update_layout(
            title="Tendencia de Ventas Mensuales",
            xaxis_title="Periodo",
            yaxis_title="Ventas (₡)",
            height=300,
            margin=dict(l=20, r=20, t=40, b=20)
        )

        st.plotly_chart(fig_crecimiento, use_container_width=True)

    # Métricas adicionales
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        st.metric("Crecimiento Promedio", f"{df_crecimiento['crecimiento_porcentaje'].mean():.1f}%")
    with col_f2:
        ultimo_crecimiento = df_crecimiento['crecimiento_porcentaje'].iloc[-1] if len(df_crecimiento) > 0 else 0
        st.metric("Último Periodo", f"{ultimo_crecimiento:.1f}%")

# ====== PERSPECTIVA DE CLIENTES ======
with col_right:
    st.markdown("""
    <div class="bsc-section">
        <h3 style="color: #2c5aa0; margin-top: 0;">👥 Perspectiva de Clientes</h3>
        <p style="color: #718096; font-size: 0.9em;">Satisfacción y retención de clientes</p>
    </div>
    """, unsafe_allow_html=True)

    # Métricas de clientes
    retencion_data = kpi_calc.calcular_tasa_retencion(meses=3)
    frecuencia_data = kpi_calc.calcular_frecuencia_compra(fecha_inicio_str, fecha_fin_str)

    # Gráfico de dona: Clientes nuevos vs retenidos
    fig_clientes = go.Figure(data=[go.Pie(
        labels=['Clientes Nuevos', 'Clientes Retenidos'],
        values=[clientes_data['clientes_nuevos'],
                clientes_data['clientes_activos'] - clientes_data['clientes_nuevos']],
        hole=0.5,
        marker_colors=[COLORES[3], COLORES[1]]
    )])

    fig_clientes.update_layout(
        title="Distribución de Clientes",
        height=300,
        margin=dict(l=20, r=20, t=40, b=20),
        showlegend=True
    )

    st.plotly_chart(fig_clientes, use_container_width=True)

    # Métricas adicionales
    col_c1, col_c2 = st.columns(2)
    with col_c1:
        st.metric("Tasa de Retención", f"{retencion_data['tasa_retencion']:.1f}%")
    with col_c2:
        st.metric("Frecuencia Promedio", f"{frecuencia_data['frecuencia_promedio']:.1f} compras")

# ====== PERSPECTIVA DE PROCESOS INTERNOS ======
with col_left:
    st.markdown("""
    <div class="bsc-section">
        <h3 style="color: #2c5aa0; margin-top: 0;">⚙️ Perspectiva de Procesos</h3>
        <p style="color: #718096; font-size: 0.9em;">Eficiencia operativa y conversión</p>
    </div>
    """, unsafe_allow_html=True)

    # Funnel de conversión
    df_funnel = kpi_calc.calcular_funnel_conversion(fecha_inicio_str, fecha_fin_str)

    if not df_funnel.empty:
        fig_funnel = go.Figure(go.Funnel(
            y=df_funnel['etapa'],
            x=df_funnel['cantidad'],
            textposition="inside",
            textinfo="value+percent initial",
            marker=dict(color=COLORES[:len(df_funnel)])
        ))

        fig_funnel.update_layout(
            title="Funnel de Conversión",
            height=350,
            margin=dict(l=20, r=20, t=40, b=20)
        )

        st.plotly_chart(fig_funnel, use_container_width=True)

    # Tasa de conversión
    conversion_data = kpi_calc.calcular_tasa_conversion(fecha_inicio_str, fecha_fin_str)
    st.metric("Tasa de Conversión Global", f"{conversion_data['tasa_conversion']:.2f}%")

# ====== PERSPECTIVA DE PRODUCTOS ======
with col_right:
    st.markdown("""
    <div class="bsc-section">
        <h3 style="color: #2c5aa0; margin-top: 0;">📦 Perspectiva de Productos</h3>
        <p style="color: #718096; font-size: 0.9em;">Performance de productos y categorías</p>
    </div>
    """, unsafe_allow_html=True)

    # Categorías con mayor margen
    df_categorias = kpi_calc.calcular_categorias_mayor_margen(fecha_inicio_str, fecha_fin_str, filtros)

    if not df_categorias.empty:
        fig_categorias = px.bar(
            df_categorias.head(8),
            x='margen_porcentaje',
            y='categoria',
            orientation='h',
            title='Top Categorías por Margen (%)',
            color='margen_porcentaje',
            color_continuous_scale='Blues',
            labels={'margen_porcentaje': 'Margen %', 'categoria': 'Categoría'}
        )

        fig_categorias.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=40, b=20),
            showlegend=False
        )

        st.plotly_chart(fig_categorias, use_container_width=True)

st.markdown("---")

# ============================================================================
# SECCIÓN 3: ANÁLISIS DETALLADO
# ============================================================================

st.markdown("## 📈 Análisis Detallado")

tab1, tab2, tab3, tab4 = st.tabs([
    "🌍 Ventas por Región",
    "🏆 Top Productos",
    "🌐 Comportamiento Web",
    "🔍 Búsquedas vs Ventas"
])

# ====== TAB 1: VENTAS POR REGIÓN ======
with tab1:
    st.markdown("### Distribución Geográfica de Ventas")

    # Query para ventas por provincia
    query_provincias = f"""
        SELECT
            g.provincia,
            SUM(fv.monto_total) AS ventas_totales,
            COUNT(DISTINCT fv.venta_id) AS num_transacciones,
            COUNT(DISTINCT fv.cliente_id) AS num_clientes
        FROM fact_ventas fv
        INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
        INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
            AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
        INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
        WHERE fv.venta_cancelada = 0
          AND t.FECHA_CAL >= '{fecha_inicio_str}'
          AND t.FECHA_CAL <= '{fecha_fin_str}'
          {f"AND p.categoria = '{filtros['categoria']}'" if 'categoria' in filtros else ''}
        GROUP BY g.provincia
        ORDER BY ventas_totales DESC
    """

    df_provincias = pd.read_sql(query_provincias, engine)

    col1, col2 = st.columns(2)

    with col1:
        # Gráfico de barras
        fig_prov_bar = px.bar(
            df_provincias,
            x='ventas_totales',
            y='provincia',
            orientation='h',
            title='Ventas por Provincia',
            color='ventas_totales',
            color_continuous_scale='Viridis',
            labels={'ventas_totales': 'Ventas (₡)', 'provincia': 'Provincia'}
        )
        fig_prov_bar.update_layout(height=400)
        st.plotly_chart(fig_prov_bar, use_container_width=True)

    with col2:
        # Gráfico de pie con contribución porcentual
        df_provincias['porcentaje'] = (df_provincias['ventas_totales'] / df_provincias['ventas_totales'].sum() * 100).round(1)

        fig_prov_pie = px.pie(
            df_provincias,
            values='ventas_totales',
            names='provincia',
            title='Contribución Porcentual por Provincia',
            color_discrete_sequence=COLORES
        )
        fig_prov_pie.update_traces(textposition='inside', textinfo='percent+label')
        fig_prov_pie.update_layout(height=400)
        st.plotly_chart(fig_prov_pie, use_container_width=True)

    # Tabla detallada
    st.markdown("#### Detalle por Provincia")
    df_provincias['ventas_totales'] = df_provincias['ventas_totales'].apply(lambda x: f"₡{x:,.0f}")
    st.dataframe(df_provincias, use_container_width=True, hide_index=True)

# ====== TAB 2: TOP PRODUCTOS ======
with tab2:
    st.markdown("### Top 20 Productos Más Vendidos")

    df_top_productos = kpi_calc.calcular_productos_mas_vendidos(20, fecha_inicio_str, fecha_fin_str, filtros)

    if not df_top_productos.empty:
        col1, col2 = st.columns([2, 1])

        with col1:
            # Gráfico de barras con productos
            fig_productos = px.bar(
                df_top_productos.head(15),
                x='valor_total',
                y='nombre_producto',
                orientation='h',
                title='Top 15 Productos por Valor de Ventas',
                color='categoria',
                labels={'valor_total': 'Ventas (₡)', 'nombre_producto': 'Producto'}
            )
            fig_productos.update_layout(height=600)
            st.plotly_chart(fig_productos, use_container_width=True)

        with col2:
            # Métricas destacadas
            st.metric("Total Productos", len(df_top_productos))
            st.metric("Mejor Producto", df_top_productos.iloc[0]['nombre_producto'][:30] + "...")
            st.metric("Ventas Top Producto", f"₡{df_top_productos.iloc[0]['valor_total']:,.0f}")

            # Distribución por categoría
            cat_dist = df_top_productos.groupby('categoria')['valor_total'].sum().reset_index()
            fig_cat_mini = px.pie(
                cat_dist,
                values='valor_total',
                names='categoria',
                title='Distribución por Categoría',
                color_discrete_sequence=COLORES
            )
            fig_cat_mini.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig_cat_mini, use_container_width=True)

        # Tabla detallada
        st.markdown("#### Ranking Completo")
        st.dataframe(df_top_productos, use_container_width=True, hide_index=True)

# ====== TAB 3: COMPORTAMIENTO WEB ======
with tab3:
    st.markdown("### Análisis de Comportamiento Digital")

    # Métricas por dispositivo
    df_dispositivos = kpi_calc.calcular_metricas_dispositivos(fecha_inicio_str, fecha_fin_str)

    if not df_dispositivos.empty:
        col1, col2, col3 = st.columns(3)

        with col1:
            # Distribución de eventos por dispositivo
            fig_disp_eventos = px.pie(
                df_dispositivos,
                values='total_eventos',
                names='tipo_dispositivo',
                title='Eventos por Tipo de Dispositivo',
                color_discrete_sequence=COLORES
            )
            st.plotly_chart(fig_disp_eventos, use_container_width=True)

        with col2:
            # Tasa de conversión por dispositivo
            fig_disp_conv = px.bar(
                df_dispositivos,
                x='tipo_dispositivo',
                y='tasa_conversion',
                title='Tasa de Conversión por Dispositivo',
                color='tasa_conversion',
                color_continuous_scale='RdYlGn',
                labels={'tasa_conversion': 'Conversión (%)', 'tipo_dispositivo': 'Dispositivo'}
            )
            st.plotly_chart(fig_disp_conv, use_container_width=True)

        with col3:
            # Tiempo promedio por dispositivo
            fig_disp_tiempo = px.bar(
                df_dispositivos,
                x='tipo_dispositivo',
                y='tiempo_promedio_segundos',
                title='Tiempo Promedio en Sitio (seg)',
                color='tiempo_promedio_segundos',
                color_continuous_scale='Blues',
                labels={'tiempo_promedio_segundos': 'Segundos', 'tipo_dispositivo': 'Dispositivo'}
            )
            st.plotly_chart(fig_disp_tiempo, use_container_width=True)

        # Tabla de métricas
        st.markdown("#### Métricas Detalladas por Dispositivo")
        st.dataframe(df_dispositivos, use_container_width=True, hide_index=True)

# ====== TAB 4: BÚSQUEDAS VS VENTAS ======
with tab4:
    st.markdown("### Productos Más Buscados vs Más Vendidos")

    df_busquedas_ventas = kpi_calc.calcular_productos_mas_buscados_vs_vendidos(20, fecha_inicio_str, fecha_fin_str)

    if not df_busquedas_ventas.empty:
        col1, col2 = st.columns(2)

        with col1:
            # Scatter plot: búsquedas vs ventas
            fig_scatter = px.scatter(
                df_busquedas_ventas,
                x='num_busquedas',
                y='num_ventas',
                size='unidades_vendidas',
                color='categoria',
                hover_name='nombre_producto',
                title='Búsquedas vs Ventas (tamaño = unidades)',
                labels={'num_busquedas': 'Número de Búsquedas', 'num_ventas': 'Número de Ventas'}
            )
            fig_scatter.update_layout(height=500)
            st.plotly_chart(fig_scatter, use_container_width=True)

        with col2:
            # Top productos por tasa de conversión de búsqueda
            df_top_conv = df_busquedas_ventas[df_busquedas_ventas['tasa_conversion_busqueda'] > 0].nlargest(10, 'tasa_conversion_busqueda')

            fig_conv = px.bar(
                df_top_conv,
                x='tasa_conversion_busqueda',
                y='nombre_producto',
                orientation='h',
                title='Top 10 Productos por Conversión de Búsqueda',
                color='tasa_conversion_busqueda',
                color_continuous_scale='Greens',
                labels={'tasa_conversion_busqueda': 'Conversión (%)', 'nombre_producto': 'Producto'}
            )
            fig_conv.update_layout(height=500)
            st.plotly_chart(fig_conv, use_container_width=True)

        # Tabla detallada
        st.markdown("#### Análisis Completo")
        st.dataframe(df_busquedas_ventas, use_container_width=True, hide_index=True)

st.markdown("---")

# ============================================================================
# FOOTER
# ============================================================================

col1, col2, col3 = st.columns(3)

with col1:
    st.info(f"""
    **📅 Periodo Analizado**
    {st.session_state.fecha_inicio.strftime('%d/%m/%Y')} - {st.session_state.fecha_fin.strftime('%d/%m/%Y')}
    """)

with col2:
    st.success(f"""
    **✅ Datos Actualizados**
    Última actualización: {datetime.now().strftime('%d/%m/%Y %H:%M')}
    """)

with col3:
    st.warning("""
    **💡 Balanced Scorecard**
    4 Perspectivas Estratégicas
    """)

st.caption("Sistema de Analítica Empresarial - Dashboard Ejecutivo | Balanced Scorecard")
