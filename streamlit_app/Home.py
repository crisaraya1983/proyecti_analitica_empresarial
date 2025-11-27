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
from datetime import datetime

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
    initial_sidebar_state="collapsed"
)

# Inicializar componentes y estilos
inicializar_componentes()

st.title("Ecommerce Cenfotec Analytics")

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

    /* Secciones del BSC */
    .bsc-section {
        background: #f8f9fa;
        padding: 15px;
        border-radius: 8px;
        border-left: 5px solid #2c5aa0;
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
        return DatabaseConnection.get_dw_engine(use_secrets=True)
    except Exception as e:
        st.error(f"Error conectando al DW: {str(e)}")
        st.stop()

# ============================================================================
# INICIALIZACIÓN
# ============================================================================

engine = get_dw_engine()
kpi_calc = KPICalculator(engine)

# ============================================================================
# SECCIÓN 1: KPIs PRINCIPALES (6 MÉTRICAS)
# ============================================================================

st.markdown("## KPIs Principales - 2025 (Hasta Octubre)")

with st.spinner("Cargando KPIs principales..."):
    kpis_2025 = kpi_calc.calcular_kpis_principales_2025(mes_hasta=10)

    # Primera fila: 3 KPIs principales
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "💰 Ventas Totales 2025",
            f"₡{kpis_2025['ventas_totales_2025']:,.0f}",
            f"{kpis_2025['ventas_variacion']:+.1f}% vs 2024",
            delta_color="normal"
        )

    with col2:
        st.metric(
            "📈 Margen de Ganancia 2025",
            f"{kpis_2025['margen_porcentaje_2025']:.1f}%",
            f"{kpis_2025['margen_variacion']:+.1f}% vs 2024",
            delta_color="normal",
            help=f"Margen total: ₡{kpis_2025['margen_total_2025']:,.0f}"
        )

    with col3:
        st.metric(
            "🛒 Ticket Promedio 2025",
            f"₡{kpis_2025['ticket_promedio_2025']:,.0f}",
            f"{kpis_2025['ticket_variacion']:+.1f}% vs 2024",
            delta_color="normal"
        )

    # Segunda fila: 3 KPIs operacionales
    col4, col5, col6 = st.columns(3)

    with col4:
        st.metric(
            "✅ Tasa Ventas Completadas",
            f"{kpis_2025['tasa_completadas_2025']:.1f}%",
            f"{kpis_2025['ventas_completadas_variacion']:+.1f}pp vs 2024",
            delta_color="normal"
        )

    with col5:
        st.metric(
            "❌ Tasa Ventas Canceladas",
            f"{kpis_2025['tasa_canceladas_2025']:.1f}%",
            f"{kpis_2025['ventas_canceladas_variacion']:+.1f}pp vs 2024",
            delta_color="inverse"
        )

    with col6:
        st.metric(
            "📦 Promedio Productos/Venta",
            f"{kpis_2025['promedio_productos_2025']:.1f}",
            f"{kpis_2025['promedio_productos_variacion']:+.1f}% vs 2024",
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

    # Gráfico de margen con línea de meta (39%)
    df_crecimiento = kpi_calc.calcular_crecimiento_ventas('mes')

    if not df_crecimiento.empty:
        # Filtrar 2023-2025 (hasta octubre 2025)
        df_filtrado = df_crecimiento[
            (df_crecimiento['ANIO_CAL'] < 2025) |
            ((df_crecimiento['ANIO_CAL'] == 2025) & (df_crecimiento['MES_CAL'] <= 10))
        ].copy()

        if not df_filtrado.empty:
            # Calcular margen porcentual
            df_filtrado['margen_porcentaje'] = (df_filtrado['margen'] / df_filtrado['ventas'] * 100).fillna(0)
            df_filtrado['periodo'] = df_filtrado['ANIO_CAL'].astype(str) + '-' + df_filtrado['MES_CAL'].astype(str).str.zfill(2)

            fig_margen = go.Figure()

            # Línea de margen real
            fig_margen.add_trace(go.Scatter(
                x=df_filtrado['periodo'],
                y=df_filtrado['margen_porcentaje'],
                mode='lines+markers',
                name='Margen Real',
                line=dict(color=COLORES[0], width=3),
                marker=dict(size=6)
            ))

            # Línea de meta al 39%
            fig_margen.add_trace(go.Scatter(
                x=df_filtrado['periodo'],
                y=[39] * len(df_filtrado),
                mode='lines',
                name='Meta (39%)',
                line=dict(color='red', width=2, dash='dash')
            ))

            fig_margen.update_layout(
                title='Margen de Ganancia (%) - Meta: 39%',
                xaxis_title='Periodo',
                yaxis_title='Margen (%)',
                height=300,
                margin=dict(l=20, r=20, t=40, b=20),
                hovermode='x unified'
            )

            st.plotly_chart(fig_margen, use_container_width=True)

    # Ventas por Categoría a través del Tiempo (Stacked Bar 100%)
    df_ventas_categoria = kpi_calc.calcular_ventas_por_categoria_tiempo()

    if not df_ventas_categoria.empty:
        # Pivotar datos para gráfico de barras apiladas
        df_pivot = df_ventas_categoria.pivot(index='periodo', columns='categoria', values='ventas').fillna(0)

        # Crear gráfico de barras apiladas al 100%
        fig_ventas_cat = go.Figure()

        for categoria in df_pivot.columns:
            fig_ventas_cat.add_trace(go.Bar(
                name=categoria,
                x=df_pivot.index,
                y=df_pivot[categoria],
                hovertemplate='<b>%{fullData.name}</b><br>' +
                             'Periodo: %{x}<br>' +
                             'Ventas: ₡%{y:,.0f}<br>' +
                             '<extra></extra>'
            ))

        fig_ventas_cat.update_layout(
            title='Distribución de Ventas por Categoría (2023 - Oct 2025)',
            xaxis_title='Periodo',
            yaxis_title='Porcentaje de Ventas (%)',
            barmode='stack',
            barnorm='percent',  # Normaliza al 100%
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.02,
                font=dict(size=9)
            ),
            hovermode='x unified'
        )

        st.plotly_chart(fig_ventas_cat, use_container_width=True)

    # Métricas adicionales debajo del gráfico stack
    col_m1, col_m2 = st.columns(2)

    with col_m1:
        # Producto más vendido
        producto_top = kpi_calc.calcular_producto_mas_vendido()
        if producto_top and producto_top.get('producto_nombre') != 'N/A':
            st.metric(
                "🏆 Producto Más Vendido",
                producto_top['producto_nombre'][:25] + "...",
                f"{producto_top['cantidad_vendida']:,} unidades"
            )

    with col_m2:
        # Producto con mayor margen
        producto_margen = kpi_calc.calcular_producto_mayor_margen()
        if producto_margen and producto_margen.get('producto_nombre') != 'N/A':
            st.metric(
                "💎 Mayor Margen Total",
                producto_margen['producto_nombre'][:25] + "...",
                f"₡{producto_margen['margen_total']:,.0f}"
            )

# ====== PERSPECTIVA DE PRODUCTOS ======
with col_right:
    st.markdown("""
    <div class="bsc-section">
        <h3 style="color: #2c5aa0; margin-top: 0;">📦 Perspectiva de Productos</h3>
        <p style="color: #718096; font-size: 0.9em;">Performance de productos y categorías</p>
    </div>
    """, unsafe_allow_html=True)

    # Categorías con mayor margen
    df_categorias_margen = kpi_calc.calcular_categorias_mayor_margen()

    if not df_categorias_margen.empty:
        # Tomar solo top 8 categorías
        df_top_categorias = df_categorias_margen.head(8)

        fig_categorias = px.bar(
            df_top_categorias,
            x='margen_porcentaje',
            y='categoria',
            orientation='h',
            title='Top 8 Categorías por Margen (%)',
            color='margen_porcentaje',
            color_continuous_scale='Blues',
            labels={'margen_porcentaje': 'Margen %', 'categoria': 'Categoría'}
        )

        fig_categorias.update_layout(
            height=600,
            margin=dict(l=20, r=20, t=40, b=20),
            showlegend=False
        )

        st.plotly_chart(fig_categorias, use_container_width=True)

# Divider entre las dos filas del Balanced Scorecard
st.markdown("---")

# ====== PERSPECTIVA DE CLIENTES ======
with col_left:
    st.markdown("""
    <div class="bsc-section">
        <h3 style="color: #2c5aa0; margin-top: 0;">👥 Perspectiva de Clientes</h3>
        <p style="color: #718096; font-size: 0.9em;">Satisfacción y retención de clientes</p>
    </div>
    """, unsafe_allow_html=True)

    # Clientes activos por mes (2025)
    df_clientes_mes = kpi_calc.calcular_clientes_activos_por_mes()

    if not df_clientes_mes.empty:
        # Filtrar solo 2025 hasta octubre
        df_clientes_2025 = df_clientes_mes[
            (df_clientes_mes['ANIO_CAL'] == 2025) & (df_clientes_mes['MES_CAL'] <= 10)
        ].copy()

        if not df_clientes_2025.empty:
            df_clientes_2025['periodo'] = df_clientes_2025['MES_CAL'].astype(str).str.zfill(2)

            fig_clientes = go.Figure()

            fig_clientes.add_trace(go.Bar(
                x=df_clientes_2025['periodo'],
                y=df_clientes_2025['clientes_activos'],
                name='Clientes Activos',
                marker_color=COLORES[1],
                hovertemplate='Mes: %{x}<br>Clientes: %{y:,}<extra></extra>'
            ))

            fig_clientes.update_layout(
                title='Clientes Activos por Mes - 2025',
                xaxis_title='Mes',
                yaxis_title='Cantidad de Clientes',
                height=250,
                margin=dict(l=20, r=20, t=40, b=20)
            )

            st.plotly_chart(fig_clientes, use_container_width=True)

    # Clientes por provincia
    df_clientes_prov = kpi_calc.calcular_clientes_por_provincia()

    if not df_clientes_prov.empty:
        fig_clientes_prov = px.bar(
            df_clientes_prov,
            x='num_clientes',
            y='provincia',
            orientation='h',
            title='Cantidad de Clientes por Provincia',
            color='num_clientes',
            color_continuous_scale='Greens',
            labels={'num_clientes': 'Cantidad de Clientes', 'provincia': 'Provincia'}
        )

        fig_clientes_prov.update_layout(
            height=250,
            margin=dict(l=20, r=20, t=40, b=20),
            showlegend=False
        )

        st.plotly_chart(fig_clientes_prov, use_container_width=True)

# ====== PERSPECTIVA GEOGRÁFICA ======
with col_right:
    st.markdown("""
    <div class="bsc-section">
        <h3 style="color: #2c5aa0; margin-top: 0;">🌍 Perspectiva Geográfica</h3>
        <p style="color: #718096; font-size: 0.9em;">Análisis de ventas por ubicación</p>
    </div>
    """, unsafe_allow_html=True)

    # Ventas por provincia (treemap)
    df_provincias = kpi_calc.calcular_ventas_por_provincia()

    if not df_provincias.empty:
        # Escala de colores oscuros mejorada para legibilidad
        color_scale = [
            [0.0, 'rgb(158, 202, 225)'],  # Azul claro para mínimo
            [0.3, 'rgb(107, 174, 214)'],  # Azul medio-claro
            [0.6, 'rgb(66, 146, 198)'],   # Azul medio
            [0.8, 'rgb(33, 113, 181)'],   # Azul medio-oscuro
            [1.0, 'rgb(8, 69, 148)']      # Azul oscuro para máximo
        ]

        fig_treemap = px.treemap(
            df_provincias,
            path=['provincia'],
            values='num_ventas',
            color='num_ventas',
            color_continuous_scale=color_scale,
            title='Distribución de Ventas por Provincia',
            custom_data=['num_ventas']
        )

        fig_treemap.update_traces(
            textfont=dict(size=14, color='white', family='Arial Black'),
            marker=dict(line=dict(width=2, color='white')),
            hovertemplate='<b>%{label}</b><br>Ventas: %{customdata[0]:,}<extra></extra>'
        )

        fig_treemap.update_layout(
            height=250,
            margin=dict(l=5, r=5, t=40, b=5)
        )

        st.plotly_chart(fig_treemap, use_container_width=True)

    # Ventas por almacén
    df_almacenes = kpi_calc.calcular_ventas_por_almacen()

    if not df_almacenes.empty:
        fig_almacenes = px.bar(
            df_almacenes,
            x='nombre_almacen',
            y='num_ventas',
            title='Órdenes por Almacén (Bodega)',
            color='num_ventas',
            color_continuous_scale='Blues',
            labels={'num_ventas': 'Cantidad de Órdenes', 'nombre_almacen': 'Almacén'}
        )

        fig_almacenes.update_layout(
            height=250,
            margin=dict(l=20, r=20, t=40, b=20),
            showlegend=False
        )

        st.plotly_chart(fig_almacenes, use_container_width=True)

    # Métricas geográficas
    col_g1, col_g2 = st.columns(2)

    with col_g1:
        canton_top = kpi_calc.calcular_canton_top()
        if canton_top and canton_top.get('canton') != 'N/A':
            st.metric(
                "🏙️ Cantón Top",
                canton_top['canton'][:20],
                f"{canton_top['num_ventas']:,} ventas"
            )

    with col_g2:
        distrito_top = kpi_calc.calcular_distrito_top()
        if distrito_top and distrito_top.get('distrito') != 'N/A':
            st.metric(
                "📍 Distrito Top",
                distrito_top['distrito'][:20],
                f"{distrito_top['num_ventas']:,} ventas"
            )

# Divider final antes de Comportamiento Web
st.markdown("---")

# ====== PERSPECTIVA DE COMPORTAMIENTO WEB (ocupando ancho completo) ======
st.markdown("""
<div class="bsc-section">
    <h3 style="color: #2c5aa0; margin-top: 0;">🌐 Perspectiva de Comportamiento Web</h3>
    <p style="color: #718096; font-size: 0.9em;">Análisis de sesiones y conversión digital</p>
</div>
""", unsafe_allow_html=True)

col_web_left, col_web_right = st.columns([2, 1])

with col_web_left:
    # Funnel de comportamiento web (agrupado por sesión)
    df_funnel_web = kpi_calc.calcular_funnel_comportamiento_web()

    if not df_funnel_web.empty:
        fig_funnel_web = go.Figure(go.Funnel(
            y=df_funnel_web['etapa'],
            x=df_funnel_web['cantidad'],
            textposition="inside",
            textinfo="value+percent initial",
            marker=dict(
                color=['rgb(33, 113, 181)', 'rgb(66, 146, 198)', 'rgb(107, 174, 214)',
                       'rgb(158, 202, 225)', 'rgb(189, 215, 231)', 'rgb(8, 81, 156)']
            )
        ))

        fig_funnel_web.update_layout(
            title="Funnel de Comportamiento Web (Agrupado por Sesión)",
            height=400,
            margin=dict(l=20, r=20, t=40, b=20)
        )

        st.plotly_chart(fig_funnel_web, use_container_width=True)

with col_web_right:
    # Métricas de comportamiento web
    metricas_web = kpi_calc.calcular_metricas_comportamiento_web()

    st.metric(
        "📊 Tasa de Conversión",
        f"{metricas_web['tasa_conversion']}%",
        help="Porcentaje de sesiones que generaron venta"
    )
    st.metric(
        "👥 Sesiones Únicas",
        f"{metricas_web['usuarios_unicos']:,}",
        help="Cantidad total de sesiones distintas"
    )
    st.metric(
        "🌐 Navegadores Diferentes",
        f"{metricas_web['navegadores_diferentes']}",
        help="Cantidad de navegadores distintos utilizados"
    )

st.markdown("---")
st.caption("Sistema de Analítica Empresarial - Dashboard Ejecutivo | Balanced Scorecard")
