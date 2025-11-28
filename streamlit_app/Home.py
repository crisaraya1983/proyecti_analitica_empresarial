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

    # Gráfico de ventas mensuales 2023-2025 (línea con puntos)
    query_ventas_mensual = """
        SELECT
            t.ANIO_CAL,
            t.MES_CAL,
            SUM(MontoFactura) AS ventas_totales
        FROM (
            SELECT
                venta_id,
                SUM(monto_total) AS MontoFactura,
                tiempo_key
            FROM fact_ventas
            WHERE venta_cancelada = 0
            GROUP BY venta_id, tiempo_key
        ) AS Facturas
        INNER JOIN dim_tiempo t ON Facturas.tiempo_key = t.ID_FECHA
        WHERE (t.ANIO_CAL < 2025 OR (t.ANIO_CAL = 2025 AND t.MES_CAL <= 10))
        GROUP BY t.ANIO_CAL, t.MES_CAL
        ORDER BY t.ANIO_CAL, t.MES_CAL
    """
    df_ventas_mensual = pd.read_sql(query_ventas_mensual, engine)

    if not df_ventas_mensual.empty:
        df_ventas_mensual['periodo'] = df_ventas_mensual['ANIO_CAL'].astype(str) + '-' + df_ventas_mensual['MES_CAL'].astype(str).str.zfill(2)

        fig_ventas_mensual = go.Figure()

        fig_ventas_mensual.add_trace(go.Scatter(
            x=df_ventas_mensual['periodo'],
            y=df_ventas_mensual['ventas_totales'],
            mode='lines+markers',
            line=dict(color=COLORES[0], width=3),
            marker=dict(size=6, color=COLORES[0]),
            hovertemplate='Periodo: %{x}<br>Ventas: ₡%{y:,.0f}<extra></extra>'
        ))

        fig_ventas_mensual.update_layout(
            title='Evolución de Ventas Mensuales (2023 - Oct 2025)',
            xaxis_title='Periodo',
            yaxis_title='Ventas (₡)',
            height=250,
            margin=dict(l=20, r=20, t=40, b=20)
        )

        st.plotly_chart(fig_ventas_mensual, use_container_width=True)

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
                height=250,
                margin=dict(l=20, r=20, t=40, b=20),
                hovermode='x unified'
            )

            st.plotly_chart(fig_margen, use_container_width=True)

    # Métricas financieras
    col_f1, col_f2 = st.columns(2)

    with col_f1:
        # Total ganancias (margen total agrupado por venta_id)
        query_ganancias = """
            SELECT
                SUM(MargenFactura) AS ganancia_total
            FROM (
                SELECT
                    venta_id,
                    SUM(margen) AS MargenFactura
                FROM fact_ventas
                WHERE venta_cancelada = 0
                GROUP BY venta_id
            ) AS Facturas
        """
        df_ganancias = pd.read_sql(query_ganancias, engine)
        ganancia_total = df_ganancias['ganancia_total'].iloc[0] if not df_ganancias.empty else 0

        st.metric(
            "💰 Total Ganancias",
            f"₡{ganancia_total:,.0f}",
            help="Suma total del margen de todas las ventas"
        )

    with col_f2:
        # Promedio de margen vs meta
        if not df_filtrado.empty:
            margen_promedio = df_filtrado['margen_porcentaje'].mean()
            diferencia_meta = margen_promedio - 39
            st.metric(
                "🎯 Promedio Margen vs Meta",
                f"{margen_promedio:.1f}%",
                f"{diferencia_meta:+.1f}pp vs 39%",
                delta_color="normal" if diferencia_meta >= 0 else "inverse"
            )

# ====== PERSPECTIVA DE PRODUCTOS ======
with col_right:
    st.markdown("""
    <div class="bsc-section">
        <h3 style="color: #2c5aa0; margin-top: 0;">📦 Perspectiva de Productos</h3>
        <p style="color: #718096; font-size: 0.9em;">Performance de productos y categorías</p>
    </div>
    """, unsafe_allow_html=True)

    # Top 10 productos con mayor margen
    df_categorias_margen = kpi_calc.calcular_categorias_mayor_margen()

    if not df_categorias_margen.empty:
        # Tomar top 10 categorías por margen
        df_top_10_margen = df_categorias_margen.head(10)

        # Crear escala de colores degradados (más oscuro = mejor margen)
        # Invertir el orden para que el primero tenga el color más oscuro
        color_values = list(range(len(df_top_10_margen), 0, -1))

        fig_margen_productos = go.Figure()

        fig_margen_productos.add_trace(go.Bar(
            x=df_top_10_margen['margen_porcentaje'],
            y=df_top_10_margen['categoria'],
            orientation='h',
            marker=dict(
                color=color_values,
                colorscale='Blues',
                showscale=False
            ),
            hovertemplate='<b>%{y}</b><br>Margen: %{x:.2f}%<extra></extra>'
        ))

        fig_margen_productos.update_layout(
            title='Top 10 Categorías con Mayor Margen (%)',
            xaxis_title='Margen (%)',
            yaxis_title='Categoría',
            height=250,
            margin=dict(l=20, r=20, t=40, b=20),
            yaxis={'categoryorder': 'total ascending'}  # Ordenar con mejor margen arriba
        )

        st.plotly_chart(fig_margen_productos, use_container_width=True)

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
                             'Ventas: %%{y:,.0f}<br>' +
                             '<extra></extra>'
            ))

        fig_ventas_cat.update_layout(
            title='Distribución de Ventas por Categoría (2023 - Oct 2025)',
            xaxis_title='Periodo',
            yaxis_title='Porcentaje de Ventas (%)',
            barmode='stack',
            barnorm='percent',  # Normaliza al 100%
            height=250,
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

    # Métricas de productos
    col_p1, col_p2 = st.columns(2)

    with col_p1:
        # Producto más vendido
        producto_top = kpi_calc.calcular_producto_mas_vendido()
        if producto_top and producto_top.get('producto_nombre') != 'N/A':
            st.metric(
                "🏆 Producto Más Vendido",
                producto_top['producto_nombre'][:25] + "...",
                f"{producto_top['cantidad_vendida']:,} unidades"
            )

    with col_p2:
        # Producto con mayor margen
        producto_margen = kpi_calc.calcular_producto_mayor_margen()
        if producto_margen and producto_margen.get('producto_nombre') != 'N/A':
            st.metric(
                "💎 Mayor Margen Total",
                producto_margen['producto_nombre'][:25] + "...",
                f"₡{producto_margen['margen_total']:,.0f}"
            )

# Divider entre Financiera/Productos y Clientes/Geográfica
st.markdown("---")

# Segunda fila del Balanced Scorecard
col_left2, col_right2 = st.columns(2)

# ====== PERSPECTIVA DE CLIENTES ======
with col_left2:
    st.markdown("""
    <div class="bsc-section">
        <h3 style="color: #2c5aa0; margin-top: 0;">👥 Perspectiva de Clientes</h3>
        <p style="color: #718096; font-size: 0.9em;">Satisfacción y retención de clientes</p>
    </div>
    """, unsafe_allow_html=True)

    # Clientes activos por mes (2023-2025)
    df_clientes_mes = kpi_calc.calcular_clientes_activos_por_mes()

    if not df_clientes_mes.empty:
        # Filtrar 2023-2025 hasta octubre
        df_clientes_filtrado = df_clientes_mes[
            (df_clientes_mes['ANIO_CAL'] < 2025) |
            ((df_clientes_mes['ANIO_CAL'] == 2025) & (df_clientes_mes['MES_CAL'] <= 10))
        ].copy()

        if not df_clientes_filtrado.empty:
            df_clientes_filtrado['periodo'] = df_clientes_filtrado['ANIO_CAL'].astype(str) + '-' + df_clientes_filtrado['MES_CAL'].astype(str).str.zfill(2)

            fig_clientes = go.Figure()

            fig_clientes.add_trace(go.Scatter(
                x=df_clientes_filtrado['periodo'],
                y=df_clientes_filtrado['clientes_activos'],
                mode='lines+markers',
                name='Clientes Activos',
                line=dict(color=COLORES[1], width=3),
                marker=dict(size=6, color=COLORES[1]),
                hovertemplate='Periodo: %{x}<br>Clientes: %{y:,}<extra></extra>'
            ))

            fig_clientes.update_layout(
                title='Evolución de Clientes Activos (2023 - Oct 2025)',
                xaxis_title='Periodo',
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

    # Métricas de clientes
    col_c1, col_c2 = st.columns(2)

    with col_c1:
        # Días promedio entre compras
        dias_promedio = kpi_calc.calcular_dias_promedio_entre_compras()
        if dias_promedio:
            st.metric(
                "📅 Días entre Compras",
                f"{dias_promedio['dias_promedio']:.0f} días",
                help="Promedio de días entre compras de los clientes"
            )

    with col_c2:
        # Promedio de compras por cliente
        query_promedio_compras = """
            SELECT
                AVG(CAST(num_compras AS FLOAT)) AS promedio_compras_cliente
            FROM (
                SELECT
                    cliente_id,
                    COUNT(DISTINCT venta_id) AS num_compras
                FROM fact_ventas
                WHERE venta_cancelada = 0
                GROUP BY cliente_id
            ) AS ComprasPorCliente
        """
        df_promedio_compras = pd.read_sql(query_promedio_compras, engine)
        promedio_compras = df_promedio_compras['promedio_compras_cliente'].iloc[0] if not df_promedio_compras.empty else 0

        st.metric(
            "🛒 Promedio Compras/Cliente",
            f"{promedio_compras:.1f}",
            help="Promedio de compras realizadas por cada cliente"
        )

# ====== PERSPECTIVA GEOGRÁFICA ======
with col_right2:
    st.markdown("""
    <div class="bsc-section">
        <h3 style="color: #2c5aa0; margin-top: 0;">🌍 Perspectiva Geográfica</h3>
        <p style="color: #718096; font-size: 0.9em;">Análisis de ventas por ubicación</p>
    </div>
    """, unsafe_allow_html=True)

    # Ventas por provincia (treemap con montos)
    query_provincias_monto = """
        SELECT
            g.provincia,
            COUNT(DISTINCT Facturas.venta_id) AS num_ventas,
            SUM(Facturas.MontoFactura) AS monto_total
        FROM (
            SELECT
                venta_id,
                provincia_id,
                canton_id,
                distrito_id,
                SUM(monto_total) AS MontoFactura
            FROM fact_ventas
            WHERE venta_cancelada = 0
            GROUP BY venta_id, provincia_id, canton_id, distrito_id
        ) AS Facturas
        INNER JOIN dim_geografia g ON Facturas.provincia_id = g.provincia_id
            AND Facturas.canton_id = g.canton_id
            AND Facturas.distrito_id = g.distrito_id
        GROUP BY g.provincia
        ORDER BY num_ventas DESC
    """
    df_provincias_monto = pd.read_sql(query_provincias_monto, engine)

    if not df_provincias_monto.empty:
        # Crear labels con provincia y monto
        df_provincias_monto['label_texto'] = df_provincias_monto.apply(
            lambda row: f"{row['provincia']}<br>₡{row['monto_total']:,.0f}",
            axis=1
        )

        # Escala de colores oscuros mejorada para legibilidad
        color_scale = [
            [0.0, 'rgb(158, 202, 225)'],  # Azul claro para mínimo
            [0.3, 'rgb(107, 174, 214)'],  # Azul medio-claro
            [0.6, 'rgb(66, 146, 198)'],   # Azul medio
            [0.8, 'rgb(33, 113, 181)'],   # Azul medio-oscuro
            [1.0, 'rgb(8, 69, 148)']      # Azul oscuro para máximo
        ]

        fig_treemap = px.treemap(
            df_provincias_monto,
            path=['provincia'],
            values='num_ventas',
            color='num_ventas',
            color_continuous_scale=color_scale,
            title='Distribución de Ventas por Provincia',
            custom_data=['num_ventas', 'monto_total']
        )

        fig_treemap.update_traces(
            textfont=dict(size=12, color='white', family='Arial Black'),
            marker=dict(line=dict(width=2, color='white')),
            texttemplate='<b>%{label}</b><br>₡%{customdata[1]:,.0f}',
            hovertemplate='<b>%{label}</b><br>Cantidad: %{customdata[0]:,}<br>Monto: ₡%{customdata[1]:,.0f}<extra></extra>'
        )

        fig_treemap.update_layout(
            height=250,
            margin=dict(l=5, r=5, t=40, b=5)
        )

        st.plotly_chart(fig_treemap, use_container_width=True)

    # Ventas por almacén (barras verticales)
    df_almacenes = kpi_calc.calcular_ventas_por_almacen()

    if not df_almacenes.empty:
        # Ordenar por cantidad descendente
        df_almacenes_sorted = df_almacenes.sort_values('num_ventas', ascending=False)

        fig_almacenes = go.Figure()

        fig_almacenes.add_trace(go.Bar(
            x=df_almacenes_sorted['nombre_almacen'],
            y=df_almacenes_sorted['num_ventas'],
            marker_color=COLORES[0],
            text=df_almacenes_sorted['num_ventas'],
            texttemplate='%{text:,}',
            textposition='outside',
            hovertemplate='<b>%{x}</b><br>Órdenes: %{y:,}<extra></extra>'
        ))

        fig_almacenes.update_layout(
            title='Órdenes por Almacén (Bodega)',
            xaxis_title='Almacén',
            yaxis_title='Cantidad de Órdenes',
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
