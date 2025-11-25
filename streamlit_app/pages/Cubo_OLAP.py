"""
================================================================================
PÁGINA STREAMLIT: CUBO OLAP - EXPLORACIÓN MULTIDIMENSIONAL
================================================================================
Autor: Sistema de Analítica Empresarial
Propósito: Interfaz para explorar el cubo OLAP con operaciones multidimensionales
================================================================================
"""

import streamlit as st
import sys
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Configurar paths
project_root = os.path.dirname(os.path.dirname(__file__))
for path in [os.path.join(project_root, 'utils'), os.path.join(project_root, 'OLAP'), os.path.join(project_root, 'modulos')]:
    if path not in sys.path:
        sys.path.insert(0, path)

from utils.db_connection import DatabaseConnection
from OLAP.cubo_olap import CuboOLAP
from modulos.componentes import inicializar_componentes, crear_seccion_encabezado

# Configuración de página
st.set_page_config(
    page_title="Cubo OLAP",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Inicializar componentes y estilos
inicializar_componentes()

# Título usando componente
crear_seccion_encabezado(
    "Cubo OLAP - Análisis Multidimensional",
    #"Exploración interactiva del Data Warehouse con operaciones OLAP",
    #badge="DW",
    badge_color="primary"
)

# ============================================================================
# FUNCIONES CON CACHÉ
# ============================================================================

@st.cache_resource
def get_olap_cube():
    """Obtiene instancia del cubo OLAP (cached)"""
    try:
        conn = DatabaseConnection.get_dw_connection(use_secrets=True)
        return CuboOLAP(conn)
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

@st.cache_data(ttl=600)
def get_funnel_conversion(_cubo):
    """Obtiene funnel de conversión (cached 10min)"""
    return _cubo.get_funnel_conversion()

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
def ejecutar_pivot(_cubo, rows, columns, values):
    """Ejecuta operación PIVOT (cached 5min)"""
    return _cubo.pivot(rows, columns, values)

# ============================================================================
# SIDEBAR
# ============================================================================

with st.sidebar:
    st.header("Configuración")

    if st.button("Probar Conexión DW", use_container_width=True):
        with st.spinner("Probando conexión..."):
            try:
                result = DatabaseConnection.test_connection("Ecommerce_DW", use_secrets=True)
                if result["success"]:
                    st.success("Conexión exitosa")
                else:
                    st.error(f"Error: {result['error']}")
            except Exception as e:
                st.error(f"Error: {str(e)}")

    st.markdown("---")

    st.subheader("Operaciones OLAP")
    st.markdown("""
    **SLICE**: Filtro por una dimensión
    **DICE**: Filtros múltiples
    **DRILL-DOWN**: Mayor detalle
    **ROLL-UP**: Mayor agregación
    **PIVOT**: Rotar dimensiones
    """)

    st.markdown("---")
    st.caption(f"Última actualización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Obtener cubo OLAP
cubo = get_olap_cube()

# ============================================================================
# TABS PRINCIPALES
# ============================================================================

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Resumen Ejecutivo",
    "SLICE",
    "DICE",
    "Análisis Dimensional",
    "PIVOT",
    "Comportamiento Web"
])

# ============================================================================
# TAB 1: RESUMEN EJECUTIVO
# ============================================================================

with tab1:
    crear_seccion_encabezado(
        "Resumen Ejecutivo del Negocio",
        "KPIs principales y métricas de desempeño",
        #badge="RESUMEN"
    )

    try:
        with st.spinner("Cargando datos..."):
            resumen = get_resumen_negocio(cubo)

            if resumen:
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric(
                        "Total Ventas",
                        f"₡{resumen.get('total_ventas', 0):,.2f}",
                        delta=f"{resumen.get('total_transacciones', 0):,} transacciones"
                    )

                with col2:
                    st.metric(
                        "Ganancia Total",
                        f"₡{resumen.get('total_ganancia', 0):,.2f}",
                        delta=f"{resumen.get('margen_porcentaje', 0):.2f}% margen"
                    )

                with col3:
                    st.metric(
                        "Clientes Únicos",
                        f"{resumen.get('clientes_unicos', 0):,}",
                        delta=f"{resumen.get('productos_vendidos', 0):,} productos"
                    )

                with col4:
                    st.metric(
                        "Promedio por Venta",
                        f"₡{resumen.get('promedio_venta', 0):,.2f}",
                        delta=f"₡{resumen.get('venta_maxima', 0):,.2f} máx"
                    )

                st.markdown("---")

                col1, col2 = st.columns(2)

                with col1:
                    st.info(f"""
                    **Estadísticas de Ventas**
                    Total Transacciones: {resumen.get('total_transacciones', 0):,}
                    Total Unidades: {resumen.get('total_unidades', 0):,}
                    Venta Mínima: ₡{resumen.get('venta_minima', 0):,.2f}
                    Venta Máxima: ₡{resumen.get('venta_maxima', 0):,.2f}
                    """)

                with col2:
                    st.success(f"""
                    **Rentabilidad**
                    Total Margen: ₡{resumen.get('total_margen', 0):,.2f}
                    Margen Porcentaje: {resumen.get('margen_porcentaje', 0):.2f}%
                    Almacenes Activos: {resumen.get('almacenes_activos', 0)}
                    """)

                st.markdown("---")
                st.subheader("Tabla de Resumen Completo")
                df_resumen = pd.DataFrame([resumen])
                st.dataframe(df_resumen, use_container_width=True)

            else:
                st.warning("No hay datos disponibles")

    except Exception as e:
        st.error(f"Error cargando resumen: {str(e)}")

# ============================================================================
# TAB 2: SLICE
# ============================================================================

with tab2:
    crear_seccion_encabezado(
        "Operación SLICE",
        "Filtrar datos por una dimensión específica",
        #badge="OLAP"
    )

    try:
        col1, col2 = st.columns(2)

        with col1:
            dimension = st.selectbox(
                "Selecciona la dimensión",
                ["provincia", "categoria", "almacen", "anio"],
                key="slice_dim"
            )

        with col2:
            # Obtener valores únicos
            if dimension == "provincia":
                query = "SELECT DISTINCT provincia FROM dim_geografia ORDER BY provincia"
            elif dimension == "categoria":
                query = "SELECT DISTINCT categoria FROM dim_producto ORDER BY categoria"
            elif dimension == "almacen":
                query = "SELECT DISTINCT nombre_almacen FROM dim_almacen ORDER BY nombre_almacen"
            elif dimension == "anio":
                query = "SELECT DISTINCT ANIO_CAL FROM dim_tiempo ORDER BY ANIO_CAL DESC"

            cursor = cubo.conn.cursor()
            cursor.execute(query)
            valores = [str(row[0]) for row in cursor.fetchall()]
            cursor.close()

            valor_seleccionado = st.selectbox("Selecciona el valor", valores, key="slice_val")

        if st.button("Ejecutar SLICE", use_container_width=True):
            with st.spinner(f"Ejecutando SLICE: {dimension} = {valor_seleccionado}..."):
                try:
                    df = ejecutar_slice(cubo, dimension, valor_seleccionado)

                    if not df.empty:
                        st.success("SLICE ejecutado exitosamente")
                        st.dataframe(df, use_container_width=True)

                        if 'total_ventas' in df.columns and 'dimension_value' in df.columns:
                            fig = px.bar(
                                df,
                                x='dimension_value',
                                y='total_ventas',
                                title=f'Ventas por {dimension}',
                                labels={'total_ventas': 'Total Ventas (₡)'},
                                color='margen_porcentaje'
                            )
                            st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("No hay datos para este filtro")

                except Exception as e:
                    st.error(f"Error: {str(e)}")

    except Exception as e:
        st.error(f"Error: {str(e)}")

# ============================================================================
# TAB 3: DICE
# ============================================================================

with tab3:
    crear_seccion_encabezado(
        "Operación DICE",
        "Aplicar filtros en múltiples dimensiones simultáneamente",
        #badge="OLAP"
    )

    try:
        st.subheader("Selecciona los filtros")

        col1, col2, col3 = st.columns(3)

        filters = {}

        with col1:
            query = "SELECT DISTINCT provincia FROM dim_geografia ORDER BY provincia"
            cursor = cubo.conn.cursor()
            cursor.execute(query)
            provincias = ['TODAS'] + [row[0] for row in cursor.fetchall()]
            cursor.close()

            prov_sel = st.selectbox("Provincia", provincias, key="dice_prov")
            if prov_sel != 'TODAS':
                filters['provincia'] = prov_sel

        with col2:
            query = "SELECT DISTINCT categoria FROM dim_producto ORDER BY categoria"
            cursor = cubo.conn.cursor()
            cursor.execute(query)
            categorias = ['TODAS'] + [row[0] for row in cursor.fetchall()]
            cursor.close()

            cat_sel = st.selectbox("Categoría", categorias, key="dice_cat")
            if cat_sel != 'TODAS':
                filters['categoria'] = cat_sel

        with col3:
            query = "SELECT DISTINCT ANIO_CAL FROM dim_tiempo ORDER BY ANIO_CAL DESC"
            cursor = cubo.conn.cursor()
            cursor.execute(query)
            anios = ['TODOS'] + [str(row[0]) for row in cursor.fetchall()]
            cursor.close()

            anio_sel = st.selectbox("Año", anios, key="dice_anio")
            if anio_sel != 'TODOS':
                filters['anio'] = int(anio_sel)

        col1, col2 = st.columns(2)

        with col1:
            query = "SELECT DISTINCT MES_CAL, MES_NOMBRE FROM dim_tiempo ORDER BY MES_CAL"
            cursor = cubo.conn.cursor()
            cursor.execute(query)
            meses_data = cursor.fetchall()
            cursor.close()
            meses = ['TODOS'] + [f"{row[1]} ({row[0]})" for row in meses_data]

            mes_sel = st.selectbox("Mes", meses, key="dice_mes")
            if mes_sel != 'TODOS':
                mes_num = int(mes_sel.split('(')[1].split(')')[0])
                filters['mes'] = mes_num

        if st.button("Ejecutar DICE", use_container_width=True):
            with st.spinner("Ejecutando DICE..."):
                try:
                    # Convertir dict a tuple para caché
                    filters_tuple = tuple(sorted(filters.items()))
                    df = ejecutar_dice(cubo, filters_tuple)

                    if not df.empty:
                        st.success(f"DICE ejecutado con {len(filters)} filtros")
                        st.dataframe(df, use_container_width=True)

                        if 'mes_nombre' in df.columns and 'total_ventas' in df.columns:
                            fig = px.line(
                                df,
                                x='mes_nombre',
                                y='total_ventas',
                                title='Evolución de Ventas',
                                markers=True
                            )
                            st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("No hay datos para estos filtros")

                except Exception as e:
                    st.error(f"Error: {str(e)}")

    except Exception as e:
        st.error(f"Error: {str(e)}")

# ============================================================================
# TAB 4: ANÁLISIS DIMENSIONAL
# ============================================================================

with tab4:
    crear_seccion_encabezado(
        "Análisis Dimensional",
        "DRILL-DOWN: aumentar detalle | ROLL-UP: aumentar agregación",
        #badge="DRILL"
    )

    subtab1, subtab2, subtab3, subtab4 = st.tabs([
        "Ventas por Tiempo",
        "Ventas por Geografía",
        "Ventas por Categoría",
        "Top N"
    ])

    with subtab1:
        st.subheader("Análisis de Ventas por Período")

        granularidad = st.radio(
            "Selecciona la granularidad temporal",
            ["Anual", "Trimestral", "Mensual", "Diaria"],
            horizontal=True
        )

        if st.button("Cargar Ventas por Tiempo"):
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
                        st.dataframe(df, use_container_width=True)

                        col1, col2 = st.columns(2)

                        with col1:
                            if 'total_ventas' in df.columns:
                                x_col = 'periodo' if 'periodo' in df.columns else df.columns[0]
                                fig = px.bar(
                                    df,
                                    x=x_col,
                                    y='total_ventas',
                                    title='Total de Ventas',
                                    labels={'total_ventas': 'Ventas (₡)'}
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

        if st.button("Cargar Ventas por Geografía"):
            with st.spinner("Cargando datos..."):
                try:
                    nivel_map = {"Provincia": "provincia", "Cantón": "canton", "Distrito": "distrito"}
                    df = get_ventas_region(cubo, nivel_map[nivel_geo])

                    if not df.empty:
                        st.dataframe(df, use_container_width=True)

                        col1, col2 = st.columns(2)

                        with col1:
                            fig = px.bar(
                                df.head(15),
                                x='total_ventas',
                                y=df.columns[0],
                                orientation='h',
                                title=f'Top 15 Ventas por {nivel_geo}',
                                labels={'total_ventas': 'Ventas (₡)'}
                            )
                            st.plotly_chart(fig, use_container_width=True)

                        with col2:
                            fig = px.pie(
                                df.head(10),
                                values='total_ventas',
                                names=df.columns[0],
                                title=f'Distribución Top 10 {nivel_geo}'
                            )
                            st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("No hay datos")

                except Exception as e:
                    st.error(f"Error: {str(e)}")

    with subtab3:
        st.subheader("Análisis de Ventas por Categoría")

        if st.button("Cargar Ventas por Categoría"):
            with st.spinner("Cargando categorías..."):
                try:
                    df = get_ventas_categoria(cubo)

                    if not df.empty:
                        st.dataframe(df, use_container_width=True)

                        col1, col2 = st.columns(2)

                        with col1:
                            fig = px.bar(
                                df,
                                x='categoria',
                                y='total_ventas',
                                title='Ventas por Categoría',
                                labels={'total_ventas': 'Ventas (₡)'},
                                color='margen_porcentaje'
                            )
                            st.plotly_chart(fig, use_container_width=True)

                        with col2:
                            fig = px.pie(
                                df,
                                values='total_ventas',
                                names='categoria',
                                title='Participación en Ventas'
                            )
                            st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("No hay datos")

                except Exception as e:
                    st.error(f"Error: {str(e)}")

    with subtab4:
        st.subheader("Top N Análisis")

        col1, col2 = st.columns(2)

        with col1:
            top_type = st.radio("Selecciona", ["Productos", "Clientes"], horizontal=True)

        with col2:
            top_n = st.slider("Cantidad", 5, 50, 10)

        if st.button("Cargar TOP N"):
            with st.spinner(f"Cargando TOP {top_n}..."):
                try:
                    if top_type == "Productos":
                        df = get_top_productos(cubo, top_n)
                        col_sort = 'total_ventas'
                    else:
                        df = get_top_clientes(cubo, top_n)
                        col_sort = 'total_gasto'

                    if not df.empty:
                        st.dataframe(df, use_container_width=True)

                        fig = px.bar(
                            df,
                            x=col_sort,
                            y=df.columns[1],
                            orientation='h',
                            title=f'Top {top_n} {top_type}',
                            labels={col_sort: 'Monto (₡)'}
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("No hay datos")

                except Exception as e:
                    st.error(f"Error: {str(e)}")

# ============================================================================
# TAB 5: PIVOT
# ============================================================================

with tab5:
    crear_seccion_encabezado(
        "Operación PIVOT",
        "Rotar dimensiones para diferentes vistas tabulares",
        #badge="OLAP"
    )

    try:
        col1, col2, col3 = st.columns(3)

        with col1:
            filas = st.selectbox(
                "Dimensión para filas",
                ["provincia", "canton", "categoria", "anio"],
                key="pivot_rows"
            )

        with col2:
            columnas = st.selectbox(
                "Dimensión para columnas",
                ["provincia", "canton", "categoria", "mes", "anio"],
                key="pivot_cols"
            )

        with col3:
            metrica = st.selectbox(
                "Métrica",
                ["monto_total", "margen", "transacciones", "cantidad"],
                key="pivot_metric"
            )

        if st.button("Ejecutar PIVOT", use_container_width=True):
            with st.spinner("Ejecutando PIVOT..."):
                try:
                    df_pivoted = ejecutar_pivot(cubo, filas, columnas, metrica)

                    if not df_pivoted.empty:
                        st.success("PIVOT ejecutado exitosamente")

                        st.subheader(f"Tabla Pivotada: {filas} × {columnas}")
                        st.dataframe(df_pivoted, use_container_width=True)

                        fig = px.imshow(
                            df_pivoted,
                            labels=dict(color=metrica.title()),
                            title=f'{metrica.title()} por {filas} y {columnas}',
                            color_continuous_scale='RdYlGn'
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("No hay datos para este pivot")

                except Exception as e:
                    st.error(f"Error: {str(e)}")

    except Exception as e:
        st.error(f"Error: {str(e)}")

# ============================================================================
# TAB 6: COMPORTAMIENTO WEB
# ============================================================================

with tab6:
    crear_seccion_encabezado(
        "Análisis de Comportamiento Web",
        "Eventos, búsquedas y funnel de conversión",
        #badge="WEB"
    )

    subtab1, subtab2, subtab3 = st.tabs([
        "Comportamiento Web",
        "Análisis de Búsquedas",
        "Funnel de Conversión"
    ])

    with subtab1:
        st.subheader("Análisis del Comportamiento de Usuarios")

        if st.button("Cargar Comportamiento Web"):
            with st.spinner("Cargando datos web..."):
                try:
                    comportamiento = get_comportamiento_web(cubo)

                    # Eventos por Tipo
                    st.markdown("### Eventos por Tipo")
                    if 'eventos_por_tipo' in comportamiento and not comportamiento['eventos_por_tipo'].empty:
                        df_eventos = comportamiento['eventos_por_tipo']
                        st.dataframe(df_eventos, use_container_width=True)

                        col1, col2 = st.columns(2)
                        with col1:
                            fig = px.bar(
                                df_eventos.head(10),
                                x='tipo_evento',
                                y='total_eventos',
                                title='Top 10 Eventos Web',
                                labels={'total_eventos': 'Eventos', 'tipo_evento': 'Tipo de Evento'},
                                color='total_eventos',
                                color_continuous_scale='Blues'
                            )
                            fig.update_xaxes(tickangle=45)
                            st.plotly_chart(fig, use_container_width=True)

                        with col2:
                            fig = px.bar(
                                df_eventos.head(10),
                                x='tipo_evento',
                                y='tasa_conversion',
                                title='Tasa de Conversión por Evento',
                                labels={'tasa_conversion': 'Conversión (%)', 'tipo_evento': 'Tipo de Evento'},
                                color='tasa_conversion',
                                color_continuous_scale='Greens'
                            )
                            fig.update_xaxes(tickangle=45)
                            st.plotly_chart(fig, use_container_width=True)

                    # Dispositivos
                    st.markdown("### Análisis por Dispositivo")
                    if 'dispositivos' in comportamiento and not comportamiento['dispositivos'].empty:
                        df_dispositivos = comportamiento['dispositivos']
                        st.dataframe(df_dispositivos, use_container_width=True)

                        col1, col2 = st.columns(2)
                        with col1:
                            fig = px.pie(
                                df_dispositivos,
                                values='total_eventos',
                                names='tipo_dispositivo',
                                title='Eventos por Tipo de Dispositivo',
                                color_discrete_sequence=px.colors.qualitative.Set2
                            )
                            st.plotly_chart(fig, use_container_width=True)

                        with col2:
                            fig = px.bar(
                                df_dispositivos,
                                x='tipo_dispositivo',
                                y='tasa_conversion',
                                title='Conversión por Tipo de Dispositivo',
                                labels={'tasa_conversion': 'Conversión (%)', 'tipo_dispositivo': 'Tipo'},
                                color='tasa_conversion',
                                color_continuous_scale='RdYlGn'
                            )
                            st.plotly_chart(fig, use_container_width=True)

                    # Navegadores
                    st.markdown("### Análisis por Navegador")
                    if 'navegadores' in comportamiento and not comportamiento['navegadores'].empty:
                        df_navegadores = comportamiento['navegadores']
                        st.dataframe(df_navegadores, use_container_width=True)

                        col1, col2 = st.columns(2)
                        with col1:
                            fig = px.bar(
                                df_navegadores.head(5),
                                x='navegador',
                                y='total_eventos',
                                title='Top 5 Navegadores',
                                labels={'total_eventos': 'Eventos', 'navegador': 'Navegador'}
                            )
                            st.plotly_chart(fig, use_container_width=True)

                        with col2:
                            fig = px.bar(
                                df_navegadores.head(5),
                                x='navegador',
                                y='tasa_conversion',
                                title='Conversión por Navegador',
                                labels={'tasa_conversion': 'Conversión (%)', 'navegador': 'Navegador'},
                                color='tasa_conversion',
                                color_continuous_scale='Viridis'
                            )
                            st.plotly_chart(fig, use_container_width=True)

                    # Productos Más Vistos
                    st.markdown("### Productos Más Vistos")
                    if 'productos_vistos' in comportamiento and not comportamiento['productos_vistos'].empty:
                        df_productos = comportamiento['productos_vistos']
                        st.dataframe(df_productos, use_container_width=True)

                        col1, col2 = st.columns(2)
                        with col1:
                            fig = px.bar(
                                df_productos.head(10),
                                x='total_visualizaciones',
                                y='producto',
                                orientation='h',
                                title='Top 10 Productos Más Vistos',
                                labels={'total_visualizaciones': 'Visualizaciones', 'producto': 'Producto'}
                            )
                            st.plotly_chart(fig, use_container_width=True)

                        with col2:
                            fig = px.scatter(
                                df_productos.head(20),
                                x='total_visualizaciones',
                                y='veces_comprado',
                                size='tasa_conversion',
                                color='categoria',
                                hover_name='producto',
                                title='Visualizaciones vs Compras',
                                labels={'total_visualizaciones': 'Visualizaciones', 'veces_comprado': 'Compras'}
                            )
                            st.plotly_chart(fig, use_container_width=True)

                except Exception as e:
                    st.error(f"Error: {str(e)}")

    with subtab2:
        st.subheader("Análisis de Búsquedas de Usuarios")

        if st.button("Cargar Análisis de Búsquedas"):
            with st.spinner("Cargando búsquedas..."):
                try:
                    busquedas = get_analisis_busquedas(cubo)

                    # Resumen General
                    if 'resumen' in busquedas and not busquedas['resumen'].empty:
                        resumen = busquedas['resumen'].iloc[0]

                        st.markdown("### Resumen de Búsquedas")
                        col1, col2, col3, col4 = st.columns(4)

                        with col1:
                            st.metric("Total Búsquedas", f"{resumen['total_busquedas']:,.0f}")
                        with col2:
                            st.metric("Usuarios Únicos", f"{resumen['usuarios_unicos']:,.0f}")
                        with col3:
                            st.metric("Conversiones", f"{resumen['conversiones_totales']:,.0f}")
                        with col4:
                            st.metric("Tasa Conversión", f"{resumen['tasa_conversion_global']:.2f}%")

                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("Promedio Resultados", f"{resumen['promedio_resultados']:.1f}")
                        with col2:
                            st.metric("Sin Resultado", f"{resumen['porcentaje_sin_resultado']:.2f}%")

                    # Búsquedas por Dispositivo
                    st.markdown("### Búsquedas por Dispositivo")
                    if 'busquedas_dispositivo' in busquedas and not busquedas['busquedas_dispositivo'].empty:
                        df_dispositivo = busquedas['busquedas_dispositivo']
                        st.dataframe(df_dispositivo, use_container_width=True)

                        col1, col2 = st.columns(2)
                        with col1:
                            fig = px.pie(
                                df_dispositivo,
                                values='total_busquedas',
                                names='tipo_dispositivo',
                                title='Distribución de Búsquedas por Dispositivo',
                                color_discrete_sequence=px.colors.qualitative.Pastel
                            )
                            st.plotly_chart(fig, use_container_width=True)

                        with col2:
                            fig = px.bar(
                                df_dispositivo,
                                x='tipo_dispositivo',
                                y='tasa_conversion',
                                title='Tasa de Conversión por Dispositivo',
                                labels={'tasa_conversion': 'Conversión (%)', 'tipo_dispositivo': 'Dispositivo'},
                                color='tasa_conversion',
                                color_continuous_scale='Oranges'
                            )
                            st.plotly_chart(fig, use_container_width=True)

                    # Búsquedas por Navegador
                    st.markdown("### Búsquedas por Navegador")
                    if 'busquedas_navegador' in busquedas and not busquedas['busquedas_navegador'].empty:
                        df_navegador = busquedas['busquedas_navegador']
                        st.dataframe(df_navegador.head(10), use_container_width=True)

                        col1, col2 = st.columns(2)
                        with col1:
                            fig = px.bar(
                                df_navegador.head(5),
                                x='navegador',
                                y='total_busquedas',
                                title='Top 5 Navegadores - Búsquedas',
                                labels={'total_busquedas': 'Búsquedas', 'navegador': 'Navegador'}
                            )
                            st.plotly_chart(fig, use_container_width=True)

                        with col2:
                            fig = px.bar(
                                df_navegador.head(5),
                                x='navegador',
                                y='tasa_conversion',
                                title='Conversión por Navegador',
                                labels={'tasa_conversion': 'Conversión (%)', 'navegador': 'Navegador'},
                                color='tasa_conversion',
                                color_continuous_scale='Teal'
                            )
                            st.plotly_chart(fig, use_container_width=True)

                    # Productos Más Buscados
                    st.markdown("### Productos Más Buscados")
                    if 'productos_buscados' in busquedas and not busquedas['productos_buscados'].empty:
                        df_productos = busquedas['productos_buscados']
                        st.dataframe(df_productos, use_container_width=True)

                        col1, col2 = st.columns(2)
                        with col1:
                            fig = px.bar(
                                df_productos.head(10),
                                x='total_busquedas',
                                y='producto',
                                orientation='h',
                                title='Top 10 Productos Más Buscados',
                                labels={'total_busquedas': 'Búsquedas', 'producto': 'Producto'},
                                color='total_busquedas',
                                color_continuous_scale='Purples'
                            )
                            st.plotly_chart(fig, use_container_width=True)

                        with col2:
                            fig = px.scatter(
                                df_productos.head(20),
                                x='total_busquedas',
                                y='veces_comprado',
                                size='tasa_conversion',
                                color='categoria',
                                hover_name='producto',
                                title='Búsquedas vs Compras',
                                labels={'total_busquedas': 'Búsquedas', 'veces_comprado': 'Compras'}
                            )
                            st.plotly_chart(fig, use_container_width=True)

                except Exception as e:
                    st.error(f"Error: {str(e)}")

    with subtab3:
        st.subheader("Análisis de Funnel de Conversión")

        if st.button("Cargar Funnel de Conversión"):
            with st.spinner("Cargando funnel..."):
                try:
                    df_funnel = get_funnel_conversion(cubo)

                    if not df_funnel.empty:
                        st.dataframe(df_funnel, use_container_width=True)

                        fig = go.Figure(go.Funnel(
                            y=df_funnel['etapa'],
                            x=df_funnel['cantidad'],
                            textposition="inside",
                            textinfo="value+percent previous",
                            marker=dict(color=['#3498db', '#2980b9', '#1c5294', '#0d3e66', '#051e3e'])
                        ))

                        fig.update_layout(
                            title="Funnel de Conversión",
                            showlegend=False,
                            height=500
                        )

                        st.plotly_chart(fig, use_container_width=True)

                        st.subheader("Análisis del Funnel")

                        if len(df_funnel) >= 2:
                            total_busquedas = df_funnel.iloc[0]['cantidad']
                            compras = df_funnel.iloc[-1]['cantidad']
                            conversion_total = (compras / total_busquedas * 100) if total_busquedas > 0 else 0

                            st.info(f"""
                            **Métricas de Conversión**
                            Total Búsquedas: {total_busquedas:,}
                            Compras Completadas: {compras:,}
                            Tasa de Conversión Total: {conversion_total:.2f}%
                            """)

                except Exception as e:
                    st.error(f"Error: {str(e)}")

st.markdown("---")
st.caption("Sistema de Analítica Empresarial - Cubo OLAP")
