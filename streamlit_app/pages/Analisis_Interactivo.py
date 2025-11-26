"""
================================================================================
ANÁLISIS INTERACTIVO - PYGWALKER
================================================================================
Exploración visual tipo Tableau para análisis ad-hoc
Usa PyGwalker para crear visualizaciones interactivas drag-and-drop
================================================================================
"""

import sys
import os
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import streamlit.components.v1 as components

# Configurar paths
project_root = os.path.dirname(os.path.dirname(__file__))
for path in [os.path.join(project_root, 'utils'),
             os.path.join(project_root, 'OLAP'),
             os.path.join(project_root, 'modulos')]:
    if path not in sys.path:
        sys.path.insert(0, path)

from utils.db_connection import DatabaseConnection
from modulos.componentes import inicializar_componentes, crear_seccion_encabezado

# Importar PyGwalker
try:
    import pygwalker as pyg
    PYGWALKER_AVAILABLE = True
except ImportError:
    PYGWALKER_AVAILABLE = False

# ============================================================================
# CONFIGURACIÓN DE PÁGINA
# ============================================================================

st.set_page_config(
    page_title="Análisis Interactivo - PyGwalker",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Inicializar componentes
inicializar_componentes()

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

@st.cache_data(ttl=1800)  # Cache por 30 minutos
def cargar_datos_cubo(_conn, dimensiones: list, limite: int = None, filtros: dict = None) -> pd.DataFrame:
    """
    Carga datos del cubo OLAP con dimensiones seleccionadas

    Args:
        _conn: Conexión a BD
        dimensiones: Lista de dimensiones a incluir
        limite: Límite de registros (None para todos)
        filtros: Filtros adicionales

    Returns:
        DataFrame con datos multidimensionales
    """
    st.info(f"🔄 Cargando datos del cubo OLAP con {len(dimensiones)} dimensiones...")

    # Construir SELECT dinámico basado en dimensiones
    select_fields = []
    join_clauses = []

    # Siempre incluir métricas de fact_ventas
    select_fields.extend([
        "fv.venta_id",
        "fv.detalle_venta_id",
        "fv.cantidad",
        "fv.precio_unitario",
        "fv.costo_unitario",
        "fv.descuento_monto",
        "fv.subtotal",
        "fv.impuesto",
        "fv.monto_total",
        "fv.margen",
        "fv.es_primera_compra",
        "fv.venta_cancelada"
    ])

    # Dimensión Tiempo (siempre incluir)
    if 'tiempo' in dimensiones or True:  # Siempre incluir tiempo
        select_fields.extend([
            "t.FECHA_CAL AS fecha",
            "t.ANIO_CAL AS anio",
            "t.MES_CAL AS mes",
            "t.MES_NOMBRE AS mes_nombre",
            "t.TRIMESTRE AS trimestre",
            "t.DIA_SEM_NOMBRE AS dia_semana",
            "t.SEM_CAL_NUM AS semana"
        ])
        join_clauses.append("INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA")

    # Dimensión Producto
    if 'producto' in dimensiones:
        select_fields.extend([
            "p.producto_id",
            "p.nombre_producto",
            "p.categoria",
            "p.marca",
            "p.precio_unitario AS producto_precio"
        ])
        join_clauses.append("INNER JOIN dim_producto p ON fv.producto_id = p.producto_id")

    # Dimensión Cliente
    if 'cliente' in dimensiones:
        select_fields.extend([
            "cl.cliente_id",
            "CONCAT(cl.nombre_cliente, ' ', cl.apellido_cliente) AS cliente_nombre",
            "cl.correo_electronico AS cliente_email",
            "cl.telefono AS cliente_telefono"
        ])
        join_clauses.append("INNER JOIN dim_cliente cl ON fv.cliente_id = cl.cliente_id")

    # Dimensión Geografía
    if 'geografia' in dimensiones:
        select_fields.extend([
            "g.provincia",
            "g.canton",
            "g.distrito"
        ])
        join_clauses.append("""
            INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
                AND fv.canton_id = g.canton_id
                AND fv.distrito_id = g.distrito_id
        """)

    # Dimensión Almacén
    if 'almacen' in dimensiones:
        select_fields.extend([
            "a.almacen_id",
            "a.nombre_almacen AS almacen",
            "a.tipo_almacen"
        ])
        join_clauses.append("INNER JOIN dim_almacen a ON fv.almacen_id = a.almacen_id")

    # Dimensión Estado Venta
    if 'estado_venta' in dimensiones:
        select_fields.extend([
            "ev.estado_venta_id",
            "ev.estado_venta",
            "ev.es_exitosa"
        ])
        join_clauses.append("INNER JOIN dim_estado_venta ev ON fv.estado_venta_id = ev.estado_venta_id")

    # Dimensión Método de Pago
    if 'metodo_pago' in dimensiones:
        select_fields.extend([
            "mp.metodo_pago_id",
            "mp.metodo_pago",
            "mp.tipo_metodo"
        ])
        join_clauses.append("INNER JOIN dim_metodo_pago mp ON fv.metodo_pago_id = mp.metodo_pago_id")

    # Construir WHERE clause
    where_clauses = ["fv.venta_cancelada = 0"]  # Solo ventas válidas

    if filtros:
        if 'fecha_inicio' in filtros and filtros['fecha_inicio']:
            where_clauses.append(f"t.FECHA_CAL >= '{filtros['fecha_inicio']}'")
        if 'fecha_fin' in filtros and filtros['fecha_fin']:
            where_clauses.append(f"t.FECHA_CAL <= '{filtros['fecha_fin']}'")
        if 'categoria' in filtros and filtros['categoria'] and filtros['categoria'] != 'Todas':
            where_clauses.append(f"p.categoria = '{filtros['categoria']}'")
        if 'provincia' in filtros and filtros['provincia'] and filtros['provincia'] != 'Todas':
            where_clauses.append(f"g.provincia = '{filtros['provincia']}'")

    # Construir query completa
    query = f"""
        SELECT {f'TOP {limite}' if limite else ''}
            {', '.join(select_fields)}
        FROM fact_ventas fv
        {' '.join(join_clauses)}
        WHERE {' AND '.join(where_clauses)}
        ORDER BY t.FECHA_CAL DESC
    """

    # Ejecutar query
    df = pd.read_sql(query, _conn)

    # Agregar columnas calculadas
    if not df.empty:
        df['ganancia'] = df['monto_total'] - (df['cantidad'] * df['costo_unitario'])
        df['margen_porcentaje'] = (df['margen'] / df['monto_total'] * 100).round(2)

        # Convertir fecha a datetime si existe
        if 'fecha' in df.columns:
            df['fecha'] = pd.to_datetime(df['fecha'])

    # Convertir tipos nullable de pandas a tipos estándar numpy para compatibilidad con PyArrow/Streamlit
    for col in df.columns:
        if hasattr(df[col].dtype, 'numpy_dtype'):  # Es un tipo nullable de pandas (Int64, Float64, etc.)
            df[col] = df[col].astype(df[col].dtype.numpy_dtype)

    return df

@st.cache_data(ttl=1800)
def obtener_opciones_filtros(_conn):
    """Obtiene opciones para filtros"""
    categorias = pd.read_sql("SELECT DISTINCT categoria FROM dim_producto ORDER BY categoria", _conn)
    provincias = pd.read_sql("SELECT DISTINCT provincia FROM dim_geografia ORDER BY provincia", _conn)

    return {
        'categorias': ['Todas'] + categorias['categoria'].tolist(),
        'provincias': ['Todas'] + provincias['provincia'].tolist()
    }

# ============================================================================
# VERIFICAR PYGWALKER
# ============================================================================

if not PYGWALKER_AVAILABLE:
    st.error("""
    ❌ **PyGwalker no está instalado**

    Para usar esta funcionalidad, instala PyGwalker:

    ```bash
    pip install pygwalker
    ```

    O agrega a tu requirements.txt:
    ```
    pygwalker>=0.4.0
    ```
    """)
    st.stop()

# ============================================================================
# HEADER
# ============================================================================

crear_seccion_encabezado(
    "Análisis Interactivo",
    "Exploración visual tipo Tableau con PyGwalker - Drag and Drop",
    badge_color="primary"
)

# ============================================================================
# INFORMACIÓN
# ============================================================================

with st.expander("ℹ️ Cómo usar PyGwalker", expanded=False):
    st.markdown("""
    ### 🎨 Características de PyGwalker

    **PyGwalker** convierte tu DataFrame en una interfaz tipo Tableau para análisis visual:

    #### 🔧 Funcionalidades Principales:

    1. **Drag and Drop**: Arrastra campos a los ejes X, Y, Color, Tamaño
    2. **Múltiples Visualizaciones**: Barras, líneas, scatter, pie, mapas de calor
    3. **Filtros Interactivos**: Filtra datos directamente en la interfaz
    4. **Agregaciones**: Suma, promedio, conteo, min, max automáticos
    5. **Exportar Gráficos**: Descarga visualizaciones como imagen

    #### 📊 Tipos de Gráficos Disponibles:

    - 📊 Barras (verticales y horizontales)
    - 📈 Líneas y áreas
    - 🔵 Scatter plots
    - 🥧 Pie charts
    - 🗺️ Mapas de calor
    - 📉 Histogramas
    - Y más...

    #### 💡 Tips:

    - Arrastra dimensiones categóricas (provincia, categoría) a Color o Filas
    - Arrastra métricas numéricas (monto_total, cantidad) a columnas
    - Usa el botón de agregación para cambiar entre suma, promedio, etc.
    - Haz clic en "+" para agregar más gráficos en la misma vista
    """)

# ============================================================================
# SIDEBAR - CONFIGURACIÓN
# ============================================================================

st.sidebar.title("⚙️ Configuración de Datos")

# Selector de dimensiones
st.sidebar.markdown("### 🎯 Seleccionar Dimensiones")

dimensiones_disponibles = {
    'producto': 'Productos (nombre, categoría, marca)',
    'cliente': 'Clientes (nombre, email)',
    'geografia': 'Geografía (provincia, cantón, distrito)',
    'almacen': 'Almacén (nombre, tipo)',
    'estado_venta': 'Estado de Venta',
    'metodo_pago': 'Método de Pago'
}

dimensiones_seleccionadas = []

for dim_key, dim_label in dimensiones_disponibles.items():
    if st.sidebar.checkbox(dim_label, value=(dim_key in ['producto', 'geografia']), key=f"dim_{dim_key}"):
        dimensiones_seleccionadas.append(dim_key)

st.sidebar.markdown("---")

# Filtros de datos
st.sidebar.markdown("### 🔍 Filtros de Datos")

engine = get_dw_engine()
opciones_filtros = obtener_opciones_filtros(engine)

with st.sidebar.expander("📅 Rango de Fechas", expanded=True):
    fecha_inicio = st.date_input(
        "Fecha inicio",
        value=(datetime.now() - timedelta(days=90)).date(),
        key="fecha_inicio_pygwalker"
    )

    fecha_fin = st.date_input(
        "Fecha fin",
        value=datetime.now().date(),
        key="fecha_fin_pygwalker"
    )

with st.sidebar.expander("🏷️ Filtros Dimensionales"):
    categoria_filtro = st.selectbox(
        "Categoría",
        opciones_filtros['categorias'],
        key="categoria_pygwalker"
    )

    provincia_filtro = st.selectbox(
        "Provincia",
        opciones_filtros['provincias'],
        key="provincia_pygwalker"
    )

st.sidebar.markdown("---")

# Límite de registros
st.sidebar.markdown("### ⚡ Performance")

limite_registros = st.sidebar.selectbox(
    "Límite de registros",
    [1000, 5000, 10000, 25000, 50000, None],
    index=2,  # Default 10000
    format_func=lambda x: f"{x:,} registros" if x else "Todos los registros",
    help="Limitar registros mejora el rendimiento"
)

# Preparar filtros
filtros = {
    'fecha_inicio': fecha_inicio.strftime('%Y-%m-%d'),
    'fecha_fin': fecha_fin.strftime('%Y-%m-%d'),
    'categoria': categoria_filtro if categoria_filtro != 'Todas' else None,
    'provincia': provincia_filtro if provincia_filtro != 'Todas' else None
}

# Botón para cargar datos
cargar_datos = st.sidebar.button("🔄 Cargar/Actualizar Datos", use_container_width=True, type="primary")

st.sidebar.markdown("---")

# Información de dimensiones seleccionadas
st.sidebar.info(f"""
**Dimensiones seleccionadas:** {len(dimensiones_seleccionadas)}

{chr(10).join([f"✓ {dimensiones_disponibles[dim]}" for dim in dimensiones_seleccionadas])}
""")

# ============================================================================
# CARGAR DATOS
# ============================================================================

if 'datos_cargados' not in st.session_state or cargar_datos:
    with st.spinner("Cargando datos del cubo OLAP..."):
        try:
            df_datos = cargar_datos_cubo(
                engine,
                dimensiones_seleccionadas,
                limite=limite_registros,
                filtros=filtros
            )

            st.session_state.datos_cargados = df_datos
            st.session_state.dimensiones_actuales = dimensiones_seleccionadas

            st.success(f"✅ {len(df_datos):,} registros cargados con {len(df_datos.columns)} columnas")

        except Exception as e:
            st.error(f"❌ Error cargando datos: {str(e)}")
            st.stop()

# ============================================================================
# MOSTRAR INFORMACIÓN DE DATOS
# ============================================================================

if 'datos_cargados' in st.session_state:
    df = st.session_state.datos_cargados

    # Métricas de datos
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("📊 Registros", f"{len(df):,}")

    with col2:
        st.metric("📋 Columnas", len(df.columns))

    with col3:
        st.metric("💰 Ventas Totales", f"₡{df['monto_total'].sum():,.0f}")

    with col4:
        st.metric("📈 Margen Total", f"₡{df['margen'].sum():,.0f}")

    # Mostrar preview de datos
    with st.expander("👀 Preview de Datos (primeros 10 registros)", expanded=False):
        st.dataframe(df.head(10), use_container_width=True)

    # Información de columnas
    with st.expander("📋 Información de Columnas", expanded=False):
        col_info = pd.DataFrame({
            'Columna': df.columns,
            'Tipo': df.dtypes.values,
            'Valores Únicos': [df[col].nunique() for col in df.columns],
            'Valores Nulos': [df[col].isnull().sum() for col in df.columns],
            'Valores Nulos %': [f"{(df[col].isnull().sum() / len(df) * 100):.1f}%" for col in df.columns]
        })
        st.dataframe(col_info, use_container_width=True, hide_index=True)

    st.markdown("---")

    # ============================================================================
    # PYGWALKER INTERFACE
    # ============================================================================

    st.markdown("## 🎨 Interfaz de Análisis Visual")

    st.info("""
    **💡 Instrucciones:**
    1. Arrastra campos desde el panel izquierdo hacia los ejes X, Y
    2. Agrega dimensiones a Color, Tamaño o Detalles
    3. Cambia el tipo de gráfico desde el selector superior
    4. Usa filtros para explorar subconjuntos de datos
    5. Haz clic en "+" para crear múltiples visualizaciones
    """)

    try:
        # Generar HTML de PyGwalker
        # Usar spec para personalizar la interfaz
        pyg_html = pyg.to_html(
            df,
            spec="./gw_config.json",  # Configuración opcional
            use_kernel_calc=True,      # Usar cálculos del kernel
            hideDataSourceConfig=False, # Mostrar configuración de datos
            theme_key='g2'             # Tema visual (corregido de themeKey)
        )

        # Renderizar con streamlit components
        components.html(pyg_html, height=1000, scrolling=True)

    except Exception as e:
        st.error(f"❌ Error al renderizar PyGwalker: {str(e)}")

        st.warning("""
        **Posibles soluciones:**
        1. Verifica que PyGwalker esté correctamente instalado
        2. Intenta reducir el número de registros
        3. Verifica que no haya valores conflictivos en los datos
        """)

        # Mostrar traceback para debugging
        import traceback
        with st.expander("🔧 Detalles del Error"):
            st.code(traceback.format_exc())

    # ============================================================================
    # EXPORTAR DATOS
    # ============================================================================

    st.markdown("---")
    st.markdown("## 💾 Exportar Datos")

    col1, col2 = st.columns(2)

    with col1:
        # Exportar a CSV
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Descargar CSV",
            data=csv,
            file_name=f'datos_cubo_olap_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
            mime='text/csv',
            use_container_width=True
        )

    with col2:
        # Exportar a Excel
        from io import BytesIO
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Datos OLAP', index=False)

        st.download_button(
            label="📥 Descargar Excel",
            data=buffer.getvalue(),
            file_name=f'datos_cubo_olap_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            use_container_width=True
        )

else:
    # No hay datos cargados
    st.warning("""
    👈 **Configura las opciones en el sidebar y presiona "Cargar/Actualizar Datos"**

    1. Selecciona las dimensiones que deseas analizar
    2. Configura los filtros (fechas, categoría, provincia)
    3. Elige el límite de registros
    4. Presiona el botón azul para cargar los datos
    """)

    st.info("""
    ### 💡 Recomendaciones:

    - **Para exploración inicial**: Usa 10,000 registros
    - **Para análisis detallado**: Usa 25,000 - 50,000 registros
    - **Para reportes ejecutivos**: Filtra por periodo específico

    Siempre puedes recargar con diferentes configuraciones.
    """)

st.markdown("---")
st.caption("Sistema de Analítica Empresarial - Análisis Interactivo con PyGwalker")
