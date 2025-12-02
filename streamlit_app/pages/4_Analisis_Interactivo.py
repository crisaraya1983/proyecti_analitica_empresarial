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
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Inicializar componentes
inicializar_componentes()

st.title("Ecommerce Cenfotec")

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
    Carga datos del cubo OLAP agregados correctamente por venta

    NOTA IMPORTANTE: fact_ventas tiene granularidad de DETALLE DE VENTA (una fila por línea de factura).
    Esta función AGRUPA PRIMERO por venta_id para obtener métricas únicas,
    luego cruza con dimensiones para análisis correcto.

    Args:
        _conn: Conexión a BD
        dimensiones: Lista de dimensiones a incluir
        limite: Límite de registros (None para todos)
        filtros: Filtros adicionales

    Returns:
        DataFrame con datos multidimensionales (agregados por venta_id)
    """
    # PASO 1: Construir CTE para agregar ventas (CRÍTICO: agrupar por venta_id primero)
    cte_ventas_agrupadas = """
    WITH VentasAgrupadas AS (
        -- Agregar primero por venta_id para obtener totales correctos
        SELECT
            fv.venta_id,
            fv.cliente_id,
            fv.tiempo_key,
            fv.provincia_id,
            fv.canton_id,
            fv.distrito_id,
            fv.almacen_id,
            fv.estado_venta_id,
            fv.metodo_pago_id,
            fv.es_primera_compra,
            fv.venta_cancelada,
            -- Medidas agregadas correctamente (no duplicadas)
            SUM(fv.cantidad) AS cantidad_total,
            SUM(fv.precio_unitario * fv.cantidad) AS monto_productos,
            SUM(fv.descuento_monto) AS descuento_total,
            SUM(fv.subtotal) AS subtotal_venta,
            SUM(fv.impuesto) AS impuesto_venta,
            SUM(fv.monto_total) AS monto_venta,
            SUM(fv.margen) AS margen_venta,
            COUNT(DISTINCT fv.detalle_venta_id) AS num_productos_venta,
            AVG(fv.precio_unitario) AS precio_promedio
        FROM fact_ventas fv
        GROUP BY
            fv.venta_id, fv.cliente_id, fv.tiempo_key, fv.provincia_id, fv.canton_id,
            fv.distrito_id, fv.almacen_id, fv.estado_venta_id, fv.metodo_pago_id,
            fv.es_primera_compra, fv.venta_cancelada
    )
    """

    # PASO 2: Construir SELECT dinámico basado en dimensiones
    select_fields = []
    join_clauses = []
    from_clause = "VentasAgrupadas va"

    # Siempre incluir ID de venta y métricas agregadas
    select_fields.extend([
        "va.venta_id",
        "va.cantidad_total",
        "va.monto_productos",
        "va.descuento_total",
        "va.subtotal_venta",
        "va.impuesto_venta",
        "va.monto_venta",
        "va.margen_venta",
        "va.num_productos_venta",
        "va.precio_promedio"
    ])

    # Dimensión Tiempo (siempre incluir - IMPORTANTE: usar ANIO_CAL, no números)
    select_fields.extend([
        "t.FECHA_CAL AS fecha",
        "t.ANIO_CAL AS anio",
        "t.ANIO_CAL AS anio_dimension",  # Para garantizar visualización correcta
        "t.MES_CAL AS mes_numero",
        "t.MES_NOMBRE AS mes",
        "t.TRIMESTRE AS trimestre",
        "t.DIA_SEM_NOMBRE AS dia_semana",
        "t.SEM_CAL_NUM AS semana"
    ])
    join_clauses.append("INNER JOIN dim_tiempo t ON va.tiempo_key = t.ID_FECHA")

    # Dimensión Producto - SOLO si se selecciona
    if 'producto' in dimensiones:
        select_fields.extend([
            "p.producto_id",
            "p.nombre_producto",
            "p.categoria",
            "p.marca"
        ])
        join_clauses.append("""
            INNER JOIN (
                SELECT DISTINCT fv.venta_id, fv.producto_id
                FROM fact_ventas fv
            ) fv_producto ON va.venta_id = fv_producto.venta_id
            INNER JOIN dim_producto p ON fv_producto.producto_id = p.producto_id
        """)

    # Dimensión Cliente
    if 'cliente' in dimensiones:
        select_fields.extend([
            "cl.cliente_id",
            "CONCAT(cl.nombre_cliente, ' ', cl.apellido_cliente) AS cliente_nombre",
            "cl.correo_electronico AS cliente_email"
        ])
        join_clauses.append("INNER JOIN dim_cliente cl ON va.cliente_id = cl.cliente_id")

    # Dimensión Geografía
    if 'geografia' in dimensiones:
        select_fields.extend([
            "g.provincia",
            "g.canton",
            "g.distrito"
        ])
        join_clauses.append("""
            INNER JOIN dim_geografia g ON va.provincia_id = g.provincia_id
                AND va.canton_id = g.canton_id
                AND va.distrito_id = g.distrito_id
        """)

    # Dimensión Almacén
    if 'almacen' in dimensiones:
        select_fields.extend([
            "a.almacen_id",
            "a.nombre_almacen AS almacen",
            "a.tipo_almacen"
        ])
        join_clauses.append("INNER JOIN dim_almacen a ON va.almacen_id = a.almacen_id")

    # Dimensión Estado Venta
    if 'estado_venta' in dimensiones:
        select_fields.extend([
            "ev.estado_venta_id",
            "ev.estado_venta",
            "ev.es_exitosa"
        ])
        join_clauses.append("INNER JOIN dim_estado_venta ev ON va.estado_venta_id = ev.estado_venta_id")

    # Dimensión Método de Pago
    if 'metodo_pago' in dimensiones:
        select_fields.extend([
            "mp.metodo_pago_id",
            "mp.metodo_pago"
        ])
        join_clauses.append("INNER JOIN dim_metodo_pago mp ON va.metodo_pago_id = mp.metodo_pago_id")

    # PASO 3: Construir WHERE clause (ANTES de los JOINs)
    where_clauses = ["va.venta_cancelada = 0"]  # Solo ventas válidas

    if filtros:
        if 'fecha_inicio' in filtros and filtros['fecha_inicio']:
            where_clauses.append(f"t.FECHA_CAL >= '{filtros['fecha_inicio']}'")
        if 'fecha_fin' in filtros and filtros['fecha_fin']:
            where_clauses.append(f"t.FECHA_CAL <= '{filtros['fecha_fin']}'")
        if 'categoria' in filtros and filtros['categoria'] and filtros['categoria'] != 'Todas':
            where_clauses.append(f"p.categoria = '{filtros['categoria']}'")
        if 'provincia' in filtros and filtros['provincia'] and filtros['provincia'] != 'Todas':
            where_clauses.append(f"g.provincia = '{filtros['provincia']}'")

    # PASO 4: Construir query completa CON CTE
    query = f"""
        {cte_ventas_agrupadas}
        SELECT {f'TOP {limite}' if limite else ''}
            {', '.join(select_fields)}
        FROM {from_clause}
        {' '.join(join_clauses)}
        WHERE {' AND '.join(where_clauses)}
        ORDER BY va.monto_venta DESC
    """

    # Ejecutar query
    df = pd.read_sql(query, _conn)

    # Agregar columnas calculadas útiles
    if not df.empty:
        df['margen_porcentaje'] = (df['margen_venta'] / df['monto_venta'] * 100).round(2)
        df['descuento_porcentaje'] = (df['descuento_total'] / df['subtotal_venta'] * 100).round(2)
        df['valor_promedio'] = (df['monto_venta'] / df['cantidad_total']).round(2)

        # Convertir fecha a datetime si existe
        if 'fecha' in df.columns:
            df['fecha'] = pd.to_datetime(df['fecha'])

    # Convertir tipos nullable de pandas a tipos estándar numpy para compatibilidad con PyArrow/Streamlit
    for col in df.columns:
        if hasattr(df[col].dtype, 'numpy_dtype'):
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
    **PyGwalker no está instalado**

    Para usar esta funcionalidad, instala PyGwalker ejecutando:
    ```bash
    pip install pygwalker
    ```
    """)
    st.stop()

# ============================================================================
# HEADER
# ============================================================================

crear_seccion_encabezado(
    "Análisis Interactivo",
    badge_color="primary"
)

# ============================================================================
# SIDEBAR - CONFIGURACIÓN
# ============================================================================

st.sidebar.title("Configuración de Datos")

# Selector de dimensiones
st.sidebar.markdown("### Dimensiones a Incluir")

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
st.sidebar.markdown("### Filtros de Datos")

engine = get_dw_engine()
opciones_filtros = obtener_opciones_filtros(engine)

with st.sidebar.expander("Rango de Fechas", expanded=True):
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

with st.sidebar.expander("Filtros Dimensionales"):
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
st.sidebar.markdown("### Performance")

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
cargar_datos = st.sidebar.button("Cargar/Actualizar Datos", use_container_width=True, type="primary")

st.sidebar.markdown("---")

# Información de dimensiones seleccionadas
if dimensiones_seleccionadas:
    st.sidebar.info(f"""
**Dimensiones seleccionadas:** {len(dimensiones_seleccionadas)}

{chr(10).join([f"• {dimensiones_disponibles[dim]}" for dim in dimensiones_seleccionadas])}
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

            st.success(f"{len(df_datos):,} registros cargados con {len(df_datos.columns)} columnas")

        except Exception as e:
            st.error(f"Error cargando datos: {str(e)}")
            st.stop()

# ============================================================================
# MOSTRAR INFORMACIÓN DE DATOS
# ============================================================================

if 'datos_cargados' in st.session_state:
    df = st.session_state.datos_cargados

    st.markdown("---")

    # ============================================================================
    # PYGWALKER INTERFACE
    # ============================================================================

    st.markdown("## Interfaz de Análisis Visual Interactivo")

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
        st.error(f"Error al renderizar PyGwalker: {str(e)}")

        # Mostrar traceback para debugging
        import traceback
        with st.expander("Detalles del Error"):
            st.code(traceback.format_exc())

    # ============================================================================
    # EXPORTAR DATOS
    # ============================================================================

    st.markdown("---")
    st.markdown("## Exportar Datos")

    col1, col2 = st.columns(2)

    with col1:
        # Exportar a CSV
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Descargar CSV",
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
            label="Descargar Excel",
            data=buffer.getvalue(),
            file_name=f'datos_cubo_olap_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            use_container_width=True
        )

else:
    # No hay datos cargados
    st.info("""
    Configura las opciones en el sidebar y presiona "Cargar/Actualizar Datos" para comenzar el análisis.
    """)

st.markdown("---")
st.caption("Sistema de Analítica Empresarial - Análisis Interactivo con PyGwalker")
