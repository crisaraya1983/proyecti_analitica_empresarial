"""
================================================================================
ASISTENTE IA - CLAUDE
================================================================================
Chat conversacional para consultas en lenguaje natural sobre datos del negocio
Usa Claude AI de Anthropic para análisis inteligente y generación de gráficos
================================================================================
"""

import sys
import os
import streamlit as st
import pandas as pd
import json
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# Configurar paths
project_root = os.path.dirname(os.path.dirname(__file__))
for path in [os.path.join(project_root, 'utils'),
             os.path.join(project_root, 'modulos')]:
    if path not in sys.path:
        sys.path.insert(0, path)

from utils.db_connection import DatabaseConnection
from modulos.componentes import inicializar_componentes, crear_seccion_encabezado, COLORES

# Intentar importar Anthropic
try:
    from anthropic import Anthropic, HUMAN_PROMPT, AI_PROMPT
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

# ============================================================================
# CONFIGURACIÓN DE PÁGINA
# ============================================================================

st.set_page_config(
    page_title="Asistente IA - Claude",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Inicializar componentes
inicializar_componentes()

st.title("Ecommerce Cenfotec")

# Estilos adicionales para chat
st.markdown("""
<style>
    /* Mensajes de chat */
    .stChatMessage {
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1rem;
    }

    /* Estadísticas de uso */
    .usage-stats {
        background: #f8f9fa;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #2c5aa0;
        margin: 10px 0;
    }

    /* Botones de ejemplo */
    .example-button {
        margin: 5px 0;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# VERIFICAR ANTHROPIC
# ============================================================================

if not ANTHROPIC_AVAILABLE:
    st.error("""
    ❌ **Librería Anthropic no instalada**

    Para usar el Asistente IA, instala la librería:

    ```bash
    pip install anthropic
    ```

    O agrega a requirements.txt:
    ```
    anthropic>=0.18.0
    ```
    """)
    st.stop()

# ============================================================================
# FUNCIONES DE CACHÉ Y UTILIDADES
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

@st.cache_data(ttl=1800)
def cargar_datos_contexto(_conn) -> dict:
    """
    Carga datos agregados del DW para contexto de Claude
    Optimizado con agregación correcta por venta_id y datos de 3 años completos
    """

    # Ventas por categoría (con agrupación correcta)
    query_categorias = """
        WITH VentasAgrupadas AS (
            SELECT
                fv.venta_id,
                fv.producto_id,
                SUM(fv.cantidad) AS total_unidades,
                SUM(fv.monto_total) AS monto_venta,
                SUM(fv.margen) AS margen_venta
            FROM fact_ventas fv
            WHERE fv.venta_cancelada = 0
            GROUP BY fv.venta_id, fv.producto_id
        )
        SELECT
            p.categoria,
            COUNT(DISTINCT va.venta_id) AS num_ventas,
            SUM(va.total_unidades) AS unidades_vendidas,
            SUM(va.monto_venta) AS ventas_totales,
            SUM(va.margen_venta) AS margen_total,
            ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje,
            ROUND(AVG(va.monto_venta), 2) AS ticket_promedio
        FROM VentasAgrupadas va
        INNER JOIN dim_producto p ON va.producto_id = p.producto_id
        GROUP BY p.categoria
        ORDER BY ventas_totales DESC
    """

    # Ventas por provincia (con agrupación correcta)
    query_provincias = """
        WITH VentasAgrupadas AS (
            SELECT
                fv.venta_id,
                fv.provincia_id,
                fv.canton_id,
                fv.distrito_id,
                fv.cliente_id,
                SUM(fv.monto_total) AS monto_venta,
                SUM(fv.margen) AS margen_venta
            FROM fact_ventas fv
            WHERE fv.venta_cancelada = 0
            GROUP BY fv.venta_id, fv.provincia_id, fv.canton_id, fv.distrito_id, fv.cliente_id
        )
        SELECT
            g.provincia,
            COUNT(DISTINCT va.venta_id) AS num_ventas,
            SUM(va.monto_venta) AS ventas_totales,
            SUM(va.margen_venta) AS margen_total,
            COUNT(DISTINCT va.cliente_id) AS num_clientes
        FROM VentasAgrupadas va
        INNER JOIN dim_geografia g ON va.provincia_id = g.provincia_id
            AND va.canton_id = g.canton_id AND va.distrito_id = g.distrito_id
        GROUP BY g.provincia
        ORDER BY ventas_totales DESC
    """

    # Ventas por AÑO (completo - todos los años disponibles)
    query_anuales = """
        WITH VentasAgrupadas AS (
            SELECT
                fv.venta_id,
                fv.tiempo_key,
                SUM(fv.cantidad) AS total_unidades,
                SUM(fv.monto_total) AS monto_venta,
                SUM(fv.margen) AS margen_venta
            FROM fact_ventas fv
            WHERE fv.venta_cancelada = 0
            GROUP BY fv.venta_id, fv.tiempo_key
        )
        SELECT
            t.ANIO_CAL AS anio,
            COUNT(DISTINCT va.venta_id) AS num_ventas,
            SUM(va.total_unidades) AS unidades_vendidas,
            SUM(va.monto_venta) AS ventas_totales,
            SUM(va.margen_venta) AS margen_total,
            ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje,
            ROUND(AVG(va.monto_venta), 2) AS ticket_promedio
        FROM VentasAgrupadas va
        INNER JOIN dim_tiempo t ON va.tiempo_key = t.ID_FECHA
        GROUP BY t.ANIO_CAL
        ORDER BY t.ANIO_CAL
    """

    # Ventas MENSUALES (todos los meses de todos los años)
    query_mensuales = """
        WITH VentasAgrupadas AS (
            SELECT
                fv.venta_id,
                fv.tiempo_key,
                SUM(fv.cantidad) AS total_unidades,
                SUM(fv.monto_total) AS monto_venta,
                SUM(fv.margen) AS margen_venta
            FROM fact_ventas fv
            WHERE fv.venta_cancelada = 0
            GROUP BY fv.venta_id, fv.tiempo_key
        )
        SELECT
            t.ANIO_CAL AS anio,
            t.MES_CAL AS mes,
            t.MES_NOMBRE AS mes_nombre,
            COUNT(DISTINCT va.venta_id) AS num_ventas,
            SUM(va.total_unidades) AS unidades_vendidas,
            SUM(va.monto_venta) AS ventas_totales,
            SUM(va.margen_venta) AS margen_total,
            ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje
        FROM VentasAgrupadas va
        INNER JOIN dim_tiempo t ON va.tiempo_key = t.ID_FECHA
        GROUP BY t.ANIO_CAL, t.MES_CAL, t.MES_NOMBRE
        ORDER BY t.ANIO_CAL, t.MES_CAL
    """

    # Top 20 productos (ampliado para mejor contexto)
    query_productos = """
        WITH VentasAgrupadas AS (
            SELECT
                fv.venta_id,
                fv.producto_id,
                SUM(fv.cantidad) AS total_unidades,
                SUM(fv.monto_total) AS monto_venta,
                SUM(fv.margen) AS margen_venta
            FROM fact_ventas fv
            WHERE fv.venta_cancelada = 0
            GROUP BY fv.venta_id, fv.producto_id
        )
        SELECT TOP 20
            p.nombre_producto,
            p.categoria,
            p.precio_unitario,
            SUM(va.total_unidades) AS unidades_vendidas,
            SUM(va.monto_venta) AS ventas_totales,
            SUM(va.margen_venta) AS margen_total,
            ROUND(100.0 * SUM(va.margen_venta) / NULLIF(SUM(va.monto_venta), 0), 2) AS margen_porcentaje
        FROM VentasAgrupadas va
        INNER JOIN dim_producto p ON va.producto_id = p.producto_id
        GROUP BY p.nombre_producto, p.categoria, p.precio_unitario
        ORDER BY ventas_totales DESC
    """

    # Métricas generales (con agrupación correcta)
    query_metricas = """
        WITH VentasAgrupadas AS (
            SELECT
                fv.venta_id,
                fv.cliente_id,
                SUM(fv.cantidad) AS total_unidades,
                SUM(fv.monto_total) AS monto_venta,
                SUM(fv.margen) AS margen_venta
            FROM fact_ventas fv
            WHERE fv.venta_cancelada = 0
            GROUP BY fv.venta_id, fv.cliente_id
        )
        SELECT
            COUNT(DISTINCT venta_id) AS total_ventas,
            COUNT(DISTINCT cliente_id) AS total_clientes,
            SUM(monto_venta) AS ventas_totales,
            SUM(margen_venta) AS margen_total,
            ROUND(100.0 * SUM(margen_venta) / NULLIF(SUM(monto_venta), 0), 2) AS margen_porcentaje,
            AVG(monto_venta) AS ticket_promedio,
            SUM(total_unidades) AS unidades_totales
        FROM VentasAgrupadas
    """

    # Productos por categoría (para análisis detallado)
    query_productos_categoria = """
        WITH VentasAgrupadas AS (
            SELECT
                fv.venta_id,
                fv.producto_id,
                SUM(fv.cantidad) AS total_unidades,
                SUM(fv.monto_total) AS monto_venta
            FROM fact_ventas fv
            WHERE fv.venta_cancelada = 0
            GROUP BY fv.venta_id, fv.producto_id
        )
        SELECT
            p.categoria,
            COUNT(DISTINCT p.producto_id) AS num_productos_distintos,
            SUM(va.total_unidades) AS unidades_vendidas
        FROM VentasAgrupadas va
        INNER JOIN dim_producto p ON va.producto_id = p.producto_id
        GROUP BY p.categoria
        ORDER BY unidades_vendidas DESC
    """

    # Función auxiliar para convertir tipos nullable a estándar
    def convertir_tipos_arrow_compatibles(df):
        for col in df.columns:
            if hasattr(df[col].dtype, 'numpy_dtype'):
                df[col] = df[col].astype(df[col].dtype.numpy_dtype)
        return df

    return {
        'categorias': convertir_tipos_arrow_compatibles(pd.read_sql(query_categorias, _conn)),
        'provincias': convertir_tipos_arrow_compatibles(pd.read_sql(query_provincias, _conn)),
        'anuales': convertir_tipos_arrow_compatibles(pd.read_sql(query_anuales, _conn)),
        'mensuales': convertir_tipos_arrow_compatibles(pd.read_sql(query_mensuales, _conn)),
        'productos': convertir_tipos_arrow_compatibles(pd.read_sql(query_productos, _conn)),
        'productos_categoria': convertir_tipos_arrow_compatibles(pd.read_sql(query_productos_categoria, _conn)),
        'metricas': convertir_tipos_arrow_compatibles(pd.read_sql(query_metricas, _conn))
    }

def formatear_datos_para_contexto(datos: dict) -> str:
    """
    Formatea los datos en un string legible para Claude con información completa de 3 años
    """
    contexto = []

    # Métricas generales
    metricas = datos['metricas'].iloc[0]
    contexto.append("=== RESUMEN EJECUTIVO DEL NEGOCIO ===")
    contexto.append(f"Ventas Totales: ₡{metricas['ventas_totales']:,.2f} | Margen: ₡{metricas['margen_total']:,.2f} ({metricas['margen_porcentaje']:.1f}%)")
    contexto.append(f"Transacciones: {metricas['total_ventas']:,} | Clientes: {metricas['total_clientes']:,} | Unidades: {metricas['unidades_totales']:,}")
    contexto.append(f"Ticket Promedio: ₡{metricas['ticket_promedio']:,.2f}")
    contexto.append("")

    # Ventas por AÑO (tendencia multi-anual)
    contexto.append("=== EVOLUCIÓN ANUAL ===")
    for _, row in datos['anuales'].iterrows():
        contexto.append(f"{int(row['anio'])}: ₡{row['ventas_totales']:,.2f} | {row['num_ventas']:,} ventas | Margen: {row['margen_porcentaje']:.1f}% | Ticket: ₡{row['ticket_promedio']:,.2f}")

    # Calcular crecimiento año a año
    if len(datos['anuales']) >= 2:
        años = datos['anuales'].sort_values('anio')
        crecimiento = []
        for i in range(1, len(años)):
            año_actual = años.iloc[i]
            año_anterior = años.iloc[i-1]
            pct_change = ((año_actual['ventas_totales'] - año_anterior['ventas_totales']) / año_anterior['ventas_totales']) * 100
            crecimiento.append(f"{int(año_anterior['anio'])}->{int(año_actual['anio'])}: {pct_change:+.1f}%")
        contexto.append(f"Crecimiento: {', '.join(crecimiento)}")
    contexto.append("")

    # Ventas por CATEGORÍA
    contexto.append("=== PERFORMANCE POR CATEGORÍA ===")
    for _, row in datos['categorias'].iterrows():
        contexto.append(f"{row['categoria']}: ₡{row['ventas_totales']:,.2f} | {row['num_ventas']:,} ventas | {row['unidades_vendidas']:,} uds | Margen: {row['margen_porcentaje']:.1f}%")
    contexto.append("")

    # Productos en cada categoría
    contexto.append("=== CATÁLOGO POR CATEGORÍA ===")
    for _, row in datos['productos_categoria'].iterrows():
        contexto.append(f"{row['categoria']}: {row['num_productos_distintos']} productos distintos")
    contexto.append("")

    # Ventas por PROVINCIA
    contexto.append("=== DISTRIBUCIÓN GEOGRÁFICA ===")
    for _, row in datos['provincias'].iterrows():
        contexto.append(f"{row['provincia']}: ₡{row['ventas_totales']:,.2f} | {row['num_ventas']:,} ventas | {row['num_clientes']:,} clientes")
    contexto.append("")

    # Top 20 productos
    contexto.append("=== TOP 20 PRODUCTOS ===")
    for idx, row in datos['productos'].iterrows():
        contexto.append(f"{idx+1}. {row['nombre_producto']} ({row['categoria']}): ₡{row['ventas_totales']:,.2f} | {row['unidades_vendidas']:,} uds | Precio: ₡{row['precio_unitario']:,.2f} | Margen: {row['margen_porcentaje']:.1f}%")
    contexto.append("")

    # Ventas MENSUALES (tendencia detallada)
    contexto.append("=== HISTÓRICO MENSUAL COMPLETO ===")
    for _, row in datos['mensuales'].iterrows():
        contexto.append(f"{row['mes_nombre']} {int(row['anio'])}: ₡{row['ventas_totales']:,.2f} | {row['num_ventas']:,} ventas | Margen: {row['margen_porcentaje']:.1f}%")

    return "\n".join(contexto)

def inicializar_claude_client():
    """
    Inicializa el cliente de Claude con API key desde secrets
    """
    try:
        api_key = st.secrets["claude"]["api_key"]

        # Verificar si es el placeholder
        if "PLACEHOLDER" in api_key:
            st.warning("""
            ⚠️ **API Key de Claude no configurada**

            Para usar el Asistente IA:
            1. Crea una cuenta en https://console.anthropic.com
            2. Genera una API key en Settings > API Keys
            3. Reemplaza el PLACEHOLDER en `.streamlit/secrets.toml` con tu API key real

            ```toml
            [claude]
            api_key = "sk-ant-api03-TU-API-KEY-AQUI"
            ```
            """)
            return None

        return Anthropic(api_key=api_key)

    except Exception as e:
        st.error(f"Error al inicializar Claude: {str(e)}")
        return None

def construir_system_prompt(contexto_datos: str) -> str:
    """
    Construye el prompt del sistema con instrucciones y datos completos de 3 años
    """
    return f"""Eres un analista de datos senior especializado en e-commerce y retail. Trabajas con datos históricos de 3 años completos de un negocio de comercio electrónico en Costa Rica.

CONTEXTO DE DATOS DISPONIBLES:
{contexto_datos}

TU ROL:
- Analizar tendencias históricas y patrones de crecimiento usando datos de múltiples años
- Realizar proyecciones basadas en histórico de 3 años
- Identificar productos de alto rendimiento y oportunidades de optimización
- Proporcionar insights sobre márgenes, rentabilidad y eficiencia operativa
- Comparar performance entre años, categorías, provincias y productos

CAPACIDADES ANALÍTICAS:
1. Análisis Temporal: Tendencias anuales, estacionalidad, crecimiento año a año
2. Análisis de Productos: Performance individual, categorías, márgenes, rotación
3. Análisis Geográfico: Distribución de ventas por provincia, penetración de mercado
4. Proyecciones: Forecasting basado en tendencias históricas (regresión lineal, promedio móvil)
5. Benchmarking: Comparaciones entre períodos, categorías y productos
6. Análisis de Rentabilidad: Márgenes, ticket promedio, eficiencia por categoría

INSTRUCCIONES DE RESPUESTA:
- Usa SIEMPRE moneda costarricense (₡) para valores monetarios
- Proporciona números específicos y porcentajes cuando sea relevante
- Para proyecciones, explica la metodología (ej: "basado en crecimiento promedio de X% de los últimos 3 años")
- Identifica patterns year-over-year para insights de estacionalidad
- Sugiere acciones concretas basadas en los datos
- Sé conciso pero preciso en tus análisis

FORMATO DE RESPUESTAS:
- Para datos históricos: cita años específicos y comparaciones
- Para proyecciones: indica el método y nivel de confianza
- Para recomendaciones: prioriza por impacto potencial

Responde siempre en español profesional."""

def calcular_costo_tokens(input_tokens: int, output_tokens: int) -> float:
    """
    Calcula el costo estimado de una consulta
    """
    cost_input = st.secrets["claude"]["cost_per_million_input_tokens"]
    cost_output = st.secrets["claude"]["cost_per_million_output_tokens"]

    costo_input = (input_tokens / 1_000_000) * cost_input
    costo_output = (output_tokens / 1_000_000) * cost_output

    return costo_input + costo_output

# ============================================================================
# INICIALIZACIÓN DE SESSION STATE
# ============================================================================

if "messages" not in st.session_state:
    st.session_state.messages = []

if "total_input_tokens" not in st.session_state:
    st.session_state.total_input_tokens = 0

if "total_output_tokens" not in st.session_state:
    st.session_state.total_output_tokens = 0

if "total_cost" not in st.session_state:
    st.session_state.total_cost = 0.0

if "contexto_cargado" not in st.session_state:
    st.session_state.contexto_cargado = False

# ============================================================================
# HEADER
# ============================================================================

crear_seccion_encabezado(
    "Asistente de Análisis Inteligente",
    "Análisis de datos empresariales con IA - Consultas en lenguaje natural",
    badge_color="primary"
)

# ============================================================================
# CARGAR CONTEXTO DE DATOS
# ============================================================================

if not st.session_state.contexto_cargado:
    with st.spinner("Cargando contexto de datos del negocio..."):
        engine = get_dw_engine()
        datos_contexto = cargar_datos_contexto(engine)
        st.session_state.datos_contexto = datos_contexto
        st.session_state.contexto_str = formatear_datos_para_contexto(datos_contexto)
        st.session_state.contexto_cargado = True

# ============================================================================
# SIDEBAR - ESTADÍSTICAS Y CONTROLES
# ============================================================================

# Información sobre el contexto de datos
with st.sidebar.expander("📊 Datos Disponibles", expanded=True):
    metricas = st.session_state.datos_contexto['metricas'].iloc[0]
    años = st.session_state.datos_contexto['anuales']
    num_años = len(años)
    año_min = int(años['anio'].min())
    año_max = int(años['anio'].max())

    st.markdown(f"""
    **Período de Datos:** {año_min} - {año_max} ({num_años} años)

    **Métricas Totales:**
    - Ventas: ₡{metricas['ventas_totales']:,.0f}
    - Transacciones: {metricas['total_ventas']:,}
    - Clientes: {metricas['total_clientes']:,}

    **Dimensiones:**
    - {len(st.session_state.datos_contexto['categorias'])} categorías de productos
    - {len(st.session_state.datos_contexto['provincias'])} provincias
    - Top 20 productos más vendidos
    - {len(st.session_state.datos_contexto['mensuales'])} meses de histórico
    """)

st.sidebar.markdown("---")

# Estadísticas de uso
st.sidebar.markdown("### 💬 Uso de Sesión")

if st.session_state.total_input_tokens > 0:
    total_tokens = st.session_state.total_input_tokens + st.session_state.total_output_tokens
    st.sidebar.markdown(f"""
    **Tokens:** {total_tokens:,}
    - Input: {st.session_state.total_input_tokens:,}
    - Output: {st.session_state.total_output_tokens:,}

    **Costo:** ${st.session_state.total_cost:.4f} USD
    """)
else:
    st.sidebar.info("Las estadísticas aparecerán tras la primera consulta")

# Botón para limpiar historial
if st.sidebar.button("🗑️ Nueva Conversación", use_container_width=True, type="primary"):
    st.session_state.messages = []
    st.session_state.total_input_tokens = 0
    st.session_state.total_output_tokens = 0
    st.session_state.total_cost = 0.0
    st.rerun()

# ============================================================================
# ÁREA PRINCIPAL - CHAT
# ============================================================================

# Mostrar mensajes del historial
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Input de chat
if prompt := st.chat_input("Escribe tu pregunta sobre el negocio..."):

    # Verificar que Claude esté configurado
    client = inicializar_claude_client()

    if client is None:
        st.error("⚠️ Claude no está configurado. Por favor, agrega tu API key a secrets.toml")
        st.stop()

    # Agregar mensaje del usuario
    st.session_state.messages.append({"role": "user", "content": prompt})

    # Mostrar mensaje del usuario
    with st.chat_message("user"):
        st.markdown(prompt)

    # Generar respuesta
    with st.chat_message("assistant"):
        message_placeholder = st.empty()

        with st.spinner("Analizando datos..."):
            try:
                # Construir mensajes para Claude
                system_prompt = construir_system_prompt(st.session_state.contexto_str)

                # Preparar historial de conversación
                messages_for_claude = []
                for msg in st.session_state.messages:
                    messages_for_claude.append({
                        "role": msg["role"],
                        "content": msg["content"]
                    })

                # Llamar a Claude API
                response = client.messages.create(
                    model=st.secrets["claude"]["model"],
                    max_tokens=int(st.secrets["claude"]["max_tokens"]),
                    temperature=float(st.secrets["claude"]["temperature"]),
                    system=system_prompt,
                    messages=messages_for_claude
                )

                # Extraer respuesta
                assistant_message = response.content[0].text

                # Mostrar respuesta
                message_placeholder.markdown(assistant_message)

                # Guardar en historial
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": assistant_message
                })

                # Actualizar estadísticas
                input_tokens = response.usage.input_tokens
                output_tokens = response.usage.output_tokens

                st.session_state.total_input_tokens += input_tokens
                st.session_state.total_output_tokens += output_tokens

                costo = calcular_costo_tokens(input_tokens, output_tokens)
                st.session_state.total_cost += costo

                # Mostrar estadísticas de esta consulta
                st.info(f"""
                **Consulta procesada:**
                - Tokens input: {input_tokens:,}
                - Tokens output: {output_tokens:,}
                - Costo: ${costo:.6f} USD
                """)

            except Exception as e:
                error_msg = f"❌ Error al procesar consulta: {str(e)}"
                st.error(error_msg)

                # Detalles del error
                if "authentication" in str(e).lower():
                    st.warning("""
                    **Error de Autenticación:**
                    - Verifica que tu API key sea válida
                    - Asegúrate de tener créditos disponibles en tu cuenta de Anthropic
                    """)
                elif "rate" in str(e).lower():
                    st.warning("""
                    **Error de Límite de Tasa:**
                    - Has excedido el límite de consultas por minuto
                    - Espera unos momentos e intenta nuevamente
                    """)
                else:
                    st.warning("Revisa los logs para más detalles del error")

                # Guardar error en historial
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_msg
                })

# ============================================================================
# MENSAJE INICIAL (solo si no hay conversación)
# ============================================================================

if len(st.session_state.messages) == 0:
    metricas = st.session_state.datos_contexto['metricas'].iloc[0]
    años = st.session_state.datos_contexto['anuales']
    año_min = int(años['anio'].min())
    año_max = int(años['anio'].max())

    st.info(f"""
    **Asistente IA con datos históricos {año_min}-{año_max}**

    Este asistente tiene acceso a {metricas['total_ventas']:,} transacciones, {metricas['total_clientes']:,} clientes y datos completos de {len(años)} años para análisis de tendencias, proyecciones y recomendaciones estratégicas.

    Puedes preguntar sobre ventas, productos, categorías, geografía, márgenes, proyecciones y más.
    """)

st.markdown("---")
st.caption("Sistema de Analítica Empresarial - Asistente IA powered by Claude")
