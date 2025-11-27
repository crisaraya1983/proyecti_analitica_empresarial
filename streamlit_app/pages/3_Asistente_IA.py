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
    Optimizado para ser usado en el prompt del sistema
    """

    # Ventas por categoría
    query_categorias = """
        SELECT
            p.categoria,
            COUNT(DISTINCT fv.venta_id) AS num_ventas,
            SUM(fv.cantidad) AS unidades_vendidas,
            SUM(fv.monto_total) AS ventas_totales,
            SUM(fv.margen) AS margen_total,
            ROUND(AVG(fv.monto_total), 2) AS ticket_promedio
        FROM fact_ventas fv
        INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
        WHERE fv.venta_cancelada = 0
        GROUP BY p.categoria
        ORDER BY ventas_totales DESC
    """

    # Ventas por provincia
    query_provincias = """
        SELECT
            g.provincia,
            COUNT(DISTINCT fv.venta_id) AS num_ventas,
            SUM(fv.monto_total) AS ventas_totales,
            COUNT(DISTINCT fv.cliente_id) AS num_clientes
        FROM fact_ventas fv
        INNER JOIN dim_geografia g ON fv.provincia_id = g.provincia_id
            AND fv.canton_id = g.canton_id AND fv.distrito_id = g.distrito_id
        WHERE fv.venta_cancelada = 0
        GROUP BY g.provincia
        ORDER BY ventas_totales DESC
    """

    # Ventas mensuales (últimos 12 meses)
    query_mensuales = """
        SELECT TOP 12
            t.ANIO_CAL AS anio,
            t.MES_CAL AS mes,
            t.MES_NOMBRE AS mes_nombre,
            COUNT(DISTINCT fv.venta_id) AS num_ventas,
            SUM(fv.monto_total) AS ventas_totales,
            SUM(fv.margen) AS margen_total
        FROM fact_ventas fv
        INNER JOIN dim_tiempo t ON fv.tiempo_key = t.ID_FECHA
        WHERE fv.venta_cancelada = 0
        GROUP BY t.ANIO_CAL, t.MES_CAL, t.MES_NOMBRE
        ORDER BY t.ANIO_CAL DESC, t.MES_CAL DESC
    """

    # Top 10 productos
    query_productos = """
        SELECT TOP 10
            p.nombre_producto,
            p.categoria,
            SUM(fv.cantidad) AS unidades_vendidas,
            SUM(fv.monto_total) AS ventas_totales
        FROM fact_ventas fv
        INNER JOIN dim_producto p ON fv.producto_id = p.producto_id
        WHERE fv.venta_cancelada = 0
        GROUP BY p.nombre_producto, p.categoria
        ORDER BY ventas_totales DESC
    """

    # Métricas generales
    query_metricas = """
        SELECT
            COUNT(DISTINCT fv.venta_id) AS total_ventas,
            COUNT(DISTINCT fv.cliente_id) AS total_clientes,
            SUM(fv.monto_total) AS ventas_totales,
            SUM(fv.margen) AS margen_total,
            AVG(fv.monto_total) AS ticket_promedio,
            SUM(fv.cantidad) AS unidades_totales
        FROM fact_ventas fv
        WHERE fv.venta_cancelada = 0
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
        'mensuales': convertir_tipos_arrow_compatibles(pd.read_sql(query_mensuales, _conn)),
        'productos': convertir_tipos_arrow_compatibles(pd.read_sql(query_productos, _conn)),
        'metricas': convertir_tipos_arrow_compatibles(pd.read_sql(query_metricas, _conn))
    }

def formatear_datos_para_contexto(datos: dict) -> str:
    """
    Formatea los datos en un string legible para Claude
    """
    contexto = []

    # Métricas generales
    metricas = datos['metricas'].iloc[0]
    contexto.append("=== MÉTRICAS GENERALES DEL NEGOCIO ===")
    contexto.append(f"Total de Ventas: {metricas['total_ventas']:,} transacciones")
    contexto.append(f"Total de Clientes: {metricas['total_clientes']:,}")
    contexto.append(f"Ventas Totales: ₡{metricas['ventas_totales']:,.2f}")
    contexto.append(f"Margen Total: ₡{metricas['margen_total']:,.2f}")
    contexto.append(f"Ticket Promedio: ₡{metricas['ticket_promedio']:,.2f}")
    contexto.append(f"Unidades Vendidas: {metricas['unidades_totales']:,}")
    contexto.append("")

    # Ventas por categoría
    contexto.append("=== VENTAS POR CATEGORÍA ===")
    for _, row in datos['categorias'].iterrows():
        contexto.append(f"- {row['categoria']}: ₡{row['ventas_totales']:,.2f} ({row['num_ventas']:,} ventas, {row['unidades_vendidas']:,} unidades)")
    contexto.append("")

    # Ventas por provincia
    contexto.append("=== VENTAS POR PROVINCIA ===")
    for _, row in datos['provincias'].iterrows():
        contexto.append(f"- {row['provincia']}: ₡{row['ventas_totales']:,.2f} ({row['num_clientes']:,} clientes)")
    contexto.append("")

    # Top 10 productos
    contexto.append("=== TOP 10 PRODUCTOS ===")
    for _, row in datos['productos'].iterrows():
        contexto.append(f"- {row['nombre_producto']} ({row['categoria']}): ₡{row['ventas_totales']:,.2f}")
    contexto.append("")

    # Ventas mensuales
    contexto.append("=== VENTAS MENSUALES (ÚLTIMOS 12 MESES) ===")
    for _, row in datos['mensuales'].iterrows():
        contexto.append(f"- {row['mes_nombre']} {row['anio']}: ₡{row['ventas_totales']:,.2f}")

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
    Construye el prompt del sistema con instrucciones y datos
    """
    return f"""Eres un asistente de análisis de datos empresariales experto. Tu trabajo es ayudar a analizar datos de un e-commerce en Costa Rica.

DATOS DEL NEGOCIO:
{contexto_datos}

INSTRUCCIONES:
1. Responde preguntas sobre ventas, productos, clientes y métricas del negocio
2. Proporciona insights accionables basados en los datos
3. Cuando sea relevante, sugiere análisis adicionales
4. Usa formato de moneda costarricense (₡)
5. Sé conciso pero completo en tus respuestas
6. Si necesitas más datos específicos, indica qué query SQL sería útil

CAPACIDADES:
- Análisis de tendencias de ventas
- Comparación de categorías y productos
- Análisis geográfico (provincias)
- Cálculo de métricas (ticket promedio, margen, etc.)
- Recomendaciones de negocio basadas en datos

Responde siempre en español y con un tono profesional pero amigable."""

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
    "Asistente IA - Claude",
    "Consulta datos en lenguaje natural y obtén insights inteligentes",
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
# SIDEBAR - EJEMPLOS Y ESTADÍSTICAS
# ============================================================================

st.sidebar.title("💡 Preguntas de Ejemplo")

ejemplos = [
    "¿Cómo han crecido las ventas este año?",
    "¿Qué categoría tiene el mejor margen de ganancia?",
    "¿Cuáles son los productos más vendidos?",
    "Compara las ventas entre provincias",
    "¿Cuál es el ticket promedio por categoría?",
    "Analiza la tendencia de ventas mensual",
    "¿Qué productos deberíamos promocionar?",
    "Dame un resumen ejecutivo del negocio",
]

st.sidebar.markdown("Haz clic en una pregunta para probarla:")

for ejemplo in ejemplos:
    if st.sidebar.button(ejemplo, key=f"ejemplo_{ejemplos.index(ejemplo)}", use_container_width=True):
        # Agregar pregunta al chat
        st.session_state.messages.append({"role": "user", "content": ejemplo})
        st.rerun()

st.sidebar.markdown("---")

# Estadísticas de uso
st.sidebar.markdown("### 📊 Estadísticas de Uso")

if st.session_state.total_input_tokens > 0:
    st.sidebar.markdown(f"""
    <div class="usage-stats">
        <strong>Tokens Consumidos:</strong><br/>
        🔹 Input: {st.session_state.total_input_tokens:,}<br/>
        🔹 Output: {st.session_state.total_output_tokens:,}<br/>
        🔹 Total: {st.session_state.total_input_tokens + st.session_state.total_output_tokens:,}<br/><br/>
        <strong>Costo Estimado:</strong><br/>
        💵 ${st.session_state.total_cost:.6f} USD
    </div>
    """, unsafe_allow_html=True)
else:
    st.sidebar.info("Las estadísticas aparecerán después de la primera consulta")

# Botón para limpiar historial
if st.sidebar.button("🗑️ Limpiar Historial", use_container_width=True):
    st.session_state.messages = []
    st.session_state.total_input_tokens = 0
    st.session_state.total_output_tokens = 0
    st.session_state.total_cost = 0.0
    st.rerun()

st.sidebar.markdown("---")

# Información sobre el contexto
with st.sidebar.expander("ℹ️ Datos Cargados"):
    metricas = st.session_state.datos_contexto['metricas'].iloc[0]
    st.markdown(f"""
    **Contexto del Negocio:**
    - ✅ {metricas['total_ventas']:,} ventas
    - ✅ {metricas['total_clientes']:,} clientes
    - ✅ {len(st.session_state.datos_contexto['categorias'])} categorías
    - ✅ {len(st.session_state.datos_contexto['provincias'])} provincias
    - ✅ Top 10 productos
    - ✅ 12 meses de histórico
    """)

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
# INFORMACIÓN INICIAL
# ============================================================================

if len(st.session_state.messages) == 0:
    st.markdown("""
    ## 👋 ¡Bienvenido al Asistente IA!

    Puedo ayudarte a analizar los datos del negocio respondiendo preguntas en lenguaje natural.

    ### 💬 Ejemplos de preguntas que puedes hacer:

    - **Análisis de Ventas:** "¿Cómo han evolucionado las ventas en los últimos meses?"
    - **Productos:** "¿Cuáles son los productos más vendidos y cuál es su margen?"
    - **Geografía:** "¿Qué provincia genera más ventas?"
    - **Categorías:** "Compara el performance de las diferentes categorías"
    - **Métricas:** "¿Cuál es el ticket promedio por categoría?"
    - **Recomendaciones:** "¿Qué productos deberíamos promocionar más?"

    ### 🚀 Cómo empezar:

    1. **Usa los ejemplos** del sidebar (←) para probar el asistente
    2. **Escribe tu pregunta** en el cuadro de texto inferior
    3. **Explora los datos** haciendo preguntas de seguimiento

    ### 📊 Datos disponibles:

    El asistente tiene acceso a datos agregados del Data Warehouse incluyendo:
    - Ventas totales y por categoría
    - Performance por provincia
    - Top productos
    - Histórico mensual (12 meses)
    - Métricas generales del negocio

    ---

    **💡 Tip:** Haz preguntas específicas para obtener insights más precisos.
    """)

st.markdown("---")
st.caption("Sistema de Analítica Empresarial - Asistente IA powered by Claude")
