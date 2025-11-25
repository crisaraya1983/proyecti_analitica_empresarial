"""
================================================================================
PÁGINA ETL - CARGA DE DATA WAREHOUSE
================================================================================
Autor: Sistema de Analítica Empresarial
Fecha: 2025-01-15
Propósito: Interfaz Streamlit para ejecutar y monitorear el proceso ETL
================================================================================
"""

import streamlit as st
import sys
import os
from datetime import datetime
import pandas as pd

# Agregar rutas al path
project_root = os.path.dirname(os.path.dirname(__file__))
etl_path = os.path.join(project_root, 'ETL')
utils_path = os.path.join(project_root, 'utils')
modulos_path = os.path.join(project_root, 'modulos')

for path in [etl_path, utils_path, modulos_path]:
    if path not in sys.path:
        sys.path.insert(0, path)

# Importar módulos
from ETL.etl_pipeline import ETLPipeline
from ETL.etl_logger import ETLLogger
from utils.db_connection import DatabaseConnection
from modulos.componentes import inicializar_componentes, crear_seccion_encabezado

# Configuración de la página
st.set_page_config(
    page_title="ETL Data Warehouse",
    page_icon="🔄",
    layout="wide"
)

# Inicializar componentes y estilos
inicializar_componentes()

# Título usando componente
crear_seccion_encabezado(
    "ETL - Carga de Data Warehouse",
    "Proceso de extracción, transformación y carga desde OLTP hacia DW",
    #badge="ETL",
    badge_color="warning"
)

# Sidebar con información
with st.sidebar:
    st.header("Información del ETL")
    st.markdown("""
    **Proceso ETL** - Carga completa de datos desde OLTP hacia el Data Warehouse.

    **Fases:**
    - Dimensiones (10 tablas)
    - Hechos (3 tablas)
    - Validación

    **Duración estimada:** 10 minutos
    """)

    st.markdown("---")

    st.subheader("Conexiones")
    test_conn = st.button("Probar Conexiones", use_container_width=True)

    if test_conn:
        with st.spinner("Probando conexiones..."):
            try:
                results = DatabaseConnection.test_all_connections(use_secrets=True)

                if results["oltp"]["success"]:
                    st.success("OLTP conectado")
                else:
                    st.error(f"OLTP: {results['oltp']['error']}")

                if results["dw"]["success"]:
                    st.success("DW conectado")
                else:
                    st.error(f"DW: {results['dw']['error']}")
            except Exception as e:
                st.error(f"Error probando conexiones: {str(e)}")


# Tabs principales
tab1, tab2, tab3 = st.tabs(["Ejecutar ETL", "Historial", "Métricas"])

# ============================================================================
# TAB 1: EJECUTAR ETL
# ============================================================================
with tab1:
    st.header("Ejecutar Proceso ETL")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.info("""
        **ℹ️ Nota:** Este proceso recarga completamente el Data Warehouse desde la base de datos transaccional.
        """)

    with col2:
        st.metric("Estado Actual", "Listo para ejecutar", delta="Esperando acción")

    st.markdown("---")

    # Botón para ejecutar ETL
    if st.button("▶️ INICIAR PROCESO ETL", type="primary", use_container_width=True):

        # Contenedor para progreso
        progress_container = st.container()

        with progress_container:
            # Barra de progreso
            progress_bar = st.progress(0)
            status_text = st.empty()

            # Tabs para logs
            log_tab1, log_tab2, log_tab3 = st.tabs(["📝 Logs Generales", "📊 Dimensiones", "📈 Hechos"])

            with log_tab1:
                log_general = st.empty()

            with log_tab2:
                log_dimensiones = st.empty()

            with log_tab3:
                log_hechos = st.empty()

            # Iniciar ETL
            try:
                status_text.text("🔄 Iniciando proceso ETL...")
                progress_bar.progress(5)

                # Crear pipeline
                pipeline = ETLPipeline(use_secrets=True)

                # Conectar
                status_text.text("🔌 Conectando a bases de datos...")
                progress_bar.progress(10)
                pipeline.conectar_bases_datos()

                # Validar prerequisitos
                status_text.text("✅ Validando prerequisitos...")
                progress_bar.progress(15)
                if not pipeline.validar_prerequisitos():
                    st.error("❌ Los prerequisitos no se cumplieron. Revisa los logs.")
                    st.stop()

                # Fase 1: Dimensiones
                status_text.text("📊 Cargando dimensiones...")
                progress_bar.progress(20)
                pipeline.ejecutar_dimensiones()
                progress_bar.progress(50)

                # Mostrar resultados de dimensiones
                dim_data = []
                for dim_nombre, (extraidos, insertados) in pipeline.results['dimensiones'].items():
                    dim_data.append({
                        'Dimensión': dim_nombre,
                        'Extraídos': f"{extraidos:,}",
                        'Insertados': f"{insertados:,}"
                    })

                with log_dimensiones:
                    st.success("✅ Dimensiones cargadas")
                    st.dataframe(pd.DataFrame(dim_data), use_container_width=True)

                # Fase 2: Hechos
                status_text.text("📈 Cargando tablas de hechos...")
                progress_bar.progress(60)
                pipeline.ejecutar_hechos()
                progress_bar.progress(90)

                # Mostrar resultados de hechos
                fact_data = []
                for fact_nombre, (extraidos, insertados) in pipeline.results['hechos'].items():
                    fact_data.append({
                        'Tabla de Hechos': fact_nombre,
                        'Extraídos': f"{extraidos:,}",
                        'Insertados': f"{insertados:,}"
                    })

                with log_hechos:
                    st.success("✅ Tablas de hechos cargadas")
                    st.dataframe(pd.DataFrame(fact_data), use_container_width=True)

                # Validar resultados
                status_text.text("✔️ Validando resultados...")
                pipeline.validar_resultados()
                progress_bar.progress(95)

                # Finalizar
                pipeline.results['success'] = True
                pipeline.results['fin'] = datetime.now()
                pipeline.results['duracion_segundos'] = int(
                    (pipeline.results['fin'] - pipeline.results['inicio']).total_seconds()
                )

                progress_bar.progress(100)
                status_text.text("✅ Proceso ETL completado exitosamente!")

                # Desconectar
                pipeline.desconectar_bases_datos()

                # Resumen final
                st.markdown("---")
                st.success("🎉 ¡PROCESO ETL COMPLETADO EXITOSAMENTE!")

                col1, col2, col3, col4 = st.columns(4)

                total_extraidos = sum(r[0] for r in pipeline.results['dimensiones'].values())
                total_extraidos += sum(r[0] for r in pipeline.results['hechos'].values())

                total_insertados = sum(r[1] for r in pipeline.results['dimensiones'].values())
                total_insertados += sum(r[1] for r in pipeline.results['hechos'].values())

                with col1:
                    st.metric("Duración", f"{pipeline.results['duracion_segundos']}s")

                with col2:
                    st.metric("Registros Extraídos", f"{total_extraidos:,}")

                with col3:
                    st.metric("Registros Insertados", f"{total_insertados:,}")

                with col4:
                    st.metric("Tablas Cargadas", "13")

                # Logs generales
                with log_general:
                    st.info(f"""
                    **Resumen del Proceso:**
                    - Inicio: {pipeline.results['inicio'].strftime('%Y-%m-%d %H:%M:%S')}
                    - Fin: {pipeline.results['fin'].strftime('%Y-%m-%d %H:%M:%S')}
                    - Duración: {pipeline.results['duracion_segundos']} segundos
                    - Dimensiones cargadas: 10
                    - Tablas de hechos cargadas: 3
                    - Total registros: {total_insertados:,}
                    """)

            except Exception as e:
                progress_bar.progress(0)
                status_text.text("❌ Error en el proceso ETL")
                st.error(f"**Error:** {str(e)}")

                if 'pipeline' in locals():
                    try:
                        pipeline.desconectar_bases_datos()
                    except:
                        pass

                st.error("""
                **El proceso ETL falló.** Por favor:
                1. Revisa los logs arriba para ver el error específico
                2. Verifica que las bases de datos OLTP y DW estén disponibles
                3. Asegúrate de que la tabla 'tiempo' tenga datos en OLTP
                4. Contacta al administrador si el error persiste
                """)

# ============================================================================
# TAB 2: HISTORIAL
# ============================================================================
with tab2:
    st.header("📋 Historial de Ejecuciones ETL")

    if st.button("🔄 Actualizar Historial"):
        st.rerun()

    try:
        conn_dw = DatabaseConnection.get_dw_connection(use_secrets=True)
        logs = ETLLogger.obtener_ultimos_logs(conn_dw, limite=20)
        conn_dw.close()

        if logs:
            # Convertir a DataFrame
            df_logs = pd.DataFrame(logs)

            # Formatear columnas (manejar valores NULL)
            df_logs['fecha_inicio'] = pd.to_datetime(df_logs['fecha_inicio'])
            df_logs['fecha_fin'] = pd.to_datetime(df_logs['fecha_fin'], errors='coerce')

            # Si duracion_segundos es NULL, calcularlo o dejarlo en blanco
            df_logs['duracion_segundos'] = df_logs['duracion_segundos'].fillna(0)

            # Mostrar tabla
            st.dataframe(
                df_logs[[
                    'log_id', 'proceso_nombre', 'tabla_destino', 'fecha_inicio',
                    'duracion_segundos', 'registros_insertados', 'estado'
                ]],
                use_container_width=True,
                column_config={
                    'log_id': 'ID',
                    'proceso_nombre': 'Proceso',
                    'tabla_destino': 'Tabla',
                    'fecha_inicio': st.column_config.DatetimeColumn('Inicio', format='DD/MM/YYYY HH:mm'),
                    'duracion_segundos': 'Duración (s)',
                    'registros_insertados': 'Registros',
                    'estado': 'Estado'
                }
            )

            # Gráfico de ejecuciones
            st.subheader("📊 Ejecuciones Recientes")

            df_etl_completo = df_logs[df_logs['proceso_nombre'] == 'ETL_COMPLETO'].copy()

            if not df_etl_completo.empty:
                import plotly.express as px

                fig = px.bar(
                    df_etl_completo.head(10),
                    x='fecha_inicio',
                    y='duracion_segundos',
                    color='estado',
                    title='Duración de Últimas 10 Ejecuciones ETL Completas',
                    labels={'duracion_segundos': 'Duración (segundos)', 'fecha_inicio': 'Fecha'}
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hay logs de ejecuciones ETL aún.")

    except Exception as e:
        st.error(f"Error cargando historial: {str(e)}")

# ============================================================================
# TAB 3: MÉTRICAS
# ============================================================================
with tab3:
    st.header("📊 Métricas del Data Warehouse")

    if st.button("🔄 Actualizar Métricas"):
        st.rerun()

    try:
        conn_dw = DatabaseConnection.get_dw_connection(use_secrets=True)
        cursor = conn_dw.cursor()

        # Métricas de dimensiones
        st.subheader("📊 Dimensiones")

        dim_metrics = []
        for dim in ['tiempo', 'producto', 'cliente', 'geografia', 'almacen',
                   'dispositivo', 'navegador', 'tipo_evento', 'estado_venta', 'metodo_pago']:
            cursor.execute(f"SELECT COUNT(*) FROM dim_{dim}")
            count = cursor.fetchone()[0]
            dim_metrics.append({'Dimensión': f'dim_{dim}', 'Registros': count})

        df_dim = pd.DataFrame(dim_metrics)

        col1, col2 = st.columns(2)

        with col1:
            st.dataframe(df_dim.head(5), use_container_width=True)

        with col2:
            st.dataframe(df_dim.tail(5), use_container_width=True)

        # Métricas de hechos
        st.subheader("📈 Tablas de Hechos")

        fact_metrics = []
        for fact in ['ventas', 'comportamiento_web', 'busquedas']:
            cursor.execute(f"SELECT COUNT(*) FROM fact_{fact}")
            count = cursor.fetchone()[0]
            fact_metrics.append({'Tabla de Hechos': f'fact_{fact}', 'Registros': count})

        df_fact = pd.DataFrame(fact_metrics)
        st.dataframe(df_fact, use_container_width=True)

        # Métricas de negocio
        st.subheader("💰 Métricas de Negocio")

        col1, col2, col3 = st.columns(3)

        # Total ventas (contar ventas únicas, no detalles)
        cursor.execute("""
            SELECT
                COUNT(DISTINCT venta_id) as total_ventas,
                SUM(monto_total) as monto_total,
                AVG(monto_total) as promedio
            FROM fact_ventas
            WHERE venta_cancelada = 0
        """)
        row = cursor.fetchone()

        with col1:
            st.metric("Total Ventas (Facturas)", f"{row[0]:,}")

        with col2:
            st.metric("Monto Total", f"₡{row[1]:,.2f}" if row[1] else "₡0.00")

        with col3:
            st.metric("Ticket Promedio", f"₡{row[2]:,.2f}" if row[2] else "₡0.00")

        # Eventos web
        cursor.execute("""
            SELECT
                COUNT(*) as total_eventos,
                SUM(CASE WHEN genero_venta = 1 THEN 1 ELSE 0 END) as eventos_conversion,
                AVG(tiempo_pagina_segundos) as promedio_tiempo
            FROM fact_comportamiento_web
        """)
        row = cursor.fetchone()

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Eventos Web", f"{row[0]:,}")

        with col2:
            st.metric("Eventos con Conversión", f"{row[1]:,}")

        with col3:
            st.metric("Tiempo Promedio (s)", f"{row[2]:.1f}" if row[2] else "0.0")

        conn_dw.close()

    except Exception as e:
        st.error(f"Error cargando métricas: {str(e)}")

st.markdown("---")
st.caption("Sistema de Analítica Empresarial - Ecommerce Data Warehouse © 2025")
