"""
================================================================================
PIPELINE PRINCIPAL ETL - ECOMMERCE DATA WAREHOUSE
================================================================================
Autor: Sistema de Analítica Empresarial
Fecha: 2025-01-15
Propósito: Orquestar el proceso completo de ETL desde OLTP a DW
================================================================================
"""

import sys
import os
from datetime import datetime
import logging

# Agregar ruta actual al path para imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import DatabaseConfig
from etl_logger import ETLLogger
from load_dimensions import DimensionLoader
from load_facts import FactLoader

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """Clase principal para el pipeline ETL"""

    def __init__(self, use_secrets: bool = False):
        """
        Inicializa el pipeline ETL

        Args:
            use_secrets: Si True, usa Streamlit secrets para conexión
        """
        self.use_secrets = use_secrets
        self.conn_oltp = None
        self.conn_dw = None
        self.results = {
            'success': False,
            'inicio': None,
            'fin': None,
            'duracion_segundos': 0,
            'dimensiones': {},
            'hechos': {},
            'errores': []
        }

    def conectar_bases_datos(self):
        """Establece conexiones a bases de datos OLTP y DW"""
        logger.info("=" * 80)
        logger.info("CONECTANDO A BASES DE DATOS")
        logger.info("=" * 80)

        try:
            logger.info("Conectando a Ecommerce_OLTP...")
            self.conn_oltp = DatabaseConfig.get_oltp_connection(self.use_secrets)
            logger.info("✓ Conexión a OLTP exitosa")

            logger.info("Conectando a Ecommerce_DW...")
            self.conn_dw = DatabaseConfig.get_dw_connection(self.use_secrets)
            logger.info("✓ Conexión a DW exitosa")

        except Exception as e:
            logger.error(f"Error conectando a bases de datos: {str(e)}")
            raise

    def desconectar_bases_datos(self):
        """Cierra conexiones a bases de datos"""
        logger.info("Cerrando conexiones a bases de datos...")

        if self.conn_oltp:
            self.conn_oltp.close()
            logger.info("✓ Conexión OLTP cerrada")

        if self.conn_dw:
            self.conn_dw.close()
            logger.info("✓ Conexión DW cerrada")

    def validar_prerequisitos(self) -> bool:
        """
        Valida que las bases de datos y tablas necesarias existan

        Returns:
            True si todo está correcto, False si hay errores
        """
        logger.info("=" * 80)
        logger.info("VALIDANDO PREREQUISITOS")
        logger.info("=" * 80)

        try:
            # Validar tablas OLTP
            cursor_oltp = self.conn_oltp.cursor()
            cursor_oltp.execute("""
                SELECT COUNT(*) as count
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME IN (
                    'tiempo', 'provincias', 'cantones', 'distritos',
                    'productos', 'categorias', 'clientes', 'almacenes',
                    'ventas', 'detalles_venta', 'eventos_web', 'busquedas_web'
                )
            """)
            count_oltp = cursor_oltp.fetchone()[0]
            logger.info(f"Tablas OLTP encontradas: {count_oltp}/12")

            if count_oltp < 12:
                logger.error("⚠ Faltan tablas en OLTP")
                return False

            # Validar tablas DW
            cursor_dw = self.conn_dw.cursor()
            cursor_dw.execute("""
                SELECT COUNT(*) as count
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME LIKE 'dim_%' OR TABLE_NAME LIKE 'fact_%'
            """)
            count_dw = cursor_dw.fetchone()[0]
            logger.info(f"Tablas DW encontradas: {count_dw}/14")

            if count_dw < 14:
                logger.error("⚠ Faltan tablas en DW")
                return False

            # Validar que haya datos en OLTP
            cursor_oltp.execute("SELECT COUNT(*) FROM tiempo")
            count_tiempo = cursor_oltp.fetchone()[0]

            cursor_oltp.execute("SELECT COUNT(*) FROM productos")
            count_productos = cursor_oltp.fetchone()[0]

            cursor_oltp.execute("SELECT COUNT(*) FROM clientes")
            count_clientes = cursor_oltp.fetchone()[0]

            cursor_oltp.execute("SELECT COUNT(*) FROM ventas")
            count_ventas = cursor_oltp.fetchone()[0]

            logger.info(f"Registros en OLTP:")
            logger.info(f"  • tiempo: {count_tiempo:,}")
            logger.info(f"  • productos: {count_productos:,}")
            logger.info(f"  • clientes: {count_clientes:,}")
            logger.info(f"  • ventas: {count_ventas:,}")

            if count_tiempo == 0:
                logger.error("⚠ No hay datos en tabla 'tiempo' (requerido)")
                return False

            if count_productos == 0 or count_clientes == 0 or count_ventas == 0:
                logger.warning("⚠ Algunas tablas OLTP están vacías")

            logger.info("✓ Prerequisitos validados correctamente")
            return True

        except Exception as e:
            logger.error(f"Error validando prerequisitos: {str(e)}")
            return False

    def ejecutar_dimensiones(self):
        """Ejecuta la carga de todas las dimensiones"""
        logger.info("\n" + "=" * 80)
        logger.info("FASE 1: CARGA DE DIMENSIONES")
        logger.info("=" * 80 + "\n")

        try:
            dimension_loader = DimensionLoader(self.conn_oltp, self.conn_dw)
            self.results['dimensiones'] = dimension_loader.load_all_dimensions()

            # Resumen de dimensiones
            logger.info("\n" + "-" * 80)
            logger.info("RESUMEN CARGA DE DIMENSIONES")
            logger.info("-" * 80)

            total_dim_extraidos = 0
            total_dim_insertados = 0

            for dim_nombre, (extraidos, insertados) in self.results['dimensiones'].items():
                logger.info(f"{dim_nombre:30} | Extraídos: {extraidos:>8,} | Insertados: {insertados:>8,}")
                total_dim_extraidos += extraidos
                total_dim_insertados += insertados

            logger.info("-" * 80)
            logger.info(f"{'TOTAL':30} | Extraídos: {total_dim_extraidos:>8,} | Insertados: {total_dim_insertados:>8,}")
            logger.info("-" * 80 + "\n")

        except Exception as e:
            logger.error(f"Error ejecutando carga de dimensiones: {str(e)}")
            self.results['errores'].append(f"Dimensiones: {str(e)}")
            raise

    def ejecutar_hechos(self):
        """Ejecuta la carga de todas las tablas de hechos"""
        logger.info("\n" + "=" * 80)
        logger.info("FASE 2: CARGA DE TABLAS DE HECHOS")
        logger.info("=" * 80 + "\n")

        try:
            fact_loader = FactLoader(self.conn_oltp, self.conn_dw)
            self.results['hechos'] = fact_loader.load_all_facts()

            # Resumen de hechos
            logger.info("\n" + "-" * 80)
            logger.info("RESUMEN CARGA DE HECHOS")
            logger.info("-" * 80)

            total_fact_extraidos = 0
            total_fact_insertados = 0

            for fact_nombre, (extraidos, insertados) in self.results['hechos'].items():
                logger.info(f"{fact_nombre:30} | Extraídos: {extraidos:>8,} | Insertados: {insertados:>8,}")
                total_fact_extraidos += extraidos
                total_fact_insertados += insertados

            logger.info("-" * 80)
            logger.info(f"{'TOTAL':30} | Extraídos: {total_fact_extraidos:>8,} | Insertados: {total_fact_insertados:>8,}")
            logger.info("-" * 80 + "\n")

        except Exception as e:
            logger.error(f"Error ejecutando carga de hechos: {str(e)}")
            self.results['errores'].append(f"Hechos: {str(e)}")
            raise

    def validar_resultados(self):
        """Valida que los datos se hayan cargado correctamente"""
        logger.info("\n" + "=" * 80)
        logger.info("VALIDANDO RESULTADOS")
        logger.info("=" * 80)

        try:
            cursor_dw = self.conn_dw.cursor()

            # Validar counts en DW
            logger.info("\nRegistros cargados en DW:")

            # Dimensiones
            for dim in ['tiempo', 'producto', 'cliente', 'geografia', 'almacen',
                       'dispositivo', 'navegador', 'tipo_evento', 'estado_venta', 'metodo_pago', 'sesion']:
                cursor_dw.execute(f"SELECT COUNT(*) FROM dim_{dim}")
                count = cursor_dw.fetchone()[0]
                logger.info(f"  dim_{dim:20} : {count:>10,}")

            # Hechos
            for fact in ['ventas', 'comportamiento_web', 'busquedas']:
                cursor_dw.execute(f"SELECT COUNT(*) FROM fact_{fact}")
                count = cursor_dw.fetchone()[0]
                logger.info(f"  fact_{fact:20} : {count:>10,}")

            # Validar totales de ventas (debe coincidir OLTP vs DW)
            cursor_oltp = self.conn_oltp.cursor()

            cursor_oltp.execute("SELECT SUM(monto_total) FROM detalles_venta")
            total_oltp = cursor_oltp.fetchone()[0] or 0

            cursor_dw.execute("SELECT SUM(monto_total) FROM fact_ventas WHERE venta_cancelada = 0")
            total_dw = cursor_dw.fetchone()[0] or 0

            logger.info(f"\nValidación de totales:")
            logger.info(f"  Total ventas OLTP: ₡{total_oltp:>15,.2f}")
            logger.info(f"  Total ventas DW:   ₡{total_dw:>15,.2f}")

            diferencia = abs(total_oltp - total_dw)
            if diferencia < 0.01:  # Considerar igual si diferencia < 1 céntimo
                logger.info("  ✓ Totales coinciden")
            else:
                logger.warning(f"  ⚠ Diferencia: ₡{diferencia:,.2f}")

            logger.info("\n✓ Validación completada")

        except Exception as e:
            logger.error(f"Error validando resultados: {str(e)}")
            self.results['errores'].append(f"Validación: {str(e)}")

    def ejecutar(self) -> dict:
        """
        Ejecuta el pipeline ETL completo

        Returns:
            Diccionario con resultados de la ejecución
        """
        self.results['inicio'] = datetime.now()
        etl_logger = None

        try:
            # Conectar a bases de datos
            self.conectar_bases_datos()

            # Iniciar log en DW
            etl_logger = ETLLogger(self.conn_dw)
            etl_logger.iniciar_proceso("ETL_COMPLETO", "ALL")

            # Validar prerequisitos
            if not self.validar_prerequisitos():
                raise Exception("Prerequisitos no cumplidos")

            # Ejecutar carga de dimensiones
            self.ejecutar_dimensiones()

            # Ejecutar carga de hechos
            self.ejecutar_hechos()

            # Validar resultados
            self.validar_resultados()

            # Marcar como exitoso
            self.results['success'] = True
            self.results['fin'] = datetime.now()
            self.results['duracion_segundos'] = int(
                (self.results['fin'] - self.results['inicio']).total_seconds()
            )

            # Calcular totales
            total_extraidos = sum(r[0] for r in self.results['dimensiones'].values())
            total_extraidos += sum(r[0] for r in self.results['hechos'].values())

            total_insertados = sum(r[1] for r in self.results['dimensiones'].values())
            total_insertados += sum(r[1] for r in self.results['hechos'].values())

            # Finalizar log
            if etl_logger:
                etl_logger.finalizar_proceso(
                    registros_extraidos=total_extraidos,
                    registros_insertados=total_insertados,
                    estado="COMPLETADO"
                )

            # Resumen final
            logger.info("\n" + "=" * 80)
            logger.info("PROCESO ETL COMPLETADO EXITOSAMENTE")
            logger.info("=" * 80)
            logger.info(f"Inicio:    {self.results['inicio'].strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Fin:       {self.results['fin'].strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Duración:  {self.results['duracion_segundos']} segundos")
            logger.info(f"Registros extraídos:  {total_extraidos:,}")
            logger.info(f"Registros insertados: {total_insertados:,}")
            logger.info("=" * 80 + "\n")

        except Exception as e:
            self.results['success'] = False
            self.results['fin'] = datetime.now()
            self.results['duracion_segundos'] = int(
                (self.results['fin'] - self.results['inicio']).total_seconds()
            )
            self.results['errores'].append(str(e))

            if etl_logger:
                etl_logger.registrar_error(str(e))

            logger.error("\n" + "=" * 80)
            logger.error("PROCESO ETL FINALIZADO CON ERRORES")
            logger.error("=" * 80)
            logger.error(f"Error: {str(e)}")
            logger.error("=" * 80 + "\n")

        finally:
            # Desconectar bases de datos
            self.desconectar_bases_datos()

        return self.results


def main():
    """Función principal para ejecutar ETL desde línea de comandos"""
    logger.info("\n" + "=" * 80)
    logger.info("ECOMMERCE ETL PIPELINE")
    logger.info("=" * 80 + "\n")

    pipeline = ETLPipeline(use_secrets=False)
    results = pipeline.ejecutar()

    # Retornar código de salida
    sys.exit(0 if results['success'] else 1)


if __name__ == "__main__":
    main()
