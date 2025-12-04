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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ETLPipeline:

    def __init__(self, use_secrets: bool = False):

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
        logger.info("Cerrando conexiones a bases de datos...")

        if self.conn_oltp:
            self.conn_oltp.close()
            logger.info("✓ Conexión OLTP cerrada")

        if self.conn_dw:
            self.conn_dw.close()
            logger.info("✓ Conexión DW cerrada")

    def validar_prerequisitos(self) -> bool:

        logger.info("=" * 80)
        logger.info("VALIDANDO PREREQUISITOS")
        logger.info("=" * 80)

        try:
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
        logger.info("\n" + "=" * 80)
        logger.info("FASE 1: CARGA DE DIMENSIONES")
        logger.info("=" * 80 + "\n")

        try:
            dimension_loader = DimensionLoader(self.conn_oltp, self.conn_dw)
            self.results['dimensiones'] = dimension_loader.load_all_dimensions()

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
        logger.info("\n" + "=" * 80)
        logger.info("FASE 2: CARGA DE TABLAS DE HECHOS")
        logger.info("=" * 80 + "\n")

        try:
            fact_loader = FactLoader(self.conn_oltp, self.conn_dw)
            self.results['hechos'] = fact_loader.load_all_facts()

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
        logger.info("\n" + "=" * 80)
        logger.info("VALIDANDO RESULTADOS")
        logger.info("=" * 80)

        try:
            cursor_dw = self.conn_dw.cursor()

            logger.info("\nRegistros cargados en DW:")

            for dim in ['tiempo', 'producto', 'cliente', 'geografia', 'almacen',
                       'dispositivo', 'navegador', 'tipo_evento', 'estado_venta', 'metodo_pago', 'sesion']:
                cursor_dw.execute(f"SELECT COUNT(*) FROM dim_{dim}")
                count = cursor_dw.fetchone()[0]
                logger.info(f"  dim_{dim:20} : {count:>10,}")

            for fact in ['ventas', 'comportamiento_web', 'busquedas']:
                cursor_dw.execute(f"SELECT COUNT(*) FROM fact_{fact}")
                count = cursor_dw.fetchone()[0]
                logger.info(f"  fact_{fact:20} : {count:>10,}")

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

        self.results['inicio'] = datetime.now()
        etl_logger = None

        try:
            self.conectar_bases_datos()
            etl_logger = ETLLogger(self.conn_dw)
            etl_logger.iniciar_proceso("ETL_COMPLETO", "ALL")

            if not self.validar_prerequisitos():
                raise Exception("Prerequisitos no cumplidos")

            self.ejecutar_dimensiones()

            self.ejecutar_hechos()

            self.validar_resultados()

            self.results['success'] = True
            self.results['fin'] = datetime.now()
            self.results['duracion_segundos'] = int(
                (self.results['fin'] - self.results['inicio']).total_seconds()
            )

            total_extraidos = sum(r[0] for r in self.results['dimensiones'].values())
            total_extraidos += sum(r[0] for r in self.results['hechos'].values())

            total_insertados = sum(r[1] for r in self.results['dimensiones'].values())
            total_insertados += sum(r[1] for r in self.results['hechos'].values())

            if etl_logger:
                etl_logger.finalizar_proceso(
                    registros_extraidos=total_extraidos,
                    registros_insertados=total_insertados,
                    estado="COMPLETADO"
                )

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
            self.desconectar_bases_datos()

        return self.results


def main():
    logger.info("\n" + "=" * 80)
    logger.info("ECOMMERCE ETL PIPELINE")
    logger.info("=" * 80 + "\n")

    pipeline = ETLPipeline(use_secrets=False)
    results = pipeline.ejecutar()

    sys.exit(0 if results['success'] else 1)


if __name__ == "__main__":
    main()
