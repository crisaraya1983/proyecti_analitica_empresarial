import pyodbc
from datetime import datetime
from typing import Optional
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ETLLogger:

    def __init__(self, conn_dw: pyodbc.Connection):
        self.conn_dw = conn_dw
        self.log_id: Optional[int] = None
        self.fecha_inicio: Optional[datetime] = None

    def iniciar_proceso(
        self,
        proceso_nombre: str,
        tabla_destino: str
    ) -> int:

        self.fecha_inicio = datetime.now()

        cursor = self.conn_dw.cursor()
        cursor.execute("""
            INSERT INTO etl_logs (
                proceso_nombre,
                tabla_destino,
                fecha_inicio,
                estado
            )
            VALUES (?, ?, ?, 'INICIADO')
        """, (proceso_nombre, tabla_destino, self.fecha_inicio))

        self.conn_dw.commit()

        # Obtener el ID del log insertado
        cursor.execute("SELECT @@IDENTITY AS log_id")
        self.log_id = cursor.fetchone()[0]
        cursor.close()

        logger.info(f"Proceso iniciado: {proceso_nombre} -> {tabla_destino} (log_id={self.log_id})")

        return self.log_id

    def finalizar_proceso(
        self,
        registros_extraidos: int = 0,
        registros_insertados: int = 0,
        registros_actualizados: int = 0,
        registros_error: int = 0,
        estado: str = "COMPLETADO",
        mensaje_error: Optional[str] = None
    ):

        if self.log_id is None:
            logger.warning("No se puede finalizar proceso: log_id no existe")
            return

        fecha_fin = datetime.now()
        duracion_segundos = int((fecha_fin - self.fecha_inicio).total_seconds())

        cursor = self.conn_dw.cursor()
        cursor.execute("""
            UPDATE etl_logs
            SET
                fecha_fin = ?,
                duracion_segundos = ?,
                registros_extraidos = ?,
                registros_insertados = ?,
                registros_actualizados = ?,
                registros_error = ?,
                estado = ?,
                mensaje_error = ?
            WHERE log_id = ?
        """, (
            fecha_fin,
            duracion_segundos,
            registros_extraidos,
            registros_insertados,
            registros_actualizados,
            registros_error,
            estado,
            mensaje_error,
            self.log_id
        ))

        self.conn_dw.commit()
        cursor.close()

        log_msg = (
            f"Proceso finalizado (log_id={self.log_id}): "
            f"{estado} | Duración: {duracion_segundos}s | "
            f"Extraídos: {registros_extraidos} | "
            f"Insertados: {registros_insertados} | "
            f"Actualizados: {registros_actualizados} | "
            f"Errores: {registros_error}"
        )

        if estado == "ERROR":
            logger.error(log_msg)
            if mensaje_error:
                logger.error(f"Error: {mensaje_error}")
        else:
            logger.info(log_msg)

    def registrar_error(self, mensaje_error: str, registros_extraidos: int = 0):

        self.finalizar_proceso(
            registros_extraidos=registros_extraidos,
            estado="ERROR",
            mensaje_error=mensaje_error
        )

    @staticmethod
    def obtener_ultimos_logs(conn_dw: pyodbc.Connection, limite: int = 10) -> list:

        cursor = conn_dw.cursor()
        cursor.execute(f"""
            SELECT TOP {limite}
                log_id,
                proceso_nombre,
                tabla_destino,
                fecha_inicio,
                fecha_fin,
                duracion_segundos,
                registros_extraidos,
                registros_insertados,
                registros_actualizados,
                registros_error,
                estado,
                mensaje_error
            FROM etl_logs
            ORDER BY fecha_inicio DESC
        """)

        columns = [column[0] for column in cursor.description]
        logs = []

        for row in cursor.fetchall():
            log_dict = dict(zip(columns, row))
            logs.append(log_dict)

        cursor.close()
        return logs

    @staticmethod
    def obtener_resumen_ejecucion(conn_dw: pyodbc.Connection) -> dict:

        cursor = conn_dw.cursor()

        cursor.execute("""
            SELECT TOP 1 fecha_inicio
            FROM etl_logs
            WHERE proceso_nombre = 'ETL_COMPLETO'
            ORDER BY fecha_inicio DESC
        """)

        row = cursor.fetchone()
        if not row:
            cursor.close()
            return {}

        ultima_fecha = row[0]

        cursor.execute("""
            SELECT
                COUNT(*) as total_procesos,
                SUM(registros_extraidos) as total_extraidos,
                SUM(registros_insertados) as total_insertados,
                SUM(registros_actualizados) as total_actualizados,
                SUM(registros_error) as total_errores,
                SUM(duracion_segundos) as duracion_total,
                MIN(fecha_inicio) as inicio,
                MAX(fecha_fin) as fin,
                SUM(CASE WHEN estado = 'ERROR' THEN 1 ELSE 0 END) as procesos_error
            FROM etl_logs
            WHERE fecha_inicio >= ?
        """, (ultima_fecha,))

        row = cursor.fetchone()
        cursor.close()

        if not row:
            return {}

        return {
            "total_procesos": row[0] or 0,
            "total_extraidos": row[1] or 0,
            "total_insertados": row[2] or 0,
            "total_actualizados": row[3] or 0,
            "total_errores": row[4] or 0,
            "duracion_total": row[5] or 0,
            "fecha_inicio": row[6],
            "fecha_fin": row[7],
            "procesos_error": row[8] or 0
        }
