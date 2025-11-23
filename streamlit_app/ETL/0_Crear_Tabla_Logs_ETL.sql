/*
================================================================================
CREACIÓN DE TABLA DE LOGS PARA PROCESO ETL
================================================================================
Autor: Sistema de Analítica Empresarial
Fecha: 2025-01-15
Propósito: Registrar todas las ejecuciones del proceso ETL con métricas
================================================================================
*/

USE Ecommerce_DW;
GO

PRINT '';
PRINT '================================================================================';
PRINT 'CREANDO TABLA DE LOGS ETL';
PRINT '================================================================================';
GO

-- Eliminar tabla si existe
IF OBJECT_ID('etl_logs', 'U') IS NOT NULL
    DROP TABLE etl_logs;
GO

CREATE TABLE etl_logs (
    log_id              BIGINT IDENTITY(1,1) PRIMARY KEY,
    proceso_nombre      NVARCHAR(100) NOT NULL,         -- Nombre del proceso ETL
    tabla_destino       NVARCHAR(100) NOT NULL,         -- Tabla que se cargó
    fecha_inicio        DATETIME NOT NULL,              -- Inicio del proceso
    fecha_fin           DATETIME,                       -- Fin del proceso
    duracion_segundos   INT,                            -- Duración en segundos
    registros_extraidos INT DEFAULT 0,                  -- Registros leídos de OLTP
    registros_insertados INT DEFAULT 0,                 -- Registros insertados en DW
    registros_actualizados INT DEFAULT 0,               -- Registros actualizados
    registros_error     INT DEFAULT 0,                  -- Registros con error
    estado              NVARCHAR(20) NOT NULL,          -- INICIADO, COMPLETADO, ERROR
    mensaje_error       NVARCHAR(MAX),                  -- Mensaje de error si aplica
    usuario_ejecucion   NVARCHAR(100) DEFAULT SUSER_NAME(),
    fecha_creacion      DATETIME DEFAULT GETDATE()
);

CREATE INDEX IX_etl_logs_proceso ON etl_logs(proceso_nombre);
CREATE INDEX IX_etl_logs_tabla ON etl_logs(tabla_destino);
CREATE INDEX IX_etl_logs_fecha_inicio ON etl_logs(fecha_inicio);
CREATE INDEX IX_etl_logs_estado ON etl_logs(estado);

PRINT '✓ Tabla etl_logs creada exitosamente';
PRINT '';
PRINT 'Estructura de la tabla:';
PRINT '  • log_id: ID único del log';
PRINT '  • proceso_nombre: Nombre del proceso ETL';
PRINT '  • tabla_destino: Tabla destino de la carga';
PRINT '  • fecha_inicio/fin: Timestamps del proceso';
PRINT '  • duracion_segundos: Duración total';
PRINT '  • registros_*: Métricas de registros procesados';
PRINT '  • estado: INICIADO, COMPLETADO, ERROR';
PRINT '  • mensaje_error: Detalles si hubo error';
PRINT '';
PRINT '================================================================================';
PRINT 'TABLA DE LOGS ETL LISTA PARA USO';
PRINT '================================================================================';
GO
