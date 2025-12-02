-- =============================================
-- Script: Creación de Base de Datos Dimensional - Ecommerce
-- Descripción: Script completo para crear la base de datos dimensional (DW)
--              con todas las dimensiones, tablas de hechos, índices y tabla de logs ETL
-- Nota: Este script crea las estructuras vacías (sin datos)
-- =============================================

-- Crear la base de datos
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'Ecommerce_DW')
BEGIN
    CREATE DATABASE Ecommerce_DW;
END
GO

-- Configurar tamaños de archivos
ALTER DATABASE Ecommerce_DW
MODIFY FILE (NAME = 'Ecommerce_DW', SIZE = 100MB, MAXSIZE = UNLIMITED, FILEGROWTH = 50MB);
GO

ALTER DATABASE Ecommerce_DW
MODIFY FILE (NAME = 'Ecommerce_DW_log', SIZE = 50MB, MAXSIZE = 500MB, FILEGROWTH = 25MB);
GO

USE Ecommerce_DW;
GO

-- =============================================
-- TABLAS DIMENSIONALES
-- =============================================

-- =============================================
-- Dimensión: dim_tiempo
-- =============================================
CREATE TABLE dim_tiempo (
    ID_FECHA            INT PRIMARY KEY,
    FECHA_CAL           DATE NOT NULL,
    DIA_CAL             INT NOT NULL,
    DIA_SEMANA          INT NOT NULL,
    DIA_SEMANA_NOMBRE   NVARCHAR(20) NOT NULL,
    DIA_SEMANA_ABR      NVARCHAR(3) NOT NULL,
    SEMANA_CAL          INT NOT NULL,
    MES_CAL             INT NOT NULL,
    MES_NOMBRE          NVARCHAR(20) NOT NULL,
    MES_ABR             NVARCHAR(3) NOT NULL,
    TRIMESTRE           INT NOT NULL,
    ANIO_CAL            INT NOT NULL,
    ES_FIN_SEMANA       BIT NOT NULL,
    ES_FERIADO          BIT NOT NULL,
    NOMBRE_FERIADO      NVARCHAR(100),
    ANIO_MES            NVARCHAR(7) NOT NULL,
    DIA_ANIO            INT NOT NULL,
    SEMANA_MES          INT NOT NULL,
    QUINCENA            INT NOT NULL,
    PERIODO_DIA         NVARCHAR(20) NOT NULL
);

-- Índices para dim_tiempo
CREATE INDEX IX_dim_tiempo_FECHA_CAL ON dim_tiempo(FECHA_CAL);
CREATE INDEX IX_dim_tiempo_ANIO_MES ON dim_tiempo(ANIO_CAL, MES_CAL);
CREATE INDEX IX_dim_tiempo_TRIMESTRE ON dim_tiempo(ANIO_CAL, TRIMESTRE);
GO

-- =============================================
-- Dimensión: dim_producto
-- =============================================
CREATE TABLE dim_producto (
    producto_id         INT PRIMARY KEY,
    codigo_producto     NVARCHAR(50) NOT NULL UNIQUE,
    nombre              NVARCHAR(255) NOT NULL,
    categoria_id        INT NOT NULL,
    categoria           NVARCHAR(100) NOT NULL,
    precio_unitario     DECIMAL(12,2) NOT NULL,
    costo_unitario      DECIMAL(12,2) NOT NULL,
    margen_unitario     DECIMAL(12,2) NOT NULL,
    descripcion         NVARCHAR(MAX)
);

-- Índices para dim_producto
CREATE INDEX IX_dim_producto_categoria_id ON dim_producto(categoria_id);
CREATE INDEX IX_dim_producto_categoria ON dim_producto(categoria);
CREATE INDEX IX_dim_producto_codigo ON dim_producto(codigo_producto);
CREATE INDEX IX_dim_producto_nombre ON dim_producto(nombre);
CREATE INDEX IX_dim_producto_precio ON dim_producto(precio_unitario);
GO

-- =============================================
-- Dimensión: dim_cliente
-- =============================================
CREATE TABLE dim_cliente (
    cliente_id          INT PRIMARY KEY,
    nombre              NVARCHAR(100) NOT NULL,
    apellido            NVARCHAR(100) NOT NULL,
    nombre_completo     NVARCHAR(201) NOT NULL,
    email               NVARCHAR(255) NOT NULL,
    telefono            NVARCHAR(20),
    genero              NVARCHAR(20),
    edad                INT,
    rango_edad          NVARCHAR(20),
    fecha_registro      DATE NOT NULL,
    antiguedad_anios    INT NOT NULL,
    segmento_cliente    NVARCHAR(50) NOT NULL
);

-- Índices para dim_cliente
CREATE INDEX IX_dim_cliente_genero ON dim_cliente(genero);
CREATE INDEX IX_dim_cliente_edad ON dim_cliente(edad);
CREATE INDEX IX_dim_cliente_rango_edad ON dim_cliente(rango_edad);
CREATE INDEX IX_dim_cliente_segmento ON dim_cliente(segmento_cliente);
CREATE INDEX IX_dim_cliente_fecha_registro ON dim_cliente(fecha_registro);
CREATE INDEX IX_dim_cliente_email ON dim_cliente(email);
CREATE INDEX IX_dim_cliente_nombre_completo ON dim_cliente(nombre_completo);
CREATE INDEX IX_dim_cliente_antiguedad ON dim_cliente(antiguedad_anios);
GO

-- =============================================
-- Dimensión: dim_geografia
-- =============================================
CREATE TABLE dim_geografia (
    provincia_id        INT NOT NULL,
    canton_id           INT NOT NULL,
    distrito_id         INT NOT NULL,
    provincia           NVARCHAR(100) NOT NULL,
    canton              NVARCHAR(100) NOT NULL,
    distrito            NVARCHAR(100) NOT NULL,
    CONSTRAINT PK_dim_geografia PRIMARY KEY (provincia_id, canton_id, distrito_id)
);

-- Índices para dim_geografia
CREATE INDEX IX_dim_geografia_provincia ON dim_geografia(provincia_id);
CREATE INDEX IX_dim_geografia_canton ON dim_geografia(provincia_id, canton_id);
CREATE INDEX IX_dim_geografia_provincia_nombre ON dim_geografia(provincia);
CREATE INDEX IX_dim_geografia_canton_nombre ON dim_geografia(provincia, canton);
GO

-- =============================================
-- Dimensión: dim_almacen
-- =============================================
CREATE TABLE dim_almacen (
    almacen_id          INT PRIMARY KEY,
    nombre              NVARCHAR(100) NOT NULL,
    provincia_id        INT NOT NULL,
    canton_id           INT NOT NULL,
    distrito_id         INT NOT NULL,
    provincia           NVARCHAR(100) NOT NULL,
    canton              NVARCHAR(100) NOT NULL,
    distrito            NVARCHAR(100) NOT NULL,
    direccion           NVARCHAR(255),
    telefono            NVARCHAR(20)
);

-- Índices para dim_almacen
CREATE INDEX IX_dim_almacen_nombre ON dim_almacen(nombre);
CREATE INDEX IX_dim_almacen_provincia ON dim_almacen(provincia_id);
CREATE INDEX IX_dim_almacen_canton ON dim_almacen(provincia_id, canton_id);
CREATE INDEX IX_dim_almacen_distrito ON dim_almacen(provincia_id, canton_id, distrito_id);
GO

-- =============================================
-- Dimensión: dim_dispositivo
-- =============================================
CREATE TABLE dim_dispositivo (
    dispositivo_id      INT IDENTITY(1,1) PRIMARY KEY,
    dispositivo         NVARCHAR(50) NOT NULL UNIQUE,
    tipo_dispositivo    NVARCHAR(20) NOT NULL,
    es_movil            BIT NOT NULL
);

-- Índices para dim_dispositivo
CREATE INDEX IX_dim_dispositivo_tipo ON dim_dispositivo(tipo_dispositivo);
CREATE INDEX IX_dim_dispositivo_es_movil ON dim_dispositivo(es_movil);
GO

-- =============================================
-- Dimensión: dim_navegador
-- =============================================
CREATE TABLE dim_navegador (
    navegador_id        INT IDENTITY(1,1) PRIMARY KEY,
    navegador           NVARCHAR(50) NOT NULL UNIQUE,
    familia_navegador   NVARCHAR(50) NOT NULL
);

-- Índices para dim_navegador
CREATE INDEX IX_dim_navegador_familia ON dim_navegador(familia_navegador);
GO

-- =============================================
-- Dimensión: dim_tipo_evento
-- =============================================
CREATE TABLE dim_tipo_evento (
    tipo_evento_id      INT IDENTITY(1,1) PRIMARY KEY,
    tipo_evento         NVARCHAR(50) NOT NULL UNIQUE,
    categoria_evento    NVARCHAR(50) NOT NULL,
    nivel_interes       NVARCHAR(20) NOT NULL
);

-- Índices para dim_tipo_evento
CREATE INDEX IX_dim_tipo_evento_categoria ON dim_tipo_evento(categoria_evento);
CREATE INDEX IX_dim_tipo_evento_nivel ON dim_tipo_evento(nivel_interes);
GO

-- =============================================
-- Dimensión: dim_estado_venta
-- =============================================
CREATE TABLE dim_estado_venta (
    estado_venta_id     INT IDENTITY(1,1) PRIMARY KEY,
    estado_venta        NVARCHAR(50) NOT NULL UNIQUE,
    descripcion         NVARCHAR(255)
);

-- Índices para dim_estado_venta
CREATE INDEX IX_dim_estado_venta_estado ON dim_estado_venta(estado_venta);
GO

-- =============================================
-- Dimensión: dim_metodo_pago
-- =============================================
CREATE TABLE dim_metodo_pago (
    metodo_pago_id      INT IDENTITY(1,1) PRIMARY KEY,
    metodo_pago         NVARCHAR(50) NOT NULL UNIQUE,
    tipo_metodo         NVARCHAR(50) NOT NULL,
    requiere_aprobacion BIT NOT NULL
);

-- Índices para dim_metodo_pago
CREATE INDEX IX_dim_metodo_pago_tipo ON dim_metodo_pago(tipo_metodo);
GO

-- =============================================
-- Dimensión: dim_sesion
-- =============================================
CREATE TABLE dim_sesion (
    evento_id           INT PRIMARY KEY,
    codigo_sesion       NVARCHAR(100) NOT NULL,
    fecha_hora_evento   DATETIME NOT NULL
);

-- Índices para dim_sesion
CREATE INDEX IX_dim_sesion_codigo_sesion ON dim_sesion(codigo_sesion);
CREATE INDEX IX_dim_sesion_fecha_hora ON dim_sesion(fecha_hora_evento);
GO

-- =============================================
-- TABLAS DE HECHOS
-- =============================================

-- =============================================
-- Tabla de Hechos: fact_ventas
-- =============================================
CREATE TABLE fact_ventas (
    venta_detalle_key   BIGINT IDENTITY(1,1) PRIMARY KEY,
    tiempo_key          INT NOT NULL,
    producto_id         INT NOT NULL,
    cliente_id          INT NOT NULL,
    almacen_id          INT NOT NULL,
    provincia_id        INT NOT NULL,
    canton_id           INT NOT NULL,
    distrito_id         INT NOT NULL,
    estado_venta_id     INT NOT NULL,
    metodo_pago_id      INT NOT NULL,
    -- Medidas
    cantidad            INT NOT NULL,
    precio_unitario     DECIMAL(12,2) NOT NULL,
    descuento           DECIMAL(12,2) NOT NULL,
    impuesto            DECIMAL(12,2) NOT NULL,
    monto_total         DECIMAL(12,2) NOT NULL,
    costo_total         DECIMAL(12,2) NOT NULL,
    margen              DECIMAL(12,2) NOT NULL,
    -- Degenerados
    venta_id            INT NOT NULL,
    detalle_venta_id    INT NOT NULL,
    -- Constraints
    CONSTRAINT FK_fact_ventas_tiempo
        FOREIGN KEY (tiempo_key) REFERENCES dim_tiempo(ID_FECHA),
    CONSTRAINT FK_fact_ventas_producto
        FOREIGN KEY (producto_id) REFERENCES dim_producto(producto_id),
    CONSTRAINT FK_fact_ventas_cliente
        FOREIGN KEY (cliente_id) REFERENCES dim_cliente(cliente_id),
    CONSTRAINT FK_fact_ventas_almacen
        FOREIGN KEY (almacen_id) REFERENCES dim_almacen(almacen_id),
    CONSTRAINT FK_fact_ventas_geografia
        FOREIGN KEY (provincia_id, canton_id, distrito_id)
        REFERENCES dim_geografia(provincia_id, canton_id, distrito_id),
    CONSTRAINT FK_fact_ventas_estado
        FOREIGN KEY (estado_venta_id) REFERENCES dim_estado_venta(estado_venta_id),
    CONSTRAINT FK_fact_ventas_metodo_pago
        FOREIGN KEY (metodo_pago_id) REFERENCES dim_metodo_pago(metodo_pago_id)
);

-- Índices para fact_ventas
CREATE INDEX IX_fact_ventas_tiempo ON fact_ventas(tiempo_key);
CREATE INDEX IX_fact_ventas_producto ON fact_ventas(producto_id);
CREATE INDEX IX_fact_ventas_cliente ON fact_ventas(cliente_id);
CREATE INDEX IX_fact_ventas_almacen ON fact_ventas(almacen_id);
CREATE INDEX IX_fact_ventas_geografia ON fact_ventas(provincia_id, canton_id, distrito_id);
CREATE INDEX IX_fact_ventas_estado ON fact_ventas(estado_venta_id);
CREATE INDEX IX_fact_ventas_metodo_pago ON fact_ventas(metodo_pago_id);

-- Índices compuestos para consultas comunes
CREATE INDEX IX_fact_ventas_tiempo_producto ON fact_ventas(tiempo_key, producto_id);
CREATE INDEX IX_fact_ventas_tiempo_cliente ON fact_ventas(tiempo_key, cliente_id);
CREATE INDEX IX_fact_ventas_tiempo_almacen ON fact_ventas(tiempo_key, almacen_id);

-- Columnstore para análisis OLAP
CREATE NONCLUSTERED COLUMNSTORE INDEX IX_fact_ventas_columnstore
ON fact_ventas (
    tiempo_key, producto_id, cliente_id, almacen_id,
    cantidad, precio_unitario, monto_total, margen, impuesto
);
GO

-- =============================================
-- Tabla de Hechos: fact_comportamiento_web
-- =============================================
CREATE TABLE fact_comportamiento_web (
    comportamiento_key  BIGINT IDENTITY(1,1) PRIMARY KEY,
    tiempo_key          INT NOT NULL,
    cliente_id          INT NOT NULL,
    producto_id         INT NOT NULL,
    tipo_evento_id      INT NOT NULL,
    dispositivo_id      INT NOT NULL,
    navegador_id        INT NOT NULL,
    -- Medidas
    duracion_segundos   INT,
    paginas_vistas      INT,
    -- Degenerados
    evento_id           INT NOT NULL,
    venta_id            INT,
    -- Constraints
    CONSTRAINT FK_fact_comportamiento_tiempo
        FOREIGN KEY (tiempo_key) REFERENCES dim_tiempo(ID_FECHA),
    CONSTRAINT FK_fact_comportamiento_cliente
        FOREIGN KEY (cliente_id) REFERENCES dim_cliente(cliente_id),
    CONSTRAINT FK_fact_comportamiento_producto
        FOREIGN KEY (producto_id) REFERENCES dim_producto(producto_id),
    CONSTRAINT FK_fact_comportamiento_tipo_evento
        FOREIGN KEY (tipo_evento_id) REFERENCES dim_tipo_evento(tipo_evento_id),
    CONSTRAINT FK_fact_comportamiento_dispositivo
        FOREIGN KEY (dispositivo_id) REFERENCES dim_dispositivo(dispositivo_id),
    CONSTRAINT FK_fact_comportamiento_navegador
        FOREIGN KEY (navegador_id) REFERENCES dim_navegador(navegador_id)
);

-- Índices para fact_comportamiento_web
CREATE INDEX IX_fact_comportamiento_tiempo ON fact_comportamiento_web(tiempo_key);
CREATE INDEX IX_fact_comportamiento_cliente ON fact_comportamiento_web(cliente_id);
CREATE INDEX IX_fact_comportamiento_producto ON fact_comportamiento_web(producto_id);
CREATE INDEX IX_fact_comportamiento_tipo_evento ON fact_comportamiento_web(tipo_evento_id);
CREATE INDEX IX_fact_comportamiento_dispositivo ON fact_comportamiento_web(dispositivo_id);
CREATE INDEX IX_fact_comportamiento_navegador ON fact_comportamiento_web(navegador_id);

-- Índices compuestos
CREATE INDEX IX_fact_comportamiento_tiempo_cliente ON fact_comportamiento_web(tiempo_key, cliente_id);
CREATE INDEX IX_fact_comportamiento_tiempo_producto ON fact_comportamiento_web(tiempo_key, producto_id);

-- Columnstore para análisis OLAP
CREATE NONCLUSTERED COLUMNSTORE INDEX IX_fact_comportamiento_columnstore
ON fact_comportamiento_web (
    tiempo_key, cliente_id, producto_id, tipo_evento_id,
    dispositivo_id, navegador_id, duracion_segundos, paginas_vistas
);
GO

-- =============================================
-- Tabla de Hechos: fact_busquedas
-- =============================================
CREATE TABLE fact_busquedas (
    busqueda_key        BIGINT IDENTITY(1,1) PRIMARY KEY,
    tiempo_key          INT NOT NULL,
    cliente_id          INT NOT NULL,
    producto_id         INT NOT NULL,
    dispositivo_id      INT NOT NULL,
    navegador_id        INT NOT NULL,
    -- Medidas
    termino_busqueda    NVARCHAR(255) NOT NULL,
    resultado_encontrado BIT NOT NULL,
    -- Degenerados
    busqueda_id         INT NOT NULL,
    venta_id            INT,
    -- Constraints
    CONSTRAINT FK_fact_busquedas_tiempo
        FOREIGN KEY (tiempo_key) REFERENCES dim_tiempo(ID_FECHA),
    CONSTRAINT FK_fact_busquedas_cliente
        FOREIGN KEY (cliente_id) REFERENCES dim_cliente(cliente_id),
    CONSTRAINT FK_fact_busquedas_producto
        FOREIGN KEY (producto_id) REFERENCES dim_producto(producto_id),
    CONSTRAINT FK_fact_busquedas_dispositivo
        FOREIGN KEY (dispositivo_id) REFERENCES dim_dispositivo(dispositivo_id),
    CONSTRAINT FK_fact_busquedas_navegador
        FOREIGN KEY (navegador_id) REFERENCES dim_navegador(navegador_id)
);

-- Índices para fact_busquedas
CREATE INDEX IX_fact_busquedas_tiempo ON fact_busquedas(tiempo_key);
CREATE INDEX IX_fact_busquedas_cliente ON fact_busquedas(cliente_id);
CREATE INDEX IX_fact_busquedas_producto ON fact_busquedas(producto_id);
CREATE INDEX IX_fact_busquedas_dispositivo ON fact_busquedas(dispositivo_id);
CREATE INDEX IX_fact_busquedas_navegador ON fact_busquedas(navegador_id);

-- Índices compuestos
CREATE INDEX IX_fact_busquedas_tiempo_cliente ON fact_busquedas(tiempo_key, cliente_id);
CREATE INDEX IX_fact_busquedas_tiempo_producto ON fact_busquedas(tiempo_key, producto_id);

-- Columnstore para análisis OLAP
CREATE NONCLUSTERED COLUMNSTORE INDEX IX_fact_busquedas_columnstore
ON fact_busquedas (
    tiempo_key, cliente_id, producto_id, dispositivo_id,
    navegador_id, resultado_encontrado
);
GO

-- =============================================
-- TABLA DE LOGS ETL
-- =============================================

CREATE TABLE etl_logs (
    log_id              BIGINT IDENTITY(1,1) PRIMARY KEY,
    proceso_nombre      NVARCHAR(100) NOT NULL,
    tabla_destino       NVARCHAR(100) NOT NULL,
    fecha_inicio        DATETIME NOT NULL,
    fecha_fin           DATETIME,
    duracion_segundos   INT,
    registros_extraidos INT DEFAULT 0,
    registros_insertados INT DEFAULT 0,
    registros_actualizados INT DEFAULT 0,
    registros_error     INT DEFAULT 0,
    estado              NVARCHAR(20) NOT NULL,
    mensaje_error       NVARCHAR(MAX),
    usuario_ejecucion   NVARCHAR(100) DEFAULT SUSER_NAME(),
    fecha_creacion      DATETIME DEFAULT GETDATE()
);

-- Índices para etl_logs
CREATE INDEX IX_etl_logs_proceso ON etl_logs(proceso_nombre);
CREATE INDEX IX_etl_logs_tabla ON etl_logs(tabla_destino);
CREATE INDEX IX_etl_logs_fecha_inicio ON etl_logs(fecha_inicio);
CREATE INDEX IX_etl_logs_estado ON etl_logs(estado);
GO

-- =============================================
-- MENSAJE FINAL
-- =============================================
PRINT 'Base de datos Ecommerce_DW creada exitosamente con:';
PRINT '- 11 tablas dimensionales';
PRINT '- 3 tablas de hechos';
PRINT '- 1 tabla de logs ETL';
PRINT '- Todos los índices y relaciones configurados';
GO
