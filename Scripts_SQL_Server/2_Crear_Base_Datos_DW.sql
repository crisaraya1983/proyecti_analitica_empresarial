-- =============================================
-- Script: Creación de Base de Datos Dimensional - Ecommerce
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

-- Dimensión: dim_tiempo
IF OBJECT_ID('dim_tiempo', 'U') IS NOT NULL
    DROP TABLE dim_tiempo;
GO

CREATE TABLE dim_tiempo (
    ID_FECHA            INT PRIMARY KEY,        -- PK: Formato YYYYMMDD (ej: 20250115)
    FECHA_CAL           DATE NOT NULL,          -- Fecha calendario
    DIA_CAL             INT NOT NULL,           -- Día del mes (1-31)
    DIA_SEM_NUM         INT NOT NULL,           -- Día de la semana número (1-7)
    DIA_SEM_ABRV        VARCHAR(3) NOT NULL,    -- Día abreviado (Lun, Mar, etc.)
    DIA_SEM_NOMBRE      VARCHAR(20) NOT NULL,   -- Día nombre completo (Lunes, Martes, etc.)
    MES_CAL             INT NOT NULL,           -- Mes (1-12)
    MES_NOMBRE          VARCHAR(20) NOT NULL,   -- Nombre del mes (Enero, Febrero, etc.)
    MES_CAL_ABRV        VARCHAR(3) NOT NULL,    -- Mes abreviado (Ene, Feb, etc.)
    MES_CAL_FECHA_INIC  DATE NOT NULL,          -- Fecha inicio del mes
    MES_CAL_FECHA_FIN   DATE NOT NULL,          -- Fecha fin del mes
    ANIO_CAL            INT NOT NULL,           -- Año
    ANIO_CAL_FECHA_INIC DATE NOT NULL,          -- Fecha inicio del año
    ANIO_CAL_FECHA_FIN  DATE NOT NULL,          -- Fecha fin del año
    ANIO_MES_CAL_NUM    INT NOT NULL,           -- Año-Mes número (YYYYMM)
    ANIO_MES_CAL_DESCR  VARCHAR(20) NOT NULL,   -- Año-Mes descripción (2025-01)
    TRIMESTRE           INT NOT NULL,           -- Trimestre (1-4)
    SEM_CAL_NUM         INT NOT NULL,           -- Semana del año (1-53)
    FECHA_INIC_SEM      DATE NOT NULL,          -- Fecha inicio de semana
    FECHA_FIN_SEM       DATE NOT NULL           -- Fecha fin de semana
);

CREATE INDEX IX_dim_tiempo_FECHA_CAL ON dim_tiempo(FECHA_CAL);
CREATE INDEX IX_dim_tiempo_ANIO_MES ON dim_tiempo(ANIO_CAL, MES_CAL);
CREATE INDEX IX_dim_tiempo_TRIMESTRE ON dim_tiempo(ANIO_CAL, TRIMESTRE);
GO

-- Dimensión: dim_producto
IF OBJECT_ID('dim_producto', 'U') IS NOT NULL
    DROP TABLE dim_producto;
GO

CREATE TABLE dim_producto (
    producto_id         INT PRIMARY KEY,
    codigo_producto     NVARCHAR(50) NOT NULL UNIQUE,
    nombre_producto     NVARCHAR(200) NOT NULL,
    categoria_id        INT NOT NULL,
    categoria           NVARCHAR(150) NOT NULL,
    descripcion         NVARCHAR(1000),
    marca               NVARCHAR(100),
    precio_unitario     DECIMAL(12,2) NOT NULL,
    costo_unitario      DECIMAL(12,2) NOT NULL,
    activo              BIT NOT NULL DEFAULT 1,
    fecha_creacion      DATETIME,
    fecha_actualizacion DATETIME
);

CREATE INDEX IX_dim_producto_categoria_id ON dim_producto(categoria_id);
CREATE INDEX IX_dim_producto_categoria ON dim_producto(categoria);
CREATE INDEX IX_dim_producto_marca ON dim_producto(marca);
CREATE INDEX IX_dim_producto_codigo ON dim_producto(codigo_producto);
CREATE INDEX IX_dim_producto_activo ON dim_producto(activo);
GO

-- Dimensión: dim_cliente
IF OBJECT_ID('dim_cliente', 'U') IS NOT NULL
    DROP TABLE dim_cliente;
GO

CREATE TABLE dim_cliente (
    cliente_id          INT PRIMARY KEY,
    nombre_cliente      NVARCHAR(150) NOT NULL,
    apellido_cliente    NVARCHAR(150) NOT NULL,
    correo_electronico  NVARCHAR(100) NOT NULL,
    telefono            NVARCHAR(20),
    numero_cedula       NVARCHAR(20),
    provincia_id        INT NOT NULL,
    canton_id           INT NOT NULL,
    distrito_id         INT NOT NULL,
    provincia           NVARCHAR(100) NOT NULL,
    canton              NVARCHAR(100) NOT NULL,
    distrito            NVARCHAR(100) NOT NULL,
    direccion           NVARCHAR(255),
    fecha_registro      DATE,
    fecha_primer_compra DATE,
    fecha_ultimo_compra DATE,
    activo              BIT DEFAULT 1
);

CREATE INDEX IX_dim_cliente_provincia_id ON dim_cliente(provincia_id);
CREATE INDEX IX_dim_cliente_canton_id ON dim_cliente(canton_id);
CREATE INDEX IX_dim_cliente_distrito_id ON dim_cliente(distrito_id);
CREATE INDEX IX_dim_cliente_provincia ON dim_cliente(provincia);
CREATE INDEX IX_dim_cliente_canton ON dim_cliente(canton);
CREATE INDEX IX_dim_cliente_cedula ON dim_cliente(numero_cedula);
CREATE INDEX IX_dim_cliente_email ON dim_cliente(correo_electronico);
CREATE INDEX IX_dim_cliente_activo ON dim_cliente(activo);
GO

-- Dimensión: dim_geografia
IF OBJECT_ID('dim_geografia', 'U') IS NOT NULL
    DROP TABLE dim_geografia;
GO

CREATE TABLE dim_geografia (
    provincia_id        INT NOT NULL,
    canton_id           INT NOT NULL,
    distrito_id         INT NOT NULL,
    provincia           NVARCHAR(100) NOT NULL,
    canton              NVARCHAR(100) NOT NULL,
    distrito            NVARCHAR(100) NOT NULL,
    CONSTRAINT PK_dim_geografia PRIMARY KEY (provincia_id, canton_id, distrito_id)
);

CREATE INDEX IX_dim_geografia_provincia ON dim_geografia(provincia);
CREATE INDEX IX_dim_geografia_canton ON dim_geografia(canton);
CREATE INDEX IX_dim_geografia_distrito ON dim_geografia(distrito);
CREATE INDEX IX_dim_geografia_provincia_id ON dim_geografia(provincia_id);
GO

-- Dimensión: dim_almacen
IF OBJECT_ID('dim_almacen', 'U') IS NOT NULL
    DROP TABLE dim_almacen;
GO

CREATE TABLE dim_almacen (
    almacen_id          INT PRIMARY KEY,
    codigo_almacen      NVARCHAR(20) NOT NULL UNIQUE,
    nombre_almacen      NVARCHAR(150) NOT NULL,
    tipo_almacen        NVARCHAR(50),
    responsable         NVARCHAR(100),
    provincia_id        INT NOT NULL,
    canton_id           INT NOT NULL,
    distrito_id         INT NOT NULL,
    direccion           NVARCHAR(255),
    telefono            NVARCHAR(20),
    correo              NVARCHAR(100),
    latitud             DECIMAL(10,8),
    longitud            DECIMAL(11,8),
    activo              BIT DEFAULT 1,
    fecha_apertura      DATE
);

CREATE INDEX IX_dim_almacen_codigo ON dim_almacen(codigo_almacen);
CREATE INDEX IX_dim_almacen_tipo ON dim_almacen(tipo_almacen);
CREATE INDEX IX_dim_almacen_provincia_id ON dim_almacen(provincia_id);
CREATE INDEX IX_dim_almacen_activo ON dim_almacen(activo);
GO

-- Dimensión: dim_dispositivo
IF OBJECT_ID('dim_dispositivo', 'U') IS NOT NULL
    DROP TABLE dim_dispositivo;
GO

CREATE TABLE dim_dispositivo (
    dispositivo_id      INT IDENTITY(1,1) PRIMARY KEY,
    tipo_dispositivo    NVARCHAR(50) NOT NULL,
    dispositivo         NVARCHAR(150),
    sistema_operativo   NVARCHAR(100)
);

CREATE INDEX IX_dim_dispositivo_tipo ON dim_dispositivo(tipo_dispositivo);
CREATE INDEX IX_dim_dispositivo_so ON dim_dispositivo(sistema_operativo);
CREATE INDEX IX_dim_dispositivo_dispositivo ON dim_dispositivo(dispositivo);
GO

-- Dimensión: dim_navegador
IF OBJECT_ID('dim_navegador', 'U') IS NOT NULL
    DROP TABLE dim_navegador;
GO

CREATE TABLE dim_navegador (
    navegador_id        INT IDENTITY(1,1) PRIMARY KEY,
    navegador           NVARCHAR(100) NOT NULL UNIQUE,
    tipo_navegador      VARCHAR(50)
);

CREATE INDEX IX_dim_navegador_navegador ON dim_navegador(navegador);
CREATE INDEX IX_dim_navegador_tipo ON dim_navegador(tipo_navegador);
GO

-- Dimensión: dim_tipo_evento
IF OBJECT_ID('dim_tipo_evento', 'U') IS NOT NULL
    DROP TABLE dim_tipo_evento;
GO

CREATE TABLE dim_tipo_evento (
    tipo_evento_id      INT IDENTITY(1,1) PRIMARY KEY,
    tipo_evento         NVARCHAR(100) NOT NULL UNIQUE,
    categoria_evento    VARCHAR(50),
    descripcion         NVARCHAR(200),
    es_conversion       BIT DEFAULT 0
);

CREATE INDEX IX_dim_tipo_evento_tipo ON dim_tipo_evento(tipo_evento);
CREATE INDEX IX_dim_tipo_evento_categoria ON dim_tipo_evento(categoria_evento);
CREATE INDEX IX_dim_tipo_evento_conversion ON dim_tipo_evento(es_conversion);
GO

-- Dimensión: dim_estado_venta
IF OBJECT_ID('dim_estado_venta', 'U') IS NOT NULL
    DROP TABLE dim_estado_venta;
GO

CREATE TABLE dim_estado_venta (
    estado_venta_id     INT IDENTITY(1,1) PRIMARY KEY,
    estado_venta        NVARCHAR(50) NOT NULL UNIQUE,
    descripcion         NVARCHAR(200),
    es_exitosa          BIT DEFAULT 1
);

CREATE INDEX IX_dim_estado_venta_estado ON dim_estado_venta(estado_venta);
CREATE INDEX IX_dim_estado_venta_exitosa ON dim_estado_venta(es_exitosa);
GO

-- Dimensión: dim_metodo_pago
IF OBJECT_ID('dim_metodo_pago', 'U') IS NOT NULL
    DROP TABLE dim_metodo_pago;
GO

CREATE TABLE dim_metodo_pago (
    metodo_pago_id      INT IDENTITY(1,1) PRIMARY KEY,
    metodo_pago         NVARCHAR(50) NOT NULL UNIQUE,
    descripcion         NVARCHAR(200),
    tipo_pago           VARCHAR(50)
);

CREATE INDEX IX_dim_metodo_pago_metodo ON dim_metodo_pago(metodo_pago);
CREATE INDEX IX_dim_metodo_pago_tipo ON dim_metodo_pago(tipo_pago);
GO

-- Dimensión: dim_sesion
IF OBJECT_ID('dim_sesion', 'U') IS NOT NULL
    DROP TABLE dim_sesion;
GO

CREATE TABLE dim_sesion (
    evento_id           INT PRIMARY KEY,
    codigo_sesion       NVARCHAR(100) NOT NULL,
    fecha_hora_evento   DATETIME NOT NULL
);

-- Índices para dim_sesion
CREATE INDEX IX_dim_sesion_codigo_sesion ON dim_sesion(codigo_sesion);
GO

-- =============================================
-- TABLAS DE HECHOS
-- =============================================

-- Tabla de Hechos: fact_ventas
IF OBJECT_ID('fact_ventas', 'U') IS NOT NULL
    DROP TABLE fact_ventas;
GO

CREATE TABLE fact_ventas (
    venta_detalle_key   BIGINT IDENTITY(1,1) PRIMARY KEY,
    tiempo_key          INT NOT NULL,
    producto_id         INT NOT NULL,
    cliente_id          INT NOT NULL,
    provincia_id        INT NOT NULL,
    canton_id           INT NOT NULL,
    distrito_id         INT NOT NULL,
    almacen_id          INT NOT NULL,
    estado_venta_id     INT NOT NULL,
    metodo_pago_id      INT NOT NULL,
    venta_id            INT NOT NULL,
    detalle_venta_id    INT NOT NULL,
    cantidad            INT NOT NULL,
    precio_unitario     DECIMAL(12,2) NOT NULL,
    costo_unitario      DECIMAL(12,2) NOT NULL,
    descuento_porcentaje DECIMAL(5,2) DEFAULT 0,
    descuento_monto     DECIMAL(12,2) DEFAULT 0,
    subtotal            DECIMAL(12,2) NOT NULL,
    impuesto            DECIMAL(12,2) NOT NULL,
    monto_total         DECIMAL(12,2) NOT NULL,
    margen              DECIMAL(12,2),
    es_primera_compra   BIT DEFAULT 0,
    venta_cancelada     BIT DEFAULT 0,
    fecha_carga         DATETIME DEFAULT GETDATE(),
    CONSTRAINT FK_fact_ventas_tiempo
        FOREIGN KEY (tiempo_key) REFERENCES dim_tiempo(ID_FECHA),
    CONSTRAINT FK_fact_ventas_producto
        FOREIGN KEY (producto_id) REFERENCES dim_producto(producto_id),
    CONSTRAINT FK_fact_ventas_cliente
        FOREIGN KEY (cliente_id) REFERENCES dim_cliente(cliente_id),
    CONSTRAINT FK_fact_ventas_geografia
        FOREIGN KEY (provincia_id, canton_id, distrito_id)
        REFERENCES dim_geografia(provincia_id, canton_id, distrito_id),
    CONSTRAINT FK_fact_ventas_almacen
        FOREIGN KEY (almacen_id) REFERENCES dim_almacen(almacen_id),
    CONSTRAINT FK_fact_ventas_estado
        FOREIGN KEY (estado_venta_id) REFERENCES dim_estado_venta(estado_venta_id),
    CONSTRAINT FK_fact_ventas_metodo_pago
        FOREIGN KEY (metodo_pago_id) REFERENCES dim_metodo_pago(metodo_pago_id)
);

CREATE INDEX IX_fact_ventas_tiempo ON fact_ventas(tiempo_key);
CREATE INDEX IX_fact_ventas_producto ON fact_ventas(producto_id);
CREATE INDEX IX_fact_ventas_cliente ON fact_ventas(cliente_id);
CREATE INDEX IX_fact_ventas_geografia ON fact_ventas(provincia_id, canton_id, distrito_id);
CREATE INDEX IX_fact_ventas_almacen ON fact_ventas(almacen_id);
CREATE INDEX IX_fact_ventas_estado ON fact_ventas(estado_venta_id);
CREATE INDEX IX_fact_ventas_metodo_pago ON fact_ventas(metodo_pago_id);
CREATE INDEX IX_fact_ventas_venta_id ON fact_ventas(venta_id);

CREATE INDEX IX_fact_ventas_tiempo_producto ON fact_ventas(tiempo_key, producto_id);
CREATE INDEX IX_fact_ventas_tiempo_cliente ON fact_ventas(tiempo_key, cliente_id);
CREATE INDEX IX_fact_ventas_tiempo_almacen ON fact_ventas(tiempo_key, almacen_id);

-- Índice columnstore para análisis OLAP
CREATE NONCLUSTERED COLUMNSTORE INDEX IX_fact_ventas_columnstore
ON fact_ventas (
    tiempo_key, producto_id, cliente_id, almacen_id,
    cantidad, precio_unitario, monto_total, margen, impuesto
);
GO

-- Tabla de Hechos: fact_comportamiento_web
IF OBJECT_ID('fact_comportamiento_web', 'U') IS NOT NULL
    DROP TABLE fact_comportamiento_web;
GO

CREATE TABLE fact_comportamiento_web (
    evento_web_key      BIGINT IDENTITY(1,1) PRIMARY KEY,
    tiempo_key          INT NOT NULL,
    sesion_id           INT NOT NULL,
    cliente_id          INT,
    producto_id         INT,
    dispositivo_id      INT NOT NULL,
    navegador_id        INT NOT NULL,
    tipo_evento_id      INT NOT NULL,
    evento_id           INT NOT NULL,
    numero_evento_sesion INT,
    venta_id            INT,
    tiempo_pagina_segundos INT DEFAULT 0,
    eventos_sesion      INT DEFAULT 1,
    cliente_reconocido  BIT DEFAULT 0,
    genero_venta        BIT DEFAULT 0,
    fecha_carga         DATETIME DEFAULT GETDATE(),
    CONSTRAINT FK_fact_comp_web_tiempo
        FOREIGN KEY (tiempo_key) REFERENCES dim_tiempo(ID_FECHA),
    CONSTRAINT FK_fact_comp_web_sesion
        FOREIGN KEY (sesion_id) REFERENCES dim_sesion(sesion_id),
    CONSTRAINT FK_fact_comp_web_cliente
        FOREIGN KEY (cliente_id) REFERENCES dim_cliente(cliente_id),
    CONSTRAINT FK_fact_comp_web_producto
        FOREIGN KEY (producto_id) REFERENCES dim_producto(producto_id),
    CONSTRAINT FK_fact_comp_web_dispositivo
        FOREIGN KEY (dispositivo_id) REFERENCES dim_dispositivo(dispositivo_id),
    CONSTRAINT FK_fact_comp_web_navegador
        FOREIGN KEY (navegador_id) REFERENCES dim_navegador(navegador_id),
    CONSTRAINT FK_fact_comp_web_tipo_evento
        FOREIGN KEY (tipo_evento_id) REFERENCES dim_tipo_evento(tipo_evento_id)
);

-- Índices
CREATE INDEX IX_fact_comp_web_tiempo ON fact_comportamiento_web(tiempo_key);
CREATE INDEX IX_fact_comp_web_sesion ON fact_comportamiento_web(sesion_id);
CREATE INDEX IX_fact_comp_web_cliente ON fact_comportamiento_web(cliente_id);
CREATE INDEX IX_fact_comp_web_producto ON fact_comportamiento_web(producto_id);
CREATE INDEX IX_fact_comp_web_dispositivo ON fact_comportamiento_web(dispositivo_id);
CREATE INDEX IX_fact_comp_web_navegador ON fact_comportamiento_web(navegador_id);
CREATE INDEX IX_fact_comp_web_tipo_evento ON fact_comportamiento_web(tipo_evento_id);
CREATE INDEX IX_fact_comp_web_evento_id ON fact_comportamiento_web(evento_id);
CREATE INDEX IX_fact_comp_web_venta_id ON fact_comportamiento_web(venta_id);
CREATE INDEX IX_fact_comp_web_conversion ON fact_comportamiento_web(genero_venta);

CREATE INDEX IX_fact_comp_web_tiempo_sesion ON fact_comportamiento_web(tiempo_key, sesion_id);
CREATE INDEX IX_fact_comp_web_sesion_cliente ON fact_comportamiento_web(sesion_id, cliente_id);
CREATE INDEX IX_fact_comp_web_tiempo_dispositivo ON fact_comportamiento_web(tiempo_key, dispositivo_id);
CREATE INDEX IX_fact_comp_web_tiempo_navegador ON fact_comportamiento_web(tiempo_key, navegador_id);
CREATE INDEX IX_fact_comp_web_tiempo_tipo_evento ON fact_comportamiento_web(tiempo_key, tipo_evento_id);
CREATE INDEX IX_fact_comp_web_dispositivo_navegador ON fact_comportamiento_web(dispositivo_id, navegador_id);

-- Índice columnstore
CREATE NONCLUSTERED COLUMNSTORE INDEX IX_fact_comp_web_columnstore
ON fact_comportamiento_web (
    tiempo_key, cliente_id, dispositivo_id, navegador_id, tipo_evento_id,
    tiempo_pagina_segundos, genero_venta
);
GO

-- Tabla de Hechos: fact_busquedas
IF OBJECT_ID('fact_busquedas', 'U') IS NOT NULL
    DROP TABLE fact_busquedas;
GO

CREATE TABLE fact_busquedas (
    busqueda_key        BIGINT IDENTITY(1,1) PRIMARY KEY,
    tiempo_key          INT NOT NULL,
    cliente_id          INT,
    producto_id         INT,
    dispositivo_id      INT NOT NULL,
    navegador_id        INT NOT NULL,
    busqueda_id         INT NOT NULL,
    venta_id            INT,
    cantidad_resultados INT DEFAULT 0,
    total_busquedas     INT DEFAULT 1,
    cliente_reconocido  BIT DEFAULT 0,
    genero_venta        BIT DEFAULT 0,
    fecha_carga         DATETIME DEFAULT GETDATE(),
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

-- Índices
CREATE INDEX IX_fact_busquedas_tiempo ON fact_busquedas(tiempo_key);
CREATE INDEX IX_fact_busquedas_cliente ON fact_busquedas(cliente_id);
CREATE INDEX IX_fact_busquedas_producto ON fact_busquedas(producto_id);
CREATE INDEX IX_fact_busquedas_dispositivo ON fact_busquedas(dispositivo_id);
CREATE INDEX IX_fact_busquedas_navegador ON fact_busquedas(navegador_id);
CREATE INDEX IX_fact_busquedas_busqueda_id ON fact_busquedas(busqueda_id);
CREATE INDEX IX_fact_busquedas_venta_id ON fact_busquedas(venta_id);
CREATE INDEX IX_fact_busquedas_conversion ON fact_busquedas(genero_venta);

CREATE INDEX IX_fact_busquedas_tiempo_dispositivo ON fact_busquedas(tiempo_key, dispositivo_id);
CREATE INDEX IX_fact_busquedas_tiempo_navegador ON fact_busquedas(tiempo_key, navegador_id);
CREATE INDEX IX_fact_busquedas_dispositivo_navegador ON fact_busquedas(dispositivo_id, navegador_id);

-- Índice columnstore
CREATE NONCLUSTERED COLUMNSTORE INDEX IX_fact_busquedas_columnstore
ON fact_busquedas (
    tiempo_key, cliente_id, dispositivo_id, navegador_id,
    cantidad_resultados, genero_venta
);
GO

-- TABLA DE LOGS ETL
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

PRINT 'Base de datos Ecommerce_DW creada exitosamente con:';
GO
