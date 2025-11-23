-- =============================================
-- Script: Creación de Base de Datos OLTP - Ecommerce
-- Descripción: Script completo para crear la base de datos relacional
--              de Ecommerce con todas sus tablas, relaciones e índices
-- Nota: Este script crea las estructuras vacías (sin datos)
-- =============================================

-- Crear la base de datos
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'Ecommerce_OLTP')
BEGIN
    CREATE DATABASE Ecommerce_OLTP;
END
GO

USE Ecommerce_OLTP;
GO

-- =============================================
-- TABLAS DE GEOGRAFÍA
-- =============================================

-- Tabla: provincias
CREATE TABLE [dbo].[provincias](
    [provincia_id] [int] IDENTITY(1,1) NOT NULL,
    [provincia] [nvarchar](100) NOT NULL,
    PRIMARY KEY CLUSTERED ([provincia_id] ASC)
    WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
          ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
    ON [PRIMARY]
) ON [PRIMARY];
GO

-- Tabla: cantones
CREATE TABLE [dbo].[cantones](
    [canton_id] [int] IDENTITY(1,1) NOT NULL,
    [provincia_id] [int] NOT NULL,
    [canton] [nvarchar](100) NOT NULL,
    PRIMARY KEY CLUSTERED ([canton_id] ASC)
    WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
          ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
    ON [PRIMARY],
    CONSTRAINT [FK_cantones_provincias] FOREIGN KEY([provincia_id])
        REFERENCES [dbo].[provincias] ([provincia_id])
) ON [PRIMARY];
GO

-- Tabla: distritos
CREATE TABLE [dbo].[distritos](
    [distrito_id] [int] IDENTITY(1,1) NOT NULL,
    [provincia_id] [int] NOT NULL,
    [canton_id] [int] NOT NULL,
    [distrito] [nvarchar](100) NOT NULL,
    PRIMARY KEY CLUSTERED ([distrito_id] ASC)
    WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
          ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
    ON [PRIMARY],
    CONSTRAINT [FK_distritos_provincias] FOREIGN KEY([provincia_id])
        REFERENCES [dbo].[provincias] ([provincia_id]),
    CONSTRAINT [FK_distritos_cantones] FOREIGN KEY([canton_id])
        REFERENCES [dbo].[cantones] ([canton_id])
) ON [PRIMARY];
GO

-- =============================================
-- TABLAS DE NEGOCIO
-- =============================================

-- Tabla: almacenes
CREATE TABLE [dbo].[almacenes](
    [almacen_id] [int] IDENTITY(1,1) NOT NULL,
    [nombre] [nvarchar](100) NOT NULL,
    [provincia_id] [int] NOT NULL,
    [canton_id] [int] NOT NULL,
    [distrito_id] [int] NOT NULL,
    [direccion] [nvarchar](255) NULL,
    [telefono] [nvarchar](20) NULL,
    PRIMARY KEY CLUSTERED ([almacen_id] ASC)
    WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
          ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
    ON [PRIMARY],
    CONSTRAINT [FK_almacenes_provincias] FOREIGN KEY([provincia_id])
        REFERENCES [dbo].[provincias] ([provincia_id]),
    CONSTRAINT [FK_almacenes_cantones] FOREIGN KEY([canton_id])
        REFERENCES [dbo].[cantones] ([canton_id]),
    CONSTRAINT [FK_almacenes_distritos] FOREIGN KEY([distrito_id])
        REFERENCES [dbo].[distritos] ([distrito_id])
) ON [PRIMARY];
GO

-- Tabla: categorias
CREATE TABLE [dbo].[categorias](
    [categoria_id] [int] IDENTITY(1,1) NOT NULL,
    [categoria] [nvarchar](100) NOT NULL,
    PRIMARY KEY CLUSTERED ([categoria_id] ASC)
    WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
          ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
    ON [PRIMARY]
) ON [PRIMARY];
GO

-- Tabla: productos
CREATE TABLE [dbo].[productos](
    [producto_id] [int] IDENTITY(1,1) NOT NULL,
    [codigo_producto] [nvarchar](50) NOT NULL,
    [nombre] [nvarchar](255) NOT NULL,
    [categoria_id] [int] NOT NULL,
    [precio_unitario] [decimal](12, 2) NOT NULL,
    [costo_unitario] [decimal](12, 2) NOT NULL,
    [descripcion] [nvarchar](max) NULL,
    PRIMARY KEY CLUSTERED ([producto_id] ASC)
    WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
          ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
    ON [PRIMARY],
    CONSTRAINT [FK_productos_categorias] FOREIGN KEY([categoria_id])
        REFERENCES [dbo].[categorias] ([categoria_id])
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
GO

-- Tabla: clientes
CREATE TABLE [dbo].[clientes](
    [cliente_id] [int] IDENTITY(1,1) NOT NULL,
    [nombre] [nvarchar](100) NOT NULL,
    [apellido] [nvarchar](100) NOT NULL,
    [email] [nvarchar](255) NOT NULL,
    [telefono] [nvarchar](20) NULL,
    [provincia_id] [int] NOT NULL,
    [canton_id] [int] NOT NULL,
    [distrito_id] [int] NOT NULL,
    [direccion] [nvarchar](255) NULL,
    [genero] [nvarchar](20) NULL,
    [edad] [int] NULL,
    [fecha_registro] [date] NOT NULL,
    PRIMARY KEY CLUSTERED ([cliente_id] ASC)
    WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
          ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
    ON [PRIMARY],
    CONSTRAINT [FK_clientes_provincias] FOREIGN KEY([provincia_id])
        REFERENCES [dbo].[provincias] ([provincia_id]),
    CONSTRAINT [FK_clientes_cantones] FOREIGN KEY([canton_id])
        REFERENCES [dbo].[cantones] ([canton_id]),
    CONSTRAINT [FK_clientes_distritos] FOREIGN KEY([distrito_id])
        REFERENCES [dbo].[distritos] ([distrito_id])
) ON [PRIMARY];
GO

-- =============================================
-- TABLAS TRANSACCIONALES
-- =============================================

-- Tabla: ventas
CREATE TABLE [dbo].[ventas](
    [venta_id] [int] IDENTITY(1,1) NOT NULL,
    [almacen_id] [int] NOT NULL,
    [cliente_id] [int] NOT NULL,
    [fecha_venta] [datetime] NOT NULL,
    [metodo_pago] [nvarchar](50) NOT NULL,
    [estado_venta] [nvarchar](50) NOT NULL,
    [monto_total] [decimal](12, 2) NOT NULL,
    [impuesto] [decimal](12, 2) NOT NULL,
    PRIMARY KEY CLUSTERED ([venta_id] ASC)
    WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
          ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
    ON [PRIMARY],
    CONSTRAINT [FK_ventas_almacenes] FOREIGN KEY([almacen_id])
        REFERENCES [dbo].[almacenes] ([almacen_id]),
    CONSTRAINT [FK_ventas_clientes] FOREIGN KEY([cliente_id])
        REFERENCES [dbo].[clientes] ([cliente_id])
) ON [PRIMARY];
GO

-- Tabla: detalles_venta
CREATE TABLE [dbo].[detalles_venta](
    [detalle_venta_id] [int] IDENTITY(1,1) NOT NULL,
    [venta_id] [int] NOT NULL,
    [producto_id] [int] NOT NULL,
    [cantidad] [int] NOT NULL,
    [precio_unitario] [decimal](12, 2) NOT NULL,
    [descuento] [decimal](12, 2) NOT NULL,
    [impuesto] [decimal](12, 2) NOT NULL,
    [monto_total] [decimal](12, 2) NOT NULL,
    PRIMARY KEY CLUSTERED ([detalle_venta_id] ASC)
    WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
          ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
    ON [PRIMARY],
    CONSTRAINT [FK_detalles_venta_ventas] FOREIGN KEY([venta_id])
        REFERENCES [dbo].[ventas] ([venta_id])
        ON DELETE CASCADE,
    CONSTRAINT [FK_detalles_venta_productos] FOREIGN KEY([producto_id])
        REFERENCES [dbo].[productos] ([producto_id])
) ON [PRIMARY];
GO

-- =============================================
-- TABLAS WEB
-- =============================================

-- Tabla: busquedas_web
CREATE TABLE [dbo].[busquedas_web](
    [busqueda_id] [int] IDENTITY(1,1) NOT NULL,
    [cliente_id] [int] NULL,
    [producto_id] [int] NULL,
    [venta_id] [int] NULL,
    [fecha_busqueda] [datetime] NOT NULL,
    [termino_busqueda] [nvarchar](255) NOT NULL,
    [dispositivo] [nvarchar](50) NULL,
    [navegador] [nvarchar](50) NULL,
    PRIMARY KEY CLUSTERED ([busqueda_id] ASC)
    WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
          ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
    ON [PRIMARY],
    CONSTRAINT [FK_busquedas_web_clientes] FOREIGN KEY([cliente_id])
        REFERENCES [dbo].[clientes] ([cliente_id]),
    CONSTRAINT [FK_busquedas_web_productos] FOREIGN KEY([producto_id])
        REFERENCES [dbo].[productos] ([producto_id]),
    CONSTRAINT [FK_busquedas_web_ventas] FOREIGN KEY([venta_id])
        REFERENCES [dbo].[ventas] ([venta_id])
) ON [PRIMARY];
GO

-- Tabla: eventos_web
CREATE TABLE [dbo].[eventos_web](
    [evento_id] [int] IDENTITY(1,1) NOT NULL,
    [cliente_id] [int] NULL,
    [producto_id] [int] NULL,
    [venta_id] [int] NULL,
    [fecha_evento] [datetime] NOT NULL,
    [tipo_evento] [nvarchar](50) NOT NULL,
    [dispositivo] [nvarchar](50) NULL,
    [navegador] [nvarchar](50) NULL,
    PRIMARY KEY CLUSTERED ([evento_id] ASC)
    WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
          ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
    ON [PRIMARY],
    CONSTRAINT [FK_eventos_web_clientes] FOREIGN KEY([cliente_id])
        REFERENCES [dbo].[clientes] ([cliente_id]),
    CONSTRAINT [FK_eventos_web_productos] FOREIGN KEY([producto_id])
        REFERENCES [dbo].[productos] ([producto_id]),
    CONSTRAINT [FK_eventos_web_ventas] FOREIGN KEY([venta_id])
        REFERENCES [dbo].[ventas] ([venta_id])
) ON [PRIMARY];
GO

-- =============================================
-- TABLA DIMENSIÓN TIEMPO
-- =============================================

-- Tabla: tiempo
CREATE TABLE [dbo].[tiempo](
    [ID_FECHA] [int] NOT NULL,
    [FECHA_CAL] [date] NULL,
    [DIA_CAL] [int] NULL,
    [DIA_SEMANA] [int] NULL,
    [DIA_SEMANA_NOMBRE] [nvarchar](20) NULL,
    [DIA_SEMANA_ABR] [nvarchar](3) NULL,
    [SEMANA_CAL] [int] NULL,
    [MES_CAL] [int] NULL,
    [MES_NOMBRE] [nvarchar](20) NULL,
    [MES_ABR] [nvarchar](3) NULL,
    [TRIMESTRE] [int] NULL,
    [ANIO_CAL] [int] NULL,
    [ES_FIN_SEMANA] [bit] NULL,
    [ES_FERIADO] [bit] NULL,
    [NOMBRE_FERIADO] [nvarchar](100) NULL,
    [ANIO_MES] [nvarchar](7) NULL,
    [DIA_ANIO] [int] NULL,
    [SEMANA_MES] [int] NULL,
    [QUINCENA] [int] NULL,
    [PERIODO_DIA] [nvarchar](20) NULL,
    PRIMARY KEY CLUSTERED ([ID_FECHA] ASC)
    WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
          ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
    ON [PRIMARY]
) ON [PRIMARY];
GO

-- =============================================
-- MENSAJE FINAL
-- =============================================
PRINT 'Base de datos Ecommerce_OLTP creada exitosamente con todas las tablas y relaciones.';
GO
