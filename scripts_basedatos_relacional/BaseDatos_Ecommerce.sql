-- PASO 1: Crear la Base de Datos
CREATE DATABASE Ecommerce_OLTP;
GO

USE Ecommerce_OLTP;
GO

-- PASO 2: Crear Tablas Geográficas

-- Tabla: Provincias
CREATE TABLE provincias (
    provincia_id INT PRIMARY KEY IDENTITY(1,1),
    nombre_provincia NVARCHAR(100) NOT NULL UNIQUE,
    fecha_creacion DATETIME DEFAULT GETDATE()
);

-- Tabla: Cantones
CREATE TABLE cantones (
    canton_id INT PRIMARY KEY IDENTITY(1,1),
    provincia_id INT NOT NULL,
    nombre_canton NVARCHAR(100) NOT NULL,
    fecha_creacion DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (provincia_id) REFERENCES provincias(provincia_id)
);

-- Tabla: Distritos
CREATE TABLE distritos (
    distrito_id INT PRIMARY KEY IDENTITY(1,1),
    provincia_id INT NOT NULL,
    canton_id INT NOT NULL,
    nombre_distrito NVARCHAR(100) NOT NULL,
    fecha_creacion DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (provincia_id) REFERENCES provincias(provincia_id),
    FOREIGN KEY (canton_id) REFERENCES cantones(canton_id)
);

-- PASO 3: Crear Tabla Almacenes (Bodegas)
CREATE TABLE almacenes (
    almacen_id INT PRIMARY KEY IDENTITY(1,1),
    nombre_almacen NVARCHAR(150) NOT NULL,
    codigo_almacen NVARCHAR(20) NOT NULL UNIQUE,
    provincia_id INT NOT NULL,
    canton_id INT NOT NULL,
    distrito_id INT NOT NULL,
    direccion NVARCHAR(255) NOT NULL,
    telefono NVARCHAR(20) NULL,
    correo_electronico NVARCHAR(100) NULL,
    latitud DECIMAL(10, 8) NULL,
    longitud DECIMAL(11, 8) NULL,
    responsable_almacen NVARCHAR(100) NULL,
    tipo_almacen NVARCHAR(50) NULL, -- Ej: Principal, Secundario
    activo BIT DEFAULT 1,
    fecha_apertura DATE NOT NULL,
    fecha_creacion DATETIME DEFAULT GETDATE(),
    fecha_actualizacion DATETIME NULL,
    FOREIGN KEY (provincia_id) REFERENCES provincias(provincia_id),
    FOREIGN KEY (canton_id) REFERENCES cantones(canton_id),
    FOREIGN KEY (distrito_id) REFERENCES distritos(distrito_id)
);

-- PASO 4: Crear Tabla Categorías
CREATE TABLE categorias (
    categoria_id INT PRIMARY KEY IDENTITY(1,1),
    nombre_categoria NVARCHAR(150) NOT NULL UNIQUE,
    descripcion NVARCHAR(500) NULL,
    activa BIT DEFAULT 1,
    fecha_creacion DATETIME DEFAULT GETDATE(),
    fecha_actualizacion DATETIME NULL
);

-- PASO 5: Crear Tabla  Productos
CREATE TABLE productos (
    producto_id INT PRIMARY KEY IDENTITY(1,1),
    nombre_producto NVARCHAR(200) NOT NULL,
    codigo_producto NVARCHAR(50) NOT NULL UNIQUE,
    categoria_id INT NOT NULL,
    descripcion NVARCHAR(1000) NULL,
    precio_unitario DECIMAL(12, 2) NOT NULL,
    costo_unitario DECIMAL(12, 2) NOT NULL,
    marca NVARCHAR(100) NULL,
    activo BIT DEFAULT 1,
    fecha_creacion DATETIME DEFAULT GETDATE(),
    fecha_actualizacion DATETIME NULL,
    FOREIGN KEY (categoria_id) REFERENCES categorias(categoria_id)
);

-- PASO 6: Crear Tabla de Clientes
CREATE TABLE clientes (
    cliente_id INT PRIMARY KEY IDENTITY(1,1),
    nombre_cliente NVARCHAR(150) NOT NULL,
    apellido_cliente NVARCHAR(150) NOT NULL,
    correo_electronico NVARCHAR(100) NOT NULL UNIQUE,
    telefono NVARCHAR(20) NULL,
    numero_cedula NVARCHAR(20) NULL UNIQUE,
    provincia_id INT NOT NULL,
    canton_id INT NOT NULL,
    distrito_id INT NOT NULL,
    direccion NVARCHAR(255) NOT NULL,
    activo BIT DEFAULT 1,
    fecha_primer_compra DATE NULL,
    fecha_ultimo_compra DATE NULL,
    fecha_creacion DATETIME DEFAULT GETDATE(),
    fecha_actualizacion DATETIME NULL,
    FOREIGN KEY (provincia_id) REFERENCES provincias(provincia_id),
    FOREIGN KEY (canton_id) REFERENCES cantones(canton_id),
    FOREIGN KEY (distrito_id) REFERENCES distritos(distrito_id)
);

-- PASO 7: Crear Tabla de Ventas
CREATE TABLE ventas (
    venta_id INT PRIMARY KEY IDENTITY(1,1),
    numero_factura NVARCHAR(50) NOT NULL UNIQUE,
    fecha_venta DATETIME NOT NULL,
    almacen_id INT NOT NULL,
    cliente_id INT NOT NULL,
    cantidad_productos INT NOT NULL,
    subtotal DECIMAL(12, 2) NOT NULL,
    descuento_total DECIMAL(12, 2) DEFAULT 0,
    impuesto_total DECIMAL(12, 2) NOT NULL,
    monto_total DECIMAL(12, 2) NOT NULL,
    costo_total DECIMAL(12, 2) NOT NULL,
    margen_total DECIMAL(12, 2) NULL,
    metodo_pago NVARCHAR(50) NULL,
    estado_venta NVARCHAR(50) DEFAULT 'Completada',
    notas NVARCHAR(500) NULL,
    fecha_creacion DATETIME DEFAULT GETDATE(),
    fecha_actualizacion DATETIME NULL,
    FOREIGN KEY (almacen_id) REFERENCES almacenes(almacen_id),
    FOREIGN KEY (cliente_id) REFERENCES clientes(cliente_id)
);

-- PASO 8: Crear Tabla de Detalles de Venta (Productos por Factura)
CREATE TABLE detalles_venta (
    detalle_venta_id INT PRIMARY KEY IDENTITY(1,1),
    venta_id INT NOT NULL,
    producto_id INT NOT NULL,
    cantidad INT NOT NULL,
    precio_unitario DECIMAL(12, 2) NOT NULL,
    descuento_porcentaje DECIMAL(5, 2) DEFAULT 0,
    descuento_monto DECIMAL(12, 2) DEFAULT 0,
    subtotal DECIMAL(12, 2) NOT NULL,
    impuesto DECIMAL(12, 2) NOT NULL,
    monto_total DECIMAL(12, 2) NOT NULL,
    costo_unitario DECIMAL(12, 2) NOT NULL,
    margen DECIMAL(12, 2) NULL,
    fecha_creacion DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (venta_id) REFERENCES ventas(venta_id) ON DELETE CASCADE,
    FOREIGN KEY (producto_id) REFERENCES productos(producto_id)
);

-- PASO 9: Crear Tabla de Búsquedas Web
CREATE TABLE busquedas_web (
    busqueda_id INT PRIMARY KEY IDENTITY(1,1),
    fecha_hora_busqueda DATETIME NOT NULL,
    cliente_id INT NULL,
    cliente_reconocido BIT NOT NULL DEFAULT 0,
    termino_busqueda NVARCHAR(255) NOT NULL,
    producto_visualizado_id INT NULL,
    cantidad_resultados INT NULL,
    direccion_ip NVARCHAR(50) NOT NULL,
    tipo_dispositivo NVARCHAR(50) NOT NULL,
    dispositivo NVARCHAR(150) NULL,
    navegador NVARCHAR(100) NULL,
    sistema_operativo NVARCHAR(100) NULL,
    url_referencia NVARCHAR(500) NULL,
    genero_venta BIT DEFAULT 0,
    venta_id INT NULL,
    fecha_creacion DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (cliente_id) REFERENCES clientes(cliente_id),
    FOREIGN KEY (producto_visualizado_id) REFERENCES productos(producto_id),
    FOREIGN KEY (venta_id) REFERENCES ventas(venta_id)
);

-- PASO 10: Crear Tabla de Eventos Web (Con sesiones para tracking de clicks)
CREATE TABLE eventos_web (
    evento_id INT PRIMARY KEY IDENTITY(1,1),
    codigo_sesion NVARCHAR(100) NOT NULL,
    numero_evento_en_sesion INT NOT NULL,
    fecha_hora_evento DATETIME NOT NULL,
    cliente_id INT NULL,
    cliente_reconocido BIT NOT NULL DEFAULT 0,
    tipo_evento NVARCHAR(100) NOT NULL,
    producto_id INT NULL,
    tiempo_pagina_segundos INT NULL,
    direccion_ip NVARCHAR(50) NOT NULL,
    tipo_dispositivo NVARCHAR(50) NOT NULL,
    dispositivo NVARCHAR(150) NULL,
    navegador NVARCHAR(100) NULL,
    sistema_operativo NVARCHAR(100) NULL,
    url_pagina NVARCHAR(500) NULL,
    url_anterior NVARCHAR(500) NULL,
    genero_venta BIT DEFAULT 0,
    venta_id INT NULL,
    descripcion_evento NVARCHAR(500) NULL,
    fecha_creacion DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (cliente_id) REFERENCES clientes(cliente_id),
    FOREIGN KEY (producto_id) REFERENCES productos(producto_id),
    FOREIGN KEY (venta_id) REFERENCES ventas(venta_id)
);

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[tiempo](
	[ID_FECHA] [int] NOT NULL,
	[FECHA_CAL] [date] NULL,
	[DIA_CAL] [int] NULL,
	[DIA_SEM_NUM] [int] NULL,
	[DIA_SEM_ABRV] [nvarchar](3) NULL,
	[DIA_SEM_NOMBRE] [nvarchar](15) NULL,
	[MES_CAL] [int] NULL,
	[MES_NOMBRE] [nvarchar](15) NULL,
	[MES_CAL_ABRV] [nvarchar](3) NULL,
	[MES_CAL_FECHA_INIC] [date] NULL,
	[MES_CAL_FECHA_FIN] [date] NULL,
	[ANO_CAL] [int] NULL,
	[ANIO_CAL_FECHA_INIC] [date] NULL,
	[ANIO_CAL_FECHA_FIN] [date] NULL,
	[ANIO_MES_CAL_NUM] [nvarchar](7) NULL,
	[ANIO_MES_CAL_DESCR] [nvarchar](15) NULL,
	[TRIMESTRE] [int] NULL,
	[SEM_CAL_NUM] [int] NULL,
	[FECHA_INIC_SEM] [date] NULL,
	[FECHA_FIN_SEM] [date] NULL,
PRIMARY KEY CLUSTERED 
(
	[ID_FECHA] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO