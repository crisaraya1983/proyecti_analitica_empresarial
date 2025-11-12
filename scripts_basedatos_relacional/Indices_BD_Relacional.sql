-- Índices para Optimización de Consultas

-- Índices en tabla ventas
CREATE INDEX idx_ventas_fecha ON ventas(fecha_venta);
CREATE INDEX idx_ventas_cliente ON ventas(cliente_id);
CREATE INDEX idx_ventas_almacen ON ventas(almacen_id);
CREATE INDEX idx_ventas_estado ON ventas(estado_venta);
CREATE INDEX idx_ventas_numero_factura ON ventas(numero_factura);

-- Índices en tabla detalles_venta
CREATE INDEX idx_detalles_venta_venta ON detalles_venta(venta_id);
CREATE INDEX idx_detalles_venta_producto ON detalles_venta(producto_id);

-- Índices en tabla búsquedas_web
CREATE INDEX idx_busquedas_fecha ON busquedas_web(fecha_hora_busqueda);
CREATE INDEX idx_busquedas_cliente ON busquedas_web(cliente_id);
CREATE INDEX idx_busquedas_termino ON busquedas_web(termino_busqueda);
CREATE INDEX idx_busquedas_producto ON busquedas_web(producto_visualizado_id);
CREATE INDEX idx_busquedas_cliente_reconocido ON busquedas_web(cliente_reconocido);

-- Índices en tabla eventos_web
CREATE INDEX idx_eventos_fecha ON eventos_web(fecha_hora_evento);
CREATE INDEX idx_eventos_cliente ON eventos_web(cliente_id);
CREATE INDEX idx_eventos_tipo ON eventos_web(tipo_evento);
CREATE INDEX idx_eventos_producto ON eventos_web(producto_id);
CREATE INDEX idx_eventos_cliente_reconocido ON eventos_web(cliente_reconocido);
CREATE INDEX idx_eventos_dispositivo ON eventos_web(tipo_dispositivo);
CREATE INDEX idx_eventos_sesion ON eventos_web(codigo_sesion);
CREATE INDEX idx_eventos_sesion_cliente ON eventos_web(codigo_sesion, cliente_id);

-- Índices en tabla clientes
CREATE INDEX idx_clientes_email ON clientes(correo_electronico);
CREATE INDEX idx_clientes_cedula ON clientes(numero_cedula);
CREATE INDEX idx_clientes_segmento ON clientes(segmento_cliente);
CREATE INDEX idx_clientes_provincia ON clientes(provincia_id);
CREATE INDEX idx_clientes_canton ON clientes(canton_id);
CREATE INDEX idx_clientes_distrito ON clientes(distrito_id);

-- Índices en tabla productos
CREATE INDEX idx_productos_categoria ON productos(categoria_id);
CREATE INDEX idx_productos_codigo ON productos(codigo_producto);
CREATE INDEX idx_productos_activo ON productos(activo);

-- Índices en tabla almacenes
CREATE INDEX idx_almacenes_distrito ON almacenes(distrito_id);
CREATE INDEX idx_almacenes_provincia ON almacenes(provincia_id);
CREATE INDEX idx_almacenes_canton ON almacenes(canton_id);
CREATE INDEX idx_almacenes_tipo ON almacenes(tipo_almacen);