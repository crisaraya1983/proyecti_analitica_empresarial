-- ================================================================================
-- PASO 12: Crear Vistas Útiles para Consultas Comunes
-- ================================================================================

-- Vista: Ventas consolidadas con información del cliente
CREATE VIEW v_ventas_consolidadas AS
SELECT
    v.venta_id,
    v.numero_factura,
    v.fecha_venta,
    a.nombre_almacen,
    a.codigo_almacen,
    a.tipo_almacen,
    a.provincia_id,
    pr.nombre_provincia,
    CONCAT(c.nombre_cliente, ' ', c.apellido_cliente) AS nombre_cliente_completo,
    c.correo_electronico,
    v.cantidad_productos,
    v.subtotal,
    v.descuento_total,
    v.impuesto_total,
    v.monto_total,
    v.costo_total,
    v.margen_total,
    v.metodo_pago,
    v.estado_venta
FROM ventas v
INNER JOIN almacenes a ON v.almacen_id = a.almacen_id
INNER JOIN provincias pr ON a.provincia_id = pr.provincia_id
INNER JOIN clientes c ON v.cliente_id = c.cliente_id;

-- Vista: Detalles de venta con información de productos
CREATE VIEW v_detalles_venta_productos AS
SELECT 
    dv.detalle_venta_id,
    dv.venta_id,
    v.numero_factura,
    v.fecha_venta,
    p.nombre_producto,
    p.codigo_producto,
    cat.nombre_categoria,
    dv.cantidad,
    dv.precio_unitario,
    dv.descuento_porcentaje,
    dv.descuento_monto,
    dv.monto_total,
    dv.margen
FROM detalles_venta dv
INNER JOIN ventas v ON dv.venta_id = v.venta_id
INNER JOIN productos p ON dv.producto_id = p.producto_id
INNER JOIN categorias cat ON p.categoria_id = cat.categoria_id;

-- Vista: Análisis de sesiones de eventos web
CREATE VIEW v_sesiones_web_analisis AS
SELECT 
    codigo_sesion,
    cliente_id,
    ISNULL(CONCAT(c.nombre_cliente, ' ', c.apellido_cliente), 'No identificado') AS nombre_cliente,
    MIN(fecha_hora_evento) AS inicio_sesion,
    MAX(fecha_hora_evento) AS fin_sesion,
    DATEDIFF(MINUTE, MIN(fecha_hora_evento), MAX(fecha_hora_evento)) AS duracion_minutos,
    COUNT(*) AS total_eventos,
    SUM(CASE WHEN tipo_evento = 'vista_producto' THEN 1 ELSE 0 END) AS eventos_vista_producto,
    SUM(CASE WHEN tipo_evento = 'clic_categoria' THEN 1 ELSE 0 END) AS eventos_clic_categoria,
    SUM(CASE WHEN tipo_evento = 'busqueda' THEN 1 ELSE 0 END) AS eventos_busqueda,
    SUM(CASE WHEN tipo_evento = 'anadir_carrito' THEN 1 ELSE 0 END) AS eventos_anadir_carrito,
    SUM(CASE WHEN tipo_evento = 'venta_completada' THEN 1 ELSE 0 END) AS eventos_venta_completada,
    MAX(CASE WHEN venta_id IS NOT NULL THEN 1 ELSE 0 END) AS sesion_genero_venta
FROM eventos_web ew
LEFT JOIN clientes c ON ew.cliente_id = c.cliente_id
GROUP BY codigo_sesion, cliente_id;
