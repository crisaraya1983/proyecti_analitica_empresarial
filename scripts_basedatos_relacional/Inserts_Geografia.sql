USE Ecommerce_OLTP;
GO

--Insertar Provincias
INSERT INTO provincias (nombre_provincia)
SELECT
    provincia
FROM Distribuidora_Vehiculos.dbo.PROVINCIAS
ORDER BY ID_PROVINCIA ASC;

-- Insertar Cantones
INSERT INTO cantones (provincia_id, nombre_canton)
SELECT
    ID_PROVINCIA,
    CANTON
FROM Distribuidora_Vehiculos.dbo.CANTONES
ORDER BY ID_CANTON ASC;


-- Insertar Distritos
INSERT INTO distritos (provincia_id, canton_id, nombre_distrito)
SELECT
    d_origen.ID_PROVINCIA,
    c_nuevo.canton_id, 
    d_origen.DISTRITO
FROM Distribuidora_Vehiculos.dbo.DISTRITOS d_origen
INNER JOIN Distribuidora_Vehiculos.dbo.CANTONES c_origen
    ON d_origen.ID_CANTON = c_origen.ID_CANTON
    AND d_origen.ID_PROVINCIA = c_origen.ID_PROVINCIA
INNER JOIN cantones c_nuevo
    ON c_origen.CANTON = c_nuevo.nombre_canton
    AND c_origen.ID_PROVINCIA = c_nuevo.provincia_id
ORDER BY d_origen.ID_DISTRITO ASC;