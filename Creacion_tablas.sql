CREATE TABLE clientes (
    cliente_id      INT PRIMARY KEY,
    nombre          VARCHAR(50)    ,
    apellido        VARCHAR(50)    ,
    telefono        VARCHAR(15)    , 
    CURP            VARCHAR(18)    , 
    RFC             VARCHAR(13)    , 
    calle           VARCHAR(100)   ,
    numero_exterior VARCHAR(10)    ,
    colonia         VARCHAR(50)    ,
    municipio       VARCHAR(50)    ,
    estado          VARCHAR(50)
);


CREATE TABLE articulos (
    articulo_id     INT PRIMARY KEY,
    nombre_articulo VARCHAR(100)   ,
    precio          DECIMAL(10, 2)   
);



CREATE TABLE tiendas (
    tienda_id     INT PRIMARY KEY,
    nombre_tienda VARCHAR(100)   ,
    coordenadas   VARCHAR(50)   
);



CREATE TABLE compras (
    rfc_cliente        VARCHAR(13)   ,  -- RFC del cliente
    nombre_articulo    VARCHAR(100)  ,  -- Nombre del artículo
    cantidad_comprada  INT           ,  -- Cantidad de artículos comprados
    id_articulo        INT           ,  -- ID del artículo
    nombre_tienda      VARCHAR(100)  ,  -- Nombre de la tienda
    total_comprado     DECIMAL(10, 2),  -- Total comprado 
    fecha_compra       DATE             -- Fecha de compra
);


CREATE TABLE monitoreo (
    Fecha_inicio         timestamp  ,  -- Fechia inicio del proceso
    Fecha_fin            timestamp  ,  -- Fecha fin del proceso
    Registros_leidos     INT        ,  -- Numero de registros leidos
    Registros_procesados INT        ,  -- Numero de registros procesados
    Status               INT          -- Status del proceso 1 ejecucion, 2 finalizado, 3 error
);







