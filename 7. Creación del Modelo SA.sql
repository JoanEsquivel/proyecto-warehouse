--------------------------------------------------------------------------------
-- Creaci�n del Modelo Staging Area.
-- Se conecta con el usuario creado para el modelo ER.
--------------------------------------------------------------------------------

-- El usuario que debe estar conectado es: SISECOMMERCE_SA

-- Es una copia del modelo ER, quitarle las restricciones y agregarle un prefijo SA_
-- Ademas los tipos de datos de las columnas deben ser VARCHAR2


DROP TABLE SA_ORDEN_COMPRA;
DROP TABLE SA_PRODUCTO;
DROP TABLE SA_CLIENTE;
DROP TABLE SA_TIPO_PRODUCTO;
DROP TABLE SA_TIPO_ENVIO;

commit;

CREATE TABLE SA_TIPO_ENVIO (
   TPE_ID                    VARCHAR2(4000),
   TPE_DESCRIPCION           VARCHAR2(4000), 
   TPE_ESTADO                VARCHAR2(4000), 
   TPE_REQUIERE_CONFIRMACION VARCHAR2(4000)
);

CREATE TABLE SA_TIPO_PRODUCTO (
   TPD_ID                   VARCHAR2(4000),
   TPD_DESCRIPCION          VARCHAR2(4000)
);

CREATE TABLE SA_CLIENTE (
   CTE_ID                   VARCHAR2(4000),
   CTE_NOMBRE               VARCHAR2(4000)
);

CREATE TABLE SA_PRODUCTO (
   PRD_ID                  VARCHAR2(4000),
   PRD_TPD_ID              VARCHAR2(4000),
   PRD_NOMBRE              VARCHAR2(4000),
   PRD_CANTIDAD            VARCHAR2(4000),
   PRD_COSTO_UNITARIO      VARCHAR2(4000)
   
);

CREATE TABLE SA_ORDEN_COMPRA (
   OCP_ID                 VARCHAR2(4000),
   OCP_PRD_ID             VARCHAR2(4000),
   OCP_CTE_ID             VARCHAR2(4000),
   OCP_TPE_ID             VARCHAR2(4000),
   OCP_FECHA              VARCHAR2(4000)
);

commit;