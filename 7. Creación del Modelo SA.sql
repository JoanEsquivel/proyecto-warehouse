--------------------------------------------------------------------------------
-- Creación del Modelo Relacional.
-- Se conecta con el usuario creado para el modelo ER.
--------------------------------------------------------------------------------
CREATE TABLE SA_TIPO_CUENTA (
   TCT_ID          VARCHAR2(4000),
   TCT_DESCRIPCION VARCHAR2(4000)
);

CREATE TABLE SA_TIPO_CLIENTE (
   TCL_ID          VARCHAR2(4000),
   TCL_DESCRIPCION VARCHAR2(4000)
);

CREATE TABLE SA_ESTADO_CLIENTE (
   ECT_ID          VARCHAR2(4000),
   ECT_DESCRIPCION VARCHAR2(4000)
);

CREATE TABLE SA_CLIENTE (
   CTE_ID     VARCHAR2(4000),
   CTE_TCL_ID VARCHAR2(4000),
   CTE_ECT_ID VARCHAR2(4000)
);

CREATE TABLE SA_CUENTA (
   CTA_ID            VARCHAR2(4000),
   CTA_CTE_ID        VARCHAR2(4000),
   CTA_TCT_ID        VARCHAR2(4000),
   CTA_NUMERO_CUENTA VARCHAR2(4000)
);

CREATE TABLE SA_MOVIMIENTO (
   MVM_ID     VARCHAR2(4000),
   MVM_CTA_ID VARCHAR2(4000),
   MVM_FECHA  VARCHAR2(4000),
   MVM_MONTO  VARCHAR2(4000)
);