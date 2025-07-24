--------------------------------------------------------------------------------
-- Creaci�n de usuario para modelo Entidad-Relaci�n.
-- Luego crea la conexi�n.

-- Notas extraidas de clase: 
-- _er al usuario que va a trabajar con modelo ER.
-- _sa al usuario que va a trabajar con el staging area.
-- _dw al usuario que va a trabajar con el datawarehouse.
--------------------------------------------------------------------------------
alter session set "_ORACLE_SCRIPT" = TRUE;
DROP USER SISECOMMERCE_ER CASCADE;
DROP USER SISECOMMERCE_SA CASCADE;
DROP USER SISECOMMERCE_DW CASCADE;

CREATE USER SISECOMMERCE_ER IDENTIFIED BY Oracle01 DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO SISECOMMERCE_ER;

DROP USER SISECOMMERCE_SA CASCADE;
CREATE USER SISECOMMERCE_SA IDENTIFIED BY Oracle01 DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO SISECOMMERCE_SA;

DROP USER SISECOMMERCE_DW CASCADE;
CREATE USER SISECOMMERCE_DW IDENTIFIED BY Oracle01 DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO SISECOMMERCE_DW;

