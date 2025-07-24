--------------------------------------------------------------------------------
-- Creaci�n de usuario para modelo Entidad-Relaci�n.
-- Luego crea la conexi�n.

-- Notas extraidas de clase: 
-- _er al usuario que va a trabajar con modelo ER.
-- _sa al usuario que va a trabajar con el staging area.
-- _dw al usuario que va a trabajar con el datawarehouse.
--------------------------------------------------------------------------------
alter session set "_ORACLE_SCRIPT" = TRUE;
DROP USER SISBANCA_ER CASCADE;
CREATE USER SISBANCA_ER IDENTIFIED BY Oracle01 DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO SISBANCA_ER;