--------------------------------------------------------------------------------
-- Creaci�n de usuario para modelo Sataging Area
-- Luego crea la conexi�n.
--------------------------------------------------------------------------------
alter session set "_ORACLE_SCRIPT" = TRUE;
DROP USER SISBANCA_SA CASCADE;
CREATE USER SISCOOPE_SA IDENTIFIED BY Oracle01 DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO SISCOOPE_SA;