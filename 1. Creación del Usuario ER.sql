--------------------------------------------------------------------------------
-- Creaci�n de usuario para modelo Entidad-Relaci�n.
-- Luego crea la conexi�n.
--------------------------------------------------------------------------------
alter session set "_ORACLE_SCRIPT" = TRUE;
DROP USER SISBANCA_ER CASCADE;
CREATE USER SISBANCA_ER IDENTIFIED BY Oracle01 DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO SISBANCA_ER;