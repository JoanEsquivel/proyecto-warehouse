--------------------------------------------------------------------------------
-- Funcion para validar numeros enteros.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION VALIDA_NUMERO_ENTERO(P_NUMERO VARCHAR2) RETURN CHAR AS
   V_NUMERO NUMBER;
BEGIN
   V_NUMERO := TO_NUMBER(P_NUMERO);
   IF V_NUMERO = TRUNC(V_NUMERO) THEN
      RETURN 'S';
   ELSE
      RETURN 'N';
   END IF;
EXCEPTION
   WHEN OTHERS THEN
      RETURN 'N';
END;
/

CREATE OR REPLACE FUNCTION VALIDA_NUMERO_DECIMAL(P_NUMERO VARCHAR2) RETURN CHAR AS
   V_NUMERO NUMBER(20,2);
BEGIN
   V_NUMERO := TO_NUMBER(P_NUMERO);
   IF V_NUMERO <> TRUNC(V_NUMERO) THEN
      RETURN 'S';
   ELSE
      RETURN 'N';
   END IF;
EXCEPTION
   WHEN OTHERS THEN
      RETURN 'N';
END;
/

-- Funcion para validar fecha.
CREATE OR REPLACE FUNCTION VALIDA_FECHA(P_FECHA VARCHAR2) RETURN CHAR AS
   V_FECHA DATE;
BEGIN
   V_FECHA := TO_DATE(P_FECHA, 'YYYY-MM-DD');
   RETURN 'S';
EXCEPTION
   WHEN OTHERS THEN
      RETURN 'N';
END;
/

--------------------------------------------------------------------------------
-- Se crea una tabla de errores por cada tabla del DW.
--------------------------------------------------------------------------------

DROP TABLE SISECOMMERCE_DW.ERROR_SA_TIPO_PRODUCTO;
DROP TABLE SISECOMMERCE_DW.ERROR_SA_TIPO_ENVIO;
DROP TABLE SISECOMMERCE_DW.ERROR_SA_CLIENTE;
DROP TABLE SISECOMMERCE_DW.ERROR_SA_PRODUCTO;
DROP TABLE SISECOMMERCE_DW.ERROR_SA_FECHA;

CREATE TABLE SISECOMMERCE_DW.ERROR_SA_TIPO_PRODUCTO (
   TPD_ID             VARCHAR2(255),
   TPD_DESCRIPCION    VARCHAR2(255),
   TPD_ERROR          VARCHAR2(4000)
);

CREATE TABLE SISECOMMERCE_DW.ERROR_SA_TIPO_ENVIO (
   TPE_ID             VARCHAR2(255),
   TPE_DESCRIPCION    VARCHAR2(255),
   TPE_ERROR          VARCHAR2(4000)
);

CREATE TABLE SISECOMMERCE_DW.ERROR_SA_CLIENTE (
   CTE_ID             VARCHAR2(255),
   CTE_NOMBRE         VARCHAR2(255),
   CTE_ERROR          VARCHAR2(4000)
);

CREATE TABLE SISECOMMERCE_DW.ERROR_SA_PRODUCTO (
   PRD_ID             VARCHAR2(255),
   PRD_NOMBRE         VARCHAR2(255),
   PRD_ERROR          VARCHAR2(4000)
);

CREATE TABLE SISECOMMERCE_DW.ERROR_SA_FECHA (
   FEC_ID             VARCHAR2(255),
   FEC_FECHA          VARCHAR2(255),
   FEC_ERROR          VARCHAR2(4000)
);

--------------------------------------------------------------------------------
-- Especificacion del parquete.
--------------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE SISECOMMERCE_DW.ETL_DW AS
   PROCEDURE MigrarTipoProducto;
   PROCEDURE MigrarTipoEnvio;
   PROCEDURE MigrarCliente;
   PROCEDURE MigrarProducto;
   PROCEDURE MigrarFecha;
   PROCEDURE MigrarDatos;
END ETL_DW;
/
--------------------------------------------------------------------------------
-- Cuerpo del parquete.
--------------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE BODY SISECOMMERCE_DW.ETL_DW AS
   -- Migracion de Tipos de Productos.
   PROCEDURE MigrarTipoProducto IS
      V_ERROR  INTEGER;
      V_NUMERO INTEGER;
      V_ERROR_MENSAJE VARCHAR2(4000);
      CURSOR C_DATOS IS
         SELECT TP.TPD_ID,
                TP.TPD_DESCRIPCION
           FROM SISECOMMERCE_SA.SA_TIPO_PRODUCTO TP
          WHERE TP.TPD_ID NOT IN (SELECT D.TPD_ID FROM SISECOMMERCE_DW.DIM_TIPO_PRODUCTO D)
          ORDER BY TP.TPD_ID;
   BEGIN
      FOR D_DATOS IN C_DATOS LOOP
         BEGIN
             V_ERROR := 0;
             V_ERROR_MENSAJE := '';
             -----------------------------------------------------------------------
             IF D_DATOS.TPD_ID IS NULL THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Codigo Nulo. ';
             END IF;
             --- Codigo de Producto no num rico.
             IF VALIDA_NUMERO_ENTERO(D_DATOS.TPD_ID) = 'N' THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Codigo no numerico. ';
             ELSE
                V_NUMERO := TO_NUMBER(D_DATOS.TPD_ID);
                --- Codigo de Producto negativo.
                IF V_NUMERO <= 0 THEN
                   V_ERROR := 1;
                   V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Codigo negativo o cero. ';
                END IF;
             END IF;
             -----------------------------------------------------------------------
             IF D_DATOS.TPD_DESCRIPCION IS NULL THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripcion Nula. ';
             END IF;
             IF LENGTH(D_DATOS.TPD_DESCRIPCION) > 40 THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripcion con longitud mayor. ';
             END IF;
             IF LENGTH(D_DATOS.TPD_DESCRIPCION) < 2 THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripcion con longitud menor. ';
             END IF;
             IF VALIDA_NUMERO_ENTERO(D_DATOS.TPD_DESCRIPCION) = 'S' THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripcion numerica. ';
             END IF;
             -----------------------------------------------------------------------
             IF V_ERROR = 0 THEN
                INSERT
                  INTO SISECOMMERCE_DW.DIM_TIPO_PRODUCTO (TPD_ID, TPD_DESCRIPCION)
                                        VALUES (TO_NUMBER(D_DATOS.TPD_ID), D_DATOS.TPD_DESCRIPCION);
             ELSE
                INSERT INTO SISECOMMERCE_DW.ERROR_SA_TIPO_PRODUCTO (TPD_ID, TPD_DESCRIPCION, TPD_ERROR)
                                                   VALUES (D_DATOS.TPD_ID, D_DATOS.TPD_DESCRIPCION, V_ERROR_MENSAJE);            
             END IF;
             EXCEPTION
                WHEN OTHERS THEN
                    INSERT INTO SISECOMMERCE_DW.ERROR_SA_TIPO_PRODUCTO (TPD_ID, TPD_DESCRIPCION, TPD_ERROR)
                                                       VALUES (D_DATOS.TPD_ID, D_DATOS.TPD_DESCRIPCION, 'Error al insertar');
         END;
      END LOOP;
   END;
   -- Migracion de Tipos de Envios.
   PROCEDURE MigrarTipoEnvio IS
      V_ERROR  INTEGER;
      V_NUMERO INTEGER;
      V_ERROR_MENSAJE VARCHAR2(4000);
      CURSOR C_DATOS IS
         SELECT TE.TPE_ID,
                TE.TPE_DESCRIPCION
           FROM SISECOMMERCE_SA.SA_TIPO_ENVIO TE
          WHERE TE.TPE_ID NOT IN (SELECT D.TPE_ID FROM SISECOMMERCE_DW.DIM_TIPO_ENVIO D)
          ORDER BY TE.TPE_ID;
   BEGIN
      FOR D_DATOS IN C_DATOS LOOP
         BEGIN
             V_ERROR := 0;
             V_ERROR_MENSAJE := '';
             -----------------------------------------------------------------------
             IF D_DATOS.TPE_ID IS NULL THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Codigo Nulo. ';
             END IF;
             --- Codigo de Tipo de Envio no num rico.
             IF VALIDA_NUMERO_ENTERO(D_DATOS.TPE_ID) = 'N' THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Codigo no numerico. ';
             ELSE
                V_NUMERO := TO_NUMBER(D_DATOS.TPE_ID);
                --- Codigo de Tipo de Envio negativo.
                IF V_NUMERO <= 0 THEN
                   V_ERROR := 1;
                   V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Codigo negativo o cero. ';
                END IF;
             END IF;
             -----------------------------------------------------------------------
             IF D_DATOS.TPE_DESCRIPCION IS NULL THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripcion Nula. ';
             END IF;
             IF LENGTH(D_DATOS.TPE_DESCRIPCION) > 40 THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripcion con longitud mayor. ';
             END IF;
             IF LENGTH(D_DATOS.TPE_DESCRIPCION) < 2 THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripcion con longitud menor. ';
             END IF;
             IF VALIDA_NUMERO_ENTERO(D_DATOS.TPE_DESCRIPCION) = 'S' THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripcion numerica. ';
             END IF;
             -----------------------------------------------------------------------
             IF V_ERROR = 0 THEN
                INSERT
                  INTO SISECOMMERCE_DW.DIM_TIPO_ENVIO (TPE_ID, TPE_DESCRIPCION)
                                        VALUES (TO_NUMBER(D_DATOS.TPE_ID), D_DATOS.TPE_DESCRIPCION);
             ELSE
                INSERT INTO SISECOMMERCE_DW.ERROR_SA_TIPO_ENVIO (TPE_ID, TPE_DESCRIPCION, TPE_ERROR)
                                                   VALUES (D_DATOS.TPE_ID, D_DATOS.TPE_DESCRIPCION, V_ERROR_MENSAJE);            
             END IF;
             EXCEPTION
                WHEN OTHERS THEN
                    INSERT INTO SISECOMMERCE_DW.ERROR_SA_TIPO_ENVIO (TPE_ID, TPE_DESCRIPCION, TPE_ERROR)
                                                       VALUES (D_DATOS.TPE_ID, D_DATOS.TPE_DESCRIPCION, 'Error al insertar');
         END;
      END LOOP;
   END;
   -- Migracion de Clientes.
   PROCEDURE MigrarCliente IS
      V_ERROR  INTEGER;
      V_NUMERO INTEGER;
      V_ERROR_MENSAJE VARCHAR2(4000);
      CURSOR C_DATOS IS
         SELECT C.CTE_ID,
                C.CTE_NOMBRE
           FROM SISECOMMERCE_SA.SA_CLIENTE C
          WHERE C.CTE_ID NOT IN (SELECT D.CTE_ID FROM SISECOMMERCE_DW.DIM_CLIENTE D)
          ORDER BY C.CTE_ID;
   BEGIN 
      FOR D_DATOS IN C_DATOS LOOP
         BEGIN
            V_ERROR := 0;
            V_ERROR_MENSAJE := '';
                      
             IF D_DATOS.CTE_ID IS NULL THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Codigo Nulo. ';
             END IF;
             --- Codigo de Cliente no num rico.
             IF VALIDA_NUMERO_ENTERO(D_DATOS.CTE_ID) = 'N' THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Codigo no numerico. ';
             ELSE
                V_NUMERO := TO_NUMBER(D_DATOS.CTE_ID);
                --- Codigo de Cliente negativo.
                IF V_NUMERO <= 0 THEN
                   V_ERROR := 1;
                   V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Codigo negativo o cero. ';
                END IF;
             END IF;
             -----------------------------------------------------------------------
             IF D_DATOS.CTE_NOMBRE IS NULL THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre Nulo. ';
             END IF;
             IF LENGTH(D_DATOS.CTE_NOMBRE) > 60 THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre con longitud mayor. ';
             END IF;
             IF LENGTH(D_DATOS.CTE_NOMBRE) < 2 THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre con longitud menor. ';
             END IF;
             IF VALIDA_NUMERO_ENTERO(D_DATOS.CTE_NOMBRE) = 'S' THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre numerico. ';
             END IF;
             -----------------------------------------------------------------------
             IF V_ERROR = 0 THEN
                INSERT
                  INTO SISECOMMERCE_DW.DIM_CLIENTE (CTE_ID, CTE_NOMBRE)
                                        VALUES (TO_NUMBER(D_DATOS.CTE_ID), D_DATOS.CTE_NOMBRE);
             ELSE
                INSERT INTO SISECOMMERCE_DW.ERROR_SA_CLIENTE (CTE_ID, CTE_NOMBRE, CTE_ERROR)
                                                   VALUES (D_DATOS.CTE_ID, D_DATOS.CTE_NOMBRE, V_ERROR_MENSAJE);            
             END IF;
             EXCEPTION
                WHEN OTHERS THEN
                    INSERT INTO SISECOMMERCE_DW.ERROR_SA_CLIENTE (CTE_ID, CTE_NOMBRE, CTE_ERROR)
                                                       VALUES (D_DATOS.CTE_ID, D_DATOS.CTE_NOMBRE, 'Error al insertar');
         END;
      END LOOP;
   END;  
   -- Migracion de Productos.
   PROCEDURE MigrarProducto IS
      V_ERROR  INTEGER;
      V_NUMERO INTEGER;
      V_ERROR_MENSAJE VARCHAR2(4000);
      CURSOR C_DATOS IS
         SELECT P.PRD_ID,
                P.PRD_NOMBRE
           FROM SISECOMMERCE_SA.SA_PRODUCTO P
          WHERE P.PRD_ID NOT IN (SELECT D.PRD_ID FROM SISECOMMERCE_DW.DIM_PRODUCTO D)
          ORDER BY P.PRD_ID;
   BEGIN 
      FOR D_DATOS IN C_DATOS LOOP
         BEGIN
            V_ERROR := 0;
            V_ERROR_MENSAJE := '';
                      
             IF D_DATOS.PRD_ID IS NULL THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Codigo Nulo. ';
             END IF;
             --- Codigo de Producto no num rico.
             IF VALIDA_NUMERO_ENTERO(D_DATOS.PRD_ID) = 'N' THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Codigo no numerico. ';
             ELSE
                V_NUMERO := TO_NUMBER(D_DATOS.PRD_ID);
                --- Codigo de Producto negativo.
                IF V_NUMERO <= 0 THEN
                   V_ERROR := 1;
                   V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Codigo negativo o cero. ';
                END IF;
             END IF;
             -----------------------------------------------------------------------
             IF D_DATOS.PRD_NOMBRE IS NULL THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre Nulo. ';
             END IF;
             IF LENGTH(D_DATOS.PRD_NOMBRE) > 40 THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre con longitud mayor. ';
             END IF;
             IF LENGTH(D_DATOS.PRD_NOMBRE) < 2 THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre con longitud menor. ';
             END IF;
             IF VALIDA_NUMERO_ENTERO(D_DATOS.PRD_NOMBRE) = 'S' THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre numerico. ';
             END IF;
             -----------------------------------------------------------------------
             IF V_ERROR = 0 THEN
                INSERT
                  INTO SISECOMMERCE_DW.DIM_PRODUCTO (PRD_ID, PRD_NOMBRE)
                                        VALUES (TO_NUMBER(D_DATOS.PRD_ID), D_DATOS.PRD_NOMBRE);
             ELSE
                INSERT INTO SISECOMMERCE_DW.ERROR_SA_PRODUCTO (PRD_ID, PRD_NOMBRE, PRD_ERROR)
                                                   VALUES (D_DATOS.PRD_ID, D_DATOS.PRD_NOMBRE, V_ERROR_MENSAJE);            
             END IF;
             EXCEPTION
                WHEN OTHERS THEN
                    INSERT INTO SISECOMMERCE_DW.ERROR_SA_PRODUCTO (PRD_ID, PRD_NOMBRE, PRD_ERROR)
                                                       VALUES (D_DATOS.PRD_ID, D_DATOS.PRD_NOMBRE, 'Error al insertar');
         END;
      END LOOP;
   END;
   -- Migracion de Fechas.
   PROCEDURE MigrarFecha IS
      V_ERROR  INTEGER;
      V_NUMERO INTEGER;
      V_ERROR_MENSAJE VARCHAR2(4000);
      CURSOR C_DATOS IS
         SELECT O.OCP_ID,
                O.OCP_FECHA
           FROM SISECOMMERCE_SA.SA_ORDEN_COMPRA O
          WHERE O.OCP_ID NOT IN (SELECT D.FEC_ID FROM SISECOMMERCE_DW.DIM_FECHA D)
          ORDER BY O.OCP_ID;
   BEGIN 
      FOR D_DATOS IN C_DATOS LOOP
         BEGIN
            V_ERROR := 0;
            V_ERROR_MENSAJE := '';
                      
             IF D_DATOS.OCP_ID IS NULL THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Codigo Nulo. ';
             END IF;
             --- Codigo de Orden de Compra no num rico.
             IF VALIDA_NUMERO_ENTERO(D_DATOS.OCP_ID) = 'N' THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Codigo no numerico. ';
             ELSE
                V_NUMERO := TO_NUMBER(D_DATOS.OCP_ID);
                --- Codigo de Orden de Compra negativo.
                IF V_NUMERO <= 0 THEN
                   V_ERROR := 1;
                   V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Codigo negativo o cero. ';
                END IF;
             END IF;
             -----------------------------------------------------------------------
             IF D_DATOS.OCP_FECHA IS NULL THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Fecha Nula. ';
             END IF;
            
             IF VALIDA_FECHA(D_DATOS.OCP_FECHA) = 'N' THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Fecha invalida. ';
             END IF;
             -----------------------------------------------------------------------
             IF V_ERROR = 0 THEN
                INSERT
                  INTO SISECOMMERCE_DW.DIM_FECHA (FEC_ID, FEC_FECHA)
                                        VALUES (TO_NUMBER(D_DATOS.OCP_ID), D_DATOS.OCP_FECHA);
             ELSE
                INSERT INTO SISECOMMERCE_DW.ERROR_SA_FECHA (FEC_ID, FEC_FECHA, FEC_ERROR)
                                                   VALUES (D_DATOS.OCP_ID, D_DATOS.OCP_FECHA, V_ERROR_MENSAJE);            
             END IF;
             EXCEPTION
                WHEN OTHERS THEN
                    INSERT INTO SISECOMMERCE_DW.ERROR_SA_FECHA (FEC_ID, FEC_FECHA, FEC_ERROR)
                                                       VALUES (D_DATOS.OCP_ID, D_DATOS.OCP_FECHA, 'Error al insertar');
         END;
      END LOOP;
   END;
   -- Migracion de los datos.
   PROCEDURE MigrarDatos IS
      BEGIN
         MigrarTipoProducto;
         MigrarTipoEnvio;
         MigrarCliente;
         MigrarProducto;
         MigrarFecha;
         COMMIT;
      END;
END ETL_DW;
/

EXECUTE ETL_DW.MigrarDatos;

-- SELECTS de prueba.
-- Tipos de Productos.
SELECT * FROM SISECOMMERCE_DW.DIM_TIPO_PRODUCTO;
SELECT * FROM SISECOMMERCE_DW.ERROR_SA_TIPO_PRODUCTO;

-- Tipos de Envios.
SELECT * FROM SISECOMMERCE_DW.DIM_TIPO_ENVIO;
SELECT * FROM SISECOMMERCE_DW.ERROR_SA_TIPO_ENVIO;

-- Clientes.
SELECT * FROM SISECOMMERCE_DW.DIM_CLIENTE;
SELECT * FROM SISECOMMERCE_DW.ERROR_SA_CLIENTE;

-- Productos.
SELECT * FROM SISECOMMERCE_DW.DIM_PRODUCTO;
SELECT * FROM SISECOMMERCE_DW.ERROR_SA_PRODUCTO;

-- Fechas.
SELECT * FROM SISECOMMERCE_DW.DIM_FECHA;
SELECT * FROM SISECOMMERCE_DW.ERROR_SA_FECHA;

-- Fact_Ordenes.  
SELECT * FROM SISECOMMERCE_DW.FACT_ORDENES;


