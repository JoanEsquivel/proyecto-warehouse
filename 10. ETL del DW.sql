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

--------------------------------------------------------------------------------
-- Especificacion del parquete.
--------------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE SISECOMMERCE_DW.ETL_DW AS
   PROCEDURE MigrarTipoProducto;
   PROCEDURE MigrarTipoEnvio;
   PROCEDURE MigrarCliente;
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

   -- Migracion de los datos.
   PROCEDURE MigrarDatos IS
      BEGIN
         MigrarTipoProducto;
         MigrarTipoEnvio;
         MigrarCliente;
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

-- Fact_Ordenes.  
SELECT * FROM SISECOMMERCE_DW.FACT_ORDENES;


