--------------------------------------------------------------------------------
-- Función para validar npumeros enteros.
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

-- FunciÓn para validar fecha.
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

CREATE TABLE SISBANCA_DW.ERROR_SA_TIPO_CLIENTE (
   TCL_ID             VARCHAR2(255),
   TCL_DESCRIPCION    VARCHAR2(255),
   TCL_ERROR          VARCHAR2(4000)
);

--------------------------------------------------------------------------------
-- Especificación del parquete.
--------------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE SISBANCA_DW.ETL_DW AS
   PROCEDURE MigrarTipoCliente;
   PROCEDURE MigrarDatos;
END ETL_DW;
/
--------------------------------------------------------------------------------
-- Cuerpo del parquete.
--------------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE BODY SISBANCA_DW.ETL_DW AS
   -- Migración de Clientes.
   PROCEDURE MigrarTipoCliente IS
      V_ERROR  INTEGER;
      V_NUMERO INTEGER;
      V_ERROR_MENSAJE VARCHAR2(4000);
      CURSOR C_DATOS IS
         SELECT TC.TCL_ID,
                TC.TCL_DESCRIPCION
           FROM SISBANCA_SA.SA_TIPO_CLIENTE TC
          WHERE TC.TCl_ID NOT IN (SELECT D.TCl_ID FROM SISBANCA_DW.DIM_TIPO_CLIENTE D)
          ORDER BY TC.TCl_ID;
   BEGIN
      FOR D_DATOS IN C_DATOS LOOP
         BEGIN
             V_ERROR := 0;
             V_ERROR_MENSAJE := '';
             -----------------------------------------------------------------------
             IF D_DATOS.TCl_ID IS NULL THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Código Nulo. ';
             END IF;
             --- Codigo de Cliente no num rico.
             IF VALIDA_NUMERO_ENTERO(D_DATOS.TCl_ID) = 'N' THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'CÓdigo no numérico. ';
             ELSE
                V_NUMERO := TO_NUMBER(D_DATOS.TCl_ID);
                --- Codigo de Cliente negativo.
                IF V_NUMERO <= 0 THEN
                   V_ERROR := 1;
                   V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Código Negativo o cero. ';
                END IF;
             END IF;
             -----------------------------------------------------------------------
             IF D_DATOS.TCl_DESCRIPCION IS NULL THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripción Nula. ';
             END IF;
             IF LENGTH(D_DATOS.TCl_DESCRIPCION) > 30 THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripción con longitud mayor. ';
             END IF;
             IF LENGTH(D_DATOS.TCl_DESCRIPCION) < 2 THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripción con longitud menor. ';
             END IF;
             IF VALIDA_NUMERO_ENTERO(D_DATOS.TCl_DESCRIPCION) = 'S' THEN
                V_ERROR := 1;
                V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripción numérica. ';
             END IF;
             -----------------------------------------------------------------------
             IF V_ERROR = 0 THEN
                INSERT
                  INTO SISBANCA_DW.DIM_TIPO_CLIENTE (TCl_ID, TCl_DESCRIPCION)
                                        VALUES (TO_NUMBER(D_DATOS.TCl_ID), D_DATOS.TCl_DESCRIPCION);
             ELSE
                INSERT INTO SISBANCA_DW.ERROR_SA_TIPO_CLIENTE (TCl_ID, TCl_DESCRIPCION, TCl_ERROR)
                                                   VALUES (D_DATOS.TCl_ID, D_DATOS.TCl_DESCRIPCION, V_ERROR_MENSAJE);            
             END IF;
             EXCEPTION
                WHEN OTHERS THEN
                    INSERT INTO SISBANCA_DW.ERROR_SA_TIPO_CLIENTE (TCl_ID, TCl_DESCRIPCION, TCl_ERROR)
                                                       VALUES (D_DATOS.TCl_ID, D_DATOS.TCl_DESCRIPCION, 'Error al insertar');
         END;
      END LOOP;
   END;
   -- Migración de los datos.
   PROCEDURE MigrarDatos IS
      BEGIN
         MigrarTipoCliente;
         COMMIT;
      END;
END ETL_DW;
/

EXECUTE ETL_DW.MigrarDatos;

