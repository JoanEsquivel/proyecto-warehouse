-- Requerimiento:
-- Insersión de datos en el modelo relacional (al menos 5 filas en las tablas de tipos y 20 filas en las tablas de producto, cliente y orden) 


INSERT INTO TIPO_ENVIO (TPE_ID, TPE_DESCRIPCION, TPE_ESTADO, TPE_REQUIERE_CONFIRMACION) VALUES
(1, 'Entrega San José', 1, 1),
(2, 'Entrega Cartago', 1, 1),
(3, 'Entrega Alajuela', 1, 0),
(4, 'Recoleccion en bodega', 1, 0),
(5, 'Entrega Express GAM', 1, 1),
(6, 'Entrega Puntarenas', 1, 1),
(7, 'Entrega Guanacaste', 1, 1);

INSERT INTO TIPO_PRODUCTO (TPD_ID, TPD_DESCRIPCION) VALUES
(1, 'Cafe y granos'),
(2, 'Frutas tropicales'),
(3, 'Productos lacteos'),
(4, 'Verduras y hortalizas'),
(5, 'Productos procesados'),
(6, 'Condimentos y especias'),
(7, 'Bebidas');


INSERT INTO CLIENTE (CTE_ID, CTE_NOMBRE) VALUES
(1, 'María José Vargas Solis'),
(2, 'Carlos Eduardo Jimenez Mora'),
(3, 'Ana Lucía Rodriguez Vega'),
(4, 'Jose Miguel Hernandez Castro'),
(5, 'Sofia Alejandra Murillo Sanchez'),
(6, 'Diego Andres Calderon Perez'),
(7, 'Gabriela Beatriz Montoya Quiros'),
(8, 'Luis Fernando Chacon Rojas'),
(9, 'Priscilla Nicole Zamora Aguilar'),
(10, 'Esteban Alejandro Mendez Villalobos'),
(11, 'Valeria Stephany Arias Campos'),
(12, 'Roberto Carlos Fonseca Madrigal'),
(13, 'Karla Tatiana Espinoza Gutierrez'),
(14, 'Adrian Mauricio Cordero Lizano'),
(15, 'Melissa Andrea Salas Barboza'),
(16, 'Pablo Enrique Navarro Chinchilla'),
(17, 'Natalia Fernanda Carvajal Umana'),
(18, 'Mauricio Alonso Trejos Vindas'),
(19, 'Daniela Carolina Alfaro Piedra'),
(20, 'Randall Steven Porras Gamboa'),
(21, 'Fabiola Esperanza Quesada Monge'),
(22, 'Kenneth Alejandro Delgado Vasquez');

INSERT INTO PRODUCTO (PRD_ID, PRD_TPD_ID, PRD_NOMBRE, PRD_CANTIDAD, PRD_COSTO_UNITARIO) VALUES
(1, 1, 'Cafe Tarrazu grano oro', 50.00, 2500.00),
(2, 1, 'Cafe Molido Tres Volcanes', 75.00, 1800.00),
(3, 2, 'Pina Premium', 120.00, 650.00),
(4, 2, 'Banano', 200.00, 450.00),
(5, 2, 'Mango', 80.00, 550.00),
(6, 3, 'Queso fresco Turrialba', 30.00, 3200.00),
(7, 3, 'Natilla Dos Pinos 500ml', 100.00, 850.00),
(8, 4, 'Chayote nacional', 150.00, 320.00),
(9, 4, 'Yuca fresca', 90.00, 450.00),
(10, 4, 'Chile dulce rojo', 60.00, 680.00),
(11, 5, 'Gallo pinto instantáneo', 40.00, 1200.00),
(12, 5, 'Salsa Lizano 700ml', 85.00, 950.00),
(13, 2, 'Cas maduro', 70.00, 480.00),
(14, 2, 'Guanábana', 25.00, 850.00),
(15, 6, 'Achiote en polvo', 20.00, 750.00),
(16, 6, 'Culantro castilla fresco', 45.00, 250.00),
(17, 7, 'Agua Cristal 600ml', 200.00, 285.00),
(18, 7, 'Refresco Tropical 2L', 60.00, 980.00),
(19, 1, 'Frijoles negros secos', 100.00, 850.00),
(20, 3, 'Crema dulce Dos Pinos', 80.00, 720.00),
(21, 4, 'Nampi Morado', 55.00, 420.00),
(22, 5, 'Chicharron Casero', 15.00, 2800.00);

INSERT INTO ORDEN_COMPRA (OCP_ID, OCP_PRD_ID, OCP_CTE_ID, OCP_TPE_ID, OCP_FECHA) VALUES
(1, 1, 1, 1, TO_DATE('2024-01-15', 'YYYY-MM-DD')),
(2, 5, 3, 2, TO_DATE('2024-01-18', 'YYYY-MM-DD')),
(3, 12, 7, 4, TO_DATE('2024-01-22', 'YYYY-MM-DD')),
(4, 8, 12, 3, TO_DATE('2024-02-01', 'YYYY-MM-DD')),
(5, 15, 5, 5, TO_DATE('2024-02-05', 'YYYY-MM-DD')),
(6, 3, 18, 1, TO_DATE('2024-02-10', 'YYYY-MM-DD')),
(7, 9, 2, 6, TO_DATE('2024-02-14', 'YYYY-MM-DD')),
(8, 20, 9, 2, TO_DATE('2024-02-18', 'YYYY-MM-DD')),
(9, 6, 15, 4, TO_DATE('2024-02-22', 'YYYY-MM-DD')),
(10, 11, 4, 7, TO_DATE('2024-03-01', 'YYYY-MM-DD')),
(11, 17, 20, 3, TO_DATE('2024-03-05', 'YYYY-MM-DD')),
(12, 2, 8, 1, TO_DATE('2024-03-08', 'YYYY-MM-DD')),
(13, 14, 11, 5, TO_DATE('2024-03-12', 'YYYY-MM-DD')),
(14, 4, 6, 2, TO_DATE('2024-03-15', 'YYYY-MM-DD')),
(15, 19, 14, 4, TO_DATE('2024-03-18', 'YYYY-MM-DD')),
(16, 7, 13, 6, TO_DATE('2024-03-22', 'YYYY-MM-DD')),
(17, 18, 16, 1, TO_DATE('2024-03-25', 'YYYY-MM-DD')),
(18, 10, 19, 3, TO_DATE('2024-03-28', 'YYYY-MM-DD')),
(19, 13, 17, 7, TO_DATE('2024-04-01', 'YYYY-MM-DD')),
(20, 16, 10, 5, TO_DATE('2024-04-05', 'YYYY-MM-DD')),
(21, 21, 21, 2, TO_DATE('2024-04-08', 'YYYY-MM-DD')),
(22, 22, 22, 4, TO_DATE('2024-04-12', 'YYYY-MM-DD'));

COMMIT;




