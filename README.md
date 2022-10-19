# DataWarehouse
Realizado por: Victor Ponce

## Descripción 
Este proyecto fue realizado para ejemplificar el proceso que se lleva a cabo en las diferentes etapas para la creación de un Data Warehouse. 
En este caso comenzamos con la extracción de los datos con un proyecto realizado en Python 3.10 que a través de un archivo .csv, almacena automáticamente
dentro de la base de datos que sea indicada.

## Requisitos para su ejecución 

- MySQL Workbench 8
- Python 3.10 con las siguientes librerias: pandas, utils, configparser, traceback, sqlalchemy y pymysql
- IDE de desarrollo, en este caso Visual Studio Code

## Proceso para su ejecución 

Se debe crear la misma estructura presentada de la base de datos SOR y STG.
Luego, clonamos el repositorio con el siguiente comando:
`git clone https://github.com/victor24-sudo/DataWarehouse.git`
Una vez que se haya clonado en el apartado .properties, se debe colocar la información de su usuario y contraseña de su cuenta de MySQL
`User = root  Password = 248816`

### Resultados 
Debe ingresar al archivo py_startup.py y ejecutarlo. En este caso si su configuración fue la correcta se deberá observar los mismos resultados

### MySQL 

Una vez que el programa no haya dado ningún error puede ingresar a su MySQL WorkBench y verificar que efectivamente toda la información a sido cargada correctamente
Puede usar el comando `SELECT * FROM channels` para poder ver la información. Y si desea ver el número de filas que tiene cada tabla puede usar el comando 
`SELECT Count(*) FROM channels`. 

Puede ser aplicado para todas las tablas, solo se debe cambiar el nombre de la tabla a la que queremos realiazar la consulta.
