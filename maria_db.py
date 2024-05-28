import mariadb

conn = mariadb.connect(
    user="root",
    password="",
)

cur = conn.cursor()

#
# Creación de la BD
#
cur.execute("DROP DATABASE IF EXISTS demo_db;")
cur.execute("CREATE DATABASE demo_db;")
cur.execute("USE demo_db;")

cur.execute(
    """
    DROP TABLE IF EXISTS fiveone_pqt;
    """
)

cur.execute(
    """
    CREATE TABLE fiveone_pqt (
        DPTOMPIO VARCHAR(50),
        DPTO_CCDGO VARCHAR(50),
        MPIO_CCDGO VARCHAR(50),
        MPIO_CNMBR VARCHAR(50),
        MPIO_CCNCT VARCHAR(50),
        geometry BLOB
    );
    """
)

cur.execute(
    """
    DROP TABLE IF EXISTS customers;
    """
)

cur.execute(
    """
    CREATE TABLE customers (
        customer_id VARCHAR(50),
        name VARCHAR(50),
        phone VARCHAR(50),
        email VARCHAR(50),
        address VARCHAR(50)
    );
    """
)

cur.execute(
    """
    DROP TABLE IF EXISTS employees;
    """
)

cur.execute(
    """
    CREATE TABLE employees (
        employee_id VARCHAR(50),
        name VARCHAR(50),
        phone VARCHAR(50),
        email VARCHAR(50),
        address VARCHAR(50),
        comission VARCHAR(50)
    );
    """
)

cur.execute(
    """
    DROP TABLE IF EXISTS medellin_neighborhoods;
    """
)

cur.execute(
    """
    CREATE TABLE medellin_neighborhoods (
        OBJECTID VARCHAR(50),
        CODIGO VARCHAR(50),
        NOMBRE VARCHAR(50),
        IDENTIFICACION VARCHAR(50),
        LIMITEMUNICIPIOID VARCHAR(50),
        SUBTIPO_COMUNACORREGIMIENTO VARCHAR(50),
        LINK_DOCUMENTO VARCHAR(50),
        SHAPEAREA VARCHAR(50),
        SHAPELEN VARCHAR(50),
        geometry BLOB
    );
    """
)

conn.commit()

import pandas as pd

# Leer datos desde los archivos parquet
fiveone_pqt = pd.read_parquet("/tmp/50001.parquet")
print(fiveone_pqt)
customers = pd.read_parquet("/tmp/customers.parquet")
print(customers)
employees = pd.read_parquet("/tmp/employees.parquet")
print(employees)
medellin_neighborhoods = pd.read_parquet("/tmp/medellin_neighborhoods.parquet")
print(medellin_neighborhoods)


# Consulta SQL para seleccionar los primeros 5 registros de cada tabla
queries = [
    "SELECT * FROM fiveone_pqt LIMIT 5;",
    "SELECT * FROM customers LIMIT 5;",
    "SELECT * FROM employees LIMIT 5;",
    "SELECT * FROM medellin_neighborhoods LIMIT 5;",
]

# Ejecutar las consultas y mostrar los resultados
for query in queries:
    cur.execute(query)
    result = cur.fetchall()
    print(result)

#
# Creación y permisos para el usuario remoto
#
cur.execute("CREATE USER 'sqoop'@'%' IDENTIFIED BY 'secret'; ")
cur.execute("GRANT ALL ON demo_db.* TO 'sqoop'@'%';")


conn.close()
