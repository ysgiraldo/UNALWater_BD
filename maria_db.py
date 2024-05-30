import mariadb
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from shapely import wkt

# Establecer conexión con la base de datos MariaDB
conn = mariadb.connect(user="root", password="", host="localhost")
# Crear una conexión SQLAlchemy para cargar los datos
engine = create_engine("mariadb+mariadbconnector://root:@localhost/demo_db")
cur = conn.cursor()

# Crear base de datos
cur.execute("DROP DATABASE IF EXISTS demo_db;")
cur.execute("CREATE DATABASE demo_db;")
cur.execute("USE demo_db;")

# Crear tablas en la base de datos
table_queries = [
    """
    CREATE TABLE t50001 (
        DPTOMPIO VARCHAR(50),
        DPTO_CCDGO VARCHAR(50),
        MPIO_CCDGO VARCHAR(50),
        MPIO_CNMBR VARCHAR(50),
        MPIO_CCNCT VARCHAR(50),
        geometry GEOMETRY
    );
    """,
    """
    CREATE TABLE customers (
        customer_id VARCHAR(50),
        name VARCHAR(50),
        phone VARCHAR(50),
        email VARCHAR(50),
        address VARCHAR(50)
    );
    """,
    """
    CREATE TABLE employees (
        employee_id VARCHAR(50),
        name VARCHAR(50),
        phone VARCHAR(50),
        email VARCHAR(50),
        address VARCHAR(50),
        comission FLOAT
    );
    """,
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
        geometry GEOMETRY
    );
    """,
]

for query in table_queries:
    cur.execute(query)

conn.commit()


##t1,2,3,4
t50001 = gpd.read_parquet("/workspace/UNALWater_BD/data/50001.parquet")
customers = pd.read_parquet("/workspace/UNALWater_BD/data/customers.parquet")
employees = pd.read_parquet("/workspace/UNALWater_BD/data/employees.parquet")
medellin_neighborhoods = gpd.read_parquet(
    "/workspace/UNALWater_BD/data/medellin_neighborhoods.parquet"
)

# Convertir geometría a WKT y convertir a DataFrame de pandas
t50001["geometry"] = t50001["geometry"].apply(lambda x: wkt.dumps(x) if x else None)
t50001 = pd.DataFrame(t50001)

medellin_neighborhoods["geometry"] = medellin_neighborhoods["geometry"].apply(
    lambda x: wkt.dumps(x) if x else None
)
medellin_neighborhoods = pd.DataFrame(medellin_neighborhoods)

# t1
for i, row in t50001.iterrows():
    sql1 = "INSERT INTO t50001 VALUES (%s,%s,%s,%s,%s,ST_GeomFromText(%s))"
    cur.execute(sql1, tuple(row))
    conn.commit()

cur.execute("SELECT * FROM t50001 LIMIT 5;")
result1 = cur.fetchall()

##t2
for i, row in customers.iterrows():
    sql2 = "INSERT INTO customers VALUES (%s,%s,%s,%s,%s)"
    cur.execute(sql2, tuple(row))
    conn.commit()

cur.execute("SELECT * FROM customers LIMIT 5;")
result2 = cur.fetchall()

# t3
for i, row in employees.iterrows():
    sql3 = "INSERT INTO employees VALUES (%s,%s,%s,%s,%s,%s)"
    cur.execute(sql3, tuple(row))
    conn.commit()

cur.execute("SELECT * FROM employees LIMIT 5;")
result3 = cur.fetchall()

# t4
for i, row in medellin_neighborhoods.iterrows():
    sql4 = "INSERT INTO medellin_neighborhoods VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,ST_GeomFromText(%s))"
    cur.execute(sql4, tuple(row))
    conn.commit()

cur.execute("SELECT * FROM medellin_neighborhoods LIMIT 5;")
result4 = cur.fetchall()


# Lista de archivos Parquet y si contienen datos geoespaciales
archivos_parquet = [
    ("/workspace/UNALWater_BD/data/50001.parquet", "t50001", True),
    ("/workspace/UNALWater_BD/data/customers.parquet", "customers", False),
    ("/workspace/UNALWater_BD/data/employees.parquet", "employees", False),
    (
        "/workspace/UNALWater_BD/data/medellin_neighborhoods.parquet",
        "medellin_neighborhoods",
        True,
    ),
]


# Crear y otorgar permisos al usuario remoto
cur.execute("CREATE USER 'sqoop'@'%' IDENTIFIED BY 'secret'; ")
cur.execute("GRANT ALL ON demo_db.* TO 'sqoop'@'%';")

conn.close()
# result1
# result2
# result3
# result4
