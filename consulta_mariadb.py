import mariadb
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from shapely import wkt

# Establecer conexión con la base de datos MariaDB
conn = mariadb.connect(user="root", password="", host="localhost")

cur = conn.cursor()

cur.execute("USE demo_db;")

# Seleccionar datos de las tablas
cur.execute("SELECT * FROM t50001 LIMIT 5;")
result1 = cur.fetchall()

cur.execute("SELECT * FROM customers LIMIT 5;")
result2 = cur.fetchall()

cur.execute("SELECT * FROM employees LIMIT 5;")
result3 = cur.fetchall()

cur.execute("SELECT * FROM medellin_neighborhoods LIMIT 5;")
result4 = cur.fetchall()

conn.close()

# Imprimir resultados
print("Resultados de t50001:")
print(result1)
print("\nResultados de customers:")
print(result2)
print("\nResultados de employees:")
print(result3)
print("\nResultados de medellin_neighborhoods:")
print(result4)
