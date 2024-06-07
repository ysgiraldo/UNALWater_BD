import pandas as pd
import geopandas as gpd
from faker import Faker
from faker.providers import date_time
from datetime import datetime
from shapely.geometry import Point
import random
import time
import subprocess
import mariadb  # Importamos mariadb

# Inicializar Faker
fake = Faker("es_ES")
fake.add_provider(date_time)

# Obtener la fecha actual
fecha_actual = datetime.now()

# Cargar archivos Parquet de clientes y empleados
customers_df = pd.read_parquet("./data/customers.parquet")
employees_df = pd.read_parquet("./data/employees.parquet")

# Cargar archivo Parquet con el registro de Medellín usando geopandas
medellin_gdf = gpd.read_parquet("./data/50001.parquet")

# Extraer el polígono de Medellín
medellin_polygon = medellin_gdf.geometry.iloc[0]

# Parámetros para la distribución normal
mu_lon, mu_lat = medellin_polygon.representative_point().coords[0]
sigma = 0.3  # Desviación estándar (ajusta según sea necesario)

# Definir la función para generar datos
def generate_data():
    data = []
    for _ in range(1000):  # Ajustar según la cantidad inicial que se requiera
        # Generar puntos aleatorios dentro del polígono de Medellín
        while True:
            latitud = random.gauss(mu_lat, sigma)
            longitud = random.gauss(mu_lon, sigma)
            new_point = Point(longitud, latitud)
            if medellin_polygon.contains(new_point):
                break

        # Generar fecha dentro del dia actual
        fecha = fake.date_time_between_dates(datetime(fecha_actual.year - 1, 1, 1), fecha_actual)
        # Formatear la fecha en una cadena de texto compatible con MariaDB (YYYY-MM-DD HH:MM:SS)
        fecha_str = fecha.strftime('%Y-%m-%d %H:%M:%S')

        # Obtener IDs de cliente y empleado
        customer_id = fake.random_element(customers_df["customer_id"])
        employee_id = fake.random_element(employees_df["employee_id"])

        # Generar número de orden y cantidad (entero)
        order = fake.random_number(digits=10)
        cantidad = fake.random_int(min=20, max=99)

        data.append({
            "latitude": latitud,
            "longitude": longitud,
            "date": fecha_str,
            "customer_id": customer_id,
            "employee_id": employee_id,
            "quantity_products": cantidad,
            "order_id": order,
        })
    return data

# Intervalo de tiempo en segundos
interval = 30  # Ajustar según sea necesario

# Limitar a un número de iteraciones, por ejemplo, 2 iteraciones por minuto
max_iterations = 1
iterations = 0


while iterations < max_iterations:
    # Generar datos
    data = generate_data()

    # Crear DataFrame
    df = pd.DataFrame(data)

    # Guardar como archivo JSON
    json_filename = "datos_generados.json"
    df.to_json(json_filename, orient="records", date_format="iso", lines=True)
    print(f"Datos generados y guardados en '{json_filename}'")

    # Convertir a Parquet
    parquet_filename = "datos_generados.parquet"
    df.to_parquet(parquet_filename, index=False)
    print(f"Datos convertidos y guardados en '{parquet_filename}'")

    
    # Incrementar el contador de iteraciones
    iterations += 1

    # Esperar el intervalo especificado
    time.sleep(interval)


######################################################################33

import mariadb

conn = mariadb.connect(
    user="root",
    password="",
)

cur = conn.cursor()

# Creación de la BD
cur.execute("DROP DATABASE IF EXISTS TF_db;")
cur.execute("CREATE DATABASE TF_db;")
cur.execute("USE TF_db;")

cur.execute(
    """
    DROP TABLE IF EXISTS ventas;
    """
)

cur.execute(
    """
    CREATE TABLE ventas (
        latitude       VARCHAR(50),
        longitude           VARCHAR(50),
        date            VARCHAR(50),
        customer_id       VARCHAR(50),
        employee_id      VARCHAR(50),
        quantity_products      VARCHAR(50),
        order_id      VARCHAR(50)
    );
    """
)

conn.commit()

# Cargar datos del archivo Parquet
ventas = pd.read_parquet("/workspace/UNALWater_BD/datos_generados.parquet")

for i, row in ventas.iterrows():
    sql = "INSERT INTO ventas VALUES (%s,%s,%s,%s,%s,%s,%s)"
    cur.execute(sql, tuple(row))
    conn.commit()

cur.execute("SELECT * FROM ventas LIMIT 5;")
result = cur.fetchall()
print(result)

# Creación y permisos para el usuario remoto
cur.execute("CREATE USER 'sqoop'@'%' IDENTIFIED BY 'secret';")
cur.execute("GRANT ALL ON TF_db.* TO 'sqoop'@'%';")

conn.close()
