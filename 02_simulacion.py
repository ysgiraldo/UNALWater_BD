import argparse
import os.path
import subprocess
import time
from collections import deque
from datetime import datetime, time as dt_time
import random

import geopandas as gpd
import pandas as pd
from faker import Faker
from faker.providers import date_time
from shapely.geometry import Point

# Inicializar Faker
fake = Faker("es_ES")
fake.add_provider(date_time)

# Obtener la fecha y hora actual
fecha_actual = datetime.now()

# Obtener solo la fecha de hoy
fecha_hoy = fecha_actual.date()

# Cargar archivos Parquet de clientes y empleados
customers_df = pd.read_parquet("./data/customers.parquet")
employees_df = pd.read_parquet("./data/employees.parquet")

# Cargar archivo Parquet con el registro de Medellín usando geopandas
medellin_gdf = gpd.read_parquet("./data/50001.parquet")

# Extraer el polígono de Medellín
medellin_polygon = medellin_gdf.geometry.iloc[0]

# Parámetros para la distribución normal
mu_lon, mu_lat = medellin_polygon.representative_point().coords[0]
sigma = 0.9  # Desviación estándar (se ajusta según sea necesario)

# Definir la función para generar datos
def generate_data(num_samples=100):
    data = []
    minx, miny, maxx, maxy = medellin_polygon.bounds
    for _ in range(num_samples):
        # Generar puntos aleatorios dentro del polígono de Medellín
        while True:
            latitud = random.gauss(mu_lat, sigma)
            longitud = random.gauss(mu_lon, sigma)
            new_point = Point(longitud, latitud)
            if medellin_polygon.contains(new_point):
                break

        # Generar del día actual una hora aleatoria que no supere la hora actual
        while True:
            hora_random = fake.time_object(end_datetime=fecha_actual)
            fecha = datetime.combine(fecha_hoy, hora_random)
            if fecha <= fecha_actual:
                break
        
        # Obtener IDs de cliente y empleado
        customer_id = fake.random_element(customers_df["customer_id"])
        employee_id = fake.random_element(employees_df["employee_id"])

        # Generar número de orden y cantidad (entero)
        order = fake.random_number(digits=10)
        cantidad = fake.random_int(min=20, max=99)  # Ajustar a un número entero

        data.append(
            {
                "latitude": latitud,
                "longitude": longitud,
                "date": fecha,
                "customer_id": customer_id,
                "employee_id": employee_id,
                "quantity_products": cantidad,
                "order_id": order,
            }
        )
    return data

# Definir la función para guardar los datos en formato json
def save_data(data, filename="datos.json"):
    df = pd.DataFrame(data)
    df.to_json(filename, orient="records", date_format="iso", lines=True)

# Definir la función para guardar los datos en formato parquet
def save_data_parquet(data, filename="datos.parquet"):
    df = pd.DataFrame(data)
    df.to_parquet(filename, index=False)

# Definir la función principal
def main(segundos=30, simulaciones=1, num_samples=100):
    # Cola para los archivos de simulación
    archivos_en_espera = deque()
    tiempo_inicio = time.time()
    
    # Generar datos una vez al inicio para todas las simulaciones
    all_data = [generate_data(num_samples) for _ in range(simulaciones)]
    
    for i, new_data in enumerate(all_data, start=1):
        # Guardar los nuevos datos en formato JSON
        json_filename = f"datos_simulacion_{i}.json"
        save_data(new_data, json_filename)
        
        # Guardar los nuevos datos en formato Parquet
        parquet_filename = f"datos_simulacion_{i}.parquet"
        save_data_parquet(new_data, parquet_filename)
        
        # Añadir el archivo Parquet a la cola
        archivos_en_espera.append(parquet_filename)

    print("Simulación completa.")
    print("Archivos en cola de espera:")
    for archivo in archivos_en_espera:
        print(archivo)

if __name__ == "__main__":
    # Parsear los argumentos de línea de comandos
    parser = argparse.ArgumentParser(description="Simulación de generación de datos")
    parser.add_argument("--segundos", type=int, default=30, help="Intervalo de ejecución en segundos (por defecto: 30)")
    parser.add_argument("--simulaciones", type=int, default=1, help="Número de simulaciones (por defecto: 1)")
    args = parser.parse_args()

    # Ejecutar la simulación
    main(args.segundos, args.simulaciones)

# # Transferir archivo con datos generados a Hadoop ejecutando comandos de shell desde Python
# hadoop_path = "/tmp/datos_rt.parquet"  # Ajustar según sea necesario
# command = f"hadoop fs -put -f {parquet_filename} {hadoop_path}"
# process = subprocess.Popen(
#     command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
#     )
# stdout, stderr = process.communicate()

# if process.returncode == 0:
#     print(f"Archivo '{parquet_filename}' transferido a Hadoop en '{hadoop_path}'")
# else:
#     print(
#         f"Error al transferir el archivo '{parquet_filename}' a Hadoop: {stderr.decode()}"
#     )
#
# py 02_simulacion.py --segundos 60 --simulaciones 2
#python -c "import pandas as pd; print(len(pd.read_parquet('datos.parquet')))"
