import pandas as pd
import geopandas as gpd
import argparse
from faker import Faker
from faker.providers import date_time
from datetime import datetime, time
from shapely.geometry import Point
import os.path
import pyarrow as pa
import pyarrow.parquet as pq  
import random
import subprocess
import time

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

# Definir la función para guardar los datos en formato JSON
def save_data(data):
    # Crear DataFrame
    df = pd.DataFrame(data)

    # Guardar como archivo JSON, sobrescribir el archivo existente
    df.to_json("datos.json", orient="records", date_format="iso", lines=True)

# Definir la función para guardar los datos en formato Parquet
def save_data_parquet(data):
    # Convertir los datos en un DataFrame
    df = pd.DataFrame(data)
    
    # Definir el nombre del archivo Parquet
    parquet_filename = "datos.parquet"
    
    # Si el archivo Parquet ya existe, agregar los nuevos datos al final
    if os.path.exists(parquet_filename):
        existing_table = pq.read_table(parquet_filename)
        existing_df = existing_table.to_pandas()
        df = pd.concat([existing_df, df], ignore_index=True)
    
    # Guardar el DataFrame actualizado como archivo Parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_filename)


# Definir la función principal
# py simulacion.py --segundos 60 --simulaciones 2
def main(segundos=30, simulaciones=1):
    tiempo_inicio = time.time()  # Tiempo de inicio de la simulación
    tiempo_actual = time.time()   # Tiempo actual
    tiempo_simulacion = segundos * simulaciones  # Tiempo total de simulación

    # Simular hasta que el tiempo transcurrido alcance el tiempo total de simulación
    while tiempo_actual - tiempo_inicio < tiempo_simulacion:
        # Generar datos
        new_data = generate_data()

        # Guardar los nuevos datos en formato JSON
        save_data(new_data)

        # Guardar los nuevos datos en formato Parquet
        save_data_parquet(new_data)
        
        # Esperar el intervalo especificado antes de continuar
        time.sleep(segundos)
        
        # Actualizar el tiempo actual
        tiempo_actual = time.time()

    print("Simulación completa.")

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
#python -c "import pandas as pd; print(len(pd.read_parquet('datos.parquet')))"
