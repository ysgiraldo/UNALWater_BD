import pandas as pd
import geopandas as gpd
from faker import Faker
from faker.providers import date_time
from datetime import datetime
import random
import time
import subprocess

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

# Definir la función para generar datos
def generate_data():
    data = []
    for _ in range(1000):  # Ajustar según la cantidad inicial que se requiera
        # Generar puntos aleatorios dentro del polígono de Medellín
        while True:
            lon, lat = medellin_polygon.representative_point().coords[0]
            latitud = random.uniform(
                lat - 0.9, lat + 0.9
            )  # Ajustar según la dispersión deseada
            longitud = random.uniform(
                lon - 0.9, lon + 0.9
            )  # Ajustar según la dispersión deseada
            new_point = (longitud, latitud)
            if medellin_polygon.contains(gpd.points_from_xy([longitud], [latitud])[0]):
                break

        # Generar fecha dentro del año actual y del año anterior, sin superar el día actual
        fecha = fake.date_time_between_dates(
            datetime(fecha_actual.year - 1, 1, 1), fecha_actual
        )

        # Obtener IDs de cliente y empleado
        customer_id = fake.random_element(customers_df["customer_id"])
        employee_id = fake.random_element(employees_df["employee_id"])

        # Generar número de orden y cantidad (entero)
        order = fake.random_number(digits=10)
        cantidad = fake.random_int(min=1, max=99)  # Ajustar a un número entero

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

# Intervalo de tiempo en segundos
interval = 0  # Ajustar según sea necesario

# Limitar a un número de iteraciones
max_iterations = 1
iterations = 0

# Crear un DataFrame vacío para almacenar todos los registros generados
all_data_df = pd.DataFrame()

while iterations < max_iterations:
    # Generar datos
    data = generate_data()

    # Crear DataFrame
    df = pd.DataFrame(data)

    # Añadir los nuevos datos al DataFrame acumulativo
    all_data_df = pd.concat([all_data_df, df], ignore_index=True)

    print(f"Datos generados en la iteración {iterations + 1}")

    # Incrementar el contador de iteraciones
    iterations += 1

    # Esperar el intervalo especificado
    time.sleep(interval)

# Formatear la fecha y hora actual
fecha_hora_actual = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Guardar todos los datos acumulados como archivo Parquet
parquet_filename = f"datos_rt_completos_{fecha_hora_actual}.parquet"
all_data_df.to_parquet(parquet_filename, index=False)
print(f"Todos los datos generados guardados en '{parquet_filename}'")


# Crear la ruta en Hadoop
hadoop_path = "/Stagging"
create_directory_command = f"hadoop fs -mkdir -p {hadoop_path}"
process = subprocess.Popen(create_directory_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout, stderr = process.communicate()


# Transferir el archivo Parquet con todos los datos a Hadoop
command = f"hadoop fs -put -f {parquet_filename} {hadoop_path}"
process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout, stderr = process.communicate()

if process.returncode == 0:
    print(f"Archivo '{parquet_filename}' transferido a Hadoop en '{hadoop_path}'")
else:
    print(f"Error al transferir el archivo '{parquet_filename}' a Hadoop: {stderr.decode()}")