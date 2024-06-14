import pandas as pd
import geopandas as gpd
from faker import Faker
from faker.providers import date_time
from datetime import datetime
from shapely.geometry import Point
import random
import time
import subprocess

# Inicializar Faker
fake = Faker("es_ES")
fake.add_provider(date_time)

# Obtener la fecha actual
fecha_actual = datetime.now()

# Cargar archivos Parquet de clientes, empleados y comunas de Medellín
customers_df = pd.read_parquet("./data/customers.parquet")
employees_df = pd.read_parquet("./data/employees.parquet")
medellin_gdf = gpd.read_parquet("./data/medellin_neighborhoods.parquet")

# Asegurarse de que ambos GeoDataFrames tengan el mismo CRS
if not medellin_gdf.crs:
    medellin_gdf = medellin_gdf.set_crs("EPSG:4326", allow_override=True)

# Extraer el polígono de Medellín (unión de todas las comunas)
medellin_polygon = medellin_gdf.geometry.unary_union

# Función para generar puntos aleatorios dentro de un polígono
def generate_random_point_within_polygon(polygon):
    minx, miny, maxx, maxy = polygon.bounds
    while True:
        latitud = random.uniform(miny, maxy)
        longitud = random.uniform(minx, maxx)
        point = Point(longitud, latitud)
        if polygon.contains(point):
            return point

# Función para obtener el nombre del cliente por customer_id
def get_customer_name(customer_id):
    customer_info = customers_df[customers_df["customer_id"] == customer_id]
    return customer_info.iloc[0]["name"] if not customer_info.empty else None

# Función para obtener el nombre del empleado y la comisión 
def get_employee_info(employee_id):
    employee_info = employees_df[employees_df["employee_id"] == employee_id]
    if not employee_info.empty:
        # Ajustar el nombre de la columna de comisión aquí si es necesario
        employee_commission = employee_info.iloc[0]["comission"]
        employee_name = employee_info.iloc[0]["name"]
    else:
        employee_name = None
        employee_commission = None
    return employee_name, employee_commission

# Función para generar datos
def generate_data(num_samples=1000):
    data = []
    for _ in range(num_samples):
        # Generar punto aleatorio dentro del polígono de Medellín
        new_point = generate_random_point_within_polygon(medellin_polygon)

        # Generar fecha dentro del año actual y del año anterior, sin superar el día actual
        fecha = fake.date_time_between_dates(
            datetime(fecha_actual.year - 1, 1, 1), fecha_actual
        )

        # Inicializar código y nombre de la comuna
        codigo_comuna = None
        nombre_comuna = None
        
        # Crear un GeoDataFrame temporal para realizar la unión espacial
        temp_gdf = gpd.GeoDataFrame([{'geometry': new_point}], geometry='geometry')
        
        # Asegurar que temp_gdf tenga el mismo CRS que medellin_gdf
        if not temp_gdf.crs:
            temp_gdf = temp_gdf.set_crs(medellin_gdf.crs, allow_override=True)

        # Realizar la unión espacial para obtener la comuna
        temp_gdf = gpd.sjoin(temp_gdf, medellin_gdf, how="left", predicate="within")

        # Verificar si temp_gdf está vacío después de la unión
        if not temp_gdf.empty:
            # Extraer el código y nombre de la comuna
            codigo_comuna = temp_gdf.iloc[0]['CODIGO']
            nombre_comuna = temp_gdf.iloc[0]['NOMBRE']
        else:
            # Manejar el caso donde no se encuentra ninguna comuna
            codigo_comuna = None
            nombre_comuna = None
        
        # Obtener IDs de cliente y empleado
        customer_id = fake.random_element(customers_df["customer_id"])
        employee_id = fake.random_element(employees_df["employee_id"])

        # Obtener el nombre del cliente y del empleado y la comision
        customer_name = get_customer_name(customer_id)
        employee_name, employee_commission = get_employee_info(employee_id)

        # Generar número de orden y cantidad (entero)
        order = fake.random_number(digits=10)
        cantidad = fake.random_int(min=20, max=99)  # Ajustar a un número entero

        data.append(
            {
                "latitude": new_point.y,
                "longitude": new_point.x,
                "date": fecha,
                "customer_id": customer_id,
                "employee_id": employee_id,
                "quantity_products": cantidad,
                "order_id": order,
                "commune_code": codigo_comuna,
                "commune_name": nombre_comuna,
                "customer_name": customer_name,
                "employee_name": employee_name,
                "employee_commission": employee_commission
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


