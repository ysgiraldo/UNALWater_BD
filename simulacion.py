import pandas as pd
import geopandas as gpd
import random
import time
import socket
import json
from faker import Faker
from faker.providers import date_time
from datetime import datetime
from shapely.geometry import Point

# Inicializar Faker
fake = Faker("es_ES")
fake.add_provider(date_time)

# Obtener la fecha actual
fecha_actual = datetime.now()

# Cargar archivos Parquet de clientes, empleados y comunas de Medellín
customers_df = pd.read_parquet("./bronze/customers.parquet")
employees_df = pd.read_parquet("./bronze/employees.parquet")
medellin_gdf = gpd.read_parquet("./bronze/medellin_neighborhoods.parquet")

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
def generate_data(num_samples=100):
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

        # Obtener el nombre del cliente y del empleado y la comisión
        customer_name = get_customer_name(customer_id)
        employee_name, employee_commission = get_employee_info(employee_id)

        # Generar número de orden y cantidad (entero)
        order = fake.random_number(digits=10)
        cantidad = fake.random_int(min=20, max=99)  # Ajustar a un número entero

        data.append(
            {
                "latitude": new_point.y,
                "longitude": new_point.x,
                "date": fecha.isoformat(),  # Convertir a ISO 8601 para asegurar compatibilidad JSON
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

# Crear socket
server_address = ('localhost', 12345)  # Asegúrate de que esto coincide con la configuración de Spark
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(server_address)

try:
    while True:
        data = generate_data()
        
        # Convertir a formato JSON y enviar por el socket
        for item in data:
            message = json.dumps(item)
            sock.sendall(message.encode('utf-8') + b'\n')  # Enviar cada registro como una línea separada

        print(f"Datos generados y enviados al socket")

        # Esperar 30 segundos antes de generar el próximo conjunto de datos
        time.sleep(30)

finally:
    print('Cerrando el socket')
    sock.close()
