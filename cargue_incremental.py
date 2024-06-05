import pandas as pd
from faker import Faker
from faker.providers import date_time
from datetime import datetime
from shapely.wkt import loads
import random
import time

# Inicializar Faker
fake = Faker('es_ES')
fake.add_provider(date_time)

# Obtener la fecha actual
fecha_actual = datetime.now()

# Cargar archivos Parquet de clientes y empleados
customers_df = pd.read_parquet('customers.parquet')
employees_df = pd.read_parquet('employees.parquet')

# Cargar archivo CSV de barrios de Medellín
medellin_neighborhoods_df = pd.read_csv('medellin_neighborhoods.csv')

# Extraer los límites geográficos de Medellín
medellin_polygon = None
for index, row in medellin_neighborhoods_df.iterrows():
    polygon = loads(row['geometry'])
    if medellin_polygon is None:
        medellin_polygon = polygon
    else:
        medellin_polygon = medellin_polygon.union(polygon)

# Obtener los límites del polígono de Medellín
lat_min, lat_max = medellin_polygon.bounds[1], medellin_polygon.bounds[3]
lon_min, lon_max = medellin_polygon.bounds[0], medellin_polygon.bounds[2]

# Función para generar datos aleatorios
def generate_data():
    data = []
    for _ in range(1000):  # Ajustar a 1000 pedidos
        # Generar latitud y longitud aleatorias dentro del rango de Medellín
        latitud = random.uniform(lat_min, lat_max)
        longitud = random.uniform(lon_min, lon_max)
        
        # Generar fecha y hora del día actual, sin superar la hora actual
        fecha = fake.date_time_between_dates(datetime(fecha_actual.year, fecha_actual.month, fecha_actual.day), fecha_actual)
        
        # Obtener IDs de cliente y empleado
        customer_id = fake.random_element(customers_df['customer_id'])
        employee_id = fake.random_element(employees_df['employee_id'])
        
        # Generar número de orden y cantidad (entero)
        order = fake.random_number(digits=10)
        cantidad = fake.random_int(min=1, max=99)  # Ajustar a un número entero

        data.append({
            'latitude': latitud,
            'longitude': longitud,
            'date': fecha,
            'customer_id': customer_id,
            'employee_id': employee_id,
            'quantity_prodcuts': cantidad,
            'order_id': order,
        })
    return data

# Intervalo de tiempo en segundos
interval = 30  # Ajustar según sea necesario

# Limitar a un número de iteraciones, por ejemplo, 2 iteraciones por minuto
max_iterations = 2
iterations = 0

while iterations < max_iterations:
    # Generar datos
    data = generate_data()
    
    # Crear DataFrame
    df = pd.DataFrame(data)
    
    # Guardar como archivo JSON
    json_filename = 'datos.json'
    df.to_json(json_filename, orient='records', date_format='iso', lines=True)
    print(f"Datos generados y guardados en '{json_filename}'")
    
    # Convertir a Parquet
    parquet_filename = 'datos.parquet'
    df.to_parquet(parquet_filename, index=False)
    print(f"Datos convertidos y guardados en '{parquet_filename}'")
    
    # Incrementar el contador de iteraciones
    iterations += 1
    
    # Esperar el intervalo especificado
    time.sleep(interval)
