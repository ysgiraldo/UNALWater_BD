import pandas as pd
from faker import Faker
from faker.providers import date_time
from datetime import datetime
from shapely.wkt import loads
import random

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

# Generar datos aleatorios
data = []
for _ in range(300000):  # Ajustar según la cantidad inicial que se requiera
    # Generar latitud y longitud aleatorias dentro del rango de Medellín
    latitud = random.uniform(lat_min, lat_max)
    longitud = random.uniform(lon_min, lon_max)
    
    # Generar fecha dentro del año actual y del año anterior, sin superar el día actual
    fecha = fake.date_time_between_dates(datetime(fecha_actual.year - 1, 1, 1), fecha_actual)
    
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

# Crear DataFrame
df = pd.DataFrame(data)

# Guardar como archivo CSV
df.to_csv('cargue_inicial.csv', index=False)

# Leer el archivo CSV y convertir a Parquet
df_parquet = pd.read_csv('cargue_inicial.csv')
df_parquet.to_parquet('cargue_inicial.parquet', index=False)


