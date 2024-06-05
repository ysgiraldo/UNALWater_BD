import pandas as pd
import geopandas as gpd
from faker import Faker
from faker.providers import date_time
from datetime import datetime
import random

# Inicializar Faker
fake = Faker('es_ES')
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

# Generar datos aleatorios dentro del polígono de Medellín
data = []
for _ in range(10000):  # Ajustar según la cantidad inicial que se requiera
    # Generar puntos aleatorios dentro del polígono de Medellín
    while True:
        lon, lat = medellin_polygon.representative_point().coords[0]
        latitud = random.uniform(lat - 0.9, lat + 0.9)  # Ajustar según la dispersión deseada
        longitud = random.uniform(lon - 0.9, lon + 0.9)  # Ajustar según la dispersión deseada
        new_point = (longitud, latitud)
        if medellin_polygon.contains(gpd.points_from_xy([longitud], [latitud])[0]):
            break
    
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
        'quantity_products': cantidad,
        'order_id': order,
        
    })

# Crear DataFrame
df = pd.DataFrame(data)

# Guardar como archivo CSV
df.to_csv('cargue_inicial.csv', index=False)

# Leer el archivo CSV y convertir a Parquet
df_parquet = pd.read_csv('cargue_inicial.csv')
df_parquet.to_parquet('cargue_inicial.parquet', index=False)
