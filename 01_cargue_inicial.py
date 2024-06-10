import pandas as pd
import geopandas as gpd
from faker import Faker
from faker.providers import date_time
from datetime import datetime, time
from shapely.geometry import Point
import random
import time  # Se añade la importación del módulo time


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

# Parámetros para la distribución normal
mu_lon, mu_lat = medellin_polygon.representative_point().coords[0]
sigma = 0.9  # Desviación estándar (se ajusta según sea necesario)

# Generar datos aleatorios dentro del polígono de Medellín
def generate_data(num_samples=10000):
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
    
        # Generar fecha dentro del año actual y del año anterior, sin superar el día actual
        fecha = fake.date_time_between_dates(datetime(fecha_actual.year - 1, 1, 1), fecha_actual)
        
        # Obtener IDs de cliente y empleado
        customer_id = fake.random_element(customers_df['customer_id'])
        employee_id = fake.random_element(employees_df['employee_id'])
        
        # Generar número de orden y cantidad (entero)
        order = fake.random_number(digits=10)
        cantidad = fake.random_int(min=20, max=99)  # Ajustar a un número entero

        data.append({
            'latitude': latitud,
            'longitude': longitud,
            'date': fecha,
            'customer_id': customer_id,
            'employee_id': employee_id,
            'quantity_products': cantidad,
            'order_id': order,
            
        })
    return data

# Generar datos
start_time = time.time()
data = generate_data()
end_time = time.time()

# Crear DataFrame
df = pd.DataFrame(data)

# Guardar como archivo JSON
json_filename = "cargue_inicial.json"
df.to_json(json_filename, orient="records", date_format="iso", lines=True)

# Convertir a parquet
parquet_filename = "datos.parquet"
df.to_parquet(parquet_filename, index=False)
print(f"Datos generados y guardados en '{parquet_filename}' en {end_time - start_time:.2f} segundos")