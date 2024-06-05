import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

# Cargar el archivo Parquet de los barrios de Medellín como un GeoDataFrame
medellin_neighborhoods = gpd.read_parquet('./data/medellin_neighborhoods.parquet')

# Cargar el archivo Parquet de cargue inicial como un DataFrame de Pandas
df_cargue = pd.read_parquet('datos.parquet')

# Convertir las latitudes y longitudes del cargue inicial en una columna de geometría
geometry = gpd.points_from_xy(df_cargue['longitude'], df_cargue['latitude'])

# Crear un GeoDataFrame con los datos del cargue inicial y la columna de geometría
gdf_cargue = gpd.GeoDataFrame(df_cargue, geometry=geometry)

# Crear un gráfico con los barrios de Medellín
fig, ax = plt.subplots(figsize=(10, 10))
medellin_neighborhoods.plot(ax=ax, color='lightgrey')

# Agregar los puntos del cargue inicial al gráfico
gdf_cargue.plot(ax=ax, color='salmon', markersize=5, alpha=0.5)

plt.title('Datos simulados de venta de botellas de vidrio en Medellín')
plt.xlabel('Longitud')
plt.ylabel('Latitud')
plt.grid(True)
plt.show()