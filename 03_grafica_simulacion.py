import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

# Cargar el archivo Parquet con las Medellín como un GeoDataFrame
medellin_neighborhoods = gpd.read_parquet('./data/medellin_neighborhoods.parquet')

# Cargar el archivo Parquet de cargue inicial como un DataFrame de Pandas
df_cargue = pd.read_parquet('datos.parquet')

# Convertir las latitudes y longitudes del cargue inicial en una columna de geometría
geometry = gpd.points_from_xy(df_cargue['longitude'], df_cargue['latitude'])

# Crear un GeoDataFrame con los datos del cargue inicial y la columna de geometría
gdf_cargue = gpd.GeoDataFrame(df_cargue, geometry=geometry)

# Crear un gráfico con las comunas de Medellín
fig, ax = plt.subplots(figsize=(20, 20))
medellin_neighborhoods.plot(ax=ax, color='lightgrey', edgecolor='darkblue')

# Agregar los puntos del cargue inicial al gráfico
gdf_cargue.plot(ax=ax, color='blue', markersize=2, alpha=0.2)

plt.title('Datos simulados de venta de botellas de agua en UNALWater. Medellín años 2023 y 2024')
plt.xlabel('Longitud')
plt.ylabel('Latitud')
plt.grid(True)
plt.savefig('./data/medellin_neighborhoods_simulacion.png')
plt.close()