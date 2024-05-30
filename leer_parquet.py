import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import os

file_path = [
    "/workspace/UNALWater_BD/data/50001.parquet",
    "/workspace/UNALWater_BD/data/customers.parquet",
    "/workspace/UNALWater_BD/data/employees.parquet",
    "/workspace/UNALWater_BD/data/medellin_neighborhoods.parquet",
]


# Función para leer un archivo Parquet y mostrar los primeros registros y la cantidad de registros
def leer_y_mostrar_parquet(file_path, is_geospatial=False):
    if is_geospatial:
        # Leer archivo Parquet con geopandas
        gdf = gpd.read_parquet(file_path)
        print(f"Primeros registros de {file_path}:\n", gdf.head())
        # Visualizar datos geoespaciales
        gdf.plot()
        plt.title(f"Visualización de {file_path}")
        # Guardar la gráfica como archivo de imagen
        plt.savefig(os.path.splitext(file_path)[0] + ".png")
        # Mostrar cantidad de registros
        print(f"Cantidad de registros en {file_path}: {len(gdf)}")
    else:
        # Leer archivo Parquet con pandas
        df = pd.read_parquet(file_path)
        print(f"Primeros registros de {file_path}:\n", df.head())
        # Mostrar los tipos de campos
        print(f"Tipos de campos de {file_path}:\n", df.dtypes)
        # Mostrar cantidad de registros
        print(f"Cantidad de registros en {file_path}: {len(df)}")


# Lista de archivos Parquet y si contienen datos geoespaciales
archivos_parquet = [
    ("./data/medellin_neighborhoods.parquet", True),
    ("./data/50001.parquet", True),
    ("./data/customers.parquet", False),
    ("./data/employees.parquet", False),
]

# Leer, mostrar los primeros registros y la cantidad de registros de cada archivo, y convertir y guardar como CSV
for file_path, is_geospatial in archivos_parquet:
    leer_y_mostrar_parquet(file_path, is_geospatial)
