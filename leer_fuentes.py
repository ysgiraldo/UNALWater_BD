import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import os

file_path = [
    "/workspace/UNALWater_BD/bronze/50001.parquet",
    "/workspace/UNALWater_BD/bronze/customers.parquet",
    "/workspace/UNALWater_BD/bronze/employees.parquet",
    "/workspace/UNALWater_BD/bronze/medellin_neighborhoods.parquet",
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
        # Guardar como CSV en la carpeta 'data/csv_files'
        gdf.to_csv(f"data/{os.path.splitext(os.path.basename(file_path))[0]}.csv", index=False)
    else:
        # Leer archivo Parquet con pandas
        df = pd.read_parquet(file_path)
        print(f"Primeros registros de {file_path}:\n", df.head())
        # Mostrar los tipos de campos
        print(f"Tipos de campos de {file_path}:\n", df.dtypes)
        # Mostrar cantidad de registros
        print(f"Cantidad de registros en {file_path}: {len(df)}")
        # Guardar como CSV
        df.to_csv(os.path.splitext(file_path)[0] + ".csv", index=False)


# Lista de archivos Parquet y si contienen datos geoespaciales
archivos_parquet = [
    ("./bronze/medellin_neighborhoods.parquet", True),
    ("./bronze/50001.parquet", True),
    ("./bronze/customers.parquet", False),
    ("./bronze/employees.parquet", False),
]

# Leer, mostrar los primeros registros y la cantidad de registros de cada archivo, y convertir y guardar como CSV
for file_path, is_geospatial in archivos_parquet:
    leer_y_mostrar_parquet(file_path, is_geospatial)
