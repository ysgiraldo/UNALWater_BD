import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

# Función para leer un archivo Parquet y mostrar los primeros registros y la cantidad de registros
def leer_y_mostrar_parquet(file_path, is_geospatial=False):
    if is_geospatial:
        # Leer archivo Parquet con geopandas
        gdf = gpd.read_parquet(file_path)
        print(f"Primeros registros de {file_path}:\n", gdf.head())
        # Visualizar datos geoespaciales
        gdf.plot()
        plt.title(f"Visualización de {file_path}")
        plt.show()
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

# Función para convertir y guardar archivos Parquet como CSV
def convertir_y_guardar_csv(file_path, is_geospatial=False):
    if is_geospatial:
        # Leer archivo Parquet con geopandas
        gdf = gpd.read_parquet(file_path)
        # Guardar como CSV
        csv_file_path = file_path.replace('.parquet', '.csv')
        gdf.to_csv(csv_file_path, index=False)
        print(f"Archivo CSV guardado en: {csv_file_path}")
    else:
        # Leer archivo Parquet con pandas
        df = pd.read_parquet(file_path)
        # Guardar como CSV
        csv_file_path = file_path.replace('.parquet', '.csv')
        df.to_csv(csv_file_path, index=False)
        print(f"Archivo CSV guardado en: {csv_file_path}")

# Lista de archivos Parquet y si contienen datos geoespaciales
archivos_parquet = [
    ("medellin_neighborhoods.parquet", True),  
    ("50001.parquet", True),  
    ("customers.parquet", False),        
    ("employees.parquet", False)               
]

# Leer, mostrar los primeros registros y la cantidad de registros de cada archivo, y convertir y guardar como CSV
for file_path, is_geospatial in archivos_parquet:
    leer_y_mostrar_parquet(file_path, is_geospatial)
    convertir_y_guardar_csv(file_path, is_geospatial)


    
