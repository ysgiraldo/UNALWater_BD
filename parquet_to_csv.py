import pandas as pd

# Lista de archivos Parquet y sus correspondientes archivos CSV de salida
files = [
    ("data/50001.parquet", "data/50001.csv"),
    ("data/customers.parquet", "data/customers.csv"),
    ("data/employees.parquet", "data/employees.csv"),
    ("data/medellin_neighborhoods.parquet", "data/medellin_neighborhoods.csv"),
]

# Convertir cada archivo Parquet a CSV
for parquet_file, csv_file in files:
    df = pd.read_parquet(parquet_file)
    df.to_csv(csv_file, index=False)
    print(f"Convertido {parquet_file} a {csv_file}")

