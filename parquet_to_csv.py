import pandas as pd

# Lista de archivos Parquet y sus correspondientes archivos CSV de salida
files = [
    ("/tmp/50001.parquet", "/tmp/50001.csv"),
    ("/tmp/customers.parquet", "/tmp/customers.csv"),
    ("/tmp/employees.parquet", "/tmp/employees.csv"),
    ("/tmp/medellin_neighborhoods.parquet", "/tmp/medellin_neighborhoods.csv"),
]

# Convertir cada archivo Parquet a CSV
for parquet_file, csv_file in files:
    df = pd.read_parquet(parquet_file)
    df.to_csv(csv_file, index=False)
    print(f"Convertido {parquet_file} a {csv_file}")

