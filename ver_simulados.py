import pandas as pd

# Cargar el archivo Parquet en un DataFrame
filename = 'datos_rt_completos_2024-06-13_20-41-01.parquet'
df = pd.read_parquet(filename)

# Mostrar las primeras filas del DataFrame
print(df.head())

# Obtener información sobre el DataFrame
print(df.info())

# Agrupar por comuna y sumar la cantidad de productos
productos_por_comuna = df.groupby('commune_name')['quantity_products'].sum()

# Agrupar por empleado y comisión, y sumar la cantidad de productos
productos_por_empleado_comision = df.groupby(['employee_id', 'employee_name', 'employee_commission'])['quantity_products'].sum()

# Mostrar los resultados
print(productos_por_comuna)
print(productos_por_empleado_comision)