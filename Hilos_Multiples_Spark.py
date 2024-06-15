import threading
import subprocess
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, LongType
import time

# Crear el bloqueo
lock = threading.Lock()

spark = SparkSession.builder.appName("Ingestar nuevos archivos Parquet").getOrCreate()
# Definir el esquema con las columnas requeridas
schema = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("date", DateType(), True),
    StructField("customer_id", LongType(), True),
    StructField("employee_id", LongType(), True),
    StructField("quantity_products", LongType(), True),
    StructField("order_id", LongType(), True)
])

# Crear el DataFrame global con el esquema definido
global_df = spark.createDataFrame([], schema)

# Definir la función para procesar los archivos Parquet
def process_parquet_files():
    global global_df

    # Definir el directorio de Stagging y el archivo de registro
    staging_dir = "hdfs:///Stagging/"
    log_file = "/tmp/processed_files.log"
    
    # Crear sesión de Spark
    spark = SparkSession.builder.appName("Ingestar nuevos archivos Parquet").getOrCreate()

    while True:
        # Leer el registro de archivos procesados si existe
        if os.path.exists(log_file):
            with open(log_file, "r") as f:
                processed_files = set(f.read().splitlines())
        else:
            processed_files = set()

        # Obtener el sistema de archivos de Hadoop
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

        # Listar los archivos en el directorio de Stagging
        file_statuses = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(staging_dir))
        all_files = [file_status.getPath().toString() for file_status in file_statuses if file_status.getPath().getName().endswith(".parquet")]

        # Filtrar los archivos nuevos
        new_files = [file for file in all_files if file not in processed_files]

        # Ingestar los nuevos archivos
        if new_files:
            df = spark.read.parquet(*new_files)
            
            # Verificar si el DataFrame global ya está definido
            if global_df is None:
                global_df = df
            else:
                # Concatenar los nuevos datos al DataFrame global
                global_df = global_df.union(df)
            
            # Realizar las operaciones necesarias con el DataFrame
            # ...

            # Actualizar el registro de archivos procesados
            with open(log_file, "a") as f:
                for file in new_files:
                    f.write(file + "\n")
            print("Archivos procesados:", new_files)

        else:
            #print("No hay archivos nuevos para procesar.")
            pass

        # Esperar un intervalo antes de la próxima verificación
        time.sleep(5)



# Crear e iniciar el hilo
thread_process_files = threading.Thread(target=process_parquet_files)
thread_process_files.start()