import threading
import subprocess
import os
from pyspark.sql import SparkSession

# Definir la función para ejecutar el script de shell
def run_shell_script():
    subprocess.run(["bash", "./Gen2.sh"])

# Definir la función para procesar los archivos Parquet
def process_parquet_files():
    # Crear sesión de Spark
    spark = SparkSession.builder.appName("Ingestar nuevos archivos Parquet").getOrCreate()

    # Definir el directorio de Stagging y el archivo de registro
    staging_dir = "hdfs:///Stagging/"
    log_file = "/tmp/processed_files.log"

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
            # Realizar las operaciones necesarias con el DataFrame
            # ...

            # Actualizar el registro de archivos procesados
            with open(log_file, "a") as f:
                for file in new_files:
                    f.write(file + "\n")
        else:
            print("No hay archivos nuevos para procesar.")

        # Esperar un intervalo antes de la próxima verificación
        time.sleep(2)

# Crear hilos
thread_shell_script = threading.Thread(target=run_shell_script)
thread_process_files = threading.Thread(target=process_parquet_files)

# Iniciar hilos
thread_shell_script.start()
thread_process_files.start()