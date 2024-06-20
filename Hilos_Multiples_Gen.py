import threading
import subprocess
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, DateType, LongType

# Definir la función para ejecutar el script de shell
def run_shell_script():
    subprocess.run(["bash", "./Gen_Datos.sh"])


# Crear hilos
thread_shell_script = threading.Thread(target=run_shell_script)

# Iniciar hilos
thread_shell_script.start()

#thread_shell_script.join()
