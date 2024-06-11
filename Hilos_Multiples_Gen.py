import threading
import subprocess
import os
from pyspark.sql import SparkSession

# Definir la función para ejecutar el script de shell
def run_shell_script():
    subprocess.run(["bash", "./Gen2.sh"])

# Crear hilos
thread_shell_script = threading.Thread(target=run_shell_script)

# Iniciar hilos
thread_shell_script.start()