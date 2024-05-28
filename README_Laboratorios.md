# Laboratorios de programación --- Curso: Analítica de Grandes Datos

Este repo contiene los laboratorios de programación para el curso de posgrado Analítica de Grandes Datos, ofertado en la Facultad de Minas, Universidad Nacional de Colombia, Sede Medellín.

A continuación se describe el procedimiento para realizar los laboratorios de programación.

## Creación del repo individual para cada estudiante y clonación al computador personal


* Entre al sitio web del curso y haga click en el vínculo `Laboratorios de Programación`.


* Ubique su nombre en la lista del curso y haga click para identificarse en GitHub Classroom. Como resultado se creará un repo único para usted.


* Clone su repositorio en su computador en una subcarpeta del directorio `vagrant4docker`, con el fin de que el repo sea visible desde la máquina virtual. Recuerde que la guía de instalación está disponible en https://github.com/jdvelasq/vagrant4docker 

## Ejecución de los laboratorios


* Inicie Vagrant de la forma usual (las instrucciones están en https://github.com/jdvelasq/vagrant4docker). 


* Entre a la máquina virtual con `vagrant ssh`.


* Vaya a la carpeta compartida con el disco duro (`cd /vagrant`).


* Entre al directorio raíz de los laboratorios de programación.


* Para ejecutar el evaluador sobre todas las tareas digite 
 ```
 docker run --rm -v "$PWD":/datalake  jdvelasq/grader:agd
 ```
Para los puntos con una solución errónea, el sistema le reportará el resultado esperado y el resultado computado por su solución. Para los puntos solucionados correctamente, el sistema solo indicará que ejecuto el punto. Finalmente, el sistema le entregará por pantalla un informe detallado de cada punto y la correspondiente nota por punto, laboratorio y para el curso.

* Si esta realizando la solución de un punto particular, resulta más apropiado que entre a la carpeta correspondiente y realizar la evaluación únicamente para dicho punto. Para ello, ingrese en modo interactivo al contenedor con:
```
docker run -it --rm -v "$PWD":/datalake  jdvelasq/grader:agd
```
Luego, ejecute `python3 grader.py` en la carpeta correspondiente para realizar la evaluación.






