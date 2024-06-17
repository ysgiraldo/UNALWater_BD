#!/bin/bash
# Intervalo de tiempo en segundos
INTERVAL=30
while true
do
  echo "Ejecutando simulacion.py: $(date)"
  python3 ./simulacion.py
  sleep $INTERVAL
done
