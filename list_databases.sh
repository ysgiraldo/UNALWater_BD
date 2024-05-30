#!/bin/bash
sqoop list-databases \
    --connect jdbc:mysql://localhost:3306/demo_db \
    --username sqoop \
    --password secret

sqoop list-tables \
    --connect jdbc:mysql://localhost:3306/demo_db \
    --username sqoop \
    --password secret