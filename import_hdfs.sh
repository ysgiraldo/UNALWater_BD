#!/bin/bash
sqoop import \
    --connect jdbc:mysql://localhost:3306/demo_db \
    --username sqoop \
    --password secret \
    --table customers \
    --target-dir /tmp/ \
    --m 1