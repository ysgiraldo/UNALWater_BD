#!/bin/bash
sqoop import \
    --connect jdbc:mysql://localhost:3306/demo_db \
    --username sqoop \
    --password secret \
    --table drivers \
    --target-dir /tmp/drivers \
    --m 1