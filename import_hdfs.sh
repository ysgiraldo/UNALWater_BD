sqoop import \
    --connect jdbc:mysql://localhost:3306/demo_db \
    --username sqoop \
    --password secret \
    --table fiveone_pqt \
    --table customers \
    --table employees \
    --table medellin_neighborhoods \
    --target-dir /tmp/tscoop \
    --m 1
