sqoop list-databases \
    --connect jdbc:mysql://localhost:3306/demo_db \
    --username sqoop \
    --password secret

sqoop list-tables \
    --connect jdbc:mysql://localhost:3306/demo_db \
    --username sqoop \
    --password secret

sqoop eval \
    --connect jdbc:mysql://localhost:3306/demo_db \
    --username sqoop \
    --password secret \
    --query "SELECT * FROM fiveone_pqt LIMIT 3" \
    --query "SELECT * FROM customers LIMIT 3" \
    --query "SELECT * FROM employees LIMIT 3" \
    --query "SELECT * FROM medellin_neighborhoods LIMIT 3"