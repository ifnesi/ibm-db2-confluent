#!/bin/bash
# Creates the EMPLOYEES table in IBM Db2 (runs once at container init via /var/custom)

echo "=== Waiting for DB2 to be ready ==="
for i in $(seq 1 30); do
    if su - db2inst1 -c "db2 connect to testdb > /dev/null 2>&1 && db2 connect reset > /dev/null 2>&1"; then
        echo "DB2 is ready."
        break
    fi
    echo "  attempt $i/30 ..."
    sleep 10
done

echo "=== Creating EMPLOYEES table ==="

# Write SQL to a temp file — avoids multiline quoting issues in -c "..."
cat > /tmp/create_employees.sql << 'EOF'
CONNECT TO testdb;
CREATE TABLE DB2INST1.EMPLOYEES (
    ID          INTEGER       NOT NULL PRIMARY KEY,
    NAME        VARCHAR(100)  NOT NULL,
    DEPARTMENT  VARCHAR(50),
    SALARY      DECIMAL(10,2),
    HIRE_DATE   DATE          NOT NULL WITH DEFAULT CURRENT DATE,
    UPDATED_AT  TIMESTAMP     NOT NULL WITH DEFAULT CURRENT TIMESTAMP
);
COMMIT;
CONNECT RESET;
EOF

su - db2inst1 -c "db2 -tvf /tmp/create_employees.sql"

echo "=== EMPLOYEES table created ==="
