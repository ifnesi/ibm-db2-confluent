#!/bin/bash
# Creates the IOT_DEVICES table in IBM Db2 (runs once at container init via /var/custom)

echo "=== Waiting for DB2 to be ready ==="
for i in $(seq 1 30); do
    if su - db2inst1 -c "db2 connect to testdb > /dev/null 2>&1 && db2 connect reset > /dev/null 2>&1"; then
        echo "DB2 is ready."
        break
    fi
    echo "  attempt $i/30 ..."
    sleep 10
done

echo "=== Creating IOT_DEVICES table ==="

# Write SQL to a temp file — avoids multiline quoting issues in -c "..."
cat > /tmp/create_iot_devices.sql << 'EOF'
CONNECT TO testdb;
CREATE TABLE DB2INST1.IOT_DEVICES (
    deviceID     VARCHAR(50)   NOT NULL PRIMARY KEY,
    vendor       VARCHAR(100)  NOT NULL,
    serialNumber VARCHAR(100)  NOT NULL,
    temperature  DOUBLE,
    humidity     DOUBLE,
    pressure     DOUBLE,
    createdAt    TIMESTAMP     NOT NULL WITH DEFAULT CURRENT TIMESTAMP,
    updatedAt    TIMESTAMP     NOT NULL WITH DEFAULT CURRENT TIMESTAMP
);
COMMIT;
CONNECT RESET;
EOF

su - db2inst1 -c "db2 -tvf /tmp/create_iot_devices.sql"

echo "=== IOT_DEVICES table created ==="
