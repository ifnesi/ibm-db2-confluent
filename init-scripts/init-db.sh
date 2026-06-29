#!/bin/bash
# Creates the IOT_DEVICES table in IBM Db2 (runs once at container init via /var/custom)

echo "=== Waiting for DB2 to be ready ==="
for i in $(seq 1 45); do
    if su - db2inst1 -c "db2 connect to testdb > /dev/null 2>&1 && db2 connect reset > /dev/null 2>&1"; then
        echo "DB2 is ready."
        break
    fi
    echo "  attempt $i/30 ..."
    sleep 10
done

echo "=== Creating IOT_DEVICES and IOT_DEVICES_MEASUREMENTS tables ==="

# Write SQL to a temp file — avoids multiline quoting issues in -c "..."
cat > /tmp/create_iot_devices.sql << 'EOF'
CONNECT TO testdb;
CREATE TABLE DB2INST1.IOT_DEVICES (
    device_identifier  VARCHAR(50)   NOT NULL,
    vendor_name        VARCHAR(100)  NOT NULL,
    serial_number      VARCHAR(100)  NOT NULL,
    lat                DOUBLE,
    long               DOUBLE,
    created_timestamp  TIMESTAMP     NOT NULL WITH DEFAULT CURRENT TIMESTAMP,
    PRIMARY KEY (device_identifier)
);
CREATE TABLE DB2INST1.IOT_DEVICES_MEASUREMENTS (
    device_identifier  VARCHAR(50)   NOT NULL,
    temp               DOUBLE,
    hmdt               DOUBLE,
    press              DOUBLE,
    created_timestamp  TIMESTAMP     NOT NULL WITH DEFAULT CURRENT TIMESTAMP,
    FOREIGN KEY (device_identifier) REFERENCES DB2INST1.IOT_DEVICES (device_identifier)
);
COMMIT;
CONNECT RESET;
EOF

su - db2inst1 -c "db2 -tvf /tmp/create_iot_devices.sql"

echo "=== IOT_DEVICES and IOT_DEVICES_MEASUREMENTS tables created ==="
