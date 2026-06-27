#!/bin/bash
# Deploy the Flink table definitions and averaging job via the SQL client container.

set -e

FLINK_HOST="${FLINK_HOST:-localhost:9081}"

echo "=== Deploying Flink Tables + Averaging Job ==="

# Wait for Flink to be ready
echo "Waiting for Flink at http://$FLINK_HOST ..."
for i in $(seq 1 30); do
    if curl -sf "http://$FLINK_HOST/v1/config" -o /dev/null 2>&1; then
        echo "Flink is ready."
        break
    fi
    echo "  attempt $i/30 ..."
    sleep 5
done

# Write full SQL (table definitions + INSERT job) to a temp file
cat > /tmp/iot_flink.sql << 'EOF'
DROP TABLE IF EXISTS `iot_devices_source`;
CREATE TABLE `iot_devices_source` (
    `deviceID`     STRING,
    `vendor`       STRING,
    `serialNumber` STRING,
    `temperature`  DOUBLE,
    `humidity`     DOUBLE,
    `pressure`     DOUBLE,
    `updatedAt`    TIMESTAMP(3),
    WATERMARK FOR `updatedAt` AS `updatedAt` - INTERVAL '10' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'iot_devices_db2',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id'          = 'flink-iot-averages',
    'scan.startup.mode'            = 'earliest-offset',
    'key.format'                   = 'raw',
    'key.fields'                   = 'deviceID',
    'value.format'                 = 'avro-confluent',
    'value.avro-confluent.url'     = 'http://schema-registry:8081'
);

DROP TABLE IF EXISTS `iot_devices_avg`;
CREATE TABLE `iot_devices_avg` (
    `deviceID`        STRING,
    `vendor`          STRING,
    `serialNumber`    STRING,
    `window_start`    TIMESTAMP(3),
    `window_end`      TIMESTAMP(3),
    `avg_temperature` DOUBLE,
    `avg_humidity`    DOUBLE,
    `avg_pressure`    DOUBLE,
    PRIMARY KEY (`deviceID`) NOT ENFORCED
) WITH (
    'connector'                    = 'upsert-kafka',
    'topic'                        = 'iot_devices_avg',
    'properties.bootstrap.servers' = 'broker:29092',
    'key.format'                   = 'raw',
    'value.format'                 = 'avro-confluent',
    'value.avro-confluent.url'     = 'http://schema-registry:8081',
    'value.fields-include'         = 'ALL'
);

INSERT INTO `iot_devices_avg`
SELECT
    `deviceID`,
    MAX(`vendor`)             AS `vendor`,
    MAX(`serialNumber`)       AS `serialNumber`,
    `window_start`,
    `window_end`,
    ROUND(AVG(`temperature`), 2) AS `avg_temperature`,
    ROUND(AVG(`humidity`), 2)    AS `avg_humidity`,
    ROUND(AVG(`pressure`), 2)    AS `avg_pressure`
FROM TABLE(
    TUMBLE(TABLE `iot_devices_source`, DESCRIPTOR(`updatedAt`), INTERVAL '15' SECOND)
)
GROUP BY `deviceID`, `window_start`, `window_end`;
EOF

# Copy SQL into the sql-client container and execute
echo "Submitting SQL via flink-sql-client..."
docker cp /tmp/iot_flink.sql flink-sql-client:/tmp/iot_flink.sql
docker exec flink-sql-client /opt/flink/bin/sql-client.sh -f /tmp/iot_flink.sql

echo ""
echo "=== Flink job submitted. Monitor at: http://$FLINK_HOST ==="
