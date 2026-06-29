#!/bin/bash
# Deploy Flink merge job: LEFT JOIN devices and measurements tables into a single enriched stream

set -e

FLINK_HOST="${FLINK_HOST:-localhost:9081}"

echo "=== Deploying Flink Merge Job (Devices + Measurements JOIN) ==="

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
cat > /tmp/iot_flink_merge.sql << 'EOF'
DROP TABLE IF EXISTS `iot_devices_db2`;
CREATE TABLE `iot_devices_db2` (
    `deviceID`     STRING,
    `vendor`       STRING,
    `serialNumber` STRING,
    `latitude`     DOUBLE,
    `longitude`    DOUBLE,
    `updatedAt`    TIMESTAMP(3),
    WATERMARK FOR `updatedAt` AS `updatedAt` - INTERVAL '10' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'iot_devices_db2',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id'          = 'flink-iot-merge-devices',
    'scan.startup.mode'            = 'earliest-offset',
    'key.format'                   = 'raw',
    'key.fields'                   = 'deviceID',
    'value.format'                 = 'avro-confluent',
    'value.avro-confluent.url'     = 'http://schema-registry:8081'
);

DROP TABLE IF EXISTS `iot_devices_measurements_db2`;
CREATE TABLE `iot_devices_measurements_db2` (
    `deviceID`     STRING,
    `temperature`  DOUBLE,
    `humidity`     DOUBLE,
    `pressure`     DOUBLE,
    `updatedAt`    TIMESTAMP(3),
    WATERMARK FOR `updatedAt` AS `updatedAt` - INTERVAL '10' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'iot_devices_measurements_db2',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id'          = 'flink-iot-merge-measurements',
    'scan.startup.mode'            = 'earliest-offset',
    'key.format'                   = 'raw',
    'key.fields'                   = 'deviceID',
    'value.format'                 = 'avro-confluent',
    'value.avro-confluent.url'     = 'http://schema-registry:8081'
);

DROP TABLE IF EXISTS `iot_devices_merged`;
CREATE TABLE `iot_devices_merged` (
    `deviceID`     STRING,
    `vendor`       STRING,
    `serialNumber` STRING,
    `temperature`  DOUBLE,
    `humidity`     DOUBLE,
    `pressure`     DOUBLE,
    `latitude`     DOUBLE,
    `longitude`    DOUBLE,
    `updatedAt`    TIMESTAMP(3),
    PRIMARY KEY (`deviceID`) NOT ENFORCED
) WITH (
    'connector'                    = 'upsert-kafka',
    'topic'                        = 'iot_devices_merged',
    'properties.bootstrap.servers' = 'broker:29092',
    'key.format'                   = 'raw',
    'value.format'                 = 'avro-confluent',
    'value.avro-confluent.url'     = 'http://schema-registry:8081',
    'value.fields-include'         = 'ALL'
);

INSERT INTO `iot_devices_merged`
SELECT
    m.`deviceID`,
    d.`vendor`,
    d.`serialNumber`,
    m.`temperature`,
    m.`humidity`,
    m.`pressure`,
    d.`latitude`,
    d.`longitude`,
    m.`updatedAt`
FROM `iot_devices_measurements_db2` m
LEFT JOIN `iot_devices_db2` d
ON m.`deviceID` = d.`deviceID`
AND d.`updatedAt` BETWEEN m.`updatedAt` - INTERVAL '30' DAY AND m.`updatedAt`;
EOF

# Copy SQL into the sql-client container and execute
echo "Submitting SQL via flink-sql-client..."
docker cp /tmp/iot_flink_merge.sql flink-sql-client:/tmp/iot_flink_merge.sql
docker exec flink-sql-client /opt/flink/bin/sql-client.sh -f /tmp/iot_flink_merge.sql

echo ""
echo "=== Flink merge job submitted. Monitor at: http://$FLINK_HOST ==="
