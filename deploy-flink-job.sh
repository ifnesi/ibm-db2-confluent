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
    `DEVICEID`     STRING,
    `VENDOR`       STRING,
    `SERIALNUMBER` STRING,
    `TEMPERATURE`  DOUBLE,
    `HUMIDITY`     DOUBLE,
    `PRESSURE`     DOUBLE,
    `CREATEDAT`    TIMESTAMP(3),
    `UPDATEDAT`    TIMESTAMP(3),
    WATERMARK FOR `UPDATEDAT` AS `UPDATEDAT`
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'DB2INST1.IOT_DEVICES',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id'          = 'flink-iot-averages',
    'scan.startup.mode'            = 'earliest-offset',
    'key.format'                   = 'raw',
    'key.fields'                   = 'DEVICEID',
    'value.format'                 = 'avro-confluent',
    'value.avro-confluent.url'     = 'http://schema-registry:8081'
);

DROP TABLE IF EXISTS `iot_devices_avg`;
CREATE TABLE `iot_devices_avg` (
    `DEVICEID`       STRING,
    `window_start`   TIMESTAMP(3),
    `window_end`     TIMESTAMP(3),
    `avg_temperature` DOUBLE,
    `avg_humidity`   DOUBLE,
    `avg_pressure`   DOUBLE,
    PRIMARY KEY (`DEVICEID`) NOT ENFORCED
) WITH (
    'connector'                    = 'upsert-kafka',
    'topic'                        = 'IOT_DEVICES_AVG',
    'properties.bootstrap.servers' = 'broker:29092',
    'key.format'                   = 'raw',
    'value.format'                 = 'avro-confluent',
    'value.avro-confluent.url'     = 'http://schema-registry:8081',
    'value.fields-include'         = 'ALL'
);

INSERT INTO `iot_devices_avg`
SELECT
    `DEVICEID`,
    TUMBLE_START(`UPDATEDAT`, INTERVAL '1' MINUTE) AS `window_start`,
    TUMBLE_END(`UPDATEDAT`, INTERVAL '1' MINUTE)   AS `window_end`,
    ROUND(AVG(`TEMPERATURE`), 2) AS `avg_temperature`,
    ROUND(AVG(`HUMIDITY`), 2)    AS `avg_humidity`,
    ROUND(AVG(`PRESSURE`), 2)    AS `avg_pressure`
FROM `iot_devices_source`
GROUP BY `DEVICEID`, TUMBLE(`UPDATEDAT`, INTERVAL '1' MINUTE);
EOF

# Copy SQL into the sql-client container and execute
echo "Submitting SQL via flink-sql-client..."
docker cp /tmp/iot_flink.sql flink-sql-client:/tmp/iot_flink.sql
docker exec flink-sql-client /opt/flink/bin/sql-client.sh -f /tmp/iot_flink.sql

echo ""
echo "=== Flink job submitted. Monitor at: http://$FLINK_HOST ==="
