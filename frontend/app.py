"""
Live IoT dashboard — consumes from Kafka (raw devices + averages) and pushes updates via WebSocket.
"""

import io
import os
import json
import time
import struct
import threading
import fastavro
import requests

from datetime import datetime
from decimal import Decimal
from collections import deque

from confluent_kafka import Consumer, KafkaError, TopicPartition
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit

from lineage import get_lineage, get_lineage_service, KAFKA_CONNECT_URL


app = Flask(__name__)
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="threading",
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "broker:29092")
SCHEMA_REGISTRY_URL = os.environ.get(
    "SCHEMA_REGISTRY_URL", "http://schema-registry:8081"
)
KAFKA_TOPIC_DEVICES = os.environ.get("KAFKA_TOPIC_DEVICES", "iot_devices_merged")
KAFKA_TOPIC_AVG = os.environ.get("KAFKA_TOPIC_AVG", "iot_devices_avg")

# Counter for total events received by Kafka consumer thread
events_received_count: int = 0
events_received_lock = threading.Lock()

# In-memory storage: device id (str) -> serializable dict
devices: dict[str, dict] = dict()
devices_lock = threading.Lock()

# In-memory storage for averages (keep last 15 mins): device_id -> deque of {timestamp, temps, humidity, pressure}
averages_history: dict[str, deque] = dict()
averages_lock = threading.Lock()
MAX_HISTORY_POINTS = 15 * 60  # 15 minutes of 1-second updates (we'll store key points)

schema_cache: dict[int, dict] = dict()


# ---------------------------------------------------------------------------
# Avro helpers
# ---------------------------------------------------------------------------


def fetch_schema(schema_id: int) -> dict:
    if schema_id not in schema_cache:
        resp = requests.get(f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}", timeout=5)
        resp.raise_for_status()
        raw = json.loads(resp.json()["schema"])
        schema_cache[schema_id] = fastavro.parse_schema(raw)
    return schema_cache[schema_id]


def deserialize(data: bytes) -> dict | None:
    """Decode a Confluent-wire-format Avro message."""
    if not data or data[0] != 0x00:
        return None
    schema_id = struct.unpack(">I", data[1:5])[0]
    schema = fetch_schema(schema_id)
    return fastavro.schemaless_reader(io.BytesIO(data[5:]), schema)


def to_json_safe(record: dict) -> dict:
    """Convert Python types that aren't JSON-serialisable."""
    out = {}
    for k, v in record.items():
        if v is None:
            out[k] = None
        elif isinstance(v, Decimal):
            out[k] = float(v)
        elif isinstance(v, (int, float, str, bool)):
            out[k] = v
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = str(v)
    return out


# ---------------------------------------------------------------------------
# Kafka consumer (background thread)
# ---------------------------------------------------------------------------


def kafka_consumer_thread():
    print("Kafka consumer starting — waiting 15 s for broker …", flush=True)
    time.sleep(15)

    # Wait for required topics to exist before subscribing
    print(f"Waiting for topics {KAFKA_TOPIC_DEVICES} and {KAFKA_TOPIC_AVG} to be created...", flush=True)
    topics_ready = False
    attempt = 0
    while not topics_ready:
        try:
            check_consumer = Consumer(
                {
                    "bootstrap.servers": KAFKA_BOOTSTRAP,
                    "group.id": "frontend-topic-check",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                }
            )
            metadata = check_consumer.list_topics(timeout=5)
            available_topics = set(metadata.topics.keys())

            if KAFKA_TOPIC_DEVICES in available_topics and KAFKA_TOPIC_AVG in available_topics:
                print(f"✓ Topics ready: {KAFKA_TOPIC_DEVICES}, {KAFKA_TOPIC_AVG}", flush=True)
                topics_ready = True
                check_consumer.close()
            else:
                attempt += 1
                missing = []
                if KAFKA_TOPIC_DEVICES not in available_topics:
                    missing.append(KAFKA_TOPIC_DEVICES)
                if KAFKA_TOPIC_AVG not in available_topics:
                    missing.append(KAFKA_TOPIC_AVG)
                print(f"  [attempt {attempt}] Waiting for: {', '.join(missing)}", flush=True)
                check_consumer.close()
                time.sleep(5)
        except Exception as e:
            attempt += 1
            print(f"  [attempt {attempt}] Topic check failed: {e} — retrying...", flush=True)
            time.sleep(5)

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "frontend-consumer-group",
            "isolation.level": "read_uncommitted",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([KAFKA_TOPIC_DEVICES, KAFKA_TOPIC_AVG])
    print(f"Subscribed to {KAFKA_TOPIC_DEVICES} and {KAFKA_TOPIC_AVG}", flush=True)

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Kafka error: {msg.error()}", flush=True)
            continue

        try:
            record = deserialize(msg.value())
            if record is None:
                continue
            safe = to_json_safe(record)
            global events_received_count
            with events_received_lock:
                events_received_count += 1
            topic = msg.topic()

            if topic == KAFKA_TOPIC_DEVICES:
                # Raw device data — DB2 column names are uppercase in Avro schema
                device_id = str(safe.get("deviceID", ""))
                with devices_lock:
                    is_new = device_id not in devices
                    devices[device_id] = safe
                safe["_new"] = is_new
                socketio.emit("device_update", safe)
            elif topic == KAFKA_TOPIC_AVG:
                # Average data — field names are lowercase (defined by Flink sink table)
                device_id = str(safe.get("deviceID", ""))
                with averages_lock:
                    if device_id not in averages_history:
                        averages_history[device_id] = deque(maxlen=MAX_HISTORY_POINTS)
                    # Store the average point
                    point = {
                        "timestamp": safe.get("window_end", datetime.now().isoformat()),
                        "avg_temperature": safe.get("avg_temperature", 0),
                        "avg_humidity": safe.get("avg_humidity", 0),
                        "avg_pressure": safe.get("avg_pressure", 0),
                    }
                    averages_history[device_id].append(point)
                # Broadcast the average
                socketio.emit("average_update", safe)
        except Exception as exc:
            print(f"Deserialization error: {exc}", flush=True)


# ---------------------------------------------------------------------------
# Routes & Socket.IO events
# ---------------------------------------------------------------------------


@app.route("/")
def index():
    with devices_lock:
        devices_snapshot = list(devices.values())
    with averages_lock:
        averages_snapshot = {k: list(v) for k, v in averages_history.items()}
    return render_template(
        "index.html",
        devices=devices_snapshot,
        averages=averages_snapshot,
    )


@app.route("/api/topic/<path:topic_name>/last-message")
def topic_last_message(topic_name: str):
    """Return the last message (key, value, offset) from a Kafka topic."""
    try:
        c = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "__lineage-last-msg__",
            "enable.auto.commit": False,
        })
        meta = c.list_topics(topic_name, timeout=5)
        if topic_name not in meta.topics:
            c.close()
            return jsonify({"error": f"Topic '{topic_name}' not found"}), 404

        partitions = list(meta.topics[topic_name].partitions.keys())
        best = None  # (offset, key, value_bytes)

        for pid in partitions:
            tp = TopicPartition(topic_name, pid)
            low, high = c.get_watermark_offsets(tp, timeout=5)
            if high <= 0:
                continue
            tp.offset = high - 1
            c.assign([tp])
            msg = c.poll(timeout=5.0)
            if msg and not msg.error():
                if best is None or msg.offset() > best[0]:
                    best = (msg.offset(), msg.key(), msg.value())

        c.close()

        if best is None:
            return jsonify({"error": "No messages in topic"}), 404

        offset, raw_key, raw_value = best
        key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else str(raw_key or "")
        try:
            value = deserialize(raw_value)
            if value:
                value = to_json_safe(value)
        except Exception:
            value = {"raw": raw_value.hex() if raw_value else None}

        return jsonify({"topic": topic_name, "offset": offset, "key": key, "value": value})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/connector/<path:connector_name>/status")
def connector_status(connector_name: str):
    """Return status and config summary for a Kafka Connect connector."""
    try:
        status_resp = requests.get(
            f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/status", timeout=5
        )
        status_resp.raise_for_status()
        status_data = status_resp.json()

        config_resp = requests.get(
            f"{KAFKA_CONNECT_URL}/connectors/{connector_name}", timeout=5
        )
        config_resp.raise_for_status()
        config_data = config_resp.json()
        config = config_data.get("config", {})

        connector_state = status_data.get("connector", {}).get("state", "UNKNOWN")
        tasks = [
            {"id": t["id"], "state": t["state"], "worker_id": t.get("worker_id", "")}
            for t in status_data.get("tasks", [])
        ]

        # Shorten connector class name
        raw_class = config.get("connector.class", "")
        connector_class = raw_class.split(".")[-1] if raw_class else raw_class

        # Determine connector type and extract config rows
        is_source = "Source" in raw_class
        config_rows = []

        if is_source:
            poll_ms = config.get("poll.interval.ms")
            if poll_ms is not None:
                config_rows.append({"label": "Poll interval", "value": f"{poll_ms} ms"})
            table = config.get("table.whitelist") or config.get("table.include.list")
            if table:
                config_rows.append({"label": "Table", "value": table})
            mode = config.get("mode")
            if mode:
                config_rows.append({"label": "Mode", "value": mode})
        else:
            topics_val = config.get("topics", "")
            if topics_val:
                config_rows.append({"label": "Topics", "value": topics_val})
            conn_url = config.get("connection.url", "")
            if conn_url:
                import re
                masked = re.sub(r'(?i)(password=)[^&;]+', r'\1***', conn_url)
                config_rows.append({"label": "Connection", "value": masked})
            consumer_group = f"connect-{connector_name}"
            config_rows.append({"label": "Consumer group", "value": consumer_group})
            # Compute lag for sink connector topics
            svc = get_lineage_service()
            topic_list = [t.strip() for t in topics_val.split(",") if t.strip()]
            for topic in topic_list:
                lag = svc.get_lag(consumer_group, topic)
                config_rows.append({"label": f"Lag · {topic}", "value": lag})

        return jsonify({
            "name": connector_name,
            "connector_class": connector_class,
            "status": connector_state,
            "tasks": tasks,
            "config_rows": config_rows,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/webapp/status")
def webapp_status():
    """Return in-process state of the webapp consumer."""
    try:
        global events_received_count
        with events_received_lock:
            total = events_received_count
        with devices_lock:
            num_devices = len(devices)

        topics = [KAFKA_TOPIC_DEVICES, KAFKA_TOPIC_AVG]
        svc = get_lineage_service()
        lag = {}
        for topic in topics:
            lag[topic] = svc.get_lag("frontend-consumer-group", topic)

        return jsonify({
            "consumer_group": "frontend-consumer-group",
            "topics": topics,
            "devices_in_memory": num_devices,
            "total_events_received": total,
            "lag": lag,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/lineage")
def lineage_api():
    """Return current data lineage graph."""
    try:
        lineage_data = get_lineage()
        return jsonify(lineage_data)
    except Exception as e:
        print(f"Error in lineage API: {e}", flush=True)
        return jsonify({"error": str(e)}), 500


def lineage_broadcast_thread():
    """Broadcast lineage updates every second."""
    print("Lineage broadcast thread starting...", flush=True)
    time.sleep(15)  # Wait for broker like Kafka consumer
    while True:
        try:
            lineage_data = get_lineage()
            socketio.emit("lineage_update", lineage_data)
            time.sleep(1)
        except Exception as e:
            print(f"Error in lineage broadcast: {e}", flush=True)
            time.sleep(1)


@socketio.on("connect")
def handle_connect():
    with devices_lock:
        devices_snapshot = list(devices.values())
    with averages_lock:
        averages_snapshot = {k: list(v) for k, v in averages_history.items()}
    emit("initial_devices", devices_snapshot)
    emit("initial_averages", averages_snapshot)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    t = threading.Thread(
        target=kafka_consumer_thread,
        daemon=True,
    )
    t.start()

    t2 = threading.Thread(
        target=lineage_broadcast_thread,
        daemon=True,
    )
    t2.start()

    socketio.run(
        app,
        host="0.0.0.0",
        port=5001,
        allow_unsafe_werkzeug=True,
    )
