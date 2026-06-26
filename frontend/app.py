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

from confluent_kafka import Consumer, KafkaError
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit

from lineage import get_lineage


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
KAFKA_TOPIC_DEVICES = os.environ.get("KAFKA_TOPIC_DEVICES", "DB2INST1.IOT_DEVICES")
KAFKA_TOPIC_AVG = os.environ.get("KAFKA_TOPIC_AVG", "IOT_DEVICES_AVG")

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
            topic = msg.topic()

            if topic == KAFKA_TOPIC_DEVICES:
                # Raw device data — DB2 column names are uppercase in Avro schema
                device_id = str(safe.get("DEVICEID", ""))
                with devices_lock:
                    is_new = device_id not in devices
                    devices[device_id] = safe
                safe["_new"] = is_new
                socketio.emit("device_update", safe)
            elif topic == KAFKA_TOPIC_AVG:
                # Average data — field names are lowercase (defined by Flink sink table)
                device_id = str(safe.get("DEVICEID", ""))
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
