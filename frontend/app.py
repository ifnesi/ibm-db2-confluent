"""
Live employee dashboard — consumes from Kafka and pushes updates to browsers via WebSocket.
"""
import io
import json
import os
import struct
import threading
import time
from decimal import Decimal

import fastavro
import requests
from confluent_kafka import Consumer, KafkaError
from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "broker:29092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "db2-EMPLOYEES")

# In-memory table: employee id (str) -> serializable dict
employees: dict[str, dict] = {}
employees_lock = threading.Lock()

schema_cache: dict[int, dict] = {}


# ---------------------------------------------------------------------------
# Avro helpers
# ---------------------------------------------------------------------------

def fetch_schema(schema_id: int) -> dict:
    if schema_id not in schema_cache:
        resp = requests.get(
            f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}", timeout=5
        )
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
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    print(f"Subscribed to {KAFKA_TOPIC}", flush=True)

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
            emp_id = str(safe.get("ID", ""))

            with employees_lock:
                is_new = emp_id not in employees
                employees[emp_id] = safe

            safe["_new"] = is_new
            socketio.emit("employee_update", safe)
        except Exception as exc:
            print(f"Deserialization error: {exc}", flush=True)


# ---------------------------------------------------------------------------
# Routes & Socket.IO events
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    with employees_lock:
        snapshot = list(employees.values())
    return render_template("index.html", employees=snapshot)


@socketio.on("connect")
def handle_connect():
    with employees_lock:
        snapshot = list(employees.values())
    emit("initial_state", snapshot)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    t = threading.Thread(target=kafka_consumer_thread, daemon=True)
    t.start()
    socketio.run(app, host="0.0.0.0", port=5001, allow_unsafe_werkzeug=True)
