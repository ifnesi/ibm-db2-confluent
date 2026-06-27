"""
IoT device data generator — inserts a new sensor reading row every 0.5 seconds.
Device metadata (identifier, vendor, serial) is stable per device; only sensor
values vary. DB2 is append-only (no updates), acting as a commit log.
"""
import os
import time
import random
import jaydebeapi


DB2_URL = os.environ.get("DB2_URL", "jdbc:db2://db2-luw:50000/testdb")
DB2_USER = os.environ.get("DB2_USER", "db2inst1")
DB2_PASSWORD = os.environ.get("DB2_PASSWORD", "db2inst1-pwd")
JDBC_DRIVER = "com.ibm.db2.jcc.DB2Driver"
JDBC_JAR = "/app/db2jcc4.jar"
NUM_DEVICES = 10

# Fixed metadata per device — stable across all inserts
DEVICES = [
    {"device_identifier": f"device-{i:02d}",
     "vendor_name": vendor,
     "serial_number": f"SN{100000 + i}"}
    for i, vendor in enumerate([
        "SensorCorp", "IoTWorks", "DataFlow", "SmartTech", "DeviceNet",
        "CloudSense", "TelemetryPro", "MeterMax", "GaugeHub", "StreamData"
    ], start=1)
]

# Per-device sensor state for realistic incremental drift
sensor_state = {
    d["device_identifier"]: {
        "temp": round(random.uniform(15, 25), 2),
        "hmdt": round(random.uniform(40, 60), 2),
        "press": round(random.uniform(1000, 1020), 2),
    }
    for d in DEVICES
}


def try_connect():
    return jaydebeapi.connect(JDBC_DRIVER, DB2_URL, [DB2_USER, DB2_PASSWORD], JDBC_JAR)


def wait_and_connect():
    start = time.time()
    attempt = 0
    print("Waiting for DB2 to be ready (first boot takes 3-5 min on Apple Silicon)...", flush=True)
    while True:
        try:
            conn = try_connect()
            elapsed = int(time.time() - start)
            print(f"DB2 is ready ({elapsed}s).", flush=True)
            return conn
        except Exception:
            attempt += 1
            elapsed = int(time.time() - start)
            if attempt % 12 == 0:
                print(f"  still waiting... ({elapsed}s elapsed)", flush=True)
            else:
                print(".", end="", flush=True)
            time.sleep(5)


def run():
    conn = wait_and_connect()
    curs = conn.cursor()

    print("Data generation started (2 inserts/second, 10 IoT devices).", flush=True)

    while True:
        try:
            device = random.choice(DEVICES)
            did = device["device_identifier"]
            state = sensor_state[did]

            new_temp  = round(max(10, min(30, state["temp"]  + random.uniform(-0.5, 0.5))), 2)
            new_hmdt  = round(max(20, min(80, state["hmdt"]  + random.uniform(-1, 1))), 2)
            new_press = round(max(990, min(1030, state["press"] + random.uniform(-0.5, 0.5))), 2)

            sensor_state[did]["temp"]  = new_temp
            sensor_state[did]["hmdt"]  = new_hmdt
            sensor_state[did]["press"] = new_press

            curs.execute(
                "INSERT INTO DB2INST1.IOT_DEVICES "
                "(device_identifier, vendor_name, serial_number, temp, hmdt, press, created_timestamp) "
                "VALUES (?, ?, ?, ?, ?, ?, CURRENT TIMESTAMP)",
                [did, device["vendor_name"], device["serial_number"], new_temp, new_hmdt, new_press],
            )
            conn.commit()
            print(f"[INSERT] {did} temp={new_temp} hmdt={new_hmdt} press={new_press}", flush=True)

        except Exception as exc:
            print(f"Error: {exc} — reconnecting...", flush=True)
            try:
                conn.close()
            except Exception:
                pass
            conn = wait_and_connect()
            curs = conn.cursor()

        time.sleep(0.5)


if __name__ == "__main__":
    run()
