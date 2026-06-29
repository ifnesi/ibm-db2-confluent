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
VENDORS = sorted([
    "SensorCorp",
    "IoTWorks",
    "DataFlow",
    "SmartTech",
    "DeviceNet",
    "CloudSense",
    "TelemetryPro",
    "MeterMax",
    "GaugeHub",
    "StreamData",
], key=lambda _: random.random())
COORDINATES = sorted([
    [51.5074, -0.1278],
    [51.5200, -0.1000],
    [51.4890, -0.0700],
    [51.5300, -0.1800],
    [51.4600, -0.1200],
    [51.5500, -0.0200],
    [51.4800, 0.0200],
    [51.5900, 0.0700],
    [51.4300, 0.1000],
    [51.5000, -0.2500],
], key=lambda _: random.random())
DEVICES = [
    {
        "device_identifier": f"device-{i:02d}",
        "vendor_name": data[0],
        "serial_number": f"SN{100000 + i}",
        "lat": data[1][0],
        "long": data[1][1],
    }
    for i, data in enumerate(zip(VENDORS, COORDINATES), start=1)
]

# Per-device sensor state for realistic incremental drift
sensor_state = {
    d["device_identifier"]: {
        "temp": round(random.uniform(5, 35), 2),
        "hmdt": round(random.uniform(30, 75), 2),
        "press": round(random.uniform(1000, 1025), 2),
    }
    for d in DEVICES
}


def try_connect():
    return jaydebeapi.connect(
        JDBC_DRIVER,
        DB2_URL,
        [
            DB2_USER,
            DB2_PASSWORD,
        ],
        JDBC_JAR,
    )


def wait_and_connect():
    start = time.time()
    attempt = 0
    print(
        "Waiting for DB2 to be ready (first boot takes 3-5 min on Apple Silicon)...",
        flush=True,
    )
    while True:
        try:
            conn = try_connect()
            elapsed = int(time.time() - start)
            print(f"DB2 is ready ({elapsed}s).", flush=True)

            # Verify both tables exist before returning
            curs = conn.cursor()
            curs.execute("SELECT COUNT(*) FROM DB2INST1.IOT_DEVICES")
            curs.fetchone()
            curs.execute("SELECT COUNT(*) FROM DB2INST1.IOT_DEVICES_MEASUREMENTS")
            curs.fetchone()
            print("Tables verified.", flush=True)
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

    print("Seeding 10 devices into DB2...", flush=True)
    for device in DEVICES:
        try:
            # Check if device already exists
            curs.execute(
                "SELECT 1 FROM DB2INST1.IOT_DEVICES WHERE device_identifier = ?",
                [device["device_identifier"]],
            )
            if curs.fetchone():
                print(f"[SEED] {device['device_identifier']} already exists (skipped)", flush=True)
                continue

            # Insert the device
            curs.execute(
                "INSERT INTO DB2INST1.IOT_DEVICES "
                "(device_identifier, vendor_name, serial_number, lat, long, created_timestamp) "
                "VALUES (?, ?, ?, ?, ?, CURRENT TIMESTAMP)",
                [
                    device["device_identifier"],
                    device["vendor_name"],
                    device["serial_number"],
                    device["lat"],
                    device["long"],
                ],
            )
            conn.commit()
            print(f"[SEED] {device['device_identifier']}", flush=True)
        except Exception as exc:
            print(f"[SEED] {device['device_identifier']} ERROR: {exc}", flush=True)
            try:
                conn.rollback()
            except Exception:
                pass
            conn = wait_and_connect()
            curs = conn.cursor()

    print("Writing initial baseline measurements for each device...", flush=True)
    for device in DEVICES:
        did = device["device_identifier"]
        state = sensor_state[did]
        try:
            curs.execute(
                "INSERT INTO DB2INST1.IOT_DEVICES_MEASUREMENTS "
                "(device_identifier, temp, hmdt, press, created_timestamp) "
                "VALUES (?, ?, ?, ?, CURRENT TIMESTAMP)",
                [
                    did,
                    state["temp"],
                    state["hmdt"],
                    state["press"],
                ],
            )
            conn.commit()
            print(
                f"[BASELINE] {did} temp={state['temp']} hmdt={state['hmdt']} press={state['press']}",
                flush=True,
            )
        except Exception as exc:
            print(f"[BASELINE] {did} ERROR: {exc}", flush=True)
            try:
                conn.rollback()
            except Exception:
                pass

    print(
        "Data generation started (2 inserts/second, 10 IoT devices, random-walk measurements).",
        flush=True,
    )

    while True:
        try:
            device = random.choice(DEVICES)
            did = device["device_identifier"]
            state = sensor_state[did]

            new_temp = round(
                max(10, min(30, state["temp"] + random.uniform(-0.5, 0.5))),
                2,
            )
            new_hmdt = round(
                max(20, min(80, state["hmdt"] + random.uniform(-1, 1))),
                2,
            )
            new_press = round(
                max(990, min(1030, state["press"] + random.uniform(-0.5, 0.5))),
                2,
            )

            sensor_state[did]["temp"] = new_temp
            sensor_state[did]["hmdt"] = new_hmdt
            sensor_state[did]["press"] = new_press

            curs.execute(
                "INSERT INTO DB2INST1.IOT_DEVICES_MEASUREMENTS "
                "(device_identifier, temp, hmdt, press, created_timestamp) "
                "VALUES (?, ?, ?, ?, CURRENT TIMESTAMP)",
                [
                    did,
                    new_temp,
                    new_hmdt,
                    new_press,
                ],
            )
            conn.commit()
            print(
                f"[MEASUREMENT] {did} temp={new_temp} hmdt={new_hmdt} press={new_press}",
                flush=True,
            )

        except Exception as exc:
            exc_str = str(exc)
            if "SQLCODE=-530" in exc_str or "foreign key" in exc_str.lower():
                # FK constraint error — device might not exist, but this is unusual
                print(f"Warning: FK constraint on {did} — skipping this measurement", flush=True)
            else:
                # Other error — reconnect and retry
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
