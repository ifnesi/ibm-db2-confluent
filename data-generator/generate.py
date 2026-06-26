"""
IoT device data generator — creates 10 devices at startup,
then makes small incremental changes to all sensor values every 0.5 seconds.
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

VENDORS = [
    "SensorCorp", "IoTWorks", "DataFlow", "SmartTech", "DeviceNet",
    "CloudSense", "TelemetryPro", "MeterMax", "GaugeHub", "StreamData"
]

# Device state: device_id -> {temperature, humidity, pressure}
device_state = {}


def try_connect():
    """Return a live connection or raise."""
    return jaydebeapi.connect(JDBC_DRIVER, DB2_URL, [DB2_USER, DB2_PASSWORD], JDBC_JAR)


def wait_and_connect():
    """Block until DB2 accepts a JDBC connection, then return it."""
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

    print("Data generation started (2 operations/second, 10 IoT devices).", flush=True)

    while True:
        try:
            print("Waiting for IOT_DEVICES table and inserting 10 devices...", flush=True)
            device_state.clear()
            for i in range(1, NUM_DEVICES + 1):
                device_id = f"device-{i:02d}"
                vendor = random.choice(VENDORS)
                serial = f"SN{random.randint(100000, 999999)}"
                temperature = round(random.uniform(15, 25), 2)
                humidity = round(random.uniform(40, 60), 2)
                pressure = round(random.uniform(1000, 1020), 2)

                device_state[device_id] = {
                    "temperature": temperature,
                    "humidity": humidity,
                    "pressure": pressure,
                }

                curs.execute(
                    "INSERT INTO DB2INST1.IOT_DEVICES "
                    "(deviceID, vendor, serialNumber, temperature, humidity, pressure, createdAt, updatedAt) "
                    "VALUES (?, ?, ?, ?, ?, ?, CURRENT TIMESTAMP, CURRENT TIMESTAMP)",
                    [device_id, vendor, serial, temperature, humidity, pressure],
                )
            conn.commit()
            print(f"[INIT] Created {NUM_DEVICES} devices", flush=True)
            break
        except Exception as exc:
            print(f"  table not ready yet ({exc}) — retrying in 5s...", flush=True)
            try:
                conn.rollback()
            except Exception:
                pass
            time.sleep(5)
            try:
                curs.execute("VALUES 1")
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = wait_and_connect()
                curs = conn.cursor()

    # Update loop: every 0.5s, pick a random device and update ALL three sensor fields at once
    while True:
        try:
            device_id = random.choice(list(device_state.keys()))
            state = device_state[device_id]

            temp_change = round(random.uniform(-0.5, 0.5), 2)
            hum_change = round(random.uniform(-1, 1), 2)
            pres_change = round(random.uniform(-0.5, 0.5), 2)

            new_temp = round(max(10, min(30, state["temperature"] + temp_change)), 2)
            new_hum = round(max(20, min(80, state["humidity"] + hum_change)), 2)
            new_pres = round(max(990, min(1030, state["pressure"] + pres_change)), 2)

            device_state[device_id]["temperature"] = new_temp
            device_state[device_id]["humidity"] = new_hum
            device_state[device_id]["pressure"] = new_pres

            curs.execute(
                "UPDATE DB2INST1.IOT_DEVICES "
                "SET temperature=?, humidity=?, pressure=?, updatedAt=CURRENT TIMESTAMP "
                "WHERE deviceID=?",
                [new_temp, new_hum, new_pres, device_id],
            )
            conn.commit()
            print(f"[UPDATE] {device_id} temp={new_temp} hum={new_hum} pres={new_pres}", flush=True)

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
