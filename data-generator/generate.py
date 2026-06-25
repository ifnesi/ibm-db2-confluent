"""
Continuously inserts and updates rows in DB2INST1.EMPLOYEES.
Keeps the table at most MAX_RECORDS rows; once full it only updates.
"""
import jaydebeapi
import time
import random
import os

DB2_URL = os.environ.get("DB2_URL", "jdbc:db2://db2-luw:50000/testdb")
DB2_USER = os.environ.get("DB2_USER", "db2inst1")
DB2_PASSWORD = os.environ.get("DB2_PASSWORD", "db2inst1-pwd")
JDBC_DRIVER = "com.ibm.db2.jcc.DB2Driver"
JDBC_JAR = "/app/db2jcc4.jar"
MAX_RECORDS = 10

NAMES = [
    "Alice Johnson", "Bob Smith", "Carol White", "David Brown",
    "Eve Davis", "Frank Wilson", "Grace Lee", "Henry Martinez",
    "Iris Thompson", "Jack Garcia", "Kim Chen", "Liam Roberts",
    "Mia Williams", "Noah Taylor", "Olivia Moore", "Paul Harris",
    "Quinn Adams", "Rachel Green", "Sam Turner", "Tina Foster",
]
DEPTS = ["Engineering", "Marketing", "Sales", "HR", "Finance",
         "Operations", "Legal", "Product", "Security", "Data"]


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
            # Print a progress dot every attempt; a full status line every 12 attempts (~60s)
            if attempt % 12 == 0:
                print(f"  still waiting... ({elapsed}s elapsed)", flush=True)
            else:
                print(".", end="", flush=True)
            time.sleep(5)


def run():
    conn = wait_and_connect()
    curs = conn.cursor()

    print("Data generation started (1 operation/second, max 10 rows).", flush=True)

    while True:
        try:
            curs.execute("SELECT COUNT(*) FROM DB2INST1.EMPLOYEES")
            count = int(curs.fetchone()[0])

            if count < MAX_RECORDS:
                curs.execute("SELECT COALESCE(MAX(ID), 0) FROM DB2INST1.EMPLOYEES")
                max_id = int(curs.fetchone()[0])
                new_id = max_id + 1
                name = random.choice(NAMES)
                dept = random.choice(DEPTS)
                salary = round(random.uniform(50000, 120000), 2)
                curs.execute(
                    "INSERT INTO DB2INST1.EMPLOYEES "
                    "(ID, NAME, DEPARTMENT, SALARY, HIRE_DATE, UPDATED_AT) "
                    "VALUES (?, ?, ?, ?, CURRENT DATE, CURRENT TIMESTAMP)",
                    [new_id, name, dept, salary],
                )
                conn.commit()
                print(f"[INSERT] id={new_id} {name} / {dept} / ${salary:,.2f}", flush=True)
            else:
                emp_id = random.randint(1, MAX_RECORDS)
                salary = round(random.uniform(50000, 120000), 2)
                curs.execute(
                    "UPDATE DB2INST1.EMPLOYEES "
                    "SET SALARY=?, UPDATED_AT=CURRENT TIMESTAMP "
                    "WHERE ID=?",
                    [salary, emp_id],
                )
                conn.commit()
                print(f"[UPDATE] id={emp_id} salary=${salary:,.2f}", flush=True)

        except Exception as exc:
            print(f"Error: {exc} — reconnecting...", flush=True)
            try:
                conn.close()
            except Exception:
                pass
            # wait_and_connect() loops until DB2 is back — never throws
            conn = wait_and_connect()
            curs = conn.cursor()

        time.sleep(1)


if __name__ == "__main__":
    run()
