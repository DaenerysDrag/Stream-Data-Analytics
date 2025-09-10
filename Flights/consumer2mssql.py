import json
import time
from kafka import KafkaConsumer, KafkaProducer
import pyodbc

# ------------------ CONFIG ------------------
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "airport-stream"
OUTPUT_TOPIC = "airport-clean"

SERVER = "DAENERYS"  # SQL Server name
DATABASE = "airport_ops"
DRIVER = "ODBC Driver 18 for SQL Server"

# ------------------ CONNECT TO MASTER ------------------
conn_str_master = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    "DATABASE=master;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
)

while True:
    try:
        conn_master = pyodbc.connect(conn_str_master, timeout=5, autocommit=True)
        cursor_master = conn_master.cursor()
        print("✅ Connected to SQL Server (master).")
        break
    except Exception as e:
        print(f"⚠ SQL Server not available: {e}. Retrying in 5s...")
        time.sleep(5)

# ------------------ CREATE DATABASE IF NOT EXISTS ------------------
cursor_master.execute(f"""
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{DATABASE}')
BEGIN
    CREATE DATABASE {DATABASE};
END
""")
print(f"✅ Verified/created database: {DATABASE}")
cursor_master.close()
conn_master.close()

# ------------------ CONNECT TO airport_ops ------------------
conn_str = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
)
conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()
print(f"✅ Connected to database: {DATABASE}")

# ------------------ CREATE TABLES IF NOT EXISTS ------------------
create_tables_sql = [
    """
    IF OBJECT_ID('flights', 'U') IS NULL
    BEGIN
        CREATE TABLE flights (
            FlightID NVARCHAR(50) PRIMARY KEY,
            Airline NVARCHAR(100),
            ScheduledDeparture NVARCHAR(50),
            ActualDeparture NVARCHAR(50),
            Gate NVARCHAR(50),
            DelayMinutes INT,
            DeparturePlace NVARCHAR(100),
            DestinationPlace NVARCHAR(100)
        );
    END
    """,
    """
    IF OBJECT_ID('passengers', 'U') IS NULL
    BEGIN
        CREATE TABLE passengers (
            PassengerID NVARCHAR(50) PRIMARY KEY,
            FlightID NVARCHAR(50),
            CheckInTime NVARCHAR(50),
            BoardingPassIssued NVARCHAR(10),
            Gate NVARCHAR(50)
        );
    END
    """,
    """
    IF OBJECT_ID('baggage', 'U') IS NULL
    BEGIN
        CREATE TABLE baggage (
            BagID NVARCHAR(50) PRIMARY KEY,
            PassengerID NVARCHAR(50),
            FlightID NVARCHAR(50),
            LoadStatus NVARCHAR(50)
        );
    END
    """
]

for query in create_tables_sql:
    cursor.execute(query)
print("✅ Verified/created tables: flights, passengers, baggage")

# ------------------ KAFKA CONSUMER & PRODUCER ------------------
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="db-loader-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("📥 DB Loader started: Consuming from airport-stream, saving to SQL Server & re-publishing to airport-clean...")

# ------------------ CONSUME STREAM ------------------
try:
    for msg in consumer:
        event = msg.value.copy()
        record_type = event.get("type")
        payload = event.get("payload")

        try:
            if record_type == "flight":
                cursor.execute(
                    """
                    INSERT INTO flights (
                        FlightID, Airline, ScheduledDeparture, ActualDeparture, Gate, DelayMinutes, DeparturePlace, DestinationPlace
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload.get("FlightID"),
                    payload.get("Airline"),
                    payload.get("ScheduledDeparture"),
                    payload.get("ActualDeparture"),
                    payload.get("Gate"),
                    payload.get("DelayMinutes"),
                    payload.get("DeparturePlace"),
                    payload.get("DestinationPlace")
                )
                print(f"✈️ Flight stored: {payload.get('FlightID')}")

            elif record_type == "passenger":
                cursor.execute(
                    """
                    INSERT INTO passengers (
                        PassengerID, FlightID, CheckInTime, BoardingPassIssued, Gate
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    payload.get("PassengerID"),
                    payload.get("FlightID"),
                    payload.get("CheckInTime"),
                    str(payload.get("BoardingPassIssued")),  # store as text (True/False)
                    payload.get("Gate")
                )
                print(f"🧳 Passenger stored: {payload.get('PassengerID')}")

            elif record_type == "baggage":
                cursor.execute(
                    """
                    INSERT INTO baggage (
                        BagID, PassengerID, FlightID, LoadStatus
                    ) VALUES (?, ?, ?, ?)
                    """,
                    payload.get("BagID"),
                    payload.get("PassengerID"),
                    payload.get("FlightID"),
                    payload.get("LoadStatus")
                )
                print(f"🎒 Baggage stored: {payload.get('BagID')}")

            conn.commit()

        except Exception as db_err:
            print(f"❌ Error inserting into SQL Server: {db_err}")
            conn.rollback()

        # ------------------ RE-PUBLISH TO CLEAN TOPIC ------------------
        event["payload"] = payload
        producer.send(OUTPUT_TOPIC, value=event)

except KeyboardInterrupt:
    print("\n🛑 DB Loader stopped.")

finally:
    consumer.close()
    producer.close()
    cursor.close()
    conn.close()
    print("✅ Connections closed.")
