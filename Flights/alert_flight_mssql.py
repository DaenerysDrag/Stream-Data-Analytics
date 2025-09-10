import json
import time
from kafka import KafkaConsumer, KafkaProducer
import pyodbc

# ------------------ CONFIG ------------------
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "airport-clean"     # produced by DB-loader
ALERT_TOPIC = "airport-alerts"    # optional, for alert downstream

SERVER = "DAENERYS"
DATABASE = "airport_ops"
DRIVER = "ODBC Driver 18 for SQL Server"

# ------------------ CONNECT TO SQL SERVER ------------------
conn_str = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
)

while True:
    try:
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        print("✅ Connected to SQL Server (Flight Delay Alerts).")
        break
    except Exception as e:
        print(f"⚠ SQL Server not available: {e}. Retrying in 5 seconds...")
        time.sleep(5)

# ------------------ CREATE ALERT TABLE ------------------
create_table_sql = """
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'alerts_flight_delay')
BEGIN
    CREATE TABLE alerts_flight_delay (
        AlertID INT IDENTITY(1,1) PRIMARY KEY,
        AlertType NVARCHAR(50),
        FlightID NVARCHAR(50),
        DelayMinutes INT,
        Message NVARCHAR(255),
        CreatedAt DATETIME DEFAULT GETDATE()
    );
END
"""
cursor.execute(create_table_sql)
conn.commit()
print("✅ Verified/created alerts_flight_delay table.")

# ------------------ KAFKA CONSUMER & PRODUCER ------------------
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="flight-delay-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("📥 Flight Delay Alerts Consumer started...")

# ------------------ PROCESS STREAM ------------------
try:
    for msg in consumer:
        event = msg.value

        if event["type"] == "flight":
            payload = event.get("payload", {})
            delay = int(payload.get("DelayMinutes", 0))
            flight_id = payload.get("FlightID")

            if delay >= 40:
                alert_type = "flight_delay"
                message = f"Flight {flight_id} delayed by {delay} minutes"

                # Insert into SQL Server
                try:
                    cursor.execute(
                        "INSERT INTO alerts_flight_delay (AlertType, FlightID, DelayMinutes, Message) VALUES (?, ?, ?, ?)",
                        alert_type, flight_id, delay, message
                    )
                    conn.commit()
                    print(f"🚨 Flight Delay Alert Triggered: {flight_id} ({delay} min)")
                except Exception as db_err:
                    print(f"❌ Error inserting alert: {db_err}")
                    conn.rollback()

                # Publish alert to Kafka for downstream consumers
                alert_event = {
                    "type": "flight_alert",
                    "payload": {
                        "FlightID": flight_id,
                        "DelayMinutes": delay,
                        "AlertType": alert_type,
                        "Message": message
                    }
                }
                producer.send(ALERT_TOPIC, value=alert_event)

except KeyboardInterrupt:
    print("\n🛑 Flight Delay Alerts Consumer stopped.")

finally:
    consumer.close()
    producer.close()
    cursor.close()
    conn.close()
    print("✅ Connections closed.")
