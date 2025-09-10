import json
import time
from datetime import datetime
from kafka import KafkaConsumer
import pyodbc

# ------------------ CONFIG ------------------
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "blinkit-clean"

SERVER = "DAENERYS"   # change to your SQL Server name
DATABASE = "blinkit_ops"
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
        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()
        print("✅ Connected to SQL Server (Delivery Alerts).")
        break
    except Exception as e:
        print(f"⚠ SQL Server not available: {e}. Retrying in 5s...")
        time.sleep(5)

# ------------------ CREATE ALERTS TABLE ------------------
cursor.execute("""
IF OBJECT_ID('alerts_delivery', 'U') IS NULL
BEGIN
    CREATE TABLE alerts_delivery (
        AlertID INT IDENTITY(1,1) PRIMARY KEY,
        AlertType NVARCHAR(50),
        OrderID NVARCHAR(50),
        RiderID NVARCHAR(50),
        StoreID NVARCHAR(50),
        ItemID NVARCHAR(50),
        Location NVARCHAR(100),
        Message NVARCHAR(500),
        CreatedAt DATETIME DEFAULT GETDATE()
    );
END
""")
print("✅ Verified/created alerts_delivery table.")

# ------------------ KAFKA CONSUMER ------------------
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="blinkit-delivery-alerts",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("📥 Blinkit Delivery Delay Alerts Consumer started...")

# ------------------ PROCESS EVENTS ------------------
try:
    for msg in consumer:
        event = msg.value
        if event.get("type") == "order":
            payload = event["payload"]

            exp = payload.get("ExpectedDeliveryTime")
            act = payload.get("ActualDeliveryTime")
            location = payload.get("Location", "Unknown")

            if exp and act:
                try:
                    exp_dt = datetime.strptime(exp, "%Y-%m-%d %H:%M:%S")
                    act_dt = datetime.strptime(act, "%Y-%m-%d %H:%M:%S")
                except ValueError as t_err:
                    print(f"⚠ Time parse error: {t_err}")
                    continue

                delay = (act_dt - exp_dt).total_seconds() / 60
                if delay >= 15:  # Trigger Alert
                    message = f"🚨 Order {payload.get('OrderID')} delayed by {int(delay)} mins in {location}."
                    cursor.execute(
                        """INSERT INTO alerts_delivery (AlertType, OrderID, RiderID, StoreID, ItemID, Location, Message)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        "DeliveryDelay",
                        payload.get("OrderID"),
                        payload.get("RiderID"),
                        payload.get("StoreID"),
                        payload.get("ItemID"),
                        location,
                        message
                    )
                    print(message)

except KeyboardInterrupt:
    print("\n🛑 Delivery Alerts Consumer stopped.")
finally:
    consumer.close()
    cursor.close()
    conn.close()
    print("✅ Kafka and SQL connections closed.")
