import json
import time
from kafka import KafkaConsumer
import pyodbc

# ------------------ CONFIG ------------------
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "blinkit-clean"

SERVER = ""  # change to your SQL Server
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
        print("✅ Connected to SQL Server (Stockout Alerts).")
        break
    except Exception as e:
        print(f"⚠ SQL Server not available: {e}. Retrying in 5s...")
        time.sleep(5)

# ------------------ CREATE ALERTS TABLE ------------------
cursor.execute("""
IF OBJECT_ID('alerts_stockout', 'U') IS NULL
BEGIN
    CREATE TABLE alerts_stockout (
        AlertID INT IDENTITY(1,1) PRIMARY KEY,
        AlertType NVARCHAR(50),
        StoreID NVARCHAR(50),
        ItemID NVARCHAR(50),
        ItemName NVARCHAR(100),
        Stock INT,
        Message NVARCHAR(500),
        CreatedAt DATETIME DEFAULT GETDATE()
    );
END
""")
print("✅ Verified/created alerts_stockout table.")

# ------------------ KAFKA CONSUMER ------------------
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="blinkit-stockout-alerts",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("📥 Blinkit Stockout Alerts Consumer started...")

# ------------------ PROCESS EVENTS ------------------
try:
    for msg in consumer:
        event = msg.value
        if event.get("type") == "inventory":
            payload = event["payload"]

            try:
                stock = int(payload.get("Stock", 0))
            except ValueError:
                stock = 0  # Fallback if Stock is missing/invalid

            if stock < 5:  # Stockout Alert condition
                message = f"📦 Store {payload.get('StoreID')} running low on {payload.get('ItemName')} (only {stock} left)."
                cursor.execute(
                    """INSERT INTO alerts_stockout (AlertType, StoreID, ItemID, ItemName, Stock, Message)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    "Stockout",
                    payload.get("StoreID"),
                    payload.get("ItemID"),
                    payload.get("ItemName"),
                    stock,
                    message
                )
                print(message)

except KeyboardInterrupt:
    print("\n🛑 Stockout Alerts Consumer stopped.")
finally:
    consumer.close()
    cursor.close()
    conn.close()
    print("✅ Kafka and SQL connections closed.")

