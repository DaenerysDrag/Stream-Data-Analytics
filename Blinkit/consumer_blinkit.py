import json
import time
from kafka import KafkaConsumer, KafkaProducer
import pyodbc

# ------------------ CONFIG ------------------
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "blinkit-stream"
OUTPUT_TOPIC = "blinkit-clean"

SERVER = "DAENERYS"  # replace with your SQL Server
DATABASE = "blinkit_ops"
DRIVER = "ODBC Driver 18 for SQL Server"

# ------------------ CONNECT MASTER ------------------
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

# ------------------ CREATE DATABASE ------------------
cursor_master.execute(f"""
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{DATABASE}')
BEGIN
    CREATE DATABASE {DATABASE};
END
""")
print(f"✅ Verified/created database: {DATABASE}")
cursor_master.close()
conn_master.close()

# ------------------ CONNECT TO BLINKIT OPS ------------------
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

# ------------------ CREATE TABLES ------------------
create_tables_sql = [
    """
    IF OBJECT_ID('orders', 'U') IS NULL
    BEGIN
        CREATE TABLE orders (
            OrderID NVARCHAR(50) PRIMARY KEY,
            RiderID NVARCHAR(50),
            StoreID NVARCHAR(50),
            ItemID NVARCHAR(50),
            DispatchLocation NVARCHAR(100),
            DeliveryLocation NVARCHAR(100),
            OrderTime NVARCHAR(50),
            ExpectedDeliveryTime NVARCHAR(50),
            ActualDeliveryTime NVARCHAR(50)
        );
    END
    """,
    """
    IF OBJECT_ID('riders', 'U') IS NULL
    BEGIN
        CREATE TABLE riders (
            RiderID NVARCHAR(50) PRIMARY KEY,
            Status NVARCHAR(50),
            CurrentLocation NVARCHAR(100)
        );
    END
    """,
    """
    IF OBJECT_ID('inventory', 'U') IS NULL
    BEGIN
        CREATE TABLE inventory (
            ItemID NVARCHAR(50),
            StoreID NVARCHAR(50),
            StockQty INT,
            Threshold INT,
            PRIMARY KEY (ItemID, StoreID) -- composite PK
        );
    END
    """
]

for query in create_tables_sql:
    cursor.execute(query)
print("✅ Verified/created tables: orders, riders, inventory")

# ------------------ KAFKA CONSUMER & PRODUCER ------------------
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="blinkit-db-loader",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("📥 Consuming from blinkit-stream, saving to SQL Server & re-publishing to blinkit-clean...")

# ------------------ CONSUME ------------------
try:
    for msg in consumer:
        event = msg.value.copy()
        record_type = event.get("type")
        payload = event.get("payload")

        try:
            if record_type == "order":
                cursor.execute(
                    """INSERT INTO orders 
                       (OrderID, RiderID, StoreID, ItemID, DispatchLocation, DeliveryLocation, OrderTime, ExpectedDeliveryTime, ActualDeliveryTime)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    payload.get("OrderID"),
                    payload.get("RiderID"),
                    payload.get("StoreID"),
                    payload.get("ItemID"),
                    payload.get("DispatchLocation"),
                    payload.get("DeliveryLocation"),
                    payload.get("OrderTime"),
                    payload.get("ExpectedDeliveryTime"),
                    payload.get("ActualDeliveryTime")
                )
                print(f"🛒 Order stored: {payload.get('OrderID')}")

            elif record_type == "rider":
                cursor.execute(
                    """INSERT INTO riders (RiderID, Status, CurrentLocation)
                       VALUES (?, ?, ?)""",
                    payload.get("RiderID"),
                    payload.get("Status"),
                    payload.get("CurrentLocation")
                )
                print(f"🚴 Rider stored: {payload.get('RiderID')}")

            elif record_type == "inventory":
                cursor.execute(
                    """
                    MERGE inventory AS target
                    USING (SELECT ? AS ItemID, ? AS StoreID, ? AS StockQty, ? AS Threshold) AS source
                    ON target.ItemID = source.ItemID AND target.StoreID = source.StoreID
                    WHEN MATCHED THEN
                        UPDATE SET StockQty = source.StockQty, Threshold = source.Threshold
                    WHEN NOT MATCHED THEN
                        INSERT (ItemID, StoreID, StockQty, Threshold)
                        VALUES (source.ItemID, source.StoreID, source.StockQty, source.Threshold);
                    """,
                    payload.get("ItemID"),
                    payload.get("StoreID"),
                    payload.get("StockQty"),
                    payload.get("Threshold")
                )
                print(f"📦 Inventory upserted: {payload.get('ItemID')} in Store {payload.get('StoreID')}")

            conn.commit()

        except Exception as db_err:
            print(f"❌ DB Insert Error: {db_err}")
            conn.rollback()

        # Republish to clean topic
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
