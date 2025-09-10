import json
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer
import pyodbc

# Kafka Config
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "airport-clean"

# SQL Server Config
SERVER = "DAENERYS"       # Update if needed
DATABASE = "airport_ops"
DRIVER = "ODBC Driver 18 for SQL Server"

# Connection string
conn_str = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
)

# Connect to SQL Server
while True:
    try:
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        print("✅ Connected to SQL Server (Passenger Alerts).")
        break
    except Exception as e:
        print(f"⚠ SQL Server not available: {e}. Retrying in 5 seconds...")
        time.sleep(5)

# --- Ensure tables exist ---
create_alerts_table = """
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'alerts_passenger_delay')
BEGIN
    CREATE TABLE alerts_passenger_delay (
        AlertID INT IDENTITY(1,1) PRIMARY KEY,
        AlertType NVARCHAR(50),
        FlightID NVARCHAR(50),
        PassengerID NVARCHAR(50),
        BoardingPass BIT,
        TimeToDepartureMin DECIMAL(10,2),
        Message NVARCHAR(255),
        CreatedAt DATETIME DEFAULT GETDATE()
    );
END
"""

create_flights_table = """
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'flights')
BEGIN
    CREATE TABLE flights (
        FlightID NVARCHAR(50) PRIMARY KEY,
        Airline NVARCHAR(100),
        Source NVARCHAR(100),
        Destination NVARCHAR(100),
        Status NVARCHAR(50),
        ScheduledDeparture NVARCHAR(50) -- store as string for simplicity
    );
END
"""

cursor.execute(create_alerts_table)
cursor.execute(create_flights_table)
conn.commit()
print("✅ Verified/created tables: alerts_passenger_delay, flights")

# Kafka Consumer
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="passenger-alert-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("📥 Passenger Boarding Alerts Consumer started...")

try:
    for msg in consumer:
        event = msg.value

        # Process passenger records
        if event["type"] == "passenger":
            flight_id = event["payload"]["FlightID"]

            # Fetch related flight details from SQL Server
            cursor.execute("SELECT ScheduledDeparture FROM flights WHERE FlightID = ?", flight_id)
            row = cursor.fetchone()

            if row and row[0]:
                sched_dep = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
                sched_dep = sched_dep.replace(tzinfo=timezone.utc)

                # Compare with current UTC time
                time_to_departure = (sched_dep - datetime.now(timezone.utc)).total_seconds() / 60

                # Trigger alert if departure is within 60 minutes
                if 0 <= time_to_departure <= 60:
                    has_bp = event["payload"].get("BoardingPassIssued", False)

                    alert_msg = (
                        f"Passenger {event['payload']['PassengerID']} has boarding pass, "
                        f"but gate closes in {round(time_to_departure, 2)} mins"
                        if has_bp else
                        f"Passenger {event['payload']['PassengerID']} has NOT claimed boarding pass, "
                        f"and gate closes in {round(time_to_departure, 2)} mins"
                    )

                    cursor.execute(
                        """
                        INSERT INTO alerts_passenger_delay
                        (AlertType, FlightID, PassengerID, BoardingPass, TimeToDepartureMin, Message)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        "boarding_warning",
                        flight_id,
                        event["payload"]["PassengerID"],
                        1 if has_bp else 0,
                        round(time_to_departure, 2),
                        alert_msg
                    )
                    conn.commit()

                    print(f"🚨 Passenger Boarding Alert Triggered: {alert_msg}")
                else:
                    print(f"ℹ️ Passenger {event['payload']['PassengerID']} not in 60 min window.")
            else:
                print(f"⚠ Flight not found for Passenger {event['payload']['PassengerID']}")

except KeyboardInterrupt:
    print("\n🛑 Passenger Boarding Alerts Consumer stopped.")
finally:
    consumer.close()
    cursor.close()
    conn.close()
    print("✅ Kafka and SQL Server connections closed.")
