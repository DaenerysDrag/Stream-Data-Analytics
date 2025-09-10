import json
import time
from kafka import KafkaConsumer
from pymongo import MongoClient, errors

# Kafka Config
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "airport-clean"

# MongoDB Config
MONGO_URI = "mongodb+srv://karanmakol1:DaenerysDrag@cluster0.zjqxxsb.mongodb.net/"
DB_NAME = "airport_ops"

# MongoDB Connection
while True:
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        mongo_client.admin.command("ping")
        print("✅ Connected to MongoDB (Flight Delay Alerts).")
        break
    except errors.ServerSelectionTimeoutError:
        print("⚠ MongoDB not available. Retrying in 5 seconds...")
        time.sleep(5)

db = mongo_client[DB_NAME]
alerts_col = db["alerts_flight_delay"]

# Kafka Consumer
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="flight-delay-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("📥 Flight Delay Alerts Consumer started...")

try:
    for msg in consumer:
        event = msg.value
        if event["type"] == "flight":
            delay = int(event["payload"].get("DelayMinutes", 0))
            if delay >= 30:
                alert = {
                    "alert_type": "flight_delay",
                    "flight_id": event["payload"]["FlightID"],
                    "delay_minutes": delay,
                    "message": f"Flight {event['payload']['FlightID']} delayed by {delay} minutes"
                }
                alerts_col.insert_one(alert)
                print(f"🚨 Flight Delay Alert Triggered: {alert}")
except KeyboardInterrupt:
    print("\n🛑 Flight Delay Alerts Consumer stopped.")
finally:
    consumer.close()
    mongo_client.close()
