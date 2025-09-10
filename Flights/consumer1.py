import json
import time
from kafka import KafkaConsumer
from pymongo import MongoClient, errors

# Kafka Config
KAFKA_BROKER = "localhost:9092"
TOPIC = "airport-stream"

# MongoDB Config
MONGO_URI = ""   
DB_NAME = "airport_ops"

# Connect to MongoDB
while True:
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        mongo_client.admin.command("ping")
        print("✅ Connected to MongoDB.")
        break
    except errors.ServerSelectionTimeoutError:
        print("⚠ MongoDB not available. Retrying in 5 seconds...")
        time.sleep(5)

db = mongo_client[DB_NAME]
flights_col = db["flights"]
passengers_col = db["passengers"]
baggage_col = db["baggage"]

# Kafka Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="airport-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("📥 Kafka Consumer started. Inserting into MongoDB... Press Ctrl+C to stop.")

try:
    for msg in consumer:
        event = msg.value
        record_type = event.get("type")
        payload = event.get("payload")

        if record_type == "flight":
            flights_col.insert_one(payload)
            print(f"✈️ Flight inserted: {payload['FlightID']}")
        elif record_type == "passenger":
            passengers_col.insert_one(payload)
            print(f"🧳 Passenger inserted: {payload['PassengerID']}")
        elif record_type == "baggage":
            baggage_col.insert_one(payload)
            print(f"🎒 Baggage inserted: {payload['BagID']}")

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped by user.")
finally:
    consumer.close()
    mongo_client.close()
    print("✅ Kafka and MongoDB connections closed.")

