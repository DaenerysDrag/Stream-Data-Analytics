import json
import time
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient, errors

# Kafka Config
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "airport-stream"
OUTPUT_TOPIC = "airport-clean"

# MongoDB Config
MONGO_URI = ""
DB_NAME = "airport_ops"

# MongoDB Connection
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
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="db-loader-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("📥 DB Loader started: Consuming from airport-stream, saving to MongoDB & re-publishing to airport-clean...")

try:
    for msg in consumer:
        event = msg.value.copy()   # make a safe copy
        record_type = event.get("type")
        payload = event.get("payload")

        # Insert into Mongo
        if record_type == "flight":
            flights_col.insert_one(payload)
            print(f"✈️ Flight stored: {payload['FlightID']}")
        elif record_type == "passenger":
            passengers_col.insert_one(payload)
            print(f"🧳 Passenger stored: {payload['PassengerID']}")
        elif record_type == "baggage":
            baggage_col.insert_one(payload)
            print(f"🎒 Baggage stored: {payload['BagID']}")

        # --- sanitize before re-publish ---
        if "_id" in payload:
            payload["_id"] = str(payload["_id"])
        event["payload"] = payload

        # Republish into clean topic
        producer.send(OUTPUT_TOPIC, value=event)

except KeyboardInterrupt:
    print("\n🛑 DB Loader stopped.")
finally:
    consumer.close()
    producer.close()
    mongo_client.close()
    print("✅ Connections closed.")

