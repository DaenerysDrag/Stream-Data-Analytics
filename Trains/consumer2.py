import json
import time
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient, errors

# Kafka Config
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "train-stream"
OUTPUT_TOPIC = "train-clean"

# MongoDB Config
MONGO_URI = "mongodb+srv://gauravverma0810:gaurav@cluster0.vx6kt.mongodb.net/"
DB_NAME = "train_ops"

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
trains_col = db["trains"]
passengers_col = db["passengers"]
luggage_col = db["luggage"]

# Kafka Consumer
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="train-db-loader",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("📥 DB Loader started: Consuming from train-stream, saving to MongoDB & re-publishing to train-clean...")

try:
    for msg in consumer:
        event = msg.value.copy()
        record_type = event.get("type")
        payload = event.get("payload")

        # Insert into Mongo
        if record_type == "train":
            trains_col.insert_one(payload)
            print(f"🚆 Train stored: {payload['TrainID']}")
        elif record_type == "passenger":
            passengers_col.insert_one(payload)
            print(f"🧍 Passenger stored: {payload['PassengerID']}")
        elif record_type == "luggage":
            luggage_col.insert_one(payload)
            print(f"🎒 Luggage stored: {payload['BagID']}")

        # Sanitize _id before republishing
        if "_id" in payload:
            payload["_id"] = str(payload["_id"])
        event["payload"] = payload

        # Republish into clean topic
        producer.send(OUTPUT_TOPIC, value=event)

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped by user.")
finally:
    consumer.close()
    producer.close()
    mongo_client.close()
    print("✅ Connections closed.")
