import json
from kafka import KafkaConsumer
from pymongo import MongoClient

# --- Configuration ---
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'flight-topic'
GROUP_ID = 'flight-consumer-group'

# MongoDB setup
MONGO_URI = 'mongodb+srv://@cluster0.zjqxxsb.mongodb.net/'  
DB_NAME = 'flight_data'
COLLECTION_NAME = 'flight_status'

# --- Connect to MongoDB ---
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]
    print(f"✅ Connected to MongoDB. DB: '{DB_NAME}', Collection: '{COLLECTION_NAME}'")
except Exception as e:
    print(f"❌ Failed to connect to MongoDB: {e}")
    exit(1)

# --- Kafka Consumer Setup ---
print(f"🔄 Connecting to Kafka topic '{TOPIC}'...")
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print(f"✅ Listening for flight data on topic '{TOPIC}'...\n")

try:
    for message in consumer:
        record = message.value
        print(f"✈️ Inserting Flight Record: {record}")
        collection.insert_one(record)

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped by user.")
finally:
    consumer.close()
    mongo_client.close()
    print("✅ Kafka consumer and MongoDB connection closed.")

