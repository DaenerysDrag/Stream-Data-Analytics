import json
import time
import threading
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient, errors

# Kafka Config
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "train-clean"
OUTPUT_TOPIC = "train-aggregate-clean"

# MongoDB Config
MONGO_URI = ""
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
aggregate_col = db["aggregate_records"]

# Kafka Consumers
train_consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id="train-group",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

passenger_consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id="passenger-group",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

luggage_consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id="luggage-group",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

# Kafka Producer (for republishing aggregate events if needed)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("📥 Train DB Loader with Aggregate Builder started...")

# --- UPDATE AGGREGATE ---
def update_aggregate(train_id):
    """Rebuild aggregated record for a given TrainID"""
    train = trains_col.find_one({"TrainID": train_id})
    passengers = list(passengers_col.find({"TrainID": train_id}))
    luggage = list(luggage_col.find({"TrainID": train_id}))

    if train:
        record = {
            "train": train,
            "passengers": passengers,
            "luggage": luggage,
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        aggregate_col.update_one(
            {"train.TrainID": train_id},
            {"$set": record},
            upsert=True
        )
        print(f"📊 Aggregate updated for TrainID {train_id}")

        # Republish the aggregate record
        producer.send(OUTPUT_TOPIC, value=record)

# --- PROCESS TRAINS ---
def process_trains():
    for msg in train_consumer:
        event = msg.value
        if event.get("type") == "train":
            payload = event["payload"]
            trains_col.insert_one(payload)
            print(f"🚆 Stored Train: {payload['TrainID']}")
            update_aggregate(payload["TrainID"])

# --- PROCESS PASSENGERS ---
def process_passengers():
    for msg in passenger_consumer:
        event = msg.value
        if event.get("type") == "passenger":
            payload = event["payload"]
            passengers_col.insert_one(payload)
            print(f"🧍 Stored Passenger: {payload['PassengerID']}")
            update_aggregate(payload["TrainID"])

# --- PROCESS LUGGAGE ---
def process_luggage():
    for msg in luggage_consumer:
        event = msg.value
        if event.get("type") == "luggage":
            payload = event["payload"]
            luggage_col.insert_one(payload)
            print(f"🎒 Stored Luggage: {payload['BagID']}")
            update_aggregate(payload["TrainID"])

# --- RUN THREADS ---
t1 = threading.Thread(target=process_trains)
t2 = threading.Thread(target=process_passengers)
t3 = threading.Thread(target=process_luggage)

t1.start()
t2.start()
t3.start()

t1.join()
t2.join()
t3.join()

