import json
import time
import threading
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient, errors

# Kafka Config
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "airport-stream"
OUTPUT_TOPIC = "airport-clean"

# MongoDB Config
MONGO_URI = "mongodb+srv://karanmakol1:DaenerysDrag@cluster0.zjqxxsb.mongodb.net/"
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
aggregate_col = db["aggregate_records"]

# Kafka Consumers
flight_consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id="airport-flight-group",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

passenger_consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id="airport-passenger-group",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

baggage_consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id="airport-baggage-group",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("📥 Airport DB Loader started (threaded, with aggregate builder)...")

# --- UPDATE AGGREGATE ---
def update_aggregate(flight_id):
    """Rebuild aggregated record for a given FlightID"""
    flight = flights_col.find_one({"FlightID": flight_id})
    passengers = list(passengers_col.find({"FlightID": flight_id}))
    baggage = list(baggage_col.find({"FlightID": flight_id}))

    if flight:
        record = {
            "flight": flight,
            "passengers": passengers,
            "baggage": baggage,
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        aggregate_col.update_one(
            {"flight.FlightID": flight_id},
            {"$set": record},
            upsert=True
        )
        print(f"📊 Aggregate updated for FlightID {flight_id}")

# --- PROCESS FLIGHTS ---
def process_flights():
    for msg in flight_consumer:
        event = msg.value
        if event.get("type") == "flight":
            payload = event["payload"]
            flights_col.insert_one(payload)
            print(f"✈️ Stored Flight: {payload['FlightID']}")

            update_aggregate(payload["FlightID"])

            if "_id" in payload: payload["_id"] = str(payload["_id"])
            producer.send(OUTPUT_TOPIC, value=event)

# --- PROCESS PASSENGERS ---
def process_passengers():
    for msg in passenger_consumer:
        event = msg.value
        if event.get("type") == "passenger":
            payload = event["payload"]
            passengers_col.insert_one(payload)
            print(f"🧳 Stored Passenger: {payload['PassengerID']}")

            update_aggregate(payload["FlightID"])

            if "_id" in payload: payload["_id"] = str(payload["_id"])
            producer.send(OUTPUT_TOPIC, value=event)

# --- PROCESS BAGGAGE ---
def process_baggage():
    for msg in baggage_consumer:
        event = msg.value
        if event.get("type") == "baggage":
            payload = event["payload"]
            baggage_col.insert_one(payload)
            print(f"🎒 Stored Baggage: {payload['BagID']}")

            update_aggregate(payload["FlightID"])

            if "_id" in payload: payload["_id"] = str(payload["_id"])
            producer.send(OUTPUT_TOPIC, value=event)

# --- RUN THREADS ---
t1 = threading.Thread(target=process_flights)
t2 = threading.Thread(target=process_passengers)
t3 = threading.Thread(target=process_baggage)

t1.start()
t2.start()
t3.start()

t1.join()
t2.join()
t3.join()
