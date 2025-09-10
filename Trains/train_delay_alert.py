import json
import time
from kafka import KafkaConsumer
from pymongo import MongoClient, errors

# Kafka Config
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "train-clean"

# MongoDB Config
MONGO_URI = ""
DB_NAME = "train_ops"   # ✅ same DB as consumer

# MongoDB Connection
while True:
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        mongo_client.admin.command("ping")
        print("✅ Connected to MongoDB (train_ops).")
        break
    except errors.ServerSelectionTimeoutError:
        print("⚠ MongoDB not available. Retrying in 5 seconds...")
        time.sleep(5)

db = mongo_client[DB_NAME]
alerts_col = db["train_delay_alerts"]   # ✅ alerts in same DB but separate collection

# Kafka Consumer
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="train-delay-alerts",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("📥 Train Delay Alerts Consumer started...")

try:
    for msg in consumer:
        event = msg.value
        if event["type"] == "train":
            payload = event["payload"]
            delay = int(payload.get("DelayMinutes", 0))

            # Alert condition
            if delay >= 30:
                alert = {
                    "TrainID": payload["TrainID"],
                    "Operator": payload["Operator"],
                    "Departure": payload["Departure"],
                    "Destination": payload["Destination"],
                    "ScheduledDeparture": payload["ScheduledDeparture"],
                    "ActualDeparture": payload["ActualDeparture"],
                    "DelayMinutes": delay,
                    "message": f"🚨 Train {payload['TrainID']} ({payload['Operator']}) delayed {delay} minutes at {payload['Departure']}."
                }
                alerts_col.insert_one(alert)
                print(f"⚡ ALERT: {alert['message']}")
except KeyboardInterrupt:
    print("\n🛑 Train Delay Alerts Consumer stopped.")
finally:
    consumer.close()
    mongo_client.close()
    print("✅ Kafka and MongoDB connections closed.")

