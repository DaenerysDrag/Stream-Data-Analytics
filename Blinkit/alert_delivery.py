import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from pymongo import MongoClient, errors

# ------------------ CONFIG ------------------
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "blinkit-clean"

MONGO_URI = "mongodb+srv://gauravverma0810:gaurav@cluster0.vx6kt.mongodb.net/"
DB_NAME = "blinkit_ops"

# ------------------ CONNECT TO MONGO ------------------
while True:
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        mongo_client.admin.command("ping")
        print("✅ Connected to MongoDB (Delivery Alerts).")
        break
    except errors.ServerSelectionTimeoutError as e:
        print(f"⚠ MongoDB not available: {e}. Retrying in 5s...")
        time.sleep(5)

db = mongo_client[DB_NAME]
alerts_col = db["alerts_delivery"]

# ------------------ KAFKA CONSUMER ------------------
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="blinkit-delivery-alerts",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("📥 Blinkit Delivery Delay Alerts Consumer started (MongoDB)...")

# ------------------ PROCESS EVENTS ------------------
try:
    for msg in consumer:
        event = msg.value
        if event.get("type") == "order":
            payload = event["payload"]

            exp = payload.get("ExpectedDeliveryTime")
            act = payload.get("ActualDeliveryTime")
            location = payload.get("DeliveryLocation", "Unknown")

            if exp and act:
                try:
                    exp_dt = datetime.strptime(exp, "%Y-%m-%d %H:%M:%S")
                    act_dt = datetime.strptime(act, "%Y-%m-%d %H:%M:%S")
                except ValueError as t_err:
                    print(f"⚠ Time parse error: {t_err}")
                    continue

                delay = (act_dt - exp_dt).total_seconds() / 60
                if delay >= 20:  # Trigger Alert
                    message = f"🚨 Order {payload.get('OrderID')} delayed by {int(delay)} mins in {location}."

                    # ✅ Upsert instead of insert_one to prevent duplicates
                    alerts_col.update_one(
                        {"OrderID": payload.get("OrderID"), "AlertType": "DeliveryDelay"},
                        {"$set": {
                            "AlertType": "DeliveryDelay",
                            "OrderID": payload.get("OrderID"),
                            "RiderID": payload.get("RiderID"),
                            "StoreID": payload.get("StoreID"),
                            "ItemID": payload.get("ItemID"),
                            "Location": location,
                            "Message": message,
                            "CreatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }},
                        upsert=True
                    )

                    print(message)

except KeyboardInterrupt:
    print("\n🛑 Delivery Alerts Consumer stopped.")
finally:
    consumer.close()
    mongo_client.close()
    print("✅ Kafka and MongoDB connections closed.")
