import json
import time
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from pymongo import MongoClient

# ------------------ CONFIG ------------------
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "blinkit-stream"
OUTPUT_TOPIC = "blinkit-clean"

MONGO_URI = ""
DB_NAME = "blinkit_ops"

# ------------------ CONNECT TO MONGO ------------------
while True:
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        mongo_client.admin.command("ping")
        print("✅ Connected to MongoDB.")
        break
    except Exception as e:
        print(f"⚠ MongoDB not available ({e}). Retrying in 5s...")
        time.sleep(5)

db = mongo_client[DB_NAME]
orders_col = db["orders"]
riders_col = db["riders"]
inventory_col = db["inventory"]
aggregate_col = db["aggregate_orders"]

# ------------------ KAFKA CONSUMER ------------------
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="blinkit-mongo-loader",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("📥 Consumer+Producer started. Listening to blinkit-stream and writing to MongoDB...")

# ------------------ AGGREGATE BUILDER ------------------
def update_aggregate(order_id):
    order = orders_col.find_one({"OrderID": order_id})
    if not order:
        return

    rider = riders_col.find_one({"RiderID": order.get("RiderID")})
    inventory = inventory_col.find_one({"ItemID": order.get("ItemID"), "StoreID": order.get("StoreID")})

    record = {
        "OrderID": order.get("OrderID"),
        "Order": order,
        "Rider": rider if rider else {},
        "Inventory": inventory if inventory else {},
        "last_updated": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    aggregate_col.update_one(
        {"OrderID": order_id},
        {"$set": record},
        upsert=True
    )
    print(f"📊 Aggregate updated for OrderID {order_id}")

# ------------------ PROCESS EVENTS ------------------
try:
    # Find how many messages producer actually wrote
    tp = TopicPartition(INPUT_TOPIC, 0)
    consumer.assign([tp])
    consumer.seek_to_beginning(tp)
    end_offset = consumer.end_offsets([tp])[tp]
    print(f"📊 Producer wrote {end_offset} messages. Will consume exactly this many.")

    count = 0
    for msg in consumer:
        event = msg.value
        record_type = event.get("type")
        payload = event.get("payload")

        try:
            if record_type == "order":
                orders_col.update_one({"OrderID": payload["OrderID"]}, {"$set": payload}, upsert=True)
                print(f"🛒 Order stored: {payload.get('OrderID')}")
                update_aggregate(payload["OrderID"])

            elif record_type == "rider":
                riders_col.update_one({"RiderID": payload["RiderID"]}, {"$set": payload}, upsert=True)
                print(f"🚴 Rider stored: {payload.get('RiderID')}")
                for order in orders_col.find({"RiderID": payload["RiderID"]}):
                    update_aggregate(order["OrderID"])

            elif record_type == "inventory":
                inventory_col.update_one(
                    {"ItemID": payload["ItemID"], "StoreID": payload["StoreID"]},
                    {"$set": payload},
                    upsert=True
                )
                print(f"📦 Inventory upserted: {payload.get('ItemID')} in Store {payload.get('StoreID')}")
                for order in orders_col.find({"ItemID": payload["ItemID"], "StoreID": payload["StoreID"]}):
                    update_aggregate(order["OrderID"])

            # ✅ Republish cleaned event to blinkit-clean
            producer.send(OUTPUT_TOPIC, value=event)
            print(f"🔁 Republished to {OUTPUT_TOPIC}: {event['type']}")

        except Exception as db_err:
            print(f"❌ MongoDB Insert/Update Error: {db_err}")

        count += 1
        if count >= end_offset:
            print("✅ Finished consuming all producer messages.")
            break

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped by user.")
finally:
    consumer.close()
    producer.close()
    mongo_client.close()
    print("✅ Kafka and MongoDB connections closed.")

