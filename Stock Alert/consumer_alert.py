import json
import time
from kafka import KafkaConsumer
from pymongo import MongoClient, errors

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'stock_topic'
MONGO_URI = "mongodb+srv://@cluster0.zjqxxsb.mongodb.net/"

# Retry MongoDB connection until available
while True:
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        mongo_client.admin.command("ping")  # Check connection
        print("✅ Connected to MongoDB.")
        break
    except errors.ServerSelectionTimeoutError:
        print("⚠ MongoDB not available. Retrying in 5 seconds...")
        time.sleep(5)

# MongoDB setup
db = mongo_client["stock_db"]
stock_collection = db["tcs_prices"]
alert_collection = db["tcs_alerts"]

# Kafka Consumer setup
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("✅ Kafka Consumer started. Listening for messages... Press Ctrl+C to stop.")

last_price = None

try:
    for message in consumer:
        stock_data = message.value
        print(f"📥 Received: {stock_data}")

        try:
            # Save stock price to MongoDB
            stock_collection.insert_one(stock_data)

            # Check price change
            if last_price is not None:
                price_diff_percent = ((stock_data['price'] - last_price) / last_price) * 100

                if abs(price_diff_percent) > 0.2:  # Alert condition
                    alert = {
                        "symbol": stock_data['symbol'],
                        "timestamp": stock_data['timestamp'],
                        "price": stock_data['price'],
                        "price_change_percent": round(price_diff_percent, 2),
                        "alert": "Significant price change"
                    }
                    alert_collection.insert_one(alert)
                    print(f"🚨 ALERT: Price changed by {round(price_diff_percent, 2)}% — Saved to MongoDB.")

            last_price = stock_data['price']

        except errors.PyMongoError as e:
            print(f"❌ MongoDB Error: {e}")

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped by user.")
finally:
    consumer.close()
    mongo_client.close()
    print("✅ Kafka and MongoDB connections closed.")

