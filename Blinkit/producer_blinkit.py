import json
import time
import pandas as pd
from kafka import KafkaProducer

# Kafka Config
KAFKA_BROKER = "localhost:9092"
TOPIC = "blinkit-stream"

# Input CSV files
FILES = {
    "order": "orders.csv",
    "rider": "riders.csv",
    "inventory": "inventory.csv"
}

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("✅ Blinkit Producer started. Streaming Orders, Riders & Inventory...")

try:
    for record_type, file_path in FILES.items():
        df = pd.read_csv(file_path)
        for _, row in df.iterrows():
            event = {
                "type": record_type,
                "payload": row.to_dict()
            }
            producer.send(TOPIC, value=event)
            print(f"📤 Sent {record_type}: {event}")
            time.sleep(0.3)  # simulate real-time
except KeyboardInterrupt:
    print("\n🛑 Producer stopped by user.")
finally:
    producer.flush()
    producer.close()
    print("✅ Producer finished streaming and closed.")
