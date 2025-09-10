import json
import time
import pandas as pd
from kafka import KafkaProducer

# Config
KAFKA_BROKER = "localhost:9092"
TOPIC = "train-stream"

# CSV Files
FILES = {
    "train": "trains.csv",
    "passenger": "passengers.csv",
    "luggage": "luggage.csv"
}

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("✅ Kafka Producer started. Streaming train data...")

try:
    for record_type, file_path in FILES.items():   # iterate each CSV once
        df = pd.read_csv(file_path)
        for _, row in df.iterrows():
            event = {
                "type": record_type,
                "payload": row.to_dict()
            }
            producer.send(TOPIC, value=event)
            print(f"📤 Sent {record_type}: {event}")
            time.sleep(0.5)  # simulate real-time stream
except KeyboardInterrupt:
    print("\n🛑 Producer stopped by user.")
finally:
    producer.flush()
    producer.close()
    print("✅ Producer finished streaming all CSVs and closed.")
