import json
import time
import pandas as pd
from kafka import KafkaProducer

# --- Config ---
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'stock_topic'
CSV_FILE = 'tcs_dummy_data.csv'   # Path to your dummy data file
DELAY = 1  # seconds between sending messages

# --- Kafka Producer ---
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("✅ Kafka Producer started. Streaming dummy TCS stock data... Press Ctrl+C to stop.")

try:
    # Load the dummy CSV data
    df = pd.read_csv(CSV_FILE)

    for _, row in df.iterrows():
        stock_info = {
            "symbol": row['symbol'],
            "timestamp": row['timestamp'],
            "open": row['open'],
            "high": row['high'],
            "low": row['low'],
            "close": row['close'],
            "previous_close": row['previous_close'],
            "volume": int(row['volume'])
        }
        # Send to Kafka
        producer.send(TOPIC, stock_info)
        print(f"📤 Sent to Kafka: {stock_info}")
        time.sleep(DELAY)

except KeyboardInterrupt:
    print("\n🛑 Producer stopped by user.")
finally:
    producer.close()
