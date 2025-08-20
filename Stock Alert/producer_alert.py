import json
import time
from kafka import KafkaProducer
import yfinance as yf

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'stock_topic'
STOCK_SYMBOL = 'TCS.NS'  # TCS NSE symbol

# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("✅ Kafka Producer started. Streaming TCS stock prices... Press Ctrl+C to stop.")

try:
    while True:
        ticker = yf.Ticker(STOCK_SYMBOL)
        data = ticker.history(period="1d", interval="1m")  # 1-minute data
        if not data.empty:
            latest_data = data.tail(1).iloc[0]
            stock_info = {
                "symbol": STOCK_SYMBOL,
                "timestamp": latest_data.name.strftime('%Y-%m-%d %H:%M:%S'),
                "price": round(latest_data['Close'], 2)
            }
            producer.send(TOPIC, stock_info)
            print(f"📤 Sent to Kafka: {stock_info}")
        else:
            print("⚠ No stock data fetched.")
        
        time.sleep(60)  # Fetch every 1 minute

except KeyboardInterrupt:
    print("\n🛑 Producer stopped by user.")
finally:
    producer.close()
