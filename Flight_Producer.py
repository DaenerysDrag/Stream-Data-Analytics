import time
import json
import requests
from kafka import KafkaProducer, errors

# --- Configuration ---
API_KEY = '47fd3566d9e2432fed163527db935eb0'  # ✅ Your API key
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'flight-topic'
FETCH_INTERVAL = 60  # Free plan allows 1 call/min

# --- Kafka Producer Setup ---
print("Connecting to Kafka broker...")
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5,
        acks='all'
    )
    producer.partitions_for(TOPIC)
    print(f"✅ Connected to Kafka broker and topic '{TOPIC}'")
except errors.NoBrokersAvailable:
    print("❌ Kafka broker unavailable. Make sure Kafka is running.")
    exit(1)

# --- Build AviationStack API URL ---
def build_url(api_key):
    return f"http://api.aviationstack.com/v1/flights?access_key={api_key}&limit=5"

# --- Fetch and Stream Flight Data ---
print(f"\n📡 Starting flight status stream every {FETCH_INTERVAL} seconds...")

try:
    while True:
        try:
            url = build_url(API_KEY)
            response = requests.get(url)
            data = response.json()

            if 'data' not in data:
                print("⚠ No flight data returned. Waiting for next attempt...")
                print("🔴 Raw response:", data)
                time.sleep(FETCH_INTERVAL)
                continue

            flights = data['data']

            for flight in flights:
                record = {
                    'airline': flight.get('airline', {}).get('name'),
                    'flight_number': flight.get('flight', {}).get('iata'),
                    'departure_airport': flight.get('departure', {}).get('airport'),
                    'departure_scheduled': flight.get('departure', {}).get('scheduled'),
                    'arrival_airport': flight.get('arrival', {}).get('airport'),
                    'arrival_scheduled': flight.get('arrival', {}).get('scheduled'),
                    'status': flight.get('flight_status'),
                    'timestamp': time.time()
                }

                producer.send(TOPIC, value=record)
                producer.flush()
                print(f"✅ Produced: {record}")

        except Exception as e:
            print(f"❌ Error fetching or sending data: {e}")

        time.sleep(FETCH_INTERVAL)

except KeyboardInterrupt:
    print("\n🛑 Stream stopped by user.")
finally:
    producer.close()
    print("✅ Kafka producer closed.")
