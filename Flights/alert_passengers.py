import json
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer
from pymongo import MongoClient, errors

# Kafka Config
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "airport-clean"

# MongoDB Config
MONGO_URI = ""
DB_NAME = "airport_ops"

# MongoDB Connection
while True:
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        mongo_client.admin.command("ping")
        print("✅ Connected to MongoDB (Passenger Alerts).")
        break
    except errors.ServerSelectionTimeoutError:
        print("⚠ MongoDB not available. Retrying in 5 seconds...")
        time.sleep(5)

db = mongo_client[DB_NAME]
alerts_col = db["alerts_passenger_delay"]

# Kafka Consumer
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="passenger-alert-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("📥 Passenger Boarding Alerts Consumer started...")

try:
    for msg in consumer:
        event = msg.value

        # Process all passenger records (boarding pass or not)
        if event["type"] == "passenger":

            # Fetch related flight details
            flight = db["flights"].find_one({"FlightID": event["payload"]["FlightID"]})

            if flight and "ScheduledDeparture" in flight:
                sched_dep = datetime.strptime(flight["ScheduledDeparture"], "%Y-%m-%d %H:%M:%S")
                sched_dep = sched_dep.replace(tzinfo=timezone.utc)

                # Compare with current UTC time
                time_to_departure = (sched_dep - datetime.now(timezone.utc)).total_seconds() / 60

                # Trigger alert if departure is within 60 minutes
                if 0 <= time_to_departure <= 60:
                    has_bp = event["payload"].get("BoardingPassIssued", False)

                    alert = {
                        "alert_type": "boarding_warning",
                        "flight_id": event["payload"]["FlightID"],
                        "passenger_id": event["payload"]["PassengerID"],
                        "boarding_pass": has_bp,
                        "time_to_departure_min": round(time_to_departure, 2),
                        "message": (
                            f"Passenger {event['payload']['PassengerID']} has boarding pass, "
                            f"but gate closes in {round(time_to_departure, 2)} mins"
                            if has_bp else
                            f"Passenger {event['payload']['PassengerID']} has NOT claimed boarding pass, "
                            f"and gate closes in {round(time_to_departure, 2)} mins"
                        )
                    }

                    alerts_col.insert_one(alert)
                    print(f"🚨 Passenger Boarding Alert Triggered: {alert}")
                else:
                    print(f"ℹ️ Passenger {event['payload']['PassengerID']} not in 60 min window.")
            else:
                print(f"⚠ Flight not found for Passenger {event['payload']['PassengerID']}")

except KeyboardInterrupt:
    print("\n🛑 Passenger Boarding Alerts Consumer stopped.")
finally:
    consumer.close()
    mongo_client.close()
    print("✅ Kafka and MongoDB connections closed.")

