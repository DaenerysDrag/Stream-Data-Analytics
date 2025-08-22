import json
import time
from kafka import KafkaConsumer
import mysql.connector
from mysql.connector import Error

# --- Config ---
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'stock_topic'

# ✅ Your MySQL credentials
MYSQL_HOST = "127.0.0.1"   # use 127.0.0.1 instead of localhost (avoids socket issues on Windows)
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "<SQL Password>"
MYSQL_DB = "stock_db"

# --- Connect to MySQL ---
def create_connection():
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        if connection.is_connected():
            print("✅ Connected to MySQL database")
            return connection
    except Error as e:
        print(f"❌ MySQL connection error: {e}")
        return None

connection = create_connection()
if connection is None:
    print("❌ Could not establish MySQL connection. Exiting.")
    exit(1)

cursor = connection.cursor()

# --- Create tables if not exist ---
cursor.execute("""
CREATE TABLE IF NOT EXISTS tcs_prices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20),
    timestamp VARCHAR(50),
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    previous_close FLOAT,
    volume INT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS tcs_alerts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20),
    timestamp VARCHAR(50),
    close FLOAT,
    previous_close FLOAT,
    price_change_percent FLOAT,
    alert_message VARCHAR(255)
)
""")
connection.commit()

# --- Kafka Consumer ---
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("✅ Kafka Consumer started. Inserting into MySQL...")

last_close = None

try:
    for message in consumer:
        stock_data = message.value
        print(f"📥 Received: {stock_data}")

        try:
            # Insert into tcs_prices
            cursor.execute("""
                INSERT INTO tcs_prices (symbol, timestamp, open, high, low, close, previous_close, volume)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                stock_data['symbol'],
                stock_data['timestamp'],
                stock_data['open'],
                stock_data['high'],
                stock_data['low'],
                stock_data['close'],
                stock_data['previous_close'],
                stock_data['volume']
            ))
            connection.commit()

            # Check for alert condition
            current_close = float(stock_data.get("close", 0))

            if last_close is not None and last_close > 0:
                price_diff_percent = ((current_close - last_close) / last_close) * 100
                if abs(price_diff_percent) > 0.2:
                    alert_msg = "Significant price change (>0.2%)"
                    cursor.execute("""
                        INSERT INTO tcs_alerts (symbol, timestamp, close, previous_close, price_change_percent, alert_message)
                        VALUES (%s,%s,%s,%s,%s,%s)
                    """, (
                        stock_data['symbol'],
                        stock_data['timestamp'],
                        current_close,
                        last_close,
                        round(price_diff_percent, 2),
                        alert_msg
                    ))
                    connection.commit()
                    print(f"🚨 ALERT Inserted: {stock_data['symbol']} changed {round(price_diff_percent, 2)}%")

            last_close = current_close

        except Error as e:
            print(f"❌ MySQL Error: {e}")
            connection.rollback()

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped by user.")
finally:
    consumer.close()
    cursor.close()
    connection.close()
    print("✅ Kafka and MySQL connections closed.")

