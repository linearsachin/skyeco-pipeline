import requests
import json
import os
import duckdb
from confluent_kafka import Producer
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError

load_dotenv()

# 1. Configurations
KAFKA_CONF = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_KEY'),
    'sasl.password': os.getenv('KAFKA_SECRET'),
    'client.id': 'skyeco-producer-v1'
}

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

# 2. Part A: Fetch from OpenSky and Send to Kafka
def fetch_and_send_to_kafka():
    print("📡 Fetching data from OpenSky...")
    producer = Producer(KAFKA_CONF)
    url = "https://opensky-network.org/api/states/all"
    
    try:
        response = requests.get(url, timeout=10)
        states = response.json().get('states', [])
        
        for s in states[:200]:
            payload = {
                "icao24": s[0],
                "callsign": s[1].strip() if s[1] else "N/A",
                "origin_country": s[2],
                "longitude": s[5],
                "latitude": s[6],
                "altitude": s[7],
                "velocity": s[9],
                "timestamp": s[3]
            }
            producer.produce(
                'flights_raw', 
                key=str(s[0]), 
                value=json.dumps(payload), 
                callback=delivery_report
            )
        
        producer.flush()
        print("🚀 Kafka Ingestion Complete.")
    except Exception as e:
        print(f"❌ Producer Error: {e}")

# 3. Part B: Sync Kafka Stream to MotherDuck (The Bridge)

def sync_kafka_to_motherduck():
    print("🦆 Syncing Kafka to MotherDuck using Python Bridge...")
    try:
        # 1. Setup Kafka Consumer
        consumer_conf = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP'),
            'group.id': 'skyeco-sync-group',
            'auto.offset.reset': 'earliest',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.getenv('KAFKA_KEY'),
            'sasl.password': os.getenv('KAFKA_SECRET')
        }
        consumer = Consumer(consumer_conf)
        consumer.subscribe(['flights_raw'])

        # 2. Connect to MotherDuck
        con = duckdb.connect(f"md:skyeco_dev")
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_flights (
                icao24 VARCHAR, callsign VARCHAR, origin_country VARCHAR,
                longitude DOUBLE, latitude DOUBLE, altitude DOUBLE,
                velocity DOUBLE, timestamp BIGINT
            );
        """)

        # 3. Consume messages and prepare for batch insert
        messages = []
        count = 0
        print("📥 Polling Kafka...")
        
        # We'll try to grab up to 100 messages in this sync
        while count < 100:
            msg = consumer.poll(1.0) # Timeout of 1s
            if msg is None: break
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                break
            
            data = json.loads(msg.value().decode('utf-8'))
            messages.append((
                data['icao24'], data['callsign'], data['origin_country'],
                data['longitude'], data['latitude'], data['altitude'],
                data['velocity'], data['timestamp']
            ))
            count += 1

        # 4. Batch Insert into MotherDuck
        if messages:
            con.executemany("INSERT INTO raw_flights VALUES (?, ?, ?, ?, ?, ?, ?, ?)", messages)
            print(f"✨ Successfully synced {len(messages)} records to MotherDuck.")
        else:
            print("ℹ️ No new messages found in Kafka.")

        consumer.close()

    except Exception as e:
        print(f"❌ Python Bridge Sync Error: {e}")


# 4. Main Execution Flow
if __name__ == "__main__":
    # Step 1: Put data into the stream
    fetch_and_send_to_kafka()
    
    # Step 2: Move data from the stream to the warehouse
    sync_kafka_to_motherduck()