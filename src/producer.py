import requests
import json
import os
import duckdb
from confluent_kafka import Producer, Consumer, KafkaError
from dotenv import load_dotenv

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
WEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')

def get_weather(lat, lon):
    if not WEATHER_API_KEY or lat is None or lon is None:
        return {"temp": None, "wind_speed": None, "weather_desc": "No Data"}
    try:
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={WEATHER_API_KEY}&units=metric"
        response = requests.get(url, timeout=5).json()
        return {
            "temp": response['main']['temp'],
            "wind_speed": response['wind']['speed'],
            "weather_desc": response['weather'][0]['description']
        }
    except Exception:
        return {"temp": None, "wind_speed": None, "weather_desc": "Error"}

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        pass # Keep logs clean during high volume

# 2. Part A: Fetch from OpenSky, Enrich with Weather, and Send to Kafka
def fetch_and_send_to_kafka():
    print("📡 Fetching data from OpenSky and enriching with Weather...")
    producer = Producer(KAFKA_CONF)
    url = "https://opensky-network.org/api/states/all"
    
    try:
        response = requests.get(url, timeout=10)
        states = response.json().get('states', [])
        
        # Taking top 50 to avoid hitting OpenWeather rate limits too hard
        for s in states[:50]:
            lat, lon = s[6], s[5]
            
            # --- NEW: Weather Enrichment ---
            weather = get_weather(lat, lon)
            payload = {
                "icao24": s[0],
                "callsign": s[1].strip() if s[1] else "N/A",
                "origin_country": s[2],
                "longitude": lon,
                "latitude": lat,
                "altitude": s[7],
                "velocity": s[9],
                "timestamp": s[3],
                "temp": weather['temp'],           # Storing as 'temp'
                "wind_speed": weather['wind_speed'], # Storing as 'wind_speed'
                "weather_desc": weather['weather_desc']
            }
            
            producer.produce(
                'flights_raw', 
                key=str(s[0]), 
                value=json.dumps(payload), 
                callback=delivery_report
            )
        
        producer.flush()
        print(f"🚀 Kafka Ingestion Complete (Enriched {min(len(states), 50)} flights).")
    except Exception as e:
        print(f"❌ Producer Error: {e}")

# 3. Part B: Sync Kafka Stream to MotherDuck (The Bridge)
def sync_kafka_to_motherduck():
    print("🦆 Syncing Enriched Data to MotherDuck...")
    try:
        consumer_conf = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP'),
            'group.id': 'skyeco-weather-final-001',
            'auto.offset.reset': 'earliest',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.getenv('KAFKA_KEY'),
            'sasl.password': os.getenv('KAFKA_SECRET')
        }
        consumer = Consumer(consumer_conf)
        consumer.subscribe(['flights_raw'])

        con = duckdb.connect(f"md:skyeco_dev")
        
        # --- Updated Table Schema with Weather Columns ---
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_flights (
                icao24 VARCHAR, callsign VARCHAR, origin_country VARCHAR,
                longitude DOUBLE, latitude DOUBLE, altitude DOUBLE,
                velocity DOUBLE, timestamp BIGINT,
                temp DOUBLE, wind_speed DOUBLE, weather_desc VARCHAR
            );
        """)

        messages = []
        count = 0
        max_retries = 10
        while count < 100 and max_retries > 0:
            msg = consumer.poll(2.0) # Increase timeout to 2 seconds
            if msg is None: 
                max_retries -= 1
                continue
            if msg is None: break            
            data = json.loads(msg.value().decode('utf-8'))
            messages.append((
                data.get('icao24'), data.get('callsign'), data.get('origin_country'),
                data.get('longitude'), data.get('latitude'), data.get('altitude'),
                data.get('velocity'), data.get('timestamp'),
                data.get('temp'), data.get('wind_speed'), data.get('weather_desc')
            ))
            print(data)
            count += 1

        if messages:
            # Clean up the duplicated line here:
            con.executemany("INSERT INTO raw_flights VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", messages)
            print(f"✨ Successfully synced {len(messages)} enriched records to MotherDuck.")
        else:
            print("ℹ️ No new messages found in Kafka.")

        consumer.close()
    except Exception as e:
        print(f"❌ Python Bridge Sync Error: {e}")

if __name__ == "__main__":
    fetch_and_send_to_kafka()
    sync_kafka_to_motherduck()