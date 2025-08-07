import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
API_KEY = '6f250dd18c104bde97c125911250108'  # Replace with your valid API key
BASE_URL = "https://api.weatherapi.com/v1/current.json"
CITY = "London"
KAFKA_TOPIC = "air_quality"
KAFKA_SERVERS = ['localhost:9092']

def create_producer():
    """Create and return Kafka producer with proper configuration"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,
            retry_backoff_ms=1000
        )
        logger.info("✅ Kafka producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"❌ Failed to create Kafka producer: {e}")
        return None

def fetch_weather_data():
    """Fetch weather data from API"""
    try:
        params = {
            "key": API_KEY,
            "q": CITY,
            "aqi": "yes"
        }
        
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()  # Raise exception for bad status codes
        
        data = response.json()
        
        # Check if response contains error
        if "error" in data:
            logger.error(f"❌ API Error: {data['error']['message']}")
            return None
            
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Network error: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON decode error: {e}")
        return None

def process_weather_data(data):
    """Process and structure weather data"""
    try:
        if not data or "location" not in data or "current" not in data:
            logger.warning("⚠️ Invalid data structure received")
            return None
            
        # Check if air quality data exists
        air_quality = data["current"].get("air_quality", {})
        
        processed_data = {
            "location": data["location"]["name"],
            "region": data["location"]["region"],
            "country": data["location"]["country"],
            "localtime": data["location"]["localtime"],
            "temp_c": data["current"]["temp_c"],
            "humidity": data["current"]["humidity"],
            "condition": data["current"]["condition"]["text"],
            "timestamp": datetime.now().isoformat(),
            # Air quality data (with defaults if not available)
            "co": air_quality.get("co", 0),
            "no2": air_quality.get("no2", 0),
            "o3": air_quality.get("o3", 0),
            "so2": air_quality.get("so2", 0),
            "pm2_5": air_quality.get("pm2_5", 0),
            "pm10": air_quality.get("pm10", 0)
        }
        
        return processed_data
        
    except KeyError as e:
        logger.error(f"❌ Missing key in data: {e}")
        return None

def main():
    """Main function to run the producer"""
    producer = create_producer()
    if not producer:
        logger.error("❌ Cannot start without Kafka producer")
        return
        
    logger.info(f"🚀 Starting weather data producer for {CITY}")
    
    try:
        while True:
            # Fetch data from API
            raw_data = fetch_weather_data()
            
            if raw_data:
                # Process the data
                processed_data = process_weather_data(raw_data)
                
                if processed_data:
                    # Send to Kafka
                    try:
                        future = producer.send(KAFKA_TOPIC, processed_data)
                        future.add_callback(lambda metadata: logger.info(f"✅ Message sent to {metadata.topic}:{metadata.partition}:{metadata.offset}"))
                        future.add_errback(lambda e: logger.error(f"❌ Failed to send message: {e}"))
                        
                        logger.info(f"📊 Sent data for {processed_data['location']} - Temp: {processed_data['temp_c']}°C, PM2.5: {processed_data['pm2_5']}")
                        
                    except Exception as e:
                        logger.error(f"❌ Error sending to Kafka: {e}")
                else:
                    logger.warning("⚠️ No valid data to send")
            else:
                logger.warning("⚠️ Failed to fetch data from API")
            
            # Wait before next fetch
            time.sleep(10)
            
    except KeyboardInterrupt:
        logger.info("🛑 Producer stopped by user")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("🔒 Producer closed")

if __name__ == "__main__":
    main()
