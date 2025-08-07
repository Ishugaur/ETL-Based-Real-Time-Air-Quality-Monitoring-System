from kafka import KafkaConsumer
from hdfs import InsecureClient
import json
import logging
from datetime import datetime
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
KAFKA_TOPIC = "air_quality"
KAFKA_SERVERS = ['localhost:9092']
HDFS_URL = 'http://localhost:9870'
HDFS_USER = 'sunbeam'
HDFS_BASE_PATH = '/user/sunbeam/air_quality'

def create_hdfs_client():
    """Create and test HDFS client connection"""
    try:
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        
        # Test connection by listing root directory
        client.list('/')
        logger.info("✅ HDFS client connected successfully")
        
        # Ensure base directory exists
        try:
            client.makedirs(HDFS_BASE_PATH)
            logger.info(f"📁 Created/verified HDFS directory: {HDFS_BASE_PATH}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"📁 HDFS directory already exists: {HDFS_BASE_PATH}")
            else:
                raise e
                
        return client
        
    except Exception as e:
        logger.error(f"❌ HDFS connection failed: {e}")
        return None

def create_kafka_consumer():
    """Create and return Kafka consumer"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVERS,
            auto_offset_reset='earliest',  # Start from beginning to process existing messages
            enable_auto_commit=True,
            group_id='hdfs_consumer_group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            # Removed consumer_timeout_ms to allow indefinite waiting for messages
        )
        logger.info("✅ Kafka consumer created successfully")
        return consumer
        
    except Exception as e:
        logger.error(f"❌ Failed to create Kafka consumer: {e}")
        return None

def save_to_hdfs(hdfs_client, data):
    """Save data to HDFS with proper file naming"""
    try:
        # Create filename with timestamp and location
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        location = data.get('location', 'unknown').replace(' ', '_')
        filename = f"air_quality_{location}_{timestamp}.json"
        file_path = f"{HDFS_BASE_PATH}/{filename}"
        
        # Convert data to JSON string
        json_data = json.dumps(data, indent=2)
        
        # Write to HDFS
        with hdfs_client.write(file_path, encoding='utf-8') as writer:
            writer.write(json_data)
            
        logger.info(f"✅ Saved to HDFS: {filename}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to save to HDFS: {e}")
        return False

def process_message(hdfs_client, message):
    """Process a single Kafka message"""
    try:
        data = message.value
        
        # Validate data structure - check for essential fields
        required_fields = ['location']
        if not all(field in data for field in required_fields):
            logger.warning(f"⚠️ Message missing required fields: {data}")
            return False
            
        # Add processing metadata
        data['processed_timestamp'] = datetime.now().isoformat()
        data['kafka_offset'] = message.offset
        data['kafka_partition'] = message.partition
        
        # Save to HDFS
        success = save_to_hdfs(hdfs_client, data)
        
        if success:
            # Log with available fields
            temp_info = f" - Temp: {data.get('temp_c', 'N/A')}°C" if 'temp_c' in data else ""
            time_info = f" at {data.get('localtime', data.get('timestamp', 'N/A'))}"
            logger.info(f"📊 Processed: {data['location']}{temp_info}{time_info}")
            
        return success
        
    except Exception as e:
        logger.error(f"❌ Error processing message: {e}")
        return False

def main():
    """Main function to run the consumer"""
    logger.info("🚀 Starting Kafka to HDFS consumer")
    
    # Initialize HDFS client
    hdfs_client = create_hdfs_client()
    if not hdfs_client:
        logger.error("❌ Cannot start without HDFS connection")
        return
        
    # Initialize Kafka consumer
    consumer = create_kafka_consumer()
    if not consumer:
        logger.error("❌ Cannot start without Kafka consumer")
        return
    
    processed_count = 0
    error_count = 0
    
    try:
        logger.info(f"👂 Starting to consume messages from topic: {KAFKA_TOPIC}")
        
        # Use polling approach for better control
        while True:
            try:
                # Poll for messages with a reasonable timeout
                message_batch = consumer.poll(timeout_ms=5000)  # 5 second timeout
                
                if not message_batch:
                    logger.info("⏰ No new messages, continuing to poll...")
                    continue
                
                # Process all messages in the batch
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            success = process_message(hdfs_client, message)
                            
                            if success:
                                processed_count += 1
                            else:
                                error_count += 1
                                
                            # Log statistics every 10 messages
                            if (processed_count + error_count) % 10 == 0:
                                logger.info(f"📈 Statistics - Processed: {processed_count}, Errors: {error_count}")
                                
                        except Exception as e:
                            error_count += 1
                            logger.error(f"❌ Error processing individual message: {e}")
                
                # Commit offsets after processing batch
                consumer.commit()
                
            except Exception as e:
                logger.error(f"❌ Error in polling loop: {e}")
                time.sleep(1)  # Brief pause before retrying
                
    except KeyboardInterrupt:
        logger.info("🛑 Consumer stopped by user")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
    finally:
        # Cleanup
        if consumer:
            consumer.close()
            logger.info("🔒 Kafka consumer closed")
            
        logger.info(f"📊 Final Statistics - Processed: {processed_count}, Errors: {error_count}")

if __name__ == "__main__":
    main()
