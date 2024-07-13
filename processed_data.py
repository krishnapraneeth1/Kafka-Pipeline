# This code is to see the processed data from the Kafka topic 'processed-user-login'.


from kafka import KafkaConsumer
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka Consumer Configuration
consumer = KafkaConsumer(
    'processed-user-login',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='processed-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Deserialize JSON data like decoder from utf-8 format
)

def consume_processed_messages():
    try:
        for message in consumer:
            data = message.value
            logger.info(f"Consumed processed data: {data}")
    except Exception as e:
        logger.error(f"Error in consumer loop: {e}")

if __name__ == '__main__':
    logger.info("Starting Kafka consumer for processed messages...")
    consume_processed_messages()
