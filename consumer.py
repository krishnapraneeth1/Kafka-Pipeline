from kafka import KafkaConsumer, KafkaProducer
import json
import logging
import time
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka Consumer Configuration
consumer = KafkaConsumer(
    'user-login',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Dimension Table to track columns and their last update times
dimension_table = {}
dimension_update_threshold = 60  # Time threshold in seconds to keep a column in the dimension table

def update_dimension_table(data):
    current_time = int(time.time())
    for key in data.keys():
        dimension_table[key] = current_time
    
    # Remove columns not updated within the threshold time
    keys_to_remove = [key for key, last_update in dimension_table.items() if current_time - last_update > dimension_update_threshold]
    for key in keys_to_remove:
        del dimension_table[key]

def handle_missing_fields(data):
    current_time = int(time.time())
    for key in dimension_table.keys():
        if key not in data:
            data[key] = "Null"
    return data

# Processing Messages
def process_message(data):
    try:
        #if data has no key, it will raise a KeyError
        
      #  if 'device_id' not in data.keys():
        #    raise KeyError("No Data Received")
    # this is added to differentiate between predefined input data and the data that is being generated for additional testing
        
        # Update the dimension table with the current data keys
        update_dimension_table(data)
        
        # Handle missing fields by filling with "Null"
        data = handle_missing_fields(data)
        
        # Data Transformation: Convert timestamp to human-readable format
        if 'time_epoch' in data:
            data['time_epoch'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(data['time_epoch'])))
        
        # Aggregation: Example - Collecting session duration statistics
        if 'session_duration_sec' in data:
            session_duration = data['session_duration_sec']
            data['session_duration_minutes'] = session_duration / 60
        
        # Filtering: Example - Only process messages from Android devices
        if data.get('device_type') != 'android':
            return
        
        # Identify Interesting Insights: Example - Flag long session durations
        if session_duration > 3600:  # 1 hour
            data['flag'] = 'long_session'
        else:
            data['flag'] = 'normal_session'
        
        # Add processed time
        data['processed_time'] = int(time.time())
        
        # Send processed data to another topic
        producer.send('processed-user-login', value=data)
        logger.info(f"Processed data: {data}")
    except KeyError as e:
        logger.error(f"KeyError: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def consume_messages():
    try:
        for message in consumer:
            data = message.value
            process_message(data)
    except Exception as e:
        logger.error(f"Error in consumer loop: {e}")

if __name__ == '__main__':
    logger.info("Starting Kafka consumer...")
    consume_messages()
