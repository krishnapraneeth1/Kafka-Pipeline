import uuid
from kafka import KafkaProducer
import json
import time
import random

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Function to send data
def send_data(topic, data):
    producer.send(topic, value=data)
    producer.flush()  # Ensure all messages are sent
    print(f"Sent data: {data}")

if __name__ == '__main__':
    while True:
        # Example data
        data = {
            'user_id': str(uuid.uuid4()),
            'user_name': random.choice(['Ravi', 'Kathey', 'Mark', 'Pradeep', 'Prnaeeth']),
            'app_version': f'{random.randint(1, 3)}.{random.randint(0, 9)}.{random.randint(0, 9)}',
            'device_type': random.choice(['android', 'ios']),
            'ip': f'{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}',
            'locale': random.choice(['RU', 'EN', 'ES', 'FR', 'DE']),
            'device_id': f'{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}',
            'time_epoch': str(int(time.time())),
            'session_duration_sec': random.randint(60, 3600),  # in seconds
            'location': random.choice(['Moscow', 'New York', 'London', 'Berlin', 'Tokyo']),
            'network_type': random.choice(['WiFi', '4G', '5G']),
            'app_activity': random.choice(['login', 'logout', 'purchase', 'browse', 'search']),
            'battery_level': random.randint(1, 100),  # in percentage
            'screen_resolution': random.choice(['1080x1920', '720x1280', '1440x2560']),
            'os_version': random.choice(['Android 9', 'Android 10', 'iOS 13', 'iOS 14']),
            'carrier': random.choice(['AT&T', 'Verizon', 'T-Mobile', 'Sprint']),
            'app_usage_type': random.choice(['foreground', 'background']),
            'start_time': str(int(time.time()) - random.randint(1, 3600))  # in epoch time
        }
        
        # Send data to 'user-login' topic
        send_data('user-login', data)
        
        # Sleep for a while before sending the next message
        #time.sleep(5)  # Adjust the sleep time as needed
