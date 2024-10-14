import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

# Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'sensor-data'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_sensor_data(sensor_id):
    """Simulate sensor data generation."""
    return {
        'timestamp': datetime.utcnow().isoformat(),
        'sensor_id': sensor_id,
        'value': random.uniform(20.0, 100.0)  # Example sensor value
    }

def main():
    sensor_ids = ['sensor_1', 'sensor_2', 'sensor_3']
    try:
        while True:
            for sensor_id in sensor_ids:
                data = generate_sensor_data(sensor_id)
                producer.send(TOPIC, value=data)
                print(f"Sent data: {data}")
            time.sleep(1)  # Send data every second
    except KeyboardInterrupt:
        print("Data producer stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
