from kafka import KafkaConsumer
import json
import logging

logging.basicConfig(level=logging.INFO)

def safe_json_deserializer(x):
    try:
        return json.loads(x.decode('utf-8'))
    except Exception as e:
        logging.warning(f"Failed to deserialize message: {x}. Error: {e}")
        return None

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test-group-2',  # Changed group id here
    value_deserializer=safe_json_deserializer
)

logging.info("Kafka Consumer started. Listening to 'test-topic'...")

try:
    for message in consumer:
        if message.value is not None:
            print(f"Received message: {message.value}")
        else:
            logging.info("Skipped an empty or invalid message.")
except KeyboardInterrupt:
    pass
finally:
    logging.info("Kafka Consumer connection closed.")
    consumer.close()

