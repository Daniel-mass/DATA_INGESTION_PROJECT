from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=['kafka:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='weather-consumer-group'
)

print("📥 Listening to real-time weather data from Kafka...")

for message in consumer:
    weather_data = message.value
    print(f"🌦️ Received: {weather_data}")
