import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Indian cities with coordinates
cities = {
    "Delhi": {"lat": 28.6139, "lon": 77.2090},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777},
    "Bangalore": {"lat": 12.9716, "lon": 77.5946},
    "Chennai": {"lat": 13.0827, "lon": 80.2707},
    "Kolkata": {"lat": 22.5726, "lon": 88.3639}
}

print("🔄 Sending real Indian weather data to Kafka...")

while True:
    for city, coords in cities.items():
        url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={coords['lat']}&longitude={coords['lon']}"
            f"&current_weather=true"
        )
        try:
            response = requests.get(url)
            data = response.json()
            weather = data.get("current_weather", {})
            payload = {
                "location": city,
                "temperature": weather.get("temperature"),
                "windspeed": weather.get("windspeed"),
                "timestamp": datetime.now().isoformat(timespec='minutes')
            }

            producer.send("test-topic", value=payload)
            print(f"✅ Sent: {payload}")
        except Exception as e:
            print(f"❌ Failed for {city}: {e}")

        time.sleep(2)  # delay to avoid hitting API rate limits

