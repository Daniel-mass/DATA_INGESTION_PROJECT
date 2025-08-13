import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime

# Connect to Kafka container via host port
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # ✅ localhost because you're running script from host
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Indian cities with coordinates
cities = {
    "Delhi": {"lat": 28.6139, "lon": 77.2090},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777},
    "Bangalore": {"lat": 12.9716, "lon": 77.5946},
    "Chennai": {"lat": 13.0827, "lon": 80.2707},
    "Kolkata": {"lat": 22.5726, "lon": 88.3639},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867},
    "Ahmedabad": {"lat": 23.0225, "lon": 72.5714},
    "Pune": {"lat": 18.5204, "lon": 73.8567},
    "Jaipur": {"lat": 26.9124, "lon": 75.7873},
    "Lucknow": {"lat": 26.8467, "lon": 80.9462},
    "Kanpur": {"lat": 26.4499, "lon": 80.3319},
    "Nagpur": {"lat": 21.1458, "lon": 79.0882},
    "Indore": {"lat": 22.7196, "lon": 75.8577},
    "Surat": {"lat": 21.1702, "lon": 72.8311},
    "Bhopal": {"lat": 23.2599, "lon": 77.4126},
    "Patna": {"lat": 25.5941, "lon": 85.1376},
    "Ludhiana": {"lat": 30.9010, "lon": 75.8573},
    "Agra": {"lat": 27.1767, "lon": 78.0078},
    "Varanasi": {"lat": 25.3176, "lon": 82.9739},
    "Madurai": {"lat": 9.9252, "lon": 78.1198},
    "Coimbatore": {"lat": 11.0168, "lon": 76.9558},
    "Visakhapatnam": {"lat": 17.6868, "lon": 83.2185},
    "Ranchi": {"lat": 23.3441, "lon": 85.3096},
    "Bhubaneswar": {"lat": 20.2961, "lon": 85.8245},
    "Guwahati": {"lat": 26.1445, "lon": 91.7362}
}

print("🔄 Sending real Indian weather data to Kafka...")

while True:
    for city, coords in cities.items():
        url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={coords['lat']}&longitude={coords['lon']}&current_weather=true"
        )
        try:
            response = requests.get(url, timeout=5)
            data = response.json()
            weather = data.get("current_weather", {})
            temperature = weather.get("temperature")
            windspeed = weather.get("windspeed")
            temperature_f = temperature * 9/5 + 32 if temperature is not None else None

            payload = {
                "location": city,
                "temperature": temperature,
                "windspeed": windspeed,
                "temperature_f": temperature_f,
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            producer.send("test-topic", value=payload)
            print(f"✅ Sent: {payload}")
            producer.flush()

        except Exception as e:
            print(f"❌ Failed for {city}: {e}")

        time.sleep(1)
