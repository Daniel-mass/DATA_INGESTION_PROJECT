import requests
from datetime import datetime
import time

cities = {
    "New York": {"lat": 40.7128, "lon": -74.0060},
    "London": {"lat": 51.5074, "lon": -0.1278}
}

while True:
    for city, coords in cities.items():
        url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={coords['lat']}&longitude={coords['lon']}&current_weather=true"
        )
        try:
            response = requests.get(url, timeout=5)
            data = response.json()  # Full API response
            
            print(f"\n Full Weather Data for {city} at {datetime.now()} ===")
            print(data)  # Prints everything returned by the API
        
        except Exception as e:
            print(f"Error fetching data for {city}: {e}")

    time.sleep(60)  # Wait 1 min before next request
