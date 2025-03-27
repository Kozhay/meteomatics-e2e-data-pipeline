import time
from geopy.exc import GeocoderUnavailable
from geopy.geocoders import Nominatim

def safe_geocode(city_name: str, retries: int = 3):
    geolocator = Nominatim(user_agent="weather-pipeline", timeout=5)
    for attempt in range(retries):
        try:
            return geolocator.geocode(city_name)
        except GeocoderUnavailable:
            print(f"⏳ Retry {attempt+1}/{retries} for {city_name}")
            time.sleep(2)
    raise Exception(f"❌ Geocoding failed after {retries} attempts: {city_name}")