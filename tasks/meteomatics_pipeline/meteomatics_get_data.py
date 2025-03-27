import logging
import yaml
import requests
from airflow.hooks.base import BaseHook
from tasks.meteomatics_pipeline.helper_validate_response import validate_weather_response
from tasks.meteomatics_pipeline.helper_geocoders import safe_geocode

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_meteomatics_config():
    config_path = "/opt/airflow/tasks/meteomatics_pipeline/api_config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    logger.info("Loaded Meteomatics API config from %s", config_path)
    return config


def get_meteomatics_credentials():
    conn = BaseHook.get_connection("meteomatics_api")
    return conn.login, conn.password


def fetch_weather_data(city_name: str, start_time: str, end_time: str):
    logger.info("Fetching weather data for city: %s", city_name)

    config = load_meteomatics_config()
    location = safe_geocode(city_name)
    if not location:
        raise ValueError(f"Could not geocode city: {city_name}")

    lat, lon = location.latitude, location.longitude
    params = ",".join(config["parameters"])

    url = (
        f"{config['base_url']}/"
        f"{start_time}--{end_time}:{config['time_step']}/"
        f"{params}/{lat},{lon}/{config['output_format']}"
    )

    username, password = get_meteomatics_credentials()
    logger.info("Requesting data from Meteomatics API: %s", url)

    response = requests.get(url, auth=(username, password))
    if response.status_code != 200:
        logger.error("Failed to fetch data: %s - %s", response.status_code, response.text)
        raise ConnectionError(f"Meteomatics API error: {response.status_code}, {response.text}")

    logger.info("Weather data retrieved successfully for %s", city_name)
    return validate_weather_response(response.json())