import logging
import yaml
import json
import requests
from pathlib import Path
from datetime import datetime, timedelta
import subprocess
from airflow.hooks.base import BaseHook
from tasks.meteomatics_pipeline.helper_geocoders import safe_geocode
from tasks.meteomatics_pipeline.helper_validate_response import validate_weather_response

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class WeatherDataFetcher:
    def __init__(self, city_name: str, run_time: str):
        self.city_name = city_name
        self.run_time = run_time
        self.start_time = self._calculate_start_time(run_time)
        self.end_time = self._calculate_end_time(run_time)
        self.config = self._load_config()
        self.username, self.password = self._get_credentials()
        self.location = self._geocode_city()

    def _calculate_start_time(self, run_time: str) -> str:
        dt = datetime.strptime(run_time, "%Y-%m-%d")
        start_dt = dt - timedelta(days=1)
        return start_dt.strftime("%Y-%m-%dT00:00:00Z")

    def _calculate_end_time(self, run_time: str) -> str:
        dt = datetime.strptime(run_time, "%Y-%m-%d")
        end_dt = dt + timedelta(days=7)
        return end_dt.strftime("%Y-%m-%dT00:00:00Z")


    def _load_config(self):
        config_path = "/opt/airflow/tasks/meteomatics_pipeline/api_config.yaml"
        with open(config_path) as f:
            config = yaml.safe_load(f)
        logger.info("Loaded Meteomatics API config from %s", config_path)
        return config

    def _get_credentials(self):
        conn = BaseHook.get_connection("meteomatics_api")
        return conn.login, conn.password

    def _geocode_city(self):
        location = safe_geocode(self.city_name)
        if not location:
            raise ValueError(f"Could not geocode city: {self.city_name}")
        return location

    def fetch(self) -> dict:
        lat, lon = self.location.latitude, self.location.longitude
        params = ",".join(self.config["parameters"])
        url = (
            f"{self.config['base_url']}/"
            f"{self.start_time}--{self.end_time}:{self.config['time_step']}/"
            f"{params}/{lat},{lon}/{self.config['output_format']}"
        )
        logger.info("Requesting weather data from: %s", url)
        response = requests.get(url, auth=(self.username, self.password))

        if response.status_code != 200:
            logger.error("API error: %s - %s", response.status_code, response.text)
            raise ConnectionError(f"API error: {response.status_code} - {response.text}")

        logger.info("Weather data fetched successfully for %s", self.city_name)
        return {
            "city_name": self.city_name,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "run_time": self.run_time,
            "data": response.json()
        }

    def validate(self, raw: dict) -> dict:
        logger.info("Validating weather data for %s", raw["city_name"])
        validate_weather_response(raw["data"])
        logger.info("Validation passed for %s", raw["city_name"])
        return raw

    def save_to_snowflake_int_stage(self, raw: dict) -> str:
        city_slug = raw["city_name"].lower().replace(",", "").replace(" ", "_")
        file_path = f"/tmp/weather_raw_{city_slug}_{raw['run_time']}.json"
        Path(file_path).write_text(json.dumps(raw["data"]))
        logger.info("Saved weather JSON to: %s", file_path)
        
         # Execute PUT command using snowsql to upload to internal stage
        put_command = (
            f"snowsql -a {self.config['account']} "
            f"-u {self.config['user']} "
            f"-p {self.config['password']} "
            f"-d {self.config['database']} "
            f"-s {self.config['schema']} "
            f"-q \"PUT file://{file_path} @meteomatics_stage AUTO_COMPRESS=TRUE\""
        )
        
        logger.info("🚀 Running PUT command: %s", put_command)
        try:
            result = subprocess.run(put_command, shell=True, check=True, capture_output=True, text=True)
            logger.info("📥 Snowflake PUT output:\n%s", result.stdout)
        except subprocess.CalledProcessError as e:
            logger.error("❌ Snowflake PUT failed:\n%s", e.stderr)
            raise RuntimeError("Failed to PUT file to Snowflake stage") from e
