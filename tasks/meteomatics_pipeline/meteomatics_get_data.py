import logging
import yaml
import json
import requests
from pathlib import Path
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from tasks.meteomatics_pipeline.helper_geocoders import safe_geocode
from tasks.meteomatics_pipeline.helper_validate_response import validate_weather_response
import boto3
import gzip
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError

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

    def save_to_s3_stage(self, raw: dict, s3_bucket: str) -> str:
        city_slug = raw["city_name"].lower().replace(",", "").replace(" ", "_")
        file_name = f"weather_raw_{city_slug}_{raw['run_time']}.json"
        local_path = f"/tmp/{file_name}"

        # Save compressed JSON locally
        with open(local_path, "wt", encoding="utf-8") as f:
            json.dump(raw["data"], f)

        logger.info("Saved compressed JSON to: %s", local_path)

        # Parse country and city from input
        city_parts = raw["city_name"].split(",")
        if len(city_parts) != 2:
            raise ValueError("City name must be in 'City, Country' format")
        city = city_parts[0].strip().lower().replace(" ", "_")
        country = city_parts[1].strip().lower().replace(" ", "_")

        # Structured key: country/city/weather_raw_city_slug_date.json
        s3_key = f"{country}/{city}/{file_name}"


        # Use Airflow AWS connection
        try:
            conn = BaseHook.get_connection("aws_conn_id")
            aws_access_key = conn.login
            aws_secret_key = conn.password
            region_name = conn.extra_dejson.get("region_name", "eu-north-1")

            session = boto3.Session(
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region_name
            )
            s3 = session.client("s3", config=Config(signature_version="s3v4"))

            # Upload to S3
            s3.upload_file(local_path, s3_bucket, s3_key)
            logger.info("Uploaded to S3: s3://%s/%s", s3_bucket, s3_key)
            return f"s3://{s3_bucket}/{s3_key}"

        except NoCredentialsError:
            logger.error('AWS credentials not found in Airflow connection')
            raise
        except ClientError as e:
            logger.error("S3 upload failed: %s", e)
            raise
