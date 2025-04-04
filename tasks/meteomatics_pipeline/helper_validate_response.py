from pydantic import BaseModel
from typing import List, Union


class DateValue(BaseModel):
    date: str
    value: Union[float, str, int]  # Handles numbers and datetime strings


class CoordinateEntry(BaseModel):
    lat: float
    lon: float
    dates: List[DateValue]


class ParameterData(BaseModel):
    parameter: str
    coordinates: List[CoordinateEntry]


class MeteomaticsResponse(BaseModel):
    version: str
    user: str
    dateGenerated: str
    status: str
    data: List[ParameterData]

class EnrichedWeatherResponse(BaseModel):
    city: str
    country: str
    latitude: float
    longitude: float
    weather: MeteomaticsResponse


def validate_weather_response(response_json):
    try:
        validated = EnrichedWeatherResponse(**response_json)
        print("✅ Response successfully validated")
    except Exception as e:
        print("❌ Validation failed:", e)
        raise
