import pytest
from unittest.mock import patch, MagicMock
from tasks.meteomatics_pipeline.meteomatics_get_data import fetch_weather_data

@pytest.fixture
def mock_config():
    return {
        "base_url": "https://api.meteomatics.com",
        "parameters": ["t_2m:C"],
        "time_step": "PT1H",
        "output_format": "json"
    }

@patch("tasks.meteomatics_pipeline.meteomatics_get_data.safe_geocode")
@patch("tasks.meteomatics_pipeline.meteomatics_get_data.validate_weather_response")
@patch("tasks.meteomatics_pipeline.meteomatics_get_data.BaseHook.get_connection")
@patch("tasks.meteomatics_pipeline.meteomatics_get_data.requests.get")
@patch("builtins.open", create=True)
def test_fetch_weather_data_success(
    mock_open, mock_requests, mock_conn, mock_validate, mock_geocode, mock_config
):
    mock_open.return_value.__enter__.return_value.read.return_value = str.encode(str(mock_config))
    mock_conn.return_value.login = "user"
    mock_conn.return_value.password = "pass"

    mock_geocode.return_value.latitude = 59.437
    mock_geocode.return_value.longitude = 24.7535

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": "ok"}
    mock_requests.return_value = mock_response

    mock_validate.return_value = {"parsed": "weather"}

    result = fetch_weather_data("Tallinn", "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z")
    assert result == {"parsed": "weather"}