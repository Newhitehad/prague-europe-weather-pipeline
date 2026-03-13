from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class CityCoordinate:
    latitude: float
    longitude: float


CITY_COORDINATES: dict[str, CityCoordinate] = {
    "prague": CityCoordinate(latitude=50.0755, longitude=14.4378),
    "vienna": CityCoordinate(latitude=48.2082, longitude=16.3738),
    "berlin": CityCoordinate(latitude=52.52, longitude=13.405),
    "warsaw": CityCoordinate(latitude=52.2297, longitude=21.0122),
    "budapest": CityCoordinate(latitude=47.4979, longitude=19.0402),
}


def _lookup_city_coordinates(city_name: str) -> CityCoordinate:
    key = city_name.strip().lower()
    coordinates = CITY_COORDINATES.get(key)
    if coordinates is None:
        supported = ", ".join(sorted(CITY_COORDINATES))
        raise ValueError(
            f"Unsupported city '{city_name}'. Supported cities: {supported}"
        )
    return coordinates


def fetch_weather_for_city(
    city_name: str, base_url: str, timeout_seconds: int
) -> dict[str, Any]:
    coordinates = _lookup_city_coordinates(city_name)

    params = {
        "latitude": coordinates.latitude,
        "longitude": coordinates.longitude,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "UTC",
        "forecast_days": 7,
    }

    response = requests.get(base_url, params=params, timeout=timeout_seconds)
    response.raise_for_status()

    return {
        "city": city_name,
        "request_params": params,
        "api_response": response.json(),
    }
