from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

from pipeline.paths import repo_path, resolve_repo_path

load_dotenv(dotenv_path=repo_path(".env"))


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    name: str
    user: str
    password: str


@dataclass(frozen=True)
class PipelineConfig:
    api_base_url: str
    request_timeout_seconds: int
    cities: list[str]
    raw_data_dir: str


def _get_env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        raise ValueError(f"Missing required environment variable: {name}")
    if value is None:
        raise ValueError(f"Missing environment variable: {name}")
    return value


def _parse_csv_list(raw_value: str) -> list[str]:
    items = [item.strip() for item in raw_value.split(",")]
    return [item for item in items if item]


def load_db_config() -> DbConfig:
    return DbConfig(
        host=_get_env("DB_HOST", "localhost"),
        port=int(_get_env("DB_PORT", "5433")),
        name=_get_env("DB_NAME", "weather"),
        user=_get_env("DB_USER", "weather"),
        password=_get_env("DB_PASSWORD", "weather"),
    )


def load_pipeline_config() -> PipelineConfig:
    cities_raw = _get_env("CITIES", "Prague,Vienna,Berlin,Warsaw,Budapest")
    cities = _parse_csv_list(cities_raw)
    if not cities:
        raise ValueError("CITIES must contain at least one city.")

    return PipelineConfig(
        api_base_url=_get_env("API_BASE_URL", "https://api.open-meteo.com/v1/forecast"),
        request_timeout_seconds=int(_get_env("REQUEST_TIMEOUT_SECONDS", "30")),
        cities=cities,
        raw_data_dir=str(resolve_repo_path(_get_env("RAW_DATA_DIR", "data/raw"))),
    )
