from __future__ import annotations

import time
from datetime import date, datetime
from typing import Any

from pipeline.config import load_pipeline_config
from pipeline.quality.checks import collect_quality_results, summarize_quality_results
from pipeline.utils.db import fetch_all, fetch_one

CACHE_TTL_SECONDS = 10.0

_dashboard_cache: dict[str, Any] = {
    "expires_at": 0.0,
    "value": None,
}

_quality_cache: dict[str, Any] = {
    "expires_at": 0.0,
    "value": None,
}

CURRENT_WEATHER_SQL = """
SELECT
    city,
    current_time_utc,
    temperature_c,
    relative_humidity_pct,
    wind_speed_kmh,
    source_ingested_at_utc
FROM mart.city_current_weather
ORDER BY city;
"""

DAILY_SUMMARY_SQL = """
SELECT
    city,
    forecast_date,
    temperature_max_c,
    temperature_min_c,
    temperature_avg_c,
    diurnal_temp_range_c,
    precipitation_mm,
    is_wet_day,
    source_ingested_at_utc
FROM mart.daily_city_weather_summary
ORDER BY forecast_date, city;
"""

RANKINGS_SQL = """
SELECT
    forecast_date,
    city,
    temperature_max_c,
    temperature_avg_c,
    precipitation_mm,
    max_temperature_rank
FROM mart.daily_temperature_rankings
ORDER BY forecast_date, max_temperature_rank, city;
"""

ROLLING_SQL = """
SELECT
    city,
    forecast_date,
    temperature_avg_c,
    rolling_3day_avg_temp_c,
    precipitation_mm,
    rolling_3day_precip_mm
FROM mart.city_rolling_3day_metrics
ORDER BY city, forecast_date;
"""

OVERVIEW_SQL = """
SELECT
    (SELECT COUNT(*) FROM mart.city_current_weather) AS current_city_count,
    (SELECT COUNT(*) FROM mart.daily_city_weather_summary) AS summary_row_count,
    (SELECT COUNT(*) FROM mart.daily_temperature_rankings) AS ranking_row_count,
    (SELECT COUNT(*) FROM mart.city_rolling_3day_metrics) AS rolling_row_count,
    (SELECT MAX(source_ingested_at_utc) FROM mart.daily_city_weather_summary)
        AS latest_snapshot_at;
"""


def _serialize_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if value is None:
        return None
    return value


def _serialize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for row in rows:
        serialized.append({key: _serialize_value(value) for key, value in row.items()})
    return serialized


def fetch_current_weather() -> list[dict[str, Any]]:
    return _serialize_rows(fetch_all(CURRENT_WEATHER_SQL))


def fetch_daily_summary() -> list[dict[str, Any]]:
    return _serialize_rows(fetch_all(DAILY_SUMMARY_SQL))


def fetch_rankings() -> list[dict[str, Any]]:
    return _serialize_rows(fetch_all(RANKINGS_SQL))


def fetch_rolling_metrics() -> list[dict[str, Any]]:
    return _serialize_rows(fetch_all(ROLLING_SQL))


def fetch_overview() -> dict[str, Any]:
    row = fetch_one(OVERVIEW_SQL) or {}
    return {key: _serialize_value(value) for key, value in row.items()}


def fetch_quality_payload() -> dict[str, Any]:
    now = time.monotonic()
    if _quality_cache["value"] is not None and now < _quality_cache["expires_at"]:
        return _quality_cache["value"]

    overview = fetch_overview()
    if int(overview.get("current_city_count") or 0) == 0:
        payload = {
            "status": "not_ready",
            "summary": {
                "passed": 0,
                "failed": 0,
                "total": 0,
            },
            "items": [],
        }
        _quality_cache["value"] = payload
        _quality_cache["expires_at"] = now + CACHE_TTL_SECONDS
        return payload

    pipeline_config = load_pipeline_config()
    results = collect_quality_results(city_count=len(pipeline_config.cities))
    summary = summarize_quality_results(results)

    payload = {
        "status": "pass" if summary["failed"] == 0 else "fail",
        "summary": summary,
        "items": [
            {
                "name": result.name,
                "description": result.description,
                "issue_count": result.issue_count,
                "passed": result.passed,
            }
            for result in results
        ],
    }
    _quality_cache["value"] = payload
    _quality_cache["expires_at"] = now + CACHE_TTL_SECONDS
    return payload


def load_dashboard_context() -> dict[str, Any]:
    now = time.monotonic()
    if _dashboard_cache["value"] is not None and now < _dashboard_cache["expires_at"]:
        return _dashboard_cache["value"]

    current_weather = fetch_current_weather()
    rankings = fetch_rankings()
    rolling_metrics = fetch_rolling_metrics()
    overview = fetch_overview()
    quality = fetch_quality_payload()

    is_ready = bool(current_weather) and bool(rankings) and bool(rolling_metrics)

    context = {
        "is_ready": is_ready,
        "overview": overview,
        "current_weather": current_weather,
        "rankings": rankings,
        "rolling_metrics": rolling_metrics,
        "quality": quality,
    }
    _dashboard_cache["value"] = context
    _dashboard_cache["expires_at"] = now + CACHE_TTL_SECONDS
    return context
