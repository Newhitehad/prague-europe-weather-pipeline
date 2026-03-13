from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pipeline.ingestion.fetch_weather import (
    _lookup_city_coordinates,
    fetch_weather_for_city,
)
from pipeline.ingestion.normalize_response import build_raw_record


class DummyResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


def test_lookup_city_coordinates_normalizes_case_and_whitespace() -> None:
    coordinates = _lookup_city_coordinates("  Prague ")

    assert coordinates.latitude == 50.0755
    assert coordinates.longitude == 14.4378


def test_lookup_city_coordinates_rejects_unknown_city() -> None:
    try:
        _lookup_city_coordinates("London")
    except ValueError as exc:
        assert "Unsupported city" in str(exc)
    else:
        raise AssertionError("Unsupported cities should raise ValueError")


def test_fetch_weather_for_city_builds_expected_request(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_get(url: str, params: dict[str, Any], timeout: int) -> DummyResponse:
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = timeout
        return DummyResponse({"current": {"temperature_2m": 21.5}})

    monkeypatch.setattr("pipeline.ingestion.fetch_weather.requests.get", fake_get)

    result = fetch_weather_for_city(
        city_name="Prague",
        base_url="https://api.example.test/forecast",
        timeout_seconds=45,
    )

    assert captured["url"] == "https://api.example.test/forecast"
    assert captured["params"]["latitude"] == 50.0755
    assert captured["params"]["longitude"] == 14.4378
    assert captured["params"]["forecast_days"] == 7
    assert captured["timeout"] == 45
    assert result["city"] == "Prague"
    assert result["api_response"] == {"current": {"temperature_2m": 21.5}}


def test_build_raw_record_formats_ingested_timestamp_as_utc_zulu() -> None:
    ingested_at = datetime(2026, 3, 13, 19, 45, tzinfo=timezone.utc)

    record = build_raw_record(
        city_name="Prague",
        fetch_result={"city": "Prague", "api_response": {}},
        ingested_at_utc=ingested_at,
    )

    assert record["source"] == "open-meteo"
    assert record["city"] == "Prague"
    assert record["ingested_at_utc"] == "2026-03-13T19:45:00Z"
    assert record["payload"]["city"] == "Prague"
