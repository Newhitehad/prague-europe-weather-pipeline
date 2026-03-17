from __future__ import annotations

from datetime import date, datetime, timezone

from pipeline.dashboard.service import (
    _serialize_rows,
    _serialize_value,
    fetch_quality_payload,
    load_dashboard_context,
)


def test_serialize_value_formats_date_as_iso() -> None:
    result = _serialize_value(date(2026, 3, 13))

    assert result == "2026-03-13"


def test_serialize_value_formats_datetime_as_iso() -> None:
    dt = datetime(2026, 3, 13, 19, 45, tzinfo=timezone.utc)

    result = _serialize_value(dt)

    assert result == "2026-03-13T19:45:00+00:00"


def test_serialize_value_passes_none_through() -> None:
    assert _serialize_value(None) is None


def test_serialize_value_passes_primitive_through() -> None:
    assert _serialize_value(42) == 42
    assert _serialize_value(3.14) == 3.14
    assert _serialize_value("hello") == "hello"


def test_serialize_rows_converts_dates_in_each_row() -> None:
    rows = [
        {"city": "Prague", "forecast_date": date(2026, 3, 13), "temperature_c": 15.9},
        {"city": "Vienna", "forecast_date": date(2026, 3, 14), "temperature_c": None},
    ]

    result = _serialize_rows(rows)

    assert result[0]["forecast_date"] == "2026-03-13"
    assert result[1]["forecast_date"] == "2026-03-14"
    assert result[1]["temperature_c"] is None


def test_fetch_quality_payload_returns_not_ready_when_mart_empty(monkeypatch) -> None:
    monkeypatch.setattr(
        "pipeline.dashboard.service.fetch_overview",
        lambda: {"current_city_count": 0, "latest_snapshot_at": None},
    )
    # Reset cache so the monkeypatch takes effect
    import pipeline.dashboard.service as svc

    svc._quality_cache["value"] = None
    svc._quality_cache["expires_at"] = 0.0

    payload = fetch_quality_payload()

    assert payload["status"] == "not_ready"
    assert payload["summary"]["total"] == 0
    assert payload["items"] == []


def test_load_dashboard_context_is_ready_false_when_no_data(monkeypatch) -> None:
    monkeypatch.setattr(
        "pipeline.dashboard.service.fetch_current_weather", lambda: []
    )
    monkeypatch.setattr("pipeline.dashboard.service.fetch_rankings", lambda: [])
    monkeypatch.setattr("pipeline.dashboard.service.fetch_rolling_metrics", lambda: [])
    monkeypatch.setattr(
        "pipeline.dashboard.service.fetch_overview",
        lambda: {"current_city_count": 0, "latest_snapshot_at": None},
    )
    monkeypatch.setattr(
        "pipeline.dashboard.service.fetch_quality_payload",
        lambda: {"status": "not_ready", "summary": {}, "items": []},
    )

    import pipeline.dashboard.service as svc

    svc._dashboard_cache["value"] = None
    svc._dashboard_cache["expires_at"] = 0.0

    context = load_dashboard_context()

    assert context["is_ready"] is False
    assert context["current_weather"] == []
    assert context["rankings"] == []
    assert context["rolling_metrics"] == []


def test_load_dashboard_context_is_ready_true_when_data_present(monkeypatch) -> None:
    monkeypatch.setattr(
        "pipeline.dashboard.service.fetch_current_weather",
        lambda: [{"city": "Prague", "temperature_c": 15.9}],
    )
    monkeypatch.setattr(
        "pipeline.dashboard.service.fetch_rankings",
        lambda: [{"city": "Prague", "max_temperature_rank": 1}],
    )
    monkeypatch.setattr(
        "pipeline.dashboard.service.fetch_rolling_metrics",
        lambda: [{"city": "Prague", "rolling_3day_avg_temp_c": 14.0}],
    )
    monkeypatch.setattr(
        "pipeline.dashboard.service.fetch_overview",
        lambda: {"current_city_count": 1, "latest_snapshot_at": "2026-03-13"},
    )
    monkeypatch.setattr(
        "pipeline.dashboard.service.fetch_quality_payload",
        lambda: {
            "status": "pass",
            "summary": {"passed": 9, "failed": 0, "total": 9},
            "items": [],
        },
    )

    import pipeline.dashboard.service as svc

    svc._dashboard_cache["value"] = None
    svc._dashboard_cache["expires_at"] = 0.0

    context = load_dashboard_context()

    assert context["is_ready"] is True
    assert len(context["current_weather"]) == 1
