from __future__ import annotations

from fastapi.testclient import TestClient

from pipeline.dashboard.app import app


def test_dashboard_root_renders_empty_state(monkeypatch) -> None:
    def fake_context() -> dict[str, object]:
        return {
            "is_ready": False,
            "overview": {
                "current_city_count": 0,
                "summary_row_count": 0,
                "latest_snapshot_at": None,
            },
            "current_weather": [],
            "daily_summary": [],
            "rankings": [],
            "rolling_metrics": [],
            "quality": {
                "status": "not_ready",
                "summary": {"passed": 0, "failed": 0, "total": 0},
                "items": [],
            },
        }

    monkeypatch.setattr(
        "pipeline.dashboard.app.service.load_dashboard_context",
        fake_context,
    )

    client = TestClient(app)
    response = client.get("/")

    assert response.status_code == 200
    assert "Dashboard not ready yet" in response.text
    assert 'data-theme-toggle' in response.text
    assert "theme.js" in response.text


def test_dashboard_current_api_returns_items(monkeypatch) -> None:
    monkeypatch.setattr(
        "pipeline.dashboard.app.service.fetch_current_weather",
        lambda: [{"city": "Prague", "temperature_c": 14.2}],
    )

    client = TestClient(app)
    response = client.get("/api/current")

    assert response.status_code == 200
    assert response.json()["items"][0]["city"] == "Prague"


def test_dashboard_quality_api_returns_summary(monkeypatch) -> None:
    monkeypatch.setattr(
        "pipeline.dashboard.app.service.fetch_quality_payload",
        lambda: {
            "status": "pass",
            "summary": {"passed": 9, "failed": 0, "total": 9},
            "items": [],
        },
    )

    client = TestClient(app)
    response = client.get("/api/quality")

    assert response.status_code == 200
    assert response.json()["status"] == "pass"


def test_health_route_returns_ok() -> None:
    client = TestClient(app)
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
