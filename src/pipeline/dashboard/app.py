from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from pipeline.dashboard import service

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="Prague Europe Weather Dashboard", version="0.1.0")
app.mount(
    "/static",
    StaticFiles(directory=str(BASE_DIR / "static")),
    name="static",
)
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request) -> HTMLResponse:
    context = service.load_dashboard_context()
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "request": request,
            **context,
        },
    )


@app.get("/api/current")
def current_weather() -> dict[str, object]:
    return {"items": service.fetch_current_weather()}


@app.get("/api/daily-summary")
def daily_summary() -> dict[str, object]:
    return {"items": service.fetch_daily_summary()}


@app.get("/api/rankings")
def rankings() -> dict[str, object]:
    return {"items": service.fetch_rankings()}


@app.get("/api/rolling")
def rolling() -> dict[str, object]:
    return {"items": service.fetch_rolling_metrics()}


@app.get("/api/quality")
def quality() -> dict[str, object]:
    return service.fetch_quality_payload()
