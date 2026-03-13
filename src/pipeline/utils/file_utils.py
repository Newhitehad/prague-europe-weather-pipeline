from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _safe_city_name(city_name: str) -> str:
    return city_name.strip().lower().replace(" ", "_")


def build_raw_output_path(
    raw_data_dir: str, city_name: str, run_at_utc: datetime
) -> Path:
    run_utc = run_at_utc.astimezone(timezone.utc)
    run_id = run_utc.strftime("%Y%m%dT%H%M%SZ")
    return (
        Path(raw_data_dir)
        / "source=open_meteo"
        / f"year={run_utc:%Y}"
        / f"month={run_utc:%m}"
        / f"day={run_utc:%d}"
        / f"run_id={run_id}"
        / f"city={_safe_city_name(city_name)}.json"
    )


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, indent=2, ensure_ascii=True)
