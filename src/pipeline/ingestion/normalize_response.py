from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def build_raw_record(
    city_name: str,
    fetch_result: dict[str, Any],
    ingested_at_utc: datetime,
) -> dict[str, Any]:
    return {
        "source": "open-meteo",
        "city": city_name,
        "ingested_at_utc": (
            ingested_at_utc.astimezone(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        ),
        "payload": fetch_result,
    }
