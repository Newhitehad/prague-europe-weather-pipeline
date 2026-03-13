from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

from psycopg.types.json import Jsonb

from pipeline.config import load_pipeline_config
from pipeline.logging_config import setup_logging
from pipeline.utils.db import get_connection

RAW_FILE_PATTERN = "source=open_meteo/year=*/month=*/day=*/run_id=*/city=*.json"

INSERT_RAW_SQL = """
INSERT INTO raw.weather_api_responses (
    source,
    city,
    ingested_at_utc,
    payload,
    raw_file_path
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (raw_file_path) DO NOTHING;
"""


def _parse_iso_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _discover_raw_files(raw_data_dir: str) -> list[Path]:
    base = Path(raw_data_dir)
    return sorted(base.glob(RAW_FILE_PATTERN))


def _read_raw_record(file_path: Path) -> dict:
    record = json.loads(file_path.read_text(encoding="utf-8"))
    required = ["source", "city", "ingested_at_utc", "payload"]
    missing = [key for key in required if key not in record]
    if missing:
        raise ValueError(f"Missing keys {missing} in {file_path}")
    return record


def load_raw_files() -> None:
    setup_logging()
    logger = logging.getLogger("pipeline.load.raw")

    cfg = load_pipeline_config()
    files = _discover_raw_files(cfg.raw_data_dir)
    if not files:
        logger.warning("No raw files found under %s", cfg.raw_data_dir)
        return

    inserted = 0
    skipped = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            for file_path in files:
                record = _read_raw_record(file_path)

                cur.execute(
                    INSERT_RAW_SQL,
                    (
                        record["source"],
                        record["city"],
                        _parse_iso_utc(record["ingested_at_utc"]),
                        Jsonb(record["payload"]),
                        str(file_path),
                    ),
                )

                if cur.rowcount == 1:
                    inserted += 1
                else:
                    skipped += 1


        conn.commit()

    logger.info(
        "Raw load completed. inserted=%d skipped=%d total=%d",
        inserted,
        skipped,
        len(files),
    )


if __name__ == "__main__":
    load_raw_files()
