from __future__ import annotations

import logging
from datetime import datetime, timezone

import requests

from pipeline.config import load_pipeline_config
from pipeline.ingestion.fetch_weather import fetch_weather_for_city
from pipeline.ingestion.normalize_response import build_raw_record
from pipeline.logging_config import setup_logging
from pipeline.utils.file_utils import build_raw_output_path, write_json


def run_ingestion() -> None:
    setup_logging()
    logger = logging.getLogger("pipeline.ingestion")

    config = load_pipeline_config()
    run_at_utc = datetime.now(timezone.utc)

    logger.info("Ingestion started for %d cities", len(config.cities))
    failed_cities: list[str] = []
    for city in config.cities:
        logger.info("Fetching weather for city=%s", city)
        try:
            fetch_result = fetch_weather_for_city(
                city_name=city,
                base_url=config.api_base_url,
                timeout_seconds=config.request_timeout_seconds,
            )
        except (requests.RequestException, ValueError) as exc:
            logger.error("Failed to fetch weather for city=%s: %s", city, exc)
            failed_cities.append(city)
            continue

        raw_record = build_raw_record(
            city_name=city,
            fetch_result=fetch_result,
            ingested_at_utc=run_at_utc,
        )
        output_path = build_raw_output_path(
            raw_data_dir=config.raw_data_dir,
            city_name=city,
            run_at_utc=run_at_utc,
        )
        write_json(output_path, raw_record)
        logger.info("Saved raw file to %s", output_path)

    if failed_cities:
        raise RuntimeError(
            "Ingestion failed for cities: " + ", ".join(failed_cities)
        )

    logger.info("Ingestion completed.")


if __name__ == "__main__":
    run_ingestion()

