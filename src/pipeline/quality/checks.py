from __future__ import annotations

import logging
from dataclasses import dataclass

from pipeline.config import load_pipeline_config
from pipeline.logging_config import setup_logging
from pipeline.utils.db import get_connection


@dataclass(frozen=True)
class QualityCheck:
    name: str
    description: str
    sql: str


@dataclass(frozen=True)
class QualityCheckResult:
    name: str
    description: str
    issue_count: int

    @property
    def passed(self) -> bool:
        return self.issue_count == 0


def build_quality_checks(city_count: int, forecast_days: int = 7) -> list[QualityCheck]:
    expected_city_count = expected_sql_int(city_count)
    expected_daily_rows = expected_sql_int(city_count * forecast_days)

    return [
        QualityCheck(
            name="mart_current_weather_nulls",
            description=(
                "Current weather mart table should not have null critical fields."
            ),
            sql="""
                SELECT COUNT(*) AS issue_count
                FROM mart.city_current_weather
                WHERE city IS NULL
                   OR current_time_utc IS NULL
                   OR source_ingested_at_utc IS NULL;
            """,
        ),
        QualityCheck(
            name="mart_daily_summary_nulls",
            description=(
                "Daily summary mart table should not have null critical fields."
            ),
            sql="""
                SELECT COUNT(*) AS issue_count
                FROM mart.daily_city_weather_summary
                WHERE city IS NULL
                   OR forecast_date IS NULL
                   OR temperature_max_c IS NULL
                   OR temperature_min_c IS NULL
                   OR precipitation_mm IS NULL
                   OR source_ingested_at_utc IS NULL;
            """,
        ),
        QualityCheck(
            name="mart_daily_summary_duplicates",
            description=(
                "Daily summary mart table should not contain duplicate city/date rows."
            ),
            sql="""
                SELECT COUNT(*) AS issue_count
                FROM (
                    SELECT city, forecast_date
                    FROM mart.daily_city_weather_summary
                    GROUP BY city, forecast_date
                    HAVING COUNT(*) > 1
                ) AS duplicate_rows;
            """,
        ),
        QualityCheck(
            name="mart_daily_temperature_ranges",
            description=(
                "Temperatures should be within a realistic range and max >= min."
            ),
            sql="""
                SELECT COUNT(*) AS issue_count
                FROM mart.daily_city_weather_summary
                WHERE temperature_max_c < temperature_min_c
                   OR temperature_max_c NOT BETWEEN -80 AND 80
                   OR temperature_min_c NOT BETWEEN -80 AND 80;
            """,
        ),
        QualityCheck(
            name="mart_daily_precipitation_ranges",
            description="Precipitation should not be negative.",
            sql="""
                SELECT COUNT(*) AS issue_count
                FROM mart.daily_city_weather_summary
                WHERE precipitation_mm < 0;
            """,
        ),
        QualityCheck(
            name="mart_current_weather_row_count",
            description=(
                "Current weather mart table should contain one row per configured city."
            ),
            sql=f"""
                SELECT CASE
                    WHEN (SELECT COUNT(*) FROM mart.city_current_weather) =
                        {expected_city_count}
                    THEN 0 ELSE 1
                END AS issue_count;
            """,
        ),
        QualityCheck(
            name="mart_daily_summary_row_count",
            description=(
                "Daily summary mart table should contain one row per city "
                "and forecast day."
            ),
            sql=f"""
                SELECT CASE
                    WHEN (SELECT COUNT(*) FROM mart.daily_city_weather_summary) =
                        {expected_daily_rows}
                    THEN 0 ELSE 1
                END AS issue_count;
            """,
        ),
        QualityCheck(
            name="mart_temperature_rankings_row_count",
            description=(
                "Temperature rankings mart table should contain one row per city "
                "and forecast day."
            ),
            sql=f"""
                SELECT CASE
                    WHEN (SELECT COUNT(*) FROM mart.daily_temperature_rankings) =
                        {expected_daily_rows}
                    THEN 0 ELSE 1
                END AS issue_count;
            """,
        ),
        QualityCheck(
            name="mart_rolling_metrics_row_count",
            description=(
                "Rolling metrics mart table should contain one row per city "
                "and forecast day."
            ),
            sql=f"""
                SELECT CASE
                    WHEN (SELECT COUNT(*) FROM mart.city_rolling_3day_metrics) =
                        {expected_daily_rows}
                    THEN 0 ELSE 1
                END AS issue_count;
            """,
        ),
    ]


def expected_sql_int(value: int) -> int:
    if value < 0:
        raise ValueError("Expected SQL integer values must be non-negative.")
    return value


def run_check(check: QualityCheck) -> QualityCheckResult:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(check.sql)
            row = cur.fetchone()

    if row is None:
        raise ValueError(f"Quality check '{check.name}' returned no rows.")

    return QualityCheckResult(
        name=check.name,
        description=check.description,
        issue_count=int(row[0]),
    )


def collect_quality_results(
    city_count: int, forecast_days: int = 7
) -> list[QualityCheckResult]:
    checks = build_quality_checks(city_count=city_count, forecast_days=forecast_days)
    return [run_check(check) for check in checks]


def has_failures(results: list[QualityCheckResult]) -> bool:
    return any(not result.passed for result in results)


def summarize_quality_results(results: list[QualityCheckResult]) -> dict[str, int]:
    failed = sum(1 for result in results if not result.passed)
    total = len(results)
    return {
        "passed": total - failed,
        "failed": failed,
        "total": total,
    }


def run_quality_checks() -> None:
    setup_logging()
    logger = logging.getLogger("pipeline.quality")

    pipeline_config = load_pipeline_config()
    results = collect_quality_results(city_count=len(pipeline_config.cities))
    logger.info("Running %d data quality checks", len(results))

    for result in results:
        status = "PASS" if result.passed else "FAIL"
        logger.info(
            "[%s] %s | issues=%d | %s",
            status,
            result.name,
            result.issue_count,
            result.description,
        )

    failed_checks = [result.name for result in results if not result.passed]
    if failed_checks:
        raise SystemExit(
            "Data quality checks failed: " + ", ".join(failed_checks)
        )

    logger.info("All data quality checks passed.")


if __name__ == "__main__":
    run_quality_checks()
