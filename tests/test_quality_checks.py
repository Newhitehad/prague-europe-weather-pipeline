from __future__ import annotations

from pipeline.quality.checks import (
    QualityCheckResult,
    build_quality_checks,
    expected_sql_int,
    has_failures,
    summarize_quality_results,
)


def test_build_quality_checks_includes_expected_row_count_checks() -> None:
    checks = build_quality_checks(city_count=5, forecast_days=7)

    names = {check.name for check in checks}

    assert "mart_current_weather_row_count" in names
    assert "mart_daily_summary_row_count" in names
    assert "mart_temperature_rankings_row_count" in names
    assert "mart_rolling_metrics_row_count" in names
    assert len(checks) == 9


def test_has_failures_returns_true_when_any_check_fails() -> None:
    results = [
        QualityCheckResult(name="ok", description="ok", issue_count=0),
        QualityCheckResult(name="bad", description="bad", issue_count=2),
    ]

    assert has_failures(results) is True


def test_expected_sql_int_rejects_negative_values() -> None:
    try:
        expected_sql_int(-1)
    except ValueError as exc:
        assert "non-negative" in str(exc)
    else:
        raise AssertionError("expected_sql_int should reject negative values")


def test_summarize_quality_results_counts_pass_and_fail() -> None:
    results = [
        QualityCheckResult(name="ok-1", description="ok", issue_count=0),
        QualityCheckResult(name="ok-2", description="ok", issue_count=0),
        QualityCheckResult(name="bad", description="bad", issue_count=1),
    ]

    summary = summarize_quality_results(results)

    assert summary == {"passed": 2, "failed": 1, "total": 3}
