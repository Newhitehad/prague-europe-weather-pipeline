from __future__ import annotations

from datetime import datetime, timezone

from pipeline.utils.file_utils import build_raw_output_path


def test_build_raw_output_path_uses_partitioned_layout() -> None:
    run_at = datetime(2026, 3, 13, 12, 30, tzinfo=timezone.utc)

    output_path = build_raw_output_path("data/raw", "Prague", run_at)

    assert (
        str(output_path)
        == "data/raw/source=open_meteo/year=2026/month=03/day=13/"
        "run_id=20260313T123000Z/city=prague.json"
    )
