from __future__ import annotations

import json
from datetime import timezone

from pipeline.load.load_raw import _parse_iso_utc, _read_raw_record


def test_parse_iso_utc_supports_z_suffix() -> None:
    parsed = _parse_iso_utc("2026-03-13T19:45:00Z")

    assert parsed.tzinfo == timezone.utc
    assert parsed.year == 2026
    assert parsed.minute == 45


def test_read_raw_record_returns_payload_for_valid_file(tmp_path) -> None:
    file_path = tmp_path / "sample.json"
    payload = {
        "source": "open-meteo",
        "city": "Prague",
        "ingested_at_utc": "2026-03-13T19:45:00Z",
        "payload": {"api_response": {"current": {"temperature_2m": 15.2}}},
    }
    file_path.write_text(json.dumps(payload), encoding="utf-8")

    record = _read_raw_record(file_path)

    assert record == payload


def test_read_raw_record_raises_for_missing_required_keys(tmp_path) -> None:
    file_path = tmp_path / "broken.json"
    file_path.write_text(json.dumps({"source": "open-meteo"}), encoding="utf-8")

    try:
        _read_raw_record(file_path)
    except ValueError as exc:
        assert "Missing keys" in str(exc)
    else:
        raise AssertionError("Files missing required keys should raise ValueError")
