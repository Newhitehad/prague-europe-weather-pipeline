from __future__ import annotations

from pipeline.config import load_pipeline_config
from pipeline.paths import repo_path


def test_load_pipeline_config_parses_city_list(monkeypatch) -> None:
    monkeypatch.setenv("CITIES", "Prague, Vienna ,Berlin")
    monkeypatch.setenv("RAW_DATA_DIR", "data/raw")

    config = load_pipeline_config()

    assert config.cities == ["Prague", "Vienna", "Berlin"]
    assert config.raw_data_dir == str(repo_path("data/raw"))
