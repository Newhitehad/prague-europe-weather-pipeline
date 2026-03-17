from __future__ import annotations

import pytest

from pipeline.sql_runner import parse_args


def test_parse_args_accepts_single_sql_file() -> None:
    args = parse_args(["sql/001_create_schemas.sql"])

    assert args.sql_files == ["sql/001_create_schemas.sql"]


def test_parse_args_accepts_multiple_sql_files() -> None:
    args = parse_args(
        ["sql/001_create_schemas.sql", "sql/002_create_raw_tables.sql"]
    )

    assert args.sql_files == [
        "sql/001_create_schemas.sql",
        "sql/002_create_raw_tables.sql",
    ]


def test_parse_args_requires_at_least_one_file() -> None:
    with pytest.raises(SystemExit):
        parse_args([])
