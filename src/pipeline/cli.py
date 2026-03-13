from __future__ import annotations

import argparse
import os
import subprocess
import sys
from contextlib import contextmanager
from typing import Iterator

from pipeline.load.load_raw import load_raw_files
from pipeline.main import run_ingestion
from pipeline.paths import repo_path
from pipeline.quality.checks import run_quality_checks
from pipeline.sql_runner import main as sql_runner_main
from pipeline.utils.db import wait_for_database


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="prague-weather",
        description="Run the Prague Europe Weather pipeline from any terminal.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "init",
        help="Run the full pipeline from DB startup to quality checks.",
    )
    subparsers.add_parser("check", help="Run lint and tests.")
    subparsers.add_parser(
        "smoke",
        help="Run the deterministic fixture-based smoke flow.",
    )
    subparsers.add_parser(
        "ingest",
        help="Fetch live weather data and write raw JSON files.",
    )
    subparsers.add_parser("raw", help="Load raw JSON files into PostgreSQL.")
    subparsers.add_parser("stage", help="Build staging tables from raw data.")
    subparsers.add_parser("mart", help="Build mart tables from staging data.")
    subparsers.add_parser("dq", help="Run mart-layer data quality checks.")

    ui_parser = subparsers.add_parser(
        "ui",
        help="Start the FastAPI read-only dashboard.",
    )
    ui_parser.add_argument("--host", default="127.0.0.1")
    ui_parser.add_argument("--port", type=int, default=8000)

    db_parser = subparsers.add_parser("db", help="Manage the local PostgreSQL service.")
    db_subparsers = db_parser.add_subparsers(dest="db_command", required=True)
    db_subparsers.add_parser("up", help="Start PostgreSQL with Docker Compose.")
    db_subparsers.add_parser("down", help="Stop PostgreSQL containers.")
    db_subparsers.add_parser("logs", help="Tail PostgreSQL logs.")
    db_wait_parser = db_subparsers.add_parser(
        "wait",
        help="Wait until PostgreSQL is ready.",
    )
    db_wait_parser.add_argument("--timeout", type=int, default=60)
    db_wait_parser.add_argument("--interval", type=float, default=1.0)
    db_subparsers.add_parser("setup", help="Create the raw, staging, and mart schemas.")

    return parser.parse_args(argv)


def build_repo_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_path("src"))
    if extra:
        env.update(extra)
    return env


def run_subprocess(args: list[str], extra_env: dict[str, str] | None = None) -> None:
    subprocess.run(
        args,
        check=True,
        cwd=str(repo_path()),
        env=build_repo_env(extra_env),
    )


def run_docker_compose(*compose_args: str) -> None:
    run_subprocess(
        [
            "docker",
            "compose",
            "-f",
            str(repo_path("docker-compose.yml")),
            *compose_args,
        ]
    )


def run_python_module(module: str, *module_args: str) -> None:
    run_subprocess([sys.executable, "-m", module, *module_args])


def run_sql_files(*filenames: str) -> None:
    sql_runner_main([str(repo_path("sql", filename)) for filename in filenames])


@contextmanager
def temporary_env(**values: str) -> Iterator[None]:
    original = {key: os.environ.get(key) for key in values}
    try:
        for key, value in values.items():
            os.environ[key] = value
        yield
    finally:
        for key, previous in original.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous


def command_check() -> None:
    run_python_module("ruff", "check", "src", "tests")
    run_python_module("pytest", "-q")


def command_db_setup() -> None:
    run_sql_files("001_create_schemas.sql")


def command_stage() -> None:
    run_sql_files("004_load_staging_from_raw.sql")


def command_mart() -> None:
    run_sql_files("006_load_mart_from_staging.sql")


def command_init() -> None:
    run_docker_compose("up", "-d")
    wait_for_database(timeout_seconds=60, interval_seconds=1.0)
    run_sql_files(
        "001_create_schemas.sql",
        "002_create_raw_tables.sql",
        "003_create_staging_tables.sql",
        "005_create_mart_tables.sql",
    )
    run_ingestion()
    load_raw_files()
    command_stage()
    command_mart()
    run_quality_checks()


def command_smoke() -> None:
    wait_for_database(timeout_seconds=60, interval_seconds=1.0)
    run_sql_files(
        "001_create_schemas.sql",
        "002_create_raw_tables.sql",
        "003_create_staging_tables.sql",
        "005_create_mart_tables.sql",
    )
    with temporary_env(RAW_DATA_DIR="tests/fixtures/raw"):
        load_raw_files()
    run_sql_files("004_load_staging_from_raw.sql", "006_load_mart_from_staging.sql")
    run_quality_checks()


def command_ui(host: str, port: int) -> None:
    run_python_module(
        "uvicorn",
        "pipeline.dashboard.app:app",
        "--host",
        host,
        "--port",
        str(port),
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    if args.command == "init":
        command_init()
        return
    if args.command == "check":
        command_check()
        return
    if args.command == "smoke":
        command_smoke()
        return
    if args.command == "ui":
        command_ui(host=args.host, port=args.port)
        return
    if args.command == "ingest":
        run_ingestion()
        return
    if args.command == "raw":
        load_raw_files()
        return
    if args.command == "stage":
        command_stage()
        return
    if args.command == "mart":
        command_mart()
        return
    if args.command == "dq":
        run_quality_checks()
        return
    if args.command == "db":
        if args.db_command == "up":
            run_docker_compose("up", "-d")
            return
        if args.db_command == "down":
            run_docker_compose("down")
            return
        if args.db_command == "logs":
            run_docker_compose("logs", "-f", "postgres")
            return
        if args.db_command == "wait":
            wait_for_database(
                timeout_seconds=args.timeout,
                interval_seconds=args.interval,
            )
            return
        if args.db_command == "setup":
            command_db_setup()
            return

    raise ValueError(f"Unsupported command selection: {args}")


if __name__ == "__main__":
    main()
