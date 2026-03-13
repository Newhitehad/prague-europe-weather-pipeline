from __future__ import annotations

from pipeline import cli
from pipeline.paths import repo_path


def test_run_docker_compose_uses_repo_compose_file(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run(args, check, cwd, env) -> None:
        captured["args"] = args
        captured["check"] = check
        captured["cwd"] = cwd
        captured["env"] = env

    monkeypatch.setattr("pipeline.cli.subprocess.run", fake_run)

    cli.run_docker_compose("up", "-d")

    assert captured["args"] == [
        "docker",
        "compose",
        "-f",
        str(repo_path("docker-compose.yml")),
        "up",
        "-d",
    ]
    assert captured["check"] is True
    assert captured["cwd"] == str(repo_path())


def test_main_init_runs_pipeline_sequence(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        "pipeline.cli.run_docker_compose",
        lambda *args: calls.append("db_up"),
    )
    monkeypatch.setattr(
        "pipeline.cli.wait_for_database",
        lambda timeout_seconds, interval_seconds: calls.append("db_wait"),
    )
    monkeypatch.setattr(
        "pipeline.cli.run_sql_files",
        lambda *args: calls.append(",".join(args)),
    )
    monkeypatch.setattr("pipeline.cli.run_ingestion", lambda: calls.append("ingest"))
    monkeypatch.setattr("pipeline.cli.load_raw_files", lambda: calls.append("raw"))
    monkeypatch.setattr("pipeline.cli.command_stage", lambda: calls.append("stage"))
    monkeypatch.setattr("pipeline.cli.command_mart", lambda: calls.append("mart"))
    monkeypatch.setattr("pipeline.cli.run_quality_checks", lambda: calls.append("dq"))

    cli.main(["init"])

    assert calls == [
        "db_up",
        "db_wait",
        "001_create_schemas.sql,002_create_raw_tables.sql,003_create_staging_tables.sql,005_create_mart_tables.sql",
        "ingest",
        "raw",
        "stage",
        "mart",
        "dq",
    ]


def test_main_ui_dispatches_to_uvicorn_module(monkeypatch) -> None:
    captured: list[tuple[str, ...]] = []

    monkeypatch.setattr(
        "pipeline.cli.run_python_module",
        lambda module, *module_args: captured.append((module, *module_args)),
    )

    cli.main(["ui", "--host", "0.0.0.0", "--port", "8010"])

    assert captured == [
        (
            "uvicorn",
            "pipeline.dashboard.app:app",
            "--host",
            "0.0.0.0",
            "--port",
            "8010",
        )
    ]
