from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Iterable

import psycopg
from psycopg.rows import dict_row

from pipeline.config import load_db_config


def get_connection(row_factory: Any | None = None) -> psycopg.Connection:
    cfg = load_db_config()
    return psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.name,
        user=cfg.user,
        password=cfg.password,
        row_factory=row_factory,
    )


def run_sql(sql: str) -> None:
    with get_connection() as conn:
        conn.execute(sql)
        conn.commit()


def run_sql_file(path: str | Path) -> None:
    sql_path = Path(path)
    sql = sql_path.read_text(encoding="utf-8")
    run_sql(sql)


def run_sql_statements(statements: Iterable[str]) -> None:
    with get_connection() as conn:
        for statement in statements:
            conn.execute(statement)
        conn.commit()


def fetch_all(sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    with get_connection(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())


def fetch_one(sql: str, params: tuple[Any, ...] | None = None) -> dict[str, Any] | None:
    with get_connection(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
    return dict(row) if row is not None else None


def wait_for_database(timeout_seconds: int = 60, interval_seconds: float = 1.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                    cur.fetchone()
            return
        except psycopg.Error as exc:
            last_error = exc
            time.sleep(interval_seconds)

    raise TimeoutError(
        f"Database was not ready within {timeout_seconds} seconds."
    ) from last_error
