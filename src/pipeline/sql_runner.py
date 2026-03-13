from __future__ import annotations

import argparse
import logging

from pipeline.logging_config import setup_logging
from pipeline.paths import resolve_repo_path
from pipeline.utils.db import run_sql_file


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Execute one or more SQL files against the configured database."
    )
    parser.add_argument("sql_files", nargs="+", help="SQL files to execute in order.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    setup_logging()
    logger = logging.getLogger("pipeline.sql_runner")
    args = parse_args(argv)

    for raw_path in args.sql_files:
        sql_path = resolve_repo_path(raw_path)
        logger.info("Executing SQL file %s", sql_path)
        run_sql_file(sql_path)

    logger.info("SQL execution completed for %d file(s).", len(args.sql_files))


if __name__ == "__main__":
    main()
