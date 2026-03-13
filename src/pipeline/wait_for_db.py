from __future__ import annotations

import argparse
import logging

from pipeline.logging_config import setup_logging
from pipeline.utils.db import wait_for_database


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Wait until the configured PostgreSQL database is ready."
    )
    parser.add_argument("--timeout", type=int, default=60, help="Timeout in seconds.")
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Polling interval in seconds.",
    )
    return parser.parse_args()


def main() -> None:
    setup_logging()
    logger = logging.getLogger("pipeline.db_wait")
    args = parse_args()

    logger.info(
        "Waiting for database readiness (timeout=%ss interval=%ss)",
        args.timeout,
        args.interval,
    )
    wait_for_database(timeout_seconds=args.timeout, interval_seconds=args.interval)
    logger.info("Database is ready.")


if __name__ == "__main__":
    main()
