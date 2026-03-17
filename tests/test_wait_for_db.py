from __future__ import annotations

import sys

from pipeline.wait_for_db import parse_args


def test_parse_args_default_timeout_and_interval(monkeypatch) -> None:
    monkeypatch.setattr(sys, "argv", ["wait_for_db"])

    args = parse_args()

    assert args.timeout == 60
    assert args.interval == 1.0


def test_parse_args_accepts_custom_timeout_and_interval(monkeypatch) -> None:
    monkeypatch.setattr(
        sys, "argv", ["wait_for_db", "--timeout", "30", "--interval", "0.5"]
    )

    args = parse_args()

    assert args.timeout == 30
    assert args.interval == 0.5
