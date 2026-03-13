from __future__ import annotations

from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=1)
def get_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def repo_path(*parts: str) -> Path:
    return get_repo_root().joinpath(*parts)


def resolve_repo_path(path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return get_repo_root() / candidate
