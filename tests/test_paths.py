from __future__ import annotations

from pipeline.paths import get_repo_root, repo_path, resolve_repo_path


def test_repo_root_contains_pyproject() -> None:
    assert repo_path("pyproject.toml").exists()
    assert get_repo_root() == repo_path()


def test_resolve_repo_path_maps_relative_paths_to_repo_root() -> None:
    assert resolve_repo_path("sql/001_create_schemas.sql") == repo_path(
        "sql", "001_create_schemas.sql"
    )
