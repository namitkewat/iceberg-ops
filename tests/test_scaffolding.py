"""Tests for iceberg_ops.scaffolding (init_project + create_revision)."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

from iceberg_ops.scaffolding import (
    GITIGNORE_MARKER,
    _docstring_text,
    _next_version,
    _slugify,
    create_revision,
    init_project,
)


def _load_module(path: str | Path) -> ModuleType:
    """Import a generated migration file the way the runner does."""
    spec = importlib.util.spec_from_file_location(f"_scaffold_{Path(path).stem}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_TRICKY_MESSAGES = [
    'add "status" column',
    "move C:\\Users data",
    "trailing\\",
    "l1\nl2",
    'tri """ q',
    "it's",
    "nul\x00",
    "größe",
    "$foo",
]


class TestInitProject:
    def test_creates_layout(self, tmp_path: Path) -> None:
        init_project(tmp_path)
        assert (tmp_path / "iceberg-ops.yaml").exists()
        assert (tmp_path / "migrations").is_dir()
        assert (tmp_path / "migrations" / "__init__.py").exists()
        assert (tmp_path / ".gitignore").exists()
        assert GITIGNORE_MARKER in (tmp_path / ".gitignore").read_text()

    def test_target_dir_must_exist(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            init_project(tmp_path / "nope")

    def test_idempotent_default(self, tmp_path: Path) -> None:
        init_project(tmp_path)
        (tmp_path / "iceberg-ops.yaml").write_text("custom: value\n")
        init_project(tmp_path)  # second call, no force
        assert "custom: value" in (tmp_path / "iceberg-ops.yaml").read_text()

    def test_force_overwrites(self, tmp_path: Path) -> None:
        init_project(tmp_path)
        (tmp_path / "iceberg-ops.yaml").write_text("custom: value\n")
        init_project(tmp_path, force=True)
        # Default config template now in place.
        assert "iceberg-ops configuration" in (tmp_path / "iceberg-ops.yaml").read_text()

    def test_gitignore_block_not_duplicated(self, tmp_path: Path) -> None:
        init_project(tmp_path)
        first = (tmp_path / ".gitignore").read_text()
        init_project(tmp_path)
        second = (tmp_path / ".gitignore").read_text()
        assert first == second
        assert second.count(GITIGNORE_MARKER) == 1

    def test_appends_to_existing_gitignore(self, tmp_path: Path) -> None:
        gitignore = tmp_path / ".gitignore"
        gitignore.write_text("# user content\n*.log\n")
        init_project(tmp_path)
        text = gitignore.read_text()
        assert "*.log" in text  # original preserved
        assert GITIGNORE_MARKER in text


class TestCreateRevision:
    def test_writes_0001_when_empty(self, tmp_path: Path) -> None:
        d = tmp_path / "migrations"
        d.mkdir()
        path = create_revision(message="create users", migrations_dir=d)
        assert Path(path).name == "0001_create_users.py"
        body = Path(path).read_text(encoding="utf-8")
        assert "from iceberg_ops.migration import" in body
        module = _load_module(path)
        assert module.Migration.version == "0001"
        assert module.Migration.description == "create users"
        assert module.__doc__ == "Migration 0001 - create users."

    @pytest.mark.parametrize("message", _TRICKY_MESSAGES)
    def test_description_escaped(self, tmp_path: Path, message: str) -> None:
        d = tmp_path / "migrations"
        d.mkdir()
        path = create_revision(message=message, migrations_dir=d)
        source = Path(path).read_text(encoding="utf-8")
        compile(source, path, "exec")
        module = _load_module(path)
        assert module.Migration.description == message
        assert module.Migration.version == "0001"
        assert module.__doc__.startswith("Migration 0001 - ")
        assert "\n" not in module.__doc__

    def test_increments_version(self, tmp_path: Path) -> None:
        d = tmp_path / "migrations"
        d.mkdir()
        (d / "0001_existing.py").write_text("# placeholder")
        path = create_revision(message="next thing", migrations_dir=d)
        assert Path(path).name == "0002_next_thing.py"

    def test_slugifies_message(self, tmp_path: Path) -> None:
        d = tmp_path / "migrations"
        d.mkdir()
        path = create_revision(message="Add User Profiles!", migrations_dir=d)
        assert Path(path).name == "0001_add_user_profiles.py"

    def test_empty_slug_falls_back_to_migration(self, tmp_path: Path) -> None:
        d = tmp_path / "migrations"
        d.mkdir()
        path = create_revision(message="!!!", migrations_dir=d)
        assert Path(path).name == "0001_migration.py"

    def test_namespace_subdir_used_when_present(self, tmp_path: Path) -> None:
        d = tmp_path / "migrations"
        d.mkdir()
        (d / "sales").mkdir()
        path = create_revision(message="x", migrations_dir=d, namespace="sales")
        assert Path(path).parent.name == "sales"

    def test_namespace_falls_back_to_flat_when_no_subdir(self, tmp_path: Path) -> None:
        d = tmp_path / "migrations"
        d.mkdir()
        path = create_revision(message="x", migrations_dir=d, namespace="sales")
        assert Path(path).parent == d.resolve()

    def test_missing_dir_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            create_revision(message="x", migrations_dir=tmp_path / "nope")


class TestPrivateHelpers:
    def test_slugify_collapses_runs(self) -> None:
        assert _slugify("Hello, World!") == "hello_world"
        assert _slugify("---x---") == "x"
        assert _slugify("!!!") == ""

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("plain", "plain"),
            ("l1\n\tl2  l3", "l1 l2 l3"),
            ("nul\x00x", "nul x"),
            ("back\\slash", "back\\\\slash"),
            ('tri """ q', 'tri \\"\\"\\" q'),
            ("  padded  ", "padded"),
        ],
    )
    def test_docstring_text(self, text: str, expected: str) -> None:
        assert _docstring_text(text) == expected

    def test_slugify_caps_length(self) -> None:
        result = _slugify("a" * 100)
        assert len(result) == 50

    def test_next_version_empty_dir(self, tmp_path: Path) -> None:
        d = tmp_path / "migrations"
        d.mkdir()
        assert _next_version(d) == "0001"

    def test_next_version_after_existing(self, tmp_path: Path) -> None:
        d = tmp_path / "migrations"
        d.mkdir()
        (d / "0001_x.py").write_text("")
        (d / "0003_y.py").write_text("")  # gap is fine, take max + 1
        (d / "README.md").write_text("")
        assert _next_version(d) == "0004"

    def test_next_version_ignores_files_the_runner_skips(self, tmp_path: Path) -> None:
        d = tmp_path / "migrations"
        d.mkdir()
        (d / "0001_x.py").write_text("")
        (d / "0007_.py").write_text("")  # no slug: the runner does not discover it
        assert _next_version(d) == "0002"

    def test_version_rule_matches_runner_discovery(self) -> None:
        from iceberg_ops import runner, scaffolding

        assert scaffolding._VERSION_RE.pattern == runner._MIGRATION_FILENAME_RE.pattern
