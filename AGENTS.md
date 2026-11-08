# AGENTS.md

Guide for coding agents (and humans) changing this repository. For using
iceberg-ops in your own project, see the README and the docs instead; a
skill for agents that write migrations lives in
`agent-skills/iceberg-ops-migrations/SKILL.md`.

iceberg-ops is a pure-Python library and CLI (`iceberg-ops`) for
Alembic-style schema migrations on Apache Iceberg tables, built on
PyIceberg and PyArrow. It also ships a cross-table data mover, basic
maintenance helpers and an experimental Apache Polaris policy executor.
Python 3.12+, `pyiceberg>=0.12.0,<0.13`, `pyarrow>=18`.

## Layout

```text
iceberg_ops/
  core/            catalog.py (CatalogFactory), schema.py (diff, SchemaContract,
                   SchemaEvolutionHelper), types.py (Arrow <-> Iceberg types),
                   utils.py (snapshot / upsert / time-travel helpers)
  migration.py     BaseMigration and MigrationContext (the migration-file API)
  history.py       MigrationHistory: the Iceberg table that records migrations
  runner.py        MigrationRunner: discover, migrate, rollback, stamp, plan
  scaffolding.py   `init` / `revision` file templates
  autogen.py       schema diff -> migration ops -> generated migration file
  cli.py           the Click CLI (the only module that imports click)
  data_migration/  reconciler, checkpoint, DataMigrator (`migrate-data`)
  maintenance/     compaction, snapshot expiry, orphan removal, health report
  polaris/         Polaris REST client, policy model, dispatcher (experimental)
  testing.py       public test doubles (FakeSession / FakeResponse for HTTP)
tests/             pytest suite; tests/conftest.py holds the catalog fixtures,
                   tests/migrations/ holds migration files used by the tests
docs/              Sphinx site; docs/api/*.rst are automodule pages
.github/workflows/ ci.yml (gates), release.yml (tag-driven), docs.yml (Pages)
.github/scripts/   check_dist.py: verifies the built sdist and wheel
```

## Layering rules

- `core` depends only on PyIceberg, PyArrow and the standard library. It
  never imports from the rest of the package.
- `migration`, `history` and `runner` build on `core`. `autogen` builds on
  `core` and `migration`. `data_migration` and `maintenance` use PyIceberg
  and `core` only. `polaris.dispatcher` calls into `maintenance`.
- `cli.py` is the top layer: nothing else imports it, and it is the only
  module that imports `click`. Subcommands import their library modules
  inside the command function so `iceberg-ops --help` stays fast.
- Optional backends are imported lazily: catalog backends inside
  `CatalogFactory`'s `_build_*` helpers (through `_lazy_import`, which
  raises an `ImportError` naming the extra to install), and optional
  libraries such as `requests` inside the functions that use them.
  `import iceberg_ops` must work on a bare `pip install iceberg-ops`.
- Library code logs through `logging.getLogger(__name__)`. Only the CLI
  and functions whose job is to display something (the runner's `status`,
  `history` and `plan`, the table-printing helpers in `core.utils`) write
  to stdout.
- Do not add runtime dependencies without discussing it in an issue first.
  Development tools go in the `dev` extra; docs tools in the `docs` extra.

## Gates

Set up once:

```bash
python -m venv .venv
# Windows (PowerShell): .venv\Scripts\Activate.ps1
# macOS / Linux:        source .venv/bin/activate
pip install -e ".[dev]"
```

These are the commands `.github/workflows/ci.yml` runs; all must pass:

```bash
ruff check .
ruff format --check .
mypy iceberg_ops/
pytest -W error::DeprecationWarning
```

When docs or docstrings change, also run (after `pip install -e ".[docs]"`):

```bash
sphinx-build -W --keep-going -b html docs docs/_build/html
```

When packaging changes, the CI package job runs:

```bash
python -m build
twine check --strict dist/*
check-wheel-contents dist/*.whl
python .github/scripts/check_dist.py dist
```

CI runs the tests on Python 3.12, 3.13 and 3.14 (Ubuntu), Python 3.12
(Windows), and once at the lowest declared dependency versions. Ruff and
mypy settings live in `pyproject.toml`.

## Tests

- **Use real PyIceberg catalogs; never mock PyIceberg.** The fixtures in
  `tests/conftest.py` are `memory_catalog` (in-memory SQLite catalog, the
  default choice), `sqlite_catalog` (file-backed, under `tmp_path`) and
  `make_table` (creates a namespace and table).
- Use `sqlite_catalog` for anything that re-opens a catalog, and for every
  CLI test: the in-memory catalog does not survive a re-open. Point the CLI
  at the same files with `--catalog sqlite --db-path ... --warehouse ...`.
- CLI tests use `click.testing.CliRunner` inside `tmp_path` with
  `monkeypatch.chdir(tmp_path)` (the `cli_cwd` fixture in
  `tests/test_cli.py`), because the CLI resolves `iceberg-ops.yaml` and
  `./migrations` against the working directory. Do not use
  `CliRunner.isolated_filesystem()`; it is deprecated and the suite runs
  with `-W error::DeprecationWarning`.
- Tests that import a target schema through `--target-schema-arrow
  module:attr` need a module name unique to the test, or `sys.modules`
  leaks one test's module into the next.
- HTTP services are stubbed, not called: pass
  `iceberg_ops.testing.FakeSession` as the session of the Polaris client and
  assert on its recorded calls.
- Crash and resume paths are tested with small wrappers that raise after N
  calls, not by killing processes.
- Use `pytest.raises(SomeSpecificError)`, never `pytest.raises(Exception)`.
- Add fixtures that only one file needs to that test file, not to
  `conftest.py`. Benchmarks carry the `bench` marker and are skipped by
  default (`pytest -m bench` runs them).
- Every behaviour change comes with a test.

## Code conventions

- Every public module, class and function has a NumPy-style docstring
  (Parameters / Returns / Raises sections). In docstrings, wrap signatures
  that contain a bare `*` in double backticks, or Sphinx reads it as
  emphasis.
- `from __future__ import annotations` in every module; PEP 604 / 585
  types (`str | None`, `list[str]`); `collections.abc` for `Callable` and
  `Iterable`; `StrEnum` rather than `class X(str, Enum)`; exception names
  end in `Error`.
- Use `datetime.now(datetime.UTC)`, never `datetime.utcnow()`.
- `upgrade()` in a migration must be idempotent. `downgrade()` raises
  `NotImplementedError` when it cannot restore the previous state; never
  leave a silent no-op downgrade.
- User-visible changes get a line under `## [Unreleased]` in
  `CHANGELOG.md`.

## Pitfalls worth knowing

- **Click renders a subcommand's `--help` before the group body runs**, so
  the CLI's switch to UTF-8 output has not happened yet. Keep option help
  text and command docstrings ASCII-only, or Windows consoles using cp1252
  crash. Output printed inside command bodies may use UTF-8.
- **Windows paths:** `add_files` and similar APIs need `file://` URIs; build
  them with `pathlib.Path(...).as_uri()`, not by string concatenation.
- `PartitionSpec.is_unpartitioned()` is a method. Without the parentheses
  you get a bound method, which is always truthy.
- PyIceberg's writers update the table object in place but do not always
  reload catalog state: call `table.refresh()` before reading
  `current_snapshot()` after a write.
- A rename plus an add of the old name cannot share one `update_schema()`
  call; it takes two commits (rename, then add).
- Use the public `Table.update_sort_order()`; do not reach into private
  sort-order attributes.
- `to_arrow_batch_reader()` yields whole row groups, which can be far
  larger than a target batch. Slice with `pa.RecordBatch.slice()` to
  enforce a row count.
- Required Iceberg fields map to `nullable=False` Arrow fields. Build
  output batches with an explicit `pa.Schema` so append-time schema checks
  pass.
- PyIceberg's type stubs are narrower than its runtime (for example,
  expression constructors called with column-name strings); a targeted
  `# type: ignore[...]` is acceptable there. `requests` has no stubs here
  and is imported with `# type: ignore[import-untyped]`.
- Sphinx: do not add a package-level `automodule` next to submodule
  `automodule` pages (duplicate objects break `-W` builds), and avoid
  attribute names that shadow builtins such as `type`.
- S3 Tables accepts single-level namespaces only.
