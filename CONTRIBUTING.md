# Contributing to iceberg-ops

Thanks for helping! Useful contributions, roughly in order of impact:

1. **Catalog compatibility reports**: run the quickstart against your catalog
   (Polaris, Lakekeeper, Nessie, Glue, S3 Tables, Hive, ...) and file a
   "Catalog compatibility report" issue, whether it worked or not.
2. **Bug reports** with a minimal migration file or CLI invocation.
3. **Docs**: anything that confused you is a docs bug.
4. **Code**: start with issues labelled `good first issue` or `help wanted`.
   For larger changes, open a Discussion (Ideas) or an issue first.

See [ROADMAP.md](ROADMAP.md) for where the project is heading.

## Development setup

```bash
git clone https://github.com/namitkewat/iceberg-ops.git
cd iceberg-ops
python -m venv .venv
# Windows (PowerShell): .venv\Scripts\Activate.ps1
# macOS / Linux:        source .venv/bin/activate
pip install -e ".[dev]"     # add ",docs" to build the documentation
```

Optional: `pip install pre-commit && pre-commit install` runs the same ruff
version as CI on every commit.

## The gates

CI runs these on every pull request: tests on Python 3.12, 3.13 and 3.14
(Ubuntu) and Python 3.12 (Windows), plus a run at the lowest declared
dependency versions. Run them locally before you push:

```bash
ruff check .
ruff format --check .
mypy iceberg_ops/
pytest -W error::DeprecationWarning
sphinx-build -W --keep-going -b html docs docs/_build/html   # needs .[docs]
```

CI also builds the sdist and wheel and checks them with
`python .github/scripts/check_dist.py dist`.

## Conventions

- **Tests use real PyIceberg catalogs, no mocking of PyIceberg.** Use the
  `memory_catalog` fixture for most tests and `sqlite_catalog` for anything
  that persists or re-opens the catalog, which includes every CLI test.
  CLI tests run in `tmp_path` with `monkeypatch.chdir(tmp_path)` (the
  `cli_cwd` fixture in `tests/test_cli.py`). External HTTP services are
  stubbed, not called: the Polaris client tests pass
  `iceberg_ops.testing.FakeSession` as the HTTP session.
- **Public functions and classes get NumPy-style docstrings** (Parameters,
  Returns, Raises).
- **CLI help text and command docstrings must be ASCII-only.** Click renders
  a subcommand's `--help` before the group body runs, so the CLI's switch to
  UTF-8 output has not happened yet; non-ASCII glyphs in help text crash on
  Windows consoles that use cp1252. Output printed inside command bodies may
  use UTF-8.
- On Windows, build `file://` URIs with `pathlib.Path(...).as_uri()`.
- Use `datetime.now(datetime.UTC)`, not `datetime.utcnow()`.
- Optional catalog backends (glue, rest, hive, s3tables, sqlite) are imported
  lazily inside `CatalogFactory`'s `_build_*` helpers, and optional libraries
  such as `requests` (Polaris client) are imported inside the functions that
  use them, so `import iceberg_ops` always works on a bare install.
- Please discuss new runtime dependencies in an issue before adding them.
  PyIceberg pin changes are made by hand together with a full test run, not
  by merging automated dependency pull requests.

## Commits and pull requests

- Commit subjects follow the existing style: `feat: ...`, `fix: ...`,
  `docs: ...`, `test: ...`, `ci: ...`, `chore: ...`.
- Add a line under `## [Unreleased]` in `CHANGELOG.md` for user-visible
  changes.
- Keep PRs focused; one logical change per PR.
- By submitting a contribution you agree it is licensed under this
  repository's MIT License (inbound = outbound, GitHub Terms of Service
  section D.6) and that you have the right to submit it; no CLA or sign-off
  is required.

## Releasing

Releases are cut by the maintainer by pushing a tag; the `Release`
workflow (`.github/workflows/release.yml`) runs the gates, builds once and
publishes with PyPI Trusted Publishing.

- One-time setup, before the first tag:
  - Create the GitHub environments `pypi` and `testpypi`, each with a
    required reviewer (the maintainer) and a deployment tag rule `v*`. On
    the Free plan both protections need a public repository; without them
    GitHub creates the environments unprotected on the first tag push.
  - Register matching Trusted Publishers on PyPI and TestPyPI: project
    `iceberg-ops`, owner `namitkewat`, repository `iceberg-ops`, workflow
    `release.yml`, environment `pypi` or `testpypi`.
- Release tags must point at a commit on `main`; the workflow refuses
  any other commit.
- Order: first a dry run with `vX.Y.ZrcN` (published to TestPyPI only),
  then the final `vX.Y.Z` (PyPI and a GitHub Release).
- Before tagging, `CHANGELOG.md` must have a `## [X.Y.Z] - YYYY-MM-DD`
  section; its text becomes the GitHub Release notes, and the workflow
  fails without it.

## Getting help

- Usage questions: GitHub Discussions -> Q&A.
- "Does it work with catalog X?": check existing "Catalog compatibility
  report" issues, or file one.
- General PyIceberg / Iceberg questions (not specific to iceberg-ops): the
  Apache Iceberg Slack, #python channel, and the PyIceberg docs.
- There is no commercial support or SLA.

## Code of conduct

Everyone taking part is expected to follow the
[Code of Conduct](CODE_OF_CONDUCT.md).

## Security

Please do not open public issues for vulnerabilities. See
[SECURITY.md](SECURITY.md).
