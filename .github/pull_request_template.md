## What and why

<!-- One or two sentences. Link the issue: "Closes #123". -->

## Type

- [ ] Bug fix
- [ ] New feature
- [ ] Docs
- [ ] Refactor / chore

## Checklist

- [ ] Gates pass locally: `ruff check .`, `ruff format --check .`, `mypy iceberg_ops/`, `pytest -W error::DeprecationWarning`, and `sphinx-build -W --keep-going -b html docs docs/_build/html` if docs changed
- [ ] Tests added/updated (real catalogs via the `memory_catalog` / `sqlite_catalog` fixtures; no mocking of PyIceberg)
- [ ] Public API has NumPy-style docstrings; CLI help text is ASCII-only
- [ ] CHANGELOG.md updated under `## [Unreleased]` (if user-visible)
- [ ] No credentials, tokens or private URIs in code, tests or logs
