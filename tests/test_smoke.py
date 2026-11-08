"""Smoke tests: the package imports and reports a version."""

from __future__ import annotations

import importlib.metadata
import importlib.resources

import iceberg_ops


def test_version_matches_metadata() -> None:
    """``__version__`` is the single source the build reads."""
    assert iceberg_ops.__version__ == importlib.metadata.version("iceberg-ops")


def test_py_typed_marker() -> None:
    """PEP 561 marker ships with the package."""
    assert importlib.resources.files("iceberg_ops").joinpath("py.typed").is_file()
