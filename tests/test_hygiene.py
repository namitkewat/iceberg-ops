"""Repository text hygiene for public files.

Scans every text file git tracks, plus the untracked files that are not
ignored (new files waiting to be added), and fails on:

* the short CLI name that was dropped before the first release (only the
  changelog line recording the rename may mention it);
* an ``iceberg-ops X.Y.Z`` version mention, outside the changelog, that
  does not match ``iceberg_ops.__version__`` (for example a stale
  placeholder in an issue form).

The old name is assembled from parts so this file does not match itself.
The test is skipped outside a git checkout (for example in an unpacked
sdist).
"""

from __future__ import annotations

import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

from iceberg_ops import __version__

ROOT = Path(__file__).resolve().parent.parent

OLD_CLI = "ice" + "ops"

_OLD_CLI = re.compile(rf"(?<![a-z]){OLD_CLI}(?![a-z])", re.IGNORECASE)
# A changelog line that records the rename may name the old CLI.
_OLD_CLI_ALLOWED = re.compile(r"\b(?:dropped|removed|renamed|replaced|no longer)\b", re.I)
# "iceberg-ops 0.1.0", "iceberg-ops==0.1.0", "iceberg-ops v0.1.0rc1", ...
_VERSION_MENTION = re.compile(r"\biceberg-ops\s*(?:==\s*|\s)v?(\d+\.\d+\.\d+[0-9A-Za-z.+-]*)")


@dataclass(frozen=True)
class TextFile:
    """One scanned file."""

    rel: str
    lines: list[str]


def _git_files() -> list[str]:
    git = shutil.which("git")
    if git is None or not (ROOT / ".git").exists():
        pytest.skip("not a git checkout")
    out: set[str] = set()
    for extra in ([], ["--others", "--exclude-standard"]):
        listed = subprocess.run(
            [git, "-C", str(ROOT), "ls-files", "-z", *extra],
            check=True,
            capture_output=True,
        ).stdout
        out.update(name for name in listed.decode("utf-8").split("\0") if name)
    return sorted(out)


def _text_files() -> list[TextFile]:
    files = []
    for rel in _git_files():
        path = ROOT / rel
        if not path.is_file():  # deleted in the working tree
            continue
        data = path.read_bytes()
        if b"\0" in data:
            continue
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            continue
        files.append(TextFile(rel, text.splitlines()))
    return files


@pytest.fixture(scope="module")
def text_files() -> list[TextFile]:
    files = _text_files()
    names = {f.rel for f in files}
    # Guard the scan itself: an empty or partial file list must not pass.
    assert {"README.md", "CHANGELOG.md", "pyproject.toml", "iceberg_ops/cli.py"} <= names
    return files


def test_scan_covers_public_files(text_files: list[TextFile]) -> None:
    names = {f.rel for f in text_files}
    assert any(n.startswith("docs/") and n.endswith(".rst") for n in names)
    assert any(n.startswith("tests/") for n in names)
    assert "tests/test_hygiene.py" in names


def test_old_cli_name(text_files: list[TextFile]) -> None:
    hits = []
    allowed = 0
    for f in text_files:
        for number, line in enumerate(f.lines, start=1):
            if not _OLD_CLI.search(line):
                continue
            if f.rel == "CHANGELOG.md" and _OLD_CLI_ALLOWED.search(line) and allowed == 0:
                allowed += 1
                continue
            hits.append(f"{f.rel}:{number}: {line.strip()!r}")
    assert not hits, "\n".join(hits)


def test_version_mentions_match_package(text_files: list[TextFile]) -> None:
    hits = []
    for f in text_files:
        # The changelog names past releases; this file holds pattern samples.
        if f.rel in {"CHANGELOG.md", "tests/test_hygiene.py"}:
            continue
        for number, line in enumerate(f.lines, start=1):
            for m in _VERSION_MENTION.finditer(line):
                if m.group(1) != __version__:
                    hits.append(f"{f.rel}:{number}: {m.group(0)!r} (package is {__version__})")
    assert not hits, "\n".join(hits)


@pytest.mark.parametrize(
    ("pattern", "sample", "expected"),
    [
        (_OLD_CLI, f"run {OLD_CLI} migrate", True),
        (_OLD_CLI, f"{OLD_CLI.upper()}_TRACEBACK=1", True),
        (_OLD_CLI, "iceberg-ops migrate", False),
        (_VERSION_MENTION, "iceberg-ops 0.1.0, pyiceberg 0.12.0", True),
        (_VERSION_MENTION, "pip install iceberg-ops==1.2.3rc1", True),
        (_VERSION_MENTION, "iceberg-ops 0.1 supports", False),
        (_VERSION_MENTION, "iceberg-ops migrate --target 0001", False),
    ],
)
def test_patterns(pattern: re.Pattern[str], sample: str, expected: bool) -> None:
    """The patterns catch what they are for and nothing nearby."""
    assert bool(pattern.search(sample)) is expected
