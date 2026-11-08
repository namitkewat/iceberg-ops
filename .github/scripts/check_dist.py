"""Verify built distributions before they are uploaded.

Usage: python .github/scripts/check_dist.py DIST_DIR [--check-urls]

Checks the single sdist and wheel in DIST_DIR against the packaging
contract: the sdist allow-list, no local-only or hidden paths, no
Markdown in the wheel and none in the sdist beyond the top-level
README.md and CHANGELOG.md, PEP 639 licence metadata, one console
script, the expected extras and pins, and the Project-URL labels. With
``--check-urls`` every Project-URL must also answer with a non-error
HTTP status. Exits 1 and prints one ``::error::`` line per problem.

Local-only paths are whatever the checkout's ``.git/info/exclude``
lists: those files exist only on one machine, so no distribution may
contain them and the shipped ``.gitignore`` must not name them.
"""

from __future__ import annotations

import email.parser
import fnmatch
import glob
import subprocess
import sys
import tarfile
import urllib.request
import zipfile
from pathlib import Path

from packaging.requirements import Requirement

SDIST_TOP_LEVEL = {
    "iceberg_ops",
    "tests",
    "docs",
    "README.md",
    "CHANGELOG.md",
    "LICENSE",
    "pyproject.toml",
    "PKG-INFO",
    ".gitignore",
}
REPO_ROOT = Path(__file__).resolve().parents[2]
# Paths that must never appear in either distribution.
NEVER = (
    "local_data/",
    "docs/_build/",
    "__pycache__/",
    ".coverage",
)
# Hidden names a public .gitignore may use (compared after normalisation,
# see _normalise_pattern): standard Python tooling caches and files,
# virtual environments, editor folders and OS files. Any other hidden name
# in the shipped .gitignore points at local-only tooling.
GITIGNORE_HIDDEN_NAMES = {
    ".cache",
    ".coverage",
    ".coverage.*",
    ".dmypy.json",
    ".DS_Store",
    ".eggs",
    ".env",
    ".hypothesis",
    ".idea",
    ".installed.cfg",
    ".ipynb_checkpoints",
    ".mypy_cache",
    ".nox",
    ".pyre",
    ".pytest_cache",
    ".pytype",
    ".Python",
    ".ruff_cache",
    ".tox",
    ".venv",
    ".vscode",
}
# Markdown files the sdist may ship (top level only). The wheel ships none.
SDIST_MARKDOWN = {"README.md", "CHANGELOG.md"}
PYICEBERG_SPEC = ">=0.12.0,<0.13"
EXPECTED_EXTRAS = {
    "sqlite",
    "contracts",
    "glue",
    "rest",
    "hive",
    "s3tables",
    "polaris",
    "transforms",
    "docs",
    "dev",
}
EXPECTED_URLS = {"Homepage", "Documentation", "Changelog", "Source", "Issues"}

errors: list[str] = []


def fail(msg: str) -> None:
    """Record one verification failure."""
    errors.append(msg)


def parse_exclude(text: str) -> set[str]:
    """Return the names a ``.git/info/exclude`` file lists.

    Parameters
    ----------
    text : str
        Contents of the exclude file.

    Returns
    -------
    set of str
        Each non-comment pattern reduced by :func:`_normalise_pattern`
        (globs such as ``*.bak`` are kept as globs).
    """
    names = set()
    for raw in text.splitlines():
        pattern = raw.strip()
        if pattern and not pattern.startswith("#"):
            names.add(_normalise_pattern(pattern))
    return names


def local_only_names(root: Path = REPO_ROOT) -> set[str]:
    """Return the local-only names of the checkout at *root*.

    Reads the checkout's ``info/exclude`` file (found through
    ``git rev-parse --git-path`` so linked worktrees work too). Outside
    a git checkout, or when git is missing, the set is empty.

    Parameters
    ----------
    root : pathlib.Path, optional
        Repository root. Defaults to the checkout this script lives in.

    Returns
    -------
    set of str
        Normalised patterns, as returned by :func:`parse_exclude`.
    """
    try:
        out = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--git-path", "info/exclude"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return set()
    path = Path(out)
    if not path.is_absolute():
        path = root / path
    try:
        return parse_exclude(path.read_text(encoding="utf-8"))
    except OSError:
        return set()


def _is_local_only(path: str, local_only: set[str]) -> bool:
    """Return True when any component of *path* matches a local-only name."""
    norm = _normalise_pattern(path)
    parts = norm.split("/")
    return any(
        fnmatch.fnmatchcase(part, name) or norm == name for name in local_only for part in parts
    )


def _hidden_component(path: str) -> str | None:
    """Return the first hidden path component of *path*, if any."""
    return next((part for part in path.split("/") if part.startswith(".")), None)


def _normalise_pattern(pattern: str) -> str:
    """Reduce a ``.gitignore`` pattern to the path it names.

    Drops a negation ``!``, any leading ``/`` or ``**/`` anchor and any
    trailing ``/**``, ``/*`` or ``/``, so ``/name/``, ``**/name``,
    ``name/**`` and ``!name/*`` all become ``name``.

    Parameters
    ----------
    pattern : str
        One stripped, non-comment line of a ``.gitignore`` file.

    Returns
    -------
    str
        The pattern without anchors, negation and trailing globs.
    """
    norm = pattern.removeprefix("!")
    while True:
        prev = norm
        norm = norm.removeprefix("/").removeprefix("**/")
        for suffix in ("/**", "/*", "/"):
            norm = norm.removesuffix(suffix)
        if norm == prev:
            return norm


def check_gitignore(text: str, local_only: set[str] | None = None) -> None:
    """Fail when the shipped ``.gitignore`` names local-only files.

    Parameters
    ----------
    text : str
        Contents of the shipped ``.gitignore``.
    local_only : set of str, optional
        Local-only names; defaults to :func:`local_only_names`.
    """
    if local_only is None:
        local_only = local_only_names()
    for raw in text.splitlines():
        pattern = raw.strip()
        if not pattern or pattern.startswith("#"):
            continue
        norm = _normalise_pattern(pattern)
        first = norm.split("/", 1)[0]
        if _is_local_only(norm, local_only):
            fail(f"sdist: shipped .gitignore names local-only path {pattern!r}")
        if pattern.lower().endswith(".md"):
            fail(f"sdist: shipped .gitignore ignores Markdown files {pattern!r}")
        if first.startswith(".") and first not in GITIGNORE_HIDDEN_NAMES:
            fail(f"sdist: shipped .gitignore names hidden path {pattern!r}")


def check_markdown(dist: str, names: list[str], allowed: set[str]) -> None:
    """Fail on every Markdown file in *names* that is not in *allowed*.

    Repository-only documents (contributor guides, policies, agent
    guides) must not leak into a distribution through a loose include.

    Parameters
    ----------
    dist : str
        Distribution label used in the error message (``"sdist"`` or
        ``"wheel"``).
    names : list of str
        Archive member paths, relative to the distribution root.
    allowed : set of str
        Exact relative paths of the Markdown files that may ship.
    """
    for n in names:
        if n.lower().endswith(".md") and n not in allowed:
            fail(f"{dist}: unexpected Markdown file {n}")


def check_sdist(path: str) -> None:
    """Check the sdist file list and its shipped ``.gitignore``."""
    with tarfile.open(path) as tar:
        members = [m for m in tar.getmembers() if "/" in m.name]
        names = [m.name.split("/", 1)[1] for m in members]
        gi = next((m for m in members if m.name.split("/", 1)[1] == ".gitignore"), None)
        handle = tar.extractfile(gi) if gi else None
        gitignore = handle.read().decode() if handle else ""
    local_only = local_only_names()
    check_gitignore(gitignore, local_only)
    tops = {n.split("/", 1)[0] for n in names if n}
    if extra := sorted(tops - SDIST_TOP_LEVEL):
        fail(f"sdist: unexpected top-level entries {extra}")
    check_markdown("sdist", names, SDIST_MARKDOWN)
    for n in names:
        if any(bad in n for bad in NEVER) or _is_local_only(n, local_only):
            fail(f"sdist: forbidden path {n}")
        if n != ".gitignore" and (hidden := _hidden_component(n)):
            fail(f"sdist: hidden path component {hidden!r} in {n}")


def check_wheel(path: str, check_urls: bool) -> None:
    """Check wheel contents and core metadata."""
    with zipfile.ZipFile(path) as zf:
        names = zf.namelist()
        meta_name = next(n for n in names if n.endswith(".dist-info/METADATA"))
        meta = email.parser.Parser().parsestr(zf.read(meta_name).decode())
        ep_name = next(n for n in names if n.endswith(".dist-info/entry_points.txt"))
        entry_points = zf.read(ep_name).decode()
    if "iceberg_ops/py.typed" not in names:
        fail("wheel: iceberg_ops/py.typed missing")
    check_markdown("wheel", names, set())
    local_only = local_only_names()
    for n in names:
        if any(bad in n for bad in NEVER) or _is_local_only(n, local_only):
            fail(f"wheel: forbidden path {n}")
        if hidden := _hidden_component(n):
            fail(f"wheel: hidden path component {hidden!r} in {n}")
    if meta["Metadata-Version"] not in {"2.4", "2.5"}:
        fail(f"wheel: Metadata-Version {meta['Metadata-Version']} not accepted by PyPI")
    if meta["License-Expression"] != "MIT" or meta["License"] is not None:
        fail("wheel: expected License-Expression: MIT and no legacy License field")
    scripts = [ln.split("=")[0].strip() for ln in entry_points.splitlines() if "=" in ln]
    if scripts != ["iceberg-ops"]:
        fail(f"wheel: console scripts {scripts}, expected ['iceberg-ops']")
    if set(meta.get_all("Provides-Extra") or []) != EXPECTED_EXTRAS:
        fail(f"wheel: extras {sorted(meta.get_all('Provides-Extra') or [])}")
    by_extra: dict[str, list[Requirement]] = {}
    for raw in meta.get_all("Requires-Dist") or []:
        req = Requirement(raw)
        extra = None
        if req.marker and "extra ==" in str(req.marker):
            extra = str(req.marker).split("extra ==")[1].strip().strip("'\"")
        by_extra.setdefault(extra or "", []).append(req)
        if req.name == "pyiceberg":
            if str(req.specifier) != str(Requirement(f"x{PYICEBERG_SPEC}").specifier):
                fail(f"wheel: pyiceberg pin drift: {raw}")
            if "rest" in req.extras:
                fail(f"wheel: non-existent pyiceberg extra 'rest': {raw}")

    def has(extra: str, name: str, pyiceberg_extra: str | None = None) -> bool:
        return any(
            r.name == name and (pyiceberg_extra is None or pyiceberg_extra in r.extras)
            for r in by_extra.get(extra, [])
        )

    if not has("s3tables", "pyiceberg", "rest-sigv4"):
        fail("wheel: [s3tables] must require pyiceberg[rest-sigv4]")
    if not has("transforms", "pyiceberg", "pyiceberg-core"):
        fail("wheel: [transforms] must require pyiceberg[pyiceberg-core]")
    if not has("", "pyyaml"):
        fail("wheel: pyyaml must be a core dependency")
    urls = dict(u.split(", ", 1) for u in meta.get_all("Project-URL") or [])
    if missing := EXPECTED_URLS - urls.keys():
        fail(f"wheel: missing Project-URL labels {sorted(missing)}")
    if check_urls:
        for label, url in urls.items():
            try:
                req = urllib.request.Request(url, method="GET")
                with urllib.request.urlopen(req, timeout=20) as resp:
                    if resp.status >= 400:
                        fail(f"url {label} -> HTTP {resp.status}")
            except Exception as exc:
                fail(f"url {label} {url} unreachable: {exc}")


def main(argv: list[str]) -> int:
    """Run every check on DIST_DIR and return the process exit code."""
    dist = argv[1]
    sdists = glob.glob(f"{dist}/*.tar.gz")
    wheels = glob.glob(f"{dist}/*.whl")
    if len(sdists) != 1 or len(wheels) != 1:
        print(f"::error::expected one sdist and one wheel, got {sdists + wheels}")
        return 1
    check_sdist(sdists[0])
    check_wheel(wheels[0], check_urls="--check-urls" in argv)
    for e in errors:
        print(f"::error::{e}")
    if not errors:
        print(f"check_dist: OK ({sdists[0]}, {wheels[0]})")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
