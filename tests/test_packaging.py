"""Static checks on pyproject.toml and the distribution verifier, so
packaging drift fails in CI."""

from __future__ import annotations

import importlib.util
import io
import re
import tarfile
import tomllib
import zipfile
from pathlib import Path
from types import ModuleType

import pytest
import yaml
from packaging.requirements import Requirement

import iceberg_ops

ROOT = Path(__file__).resolve().parents[1]
PYICEBERG_SPEC = ">=0.12.0,<0.13"


@pytest.fixture(scope="module")
def project() -> dict:
    """Return the parsed ``pyproject.toml``."""
    return tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def _all_requirements(project: dict) -> list[tuple[str, Requirement]]:
    reqs = [("", Requirement(r)) for r in project["project"]["dependencies"]]
    for extra, items in project["project"]["optional-dependencies"].items():
        reqs += [(extra, Requirement(r)) for r in items]
    return reqs


def test_single_console_script(project: dict) -> None:
    """Only the ``iceberg-ops`` console script is installed."""
    assert project["project"]["scripts"] == {"iceberg-ops": "iceberg_ops.cli:main"}


def test_version_is_single_sourced(project: dict) -> None:
    """The version comes from ``iceberg_ops/__init__.py`` only."""
    assert "version" not in project["project"]
    assert project["project"]["dynamic"] == ["version"]
    assert project["tool"]["hatch"]["version"]["path"] == "iceberg_ops/__init__.py"


def test_pyiceberg_pins_agree(project: dict) -> None:
    """Every pyiceberg requirement carries the same range and no bogus extra."""
    expected = Requirement(f"pyiceberg{PYICEBERG_SPEC}").specifier
    for extra, req in _all_requirements(project):
        if req.name == "pyiceberg":
            assert req.specifier == expected, (extra, str(req))
            assert "rest" not in req.extras, (extra, str(req))


@pytest.mark.parametrize(
    ("extra", "pyiceberg_extra"),
    [("s3tables", "rest-sigv4"), ("transforms", "pyiceberg-core"), ("sqlite", "sql-sqlite")],
)
def test_backend_extras(project: dict, extra: str, pyiceberg_extra: str) -> None:
    """Backend extras map to the real PyIceberg extras."""
    reqs = [Requirement(r) for r in project["project"]["optional-dependencies"][extra]]
    assert any(r.name == "pyiceberg" and pyiceberg_extra in r.extras for r in reqs)


def test_core_dependencies(project: dict) -> None:
    """The CLI config file and Windows time zones work on a bare install."""
    names = {Requirement(r).name for r in project["project"]["dependencies"]}
    assert {"pyiceberg", "pyarrow", "click", "pyyaml", "tzdata"} <= names
    assert "maintenance" not in project["project"]["optional-dependencies"]


def test_click_floor_supports_separate_stderr(project: dict) -> None:
    """The Click floor is one whose ``CliRunner`` captures stderr separately.

    Click 8.1's ``CliRunner`` mixes stderr into stdout, so ``Result.stderr``
    raises and the lowest-direct CI job fails on 8.1.x.
    """
    click = next(
        Requirement(r)
        for r in project["project"]["dependencies"]
        if Requirement(r).name == "click"
    )
    assert not click.specifier.contains("8.1.8"), str(click.specifier)
    assert click.specifier.contains("8.2.0"), str(click.specifier)


def test_license_and_classifiers(project: dict) -> None:
    """PEP 639 licence metadata and honest classifiers."""
    meta = project["project"]
    assert meta["license"] == "MIT"
    assert meta["license-files"] == ["LICENSE"]
    classifiers = meta["classifiers"]
    assert not [c for c in classifiers if c.startswith("License ::")]
    assert "Development Status :: 3 - Alpha" in classifiers
    assert "Programming Language :: Python :: 3.14" in classifiers
    assert "Programming Language :: Python :: 3.15" not in classifiers
    assert "Typing :: Typed" in classifiers


def test_summary_and_keywords(project: dict) -> None:
    """The PyPI summary leads with schema migrations."""
    meta = project["project"]
    assert meta["description"].startswith("Alembic-style schema migrations")
    assert "schema-migration" in meta["keywords"]


def test_project_urls(project: dict) -> None:
    """The five Project-URL labels PyPI shows in the sidebar."""
    urls = project["project"]["urls"]
    assert set(urls) == {"Homepage", "Documentation", "Changelog", "Source", "Issues"}
    assert urls["Documentation"] == "https://namitkewat.github.io/iceberg-ops/"
    assert all(u.startswith("https://") for u in urls.values())


def test_sdist_allow_list_is_anchored(project: dict) -> None:
    """Unanchored hatch patterns match nested files of the same name."""
    include = project["tool"]["hatch"]["build"]["targets"]["sdist"]["include"]
    assert include and all(p.startswith("/") for p in include)


def test_pre_commit_ruff_matches_dev_pin(project: dict) -> None:
    """The pre-commit ruff hook runs the same minor version as CI."""
    cfg = ROOT / ".pre-commit-config.yaml"
    if not cfg.exists():  # not shipped in the sdist
        pytest.skip("no .pre-commit-config.yaml in this tree")
    rev = re.search(r"ruff-pre-commit\s+rev:\s*v(\d+\.\d+)", cfg.read_text(encoding="utf-8"))
    ruff = next(
        Requirement(r)
        for r in project["project"]["optional-dependencies"]["dev"]
        if Requirement(r).name == "ruff"
    )
    assert rev is not None
    assert ruff.specifier.contains(f"{rev.group(1)}.99"), (rev.group(1), str(ruff.specifier))


def test_changelog_has_section_for_final_version() -> None:
    """A final version has the ``## [X.Y.Z]`` section the release notes come from."""
    version = iceberg_ops.__version__
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        pytest.skip(f"{version} is not a final version")
    lines = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8").splitlines()
    starts = [i for i, line in enumerate(lines) if line.startswith(f"## [{version}]")]
    assert len(starts) == 1, f"expected one '## [{version}]' heading in CHANGELOG.md"
    body = []
    for line in lines[starts[0] + 1 :]:
        if line.startswith("## ["):
            break
        body.append(line)
    assert any(line.strip() for line in body), f"empty [{version}] section in CHANGELOG.md"


# ---------------------------------------------------------------------------
# .github/workflows/release.yml: every check that can stop a release must run
# before the irreversible PyPI upload. The workflow is repository-only.
# ---------------------------------------------------------------------------

RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"


@pytest.fixture(scope="module")
def release_jobs() -> dict:
    """Return the ``jobs`` mapping of the release workflow."""
    if not RELEASE_WORKFLOW.exists():  # not shipped in the sdist
        pytest.skip("no .github/workflows/release.yml in this tree")
    return yaml.safe_load(RELEASE_WORKFLOW.read_text(encoding="utf-8"))["jobs"]


def _step_index(steps: list[dict], name: str) -> int:
    return next(i for i, step in enumerate(steps) if step.get("name") == name)


def _steps_running(steps: list[dict], text: str) -> list[int]:
    return [i for i, step in enumerate(steps) if text in step.get("run", "")]


def test_release_notes_extracted_before_publish(release_jobs: dict) -> None:
    """A missing CHANGELOG section fails ``build``, before the PyPI upload."""
    build = release_jobs["build"]["steps"]
    notes = _steps_running(build, "CHANGELOG.md")
    assert len(notes) == 1, notes
    assert notes[0] < _step_index(build, "Upload distributions")
    assert build[notes[0]]["if"] == "steps.tag.outputs.prerelease == 'false'"
    assert release_jobs["publish-pypi"]["needs"] == "build"
    release = release_jobs["github-release"]["steps"]
    assert _steps_running(release, "CHANGELOG.md") == []
    assert any(step.get("with", {}).get("name") == "release-notes" for step in release)


def test_release_requires_tagged_commit_on_main(release_jobs: dict) -> None:
    """``build`` refuses a tag whose commit is not on main, before building."""
    build = release_jobs["build"]["steps"]
    checkout = _step_index(build, "Checkout")
    assert build[checkout]["with"]["fetch-depth"] == 0
    assert build[checkout]["with"]["persist-credentials"] is False
    guard = _steps_running(build, "git merge-base --is-ancestor")
    assert len(guard) == 1, guard
    assert "origin/main" in build[guard[0]]["run"]
    assert checkout < guard[0] < _step_index(build, "Build sdist and wheel")


# ---------------------------------------------------------------------------
# .github/scripts/check_dist.py: the distribution verifier CI runs on the
# built sdist and wheel. The script is repository-only (not in the sdist).
# ---------------------------------------------------------------------------

CHECK_DIST = ROOT / ".github" / "scripts" / "check_dist.py"


@pytest.fixture
def check_dist() -> ModuleType:
    """Load a fresh copy of ``check_dist.py`` (its error list starts empty)."""
    if not CHECK_DIST.exists():  # not shipped in the sdist
        pytest.skip("no .github/scripts/check_dist.py in this tree")
    spec = importlib.util.spec_from_file_location("check_dist", CHECK_DIST)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _errors(module: ModuleType, marker: str) -> list[str]:
    return [e for e in module.errors if marker in e]


@pytest.mark.parametrize(
    "template",
    ["{}/", "{}", "/{}/", "**/{}", "**/{}/", "{}/**", "/{}/*", "!{}/", "{}/notes.txt"],
)
def test_gitignore_flags_local_only_names_in_any_spelling(
    check_dist: ModuleType, template: str
) -> None:
    """Anchored, globbed and nested spellings of a local-only path all fail."""
    names = ["notes", "scratch"]
    text = "# comment\n" + "\n".join(template.format(n) for n in names)
    check_dist.check_gitignore(text, local_only=set(names))
    assert len(_errors(check_dist, "local-only path")) == len(names), check_dist.errors


def test_parse_exclude_reduces_patterns(check_dist: ModuleType) -> None:
    """Comments are skipped; anchors and trailing slashes are dropped; globs stay."""
    text = "# comment\n\n/notes/\nscratch/\n**/drafts/**\n*.bak\n"
    assert check_dist.parse_exclude(text) == {"notes", "scratch", "drafts", "*.bak"}


def test_local_only_names_without_git_checkout(check_dist: ModuleType, tmp_path: Path) -> None:
    """Outside a git checkout there are no local-only names."""
    assert check_dist.local_only_names(tmp_path) == set()


def test_sdist_refuses_local_only_paths(
    check_dist: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A member whose path names a local-only entry fails the sdist check."""
    monkeypatch.setattr(check_dist, "local_only_names", lambda root=None: {"notes", "*.bak"})
    sdist = tmp_path / "iceberg_ops-0.0.0.tar.gz"
    with tarfile.open(sdist, "w:gz") as tar:
        for name in ["pyproject.toml", "docs/notes/a.rst", "tests/x.bak", "docs/index.rst"]:
            info = tarfile.TarInfo(f"iceberg_ops-0.0.0/{name}")
            info.size = 1
            tar.addfile(info, io.BytesIO(b"x"))
    check_dist.check_sdist(str(sdist))
    flagged = {e.rsplit(" ", 1)[1] for e in _errors(check_dist, "forbidden path")}
    assert flagged == {"docs/notes/a.rst", "tests/x.bak"}


@pytest.mark.parametrize(
    "pattern",
    [
        ".private-tool/",
        ".private-tool",
        "/.private-tool/",
        "**/.private-tool/**",
        ".private-tool/settings.json",
        ".secrets.toml",
    ],
)
def test_gitignore_flags_unknown_hidden_names(check_dist: ModuleType, pattern: str) -> None:
    """Any hidden name outside the allow-list fails, file or directory."""
    check_dist.check_gitignore(pattern)
    assert len(_errors(check_dist, "hidden path")) == 1, check_dist.errors


@pytest.mark.parametrize(
    "pattern",
    [
        ".Python",
        ".env",
        ".coverage",
        ".coverage.*",
        ".installed.cfg",
        ".dmypy.json",
        ".DS_Store",
        ".venv/",
        "/.venv",
        "**/.mypy_cache/",
        ".pytest_cache/**",
    ],
)
def test_gitignore_allows_standard_hidden_names(check_dist: ModuleType, pattern: str) -> None:
    """Standard Python and OS hidden names pass in any anchored spelling."""
    check_dist.check_gitignore(pattern)
    assert check_dist.errors == []


def test_repository_gitignore_passes(check_dist: ModuleType) -> None:
    """The ``.gitignore`` that ships in the sdist passes its own check."""
    check_dist.check_gitignore((ROOT / ".gitignore").read_text(encoding="utf-8"))
    assert check_dist.errors == []


def test_sdist_ships_only_top_level_readme_and_changelog(
    check_dist: ModuleType, tmp_path: Path
) -> None:
    """Repository-only Markdown anywhere in the sdist fails the check."""
    files = [
        "README.md",
        "CHANGELOG.md",
        "pyproject.toml",
        "docs/index.rst",
        "docs/notes.md",
        "tests/README.md",
        "iceberg_ops/GUIDE.MD",
        "CONTRIBUTING.md",
    ]
    sdist = tmp_path / "iceberg_ops-0.0.0.tar.gz"
    with tarfile.open(sdist, "w:gz") as tar:
        for name in files:
            info = tarfile.TarInfo(f"iceberg_ops-0.0.0/{name}")
            info.size = 1
            tar.addfile(info, io.BytesIO(b"x"))
    check_dist.check_sdist(str(sdist))
    flagged = {e.rsplit(" ", 1)[1] for e in _errors(check_dist, "unexpected Markdown")}
    assert flagged == {
        "docs/notes.md",
        "tests/README.md",
        "iceberg_ops/GUIDE.MD",
        "CONTRIBUTING.md",
    }


def test_wheel_ships_no_markdown(check_dist: ModuleType, tmp_path: Path) -> None:
    """Any Markdown file inside the wheel fails the check."""
    wheel = tmp_path / "iceberg_ops-0.0.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as zf:
        zf.writestr("iceberg_ops/__init__.py", "")
        zf.writestr("iceberg_ops/py.typed", "")
        zf.writestr("iceberg_ops/README.md", "x")
        zf.writestr("iceberg_ops-0.0.0.dist-info/METADATA", "Metadata-Version: 2.4\n\n")
        zf.writestr(
            "iceberg_ops-0.0.0.dist-info/entry_points.txt",
            "[console_scripts]\niceberg-ops = iceberg_ops.cli:main\n",
        )
    check_dist.check_wheel(str(wheel), check_urls=False)
    assert _errors(check_dist, "unexpected Markdown") == [
        "wheel: unexpected Markdown file iceberg_ops/README.md"
    ]


def test_markdown_check_accepts_the_allow_list(check_dist: ModuleType) -> None:
    """The top-level README and CHANGELOG are the only Markdown the sdist ships."""
    check_dist.check_markdown(
        "sdist", ["README.md", "CHANGELOG.md", "docs/index.rst"], check_dist.SDIST_MARKDOWN
    )
    assert check_dist.errors == []
