"""The documentation's examples run against the real code.

Four gates keep the README, the Sphinx pages and the agent guides honest:

* every Python code block runs as written, unless its first line is
  ``# illustrative`` (then it only has to compile);
* every YAML schema contract and config file shown in the docs loads;
* every documented ``iceberg-ops`` command line names real subcommands
  and options, with values the options accept;
* the README and Quick Start walk-throughs replay end to end with
  ``CliRunner`` against a local SQLite catalog.

Blocks run inside ``tmp_path`` with the working directory changed to it.
The blocks of one page share one namespace and run in page order, so a
later block may use names an earlier block defined. A Python block whose
first line is a ``# path/to/file.py`` comment is also written to that
path, so a page's migration files exist when its runner example runs.
The repository-only guides (AGENTS.md and the agent skill) are not in
the sdist; their checks are skipped when the files are absent.
"""

from __future__ import annotations

import os
import re
import shlex
import tempfile
import textwrap
import traceback
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import click
import pytest
from click.testing import CliRunner
from pyiceberg import catalog as pyiceberg_catalog
from pyiceberg.catalog import Catalog
from pyiceberg.utils.config import Config

from iceberg_ops.cli import main

ROOT = Path(__file__).resolve().parent.parent
README = ROOT / "README.md"
QUICKSTART = ROOT / "docs" / "quickstart.rst"
AGENTS = ROOT / "AGENTS.md"
SKILL = ROOT / "agent-skills" / "iceberg-ops-migrations" / "SKILL.md"

# Sphinx highlights ``::`` literal blocks and a ``code-block`` without a
# language with ``highlight_language``; docs/conf.py leaves it at the
# default, which is Python.
PYTHON_LANGS = {"python", "python3", "py", "default"}
SHELL_LANGS = {"bash", "sh", "shell", "console", "shell-session", "powershell", "ps1", "pwsh"}
YAML_LANGS = {"yaml", "yml"}
ILLUSTRATIVE = "# illustrative"
# A first-line comment naming the file a block shows, e.g. "# migrations/0001_x.py".
_PATH_HEADER = re.compile(r"^#\s+(?P<path>[\w.-]+(?:/[\w.-]+)*\.(?:py|ya?ml))\s*$")

# Subcommands the docs announce as planned (README "Next up", ROADMAP).
PLANNED_COMMANDS = {"check"}
# Tokens that mark a usage synopsis such as "iceberg-ops COMMAND --help".
_SYNOPSIS_WORDS = {"COMMAND", "OPTIONS", "GLOBAL"}
# An upper-case placeholder for a value, such as N, V, PATH or SRC:DST.
_PLACEHOLDER = re.compile(r"^[A-Z][A-Z0-9_]*(?::[A-Z][A-Z0-9_]*)?$")


def _existing(*paths: Path) -> list[Path]:
    return [p for p in paths if p.is_file()]


def _rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


SNIPPET_PAGES = _existing(README, SKILL, *sorted((ROOT / "docs").rglob("*.rst")))
COMMAND_PAGES = _existing(
    README, AGENTS, SKILL, ROOT / "CHANGELOG.md", *sorted((ROOT / "docs").rglob("*.rst"))
)
# Pages whose bare option mentions (``--rename-hint``) all mean iceberg-ops
# options. AGENTS.md also names pytest and sphinx-build flags, and the
# changelog may name options that were dropped.
OPTION_PAGES = _existing(README, SKILL, *sorted((ROOT / "docs").rglob("*.rst")))


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Block:
    """One code block of a documentation page."""

    path: Path
    line: int  # 1-based line number of the block's first code line
    lang: str
    code: str

    @property
    def where(self) -> str:
        return f"{_rel(self.path)}:{self.line}"

    @property
    def first_line(self) -> str:
        return next((ln.strip() for ln in self.code.splitlines() if ln.strip()), "")

    @property
    def illustrative(self) -> bool:
        return self.first_line.startswith(ILLUSTRATIVE)

    @property
    def file_path(self) -> str | None:
        """The file the block shows, from a ``# path`` first-line comment."""
        m = _PATH_HEADER.match(self.first_line)
        return m["path"] if m else None


@dataclass(frozen=True)
class Page:
    """A documentation page split into code blocks and prose."""

    path: Path
    blocks: list[Block]
    prose: str  # the page text with every code block blanked out
    lines: list[str]


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip())


_RST_DIRECTIVE = re.compile(
    r"^(?P<indent>\s*)\.\.\s+(?:code-block|code|sourcecode)::\s*(?P<lang>\S*)\s*$"
)
_MD_FENCE = re.compile(r"^(?P<indent>\s*)(?P<fence>`{3,}|~{3,})\s*(?P<lang>[\w+-]*)")


def _rst_page(path: Path) -> Page:
    lines = path.read_text(encoding="utf-8").splitlines()
    blocks: list[Block] = []
    blanked = list(lines)
    i = 0
    while i < len(lines):
        line = lines[i]
        directive = _RST_DIRECTIVE.match(line)
        if directive:
            lang, base = (directive["lang"] or "default").lower(), len(directive["indent"])
        elif line.rstrip().endswith("::") and not line.lstrip().startswith(".."):
            lang, base = "default", _indent(line)
        else:
            i += 1
            continue
        j = i + 1
        body: list[tuple[int, str]] = []
        while j < len(lines) and (not lines[j].strip() or _indent(lines[j]) > base):
            body.append((j, lines[j]))
            j += 1
        if directive:  # directive options such as ":caption:"
            while body and body[0][1].strip().startswith(":"):
                body.pop(0)
        while body and not body[0][1].strip():
            body.pop(0)
        while body and not body[-1][1].strip():
            body.pop()
        if body:
            code = textwrap.dedent("\n".join(text for _, text in body)) + "\n"
            blocks.append(Block(path, body[0][0] + 1, lang, code))
        for k in range(i + 1 if not directive else i, j):
            blanked[k] = ""
        i = j
    return Page(path, blocks, "\n".join(blanked), lines)


def _md_page(path: Path) -> Page:
    lines = path.read_text(encoding="utf-8").splitlines()
    blocks: list[Block] = []
    blanked = list(lines)
    i = 0
    while i < len(lines):
        fence = _MD_FENCE.match(lines[i])
        if not fence:
            i += 1
            continue
        marker = fence["fence"]
        j = i + 1
        while j < len(lines) and not (
            lines[j].strip().startswith(marker) and set(lines[j].strip()) == {marker[0]}
        ):
            j += 1
        body = lines[i + 1 : j]
        if any(text.strip() for text in body):
            code = textwrap.dedent("\n".join(body)) + "\n"
            blocks.append(Block(path, i + 2, (fence["lang"] or "text").lower(), code))
        for k in range(i, min(j + 1, len(lines))):
            blanked[k] = ""
        i = j + 1
    return Page(path, blocks, "\n".join(blanked), lines)


def load_page(path: Path) -> Page:
    """Parse a reStructuredText or Markdown page."""
    return _md_page(path) if path.suffix == ".md" else _rst_page(path)


def python_blocks(page: Page) -> list[Block]:
    return [b for b in page.blocks if b.lang in PYTHON_LANGS]


def inline_literals(page: Page) -> list[tuple[int, str]]:
    """Return ``(line, text)`` for every inline code span of the page's prose.

    Whitespace inside a span (including a line break) collapses to one space.
    """
    pattern = r"``(.+?)``" if page.path.suffix == ".rst" else r"(?<!`)`([^`]+)`(?!`)"
    spans = []
    for m in re.finditer(pattern, page.prose, flags=re.DOTALL):
        line = page.prose.count("\n", 0, m.start()) + 1
        spans.append((line, " ".join(m.group(1).split())))
    return spans


# ---------------------------------------------------------------------------
# Command lines
# ---------------------------------------------------------------------------

_OPERATORS = {"&&", "||", ";", "|", "&"}


def _split_shell(text: str) -> list[list[str]]:
    """Split one logical shell line into simple commands (tokens, no comments)."""
    lexer = shlex.shlex(text, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    commands: list[list[str]] = [[]]
    for token in lexer:
        if token in _OPERATORS:
            commands.append([])
        else:
            commands[-1].append(token)
    return [c for c in commands if c]


def shell_commands(block: Block) -> list[list[str]]:
    """Return the simple commands of a shell block, continuation lines joined."""
    continuation = "`" if block.lang in {"powershell", "ps1", "pwsh"} else "\\"
    logical: list[str] = []
    pending = ""
    for raw in block.code.splitlines():
        text = raw.strip()
        for prompt in ("$ ", "PS> ", "> "):
            if text.startswith(prompt):
                text = text[len(prompt) :]
        if text.endswith(continuation):
            pending += text[: -len(continuation)] + " "
            continue
        logical.append(pending + text)
        pending = ""
    if pending:
        logical.append(pending)
    return [cmd for line in logical for cmd in _split_shell(line)]


@dataclass(frozen=True)
class Command:
    """One documented ``iceberg-ops`` command line."""

    where: str
    tokens: tuple[str, ...]
    inline: bool = False

    @property
    def text(self) -> str:
        return " ".join(self.tokens)

    def check(self) -> list[str]:
        """Run :func:`check_command`; return the resolved subcommand path.

        Inline prose may name an option without its value
        (``rollback --steps`` combined with ``--target``); such a trailing
        option is checked with a placeholder value.
        """
        try:
            return check_command(self.tokens)
        except click.BadOptionUsage:
            if not (self.inline and self.tokens[-1].startswith("-")):
                raise
            return check_command((*self.tokens, "VALUE"))


def documented_commands(page: Page) -> list[Command]:
    """Collect ``iceberg-ops ...`` lines from shell blocks and inline code.

    Prose that only names the project (``iceberg-ops`` alone, or
    ``iceberg-ops.yaml``) is not a command. An inline span that starts
    with a subcommand and then an option (``rollback --target 0000``) is
    read as that subcommand's command line.
    """
    subcommands = set(main.commands)
    found: list[Command] = []
    for block in page.blocks:
        if block.lang not in SHELL_LANGS | {"text"}:
            continue
        for tokens in shell_commands(block):
            if tokens[0] == "iceberg-ops" and len(tokens) > 1:
                found.append(Command(block.where, tuple(tokens)))
    for line, span in inline_literals(page):
        tokens = [t.strip("[]") for t in span.split()]
        tokens = [t for t in tokens if t and t not in {"...", "|"}]
        if len(tokens) > 1 and tokens[0] in subcommands and tokens[1].startswith("-"):
            tokens.insert(0, "iceberg-ops")
        if len(tokens) > 1 and tokens[0] == "iceberg-ops":
            found.append(Command(f"{_rel(page.path)}:{line}", tuple(tokens), inline=True))
    return found


def _is_placeholder(value: str) -> bool:
    return bool(_PLACEHOLDER.match(value))


def _check_values(cmd: click.Command, ctx: click.Context, opts: dict[str, Any]) -> None:
    """Convert documented values for typed options (choices, integers, targets)."""
    for param in cmd.get_params(ctx):
        if not isinstance(param, click.Option) or param.name not in opts or param.is_flag:
            continue
        raw = opts[param.name]
        values = raw if isinstance(raw, list | tuple) else [raw]
        for value in values:
            if not isinstance(value, str) or _is_placeholder(value):
                continue
            converted = value
            if isinstance(param.type, click.Choice | click.types.IntParamType):
                converted = param.type.convert(value, param, ctx)
            if param.callback is not None and not param.is_eager:
                param.callback(ctx, param, converted)


def check_command(tokens: tuple[str, ...]) -> list[str]:
    """Parse *tokens* with Click's own parsers, command by command.

    Returns the resolved subcommand path. Raises ``click.UsageError`` (or
    ``click.BadParameter``) when a subcommand or option does not exist,
    an option lacks its value, or a value is not accepted.
    """
    command: click.Command = main
    ctx = click.Context(main, info_name="iceberg-ops")
    args = list(tokens[1:])
    path: list[str] = []
    while True:
        opts, rest, _ = command.make_parser(ctx).parse_args(args=list(args))
        _check_values(command, ctx, opts)
        if not isinstance(command, click.Group):
            if rest and not any(isinstance(p, click.Argument) for p in command.params):
                raise click.UsageError(f"unexpected extra arguments {rest}", ctx=ctx)
            return path
        if not rest:  # group options only, e.g. "iceberg-ops --version"
            return path
        name = rest[0]
        sub = command.get_command(ctx, name)
        if sub is None:
            raise click.UsageError(f"no such command {name!r}", ctx=ctx)
        path.append(name)
        command = sub
        ctx = click.Context(sub, info_name=name, parent=ctx)
        args = rest[1:]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def docs_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Run inside ``tmp_path`` with a named SQLite catalog ``prod`` configured.

    ``tempfile.mkdtemp()`` (used by several examples and by the memory
    catalog) lands under ``tmp_path``. ``PYICEBERG_HOME`` points at a
    ``.pyiceberg.yaml`` that declares ``prod``, so examples that call
    ``load_catalog("prod")`` get a real catalog; PyIceberg reads that file
    once at import, so its module-level config is rebuilt here.
    """
    for name in list(os.environ):
        if name.upper().startswith(("PYICEBERG_", "ICEBERG_OPS_")):
            monkeypatch.delenv(name)
    scratch = tmp_path / "tmp"
    scratch.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(scratch))

    home = tmp_path / "pyiceberg_home"
    store = tmp_path / "prod_catalog"
    (store / "warehouse").mkdir(parents=True)
    home.mkdir()
    (home / ".pyiceberg.yaml").write_text(
        "catalog:\n"
        "  prod:\n"
        "    type: sql\n"
        f"    uri: 'sqlite:///{store / 'catalog.db'}'\n"
        # The "file://<abspath>" form CatalogFactory writes: PyIceberg maps a
        # canonical Windows file URI (file:///C:/...) to an invalid path.
        f"    warehouse: 'file://{store / 'warehouse'}'\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("PYICEBERG_HOME", str(home))
    monkeypatch.setattr(pyiceberg_catalog, "_ENV_CONFIG", Config())

    work = tmp_path / "work"
    work.mkdir()
    monkeypatch.chdir(work)
    yield work


def _close_catalogs(namespace: dict[str, Any]) -> None:
    for value in namespace.values():
        if isinstance(value, Catalog):
            value.close()


def _run_block(block: Block, namespace: dict[str, Any]) -> None:
    code = compile(block.code, block.where, "exec", dont_inherit=True)
    if block.illustrative:
        return
    if block.file_path:
        target = Path(block.file_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(block.code, encoding="utf-8")
    try:
        exec(code, namespace)
    except Exception:
        pytest.fail(f"{block.where}: example raised\n{traceback.format_exc()}")


# ---------------------------------------------------------------------------
# Python blocks
# ---------------------------------------------------------------------------

_PAGES_WITH_PYTHON = [p for p in SNIPPET_PAGES if python_blocks(load_page(p))]


def test_extractor_finds_the_examples() -> None:
    """Guard the extractor itself: a parsing bug must not empty the suite."""
    blocks = [b for p in SNIPPET_PAGES for b in python_blocks(load_page(p))]
    assert len(blocks) >= 20
    assert {_rel(p) for p in _PAGES_WITH_PYTHON} >= {
        "README.md",
        "docs/quickstart.rst",
        "docs/schema_migrations.rst",
        "docs/autogenerate.rst",
    }


@pytest.mark.parametrize("path", _PAGES_WITH_PYTHON, ids=_rel)
def test_python_blocks_run(path: Path, docs_env: Path) -> None:
    """Every Python block of a page runs, in page order, in one namespace.

    The blocks run in a project that ``iceberg-ops init`` set up, as every
    walk-through starts with that command.
    """
    result = CliRunner().invoke(main, ["init"])
    assert result.exit_code == 0, result.output
    namespace: dict[str, Any] = {"__name__": f"docs_example_{path.stem}"}
    try:
        for block in python_blocks(load_page(path)):
            _run_block(block, namespace)
    finally:
        _close_catalogs(namespace)


_PAGES_WITH_YAML = [
    p for p in SNIPPET_PAGES if any(b.lang in YAML_LANGS for b in load_page(p).blocks)
]


@pytest.mark.parametrize("path", _PAGES_WITH_YAML, ids=_rel)
def test_yaml_blocks_load(path: Path, docs_env: Path) -> None:
    """Schema contracts and config files shown in the docs are valid.

    A config file may only use the keys the CLI knows: an unknown key is
    only a warning at run time, so a typo in the docs would go unnoticed.
    """
    import yaml

    from iceberg_ops.cli import _CONFIG_KEYS, _load_config
    from iceberg_ops.core.schema import SchemaContract

    for block in load_page(path).blocks:
        if block.lang not in YAML_LANGS:
            continue
        name = Path(block.file_path or "").name
        target = docs_env / f"block_{block.line}.yaml"
        target.write_text(block.code, encoding="utf-8")
        is_contract = block.file_path is not None and block.file_path.startswith("contracts/")
        if is_contract:
            contract = SchemaContract.from_yaml(str(target))
            assert contract.columns, block.where
        elif name == "iceberg-ops.yaml" or "namespace:" in block.code:
            assert _load_config(str(target)), block.where
            unknown = set(yaml.safe_load(block.code)) - set(_CONFIG_KEYS)
            assert not unknown, (block.where, unknown)


# ---------------------------------------------------------------------------
# Command lines
# ---------------------------------------------------------------------------


def _all_commands() -> list[Command]:
    return [c for p in COMMAND_PAGES for c in documented_commands(load_page(p))]


@pytest.mark.parametrize(
    "line",
    [
        "iceberg-ops upgrade",  # no such subcommand
        "iceberg-ops migrate --dry-run",  # no such option on migrate
        "iceberg-ops --namespace",  # option without its value
        "iceberg-ops migrate --namespace sales",  # group option after the subcommand
        "iceberg-ops rollback --steps two",  # not an integer
        "iceberg-ops rollback --target v2",  # not a version
        "iceberg-ops migrate-data --write-mode replace",  # not a choice
        "iceberg-ops status extra",  # unexpected argument
    ],
)
def test_command_checker_rejects(line: str) -> None:
    """The checker itself catches the mistakes it exists for."""
    with pytest.raises(click.ClickException):
        check_command(tuple(shlex.split(line)))


def test_command_checker_reads_inline_forms() -> None:
    page = Page(
        ROOT / "docs" / "x.rst",
        [],
        "Run ``rollback --target 0000`` or ``iceberg-ops plan [--target V]``; see "
        "``iceberg-ops.yaml`` and ``status``.",
        [],
    )
    assert [c.text for c in documented_commands(page)] == [
        "iceberg-ops rollback --target 0000",
        "iceberg-ops plan --target V",
    ]
    for command in documented_commands(page):
        command.check()


def test_documented_commands_parse() -> None:
    """Every documented ``iceberg-ops`` line uses real commands and options."""
    commands = _all_commands()
    assert len(commands) >= 40
    problems = []
    for command in commands:
        if _SYNOPSIS_WORDS & set(command.tokens):
            continue
        if command.tokens[1] in PLANNED_COMMANDS:
            assert main.get_command(click.Context(main), command.tokens[1]) is None, (
                f"{command.where}: {command.tokens[1]!r} exists now; drop it from PLANNED_COMMANDS"
            )
            continue
        try:
            command.check()
        except click.ClickException as exc:
            problems.append(f"{command.where}: {command.text}\n    {exc.format_message()}")
    assert not problems, "\n".join(problems)


def test_documented_subcommands_render_help(docs_env: Path) -> None:
    """``--help`` works for every subcommand the docs use."""
    paths = set()
    for command in _all_commands():
        if _SYNOPSIS_WORDS & set(command.tokens) or command.tokens[1] in PLANNED_COMMANDS:
            continue
        paths.add(tuple(command.check()))
    assert len(paths) >= 8
    runner = CliRunner()
    for path in sorted(paths):
        result = runner.invoke(main, [*path, "--help"])
        assert result.exit_code == 0, (path, result.output)


def _known_options() -> set[str]:
    ctx = click.Context(main)
    known = set()
    for command in [main, *main.commands.values()]:
        for param in command.get_params(ctx):
            if isinstance(param, click.Option):
                known.update(param.opts)
                known.update(param.secondary_opts)
    return known


_OPTION_TOKEN = re.compile(r"(?<![\w-])(--[a-z][a-z0-9-]*|-[a-zA-Z])(?![\w-])")


def test_documented_option_names_exist() -> None:
    """Inline mentions such as ``--rename-hint old:new`` name real options."""
    known = _known_options()
    problems = []
    checked = 0
    for path in OPTION_PAGES:
        page = load_page(path)
        for line, span in inline_literals(page):
            if not span.startswith("-"):
                continue
            for option in _OPTION_TOKEN.findall(span):
                checked += 1
                if option not in known:
                    problems.append(f"{_rel(path)}:{line}: {option} in {span!r}")
    assert checked >= 30
    assert not problems, "\n".join(problems)


# ---------------------------------------------------------------------------
# Walk-throughs
# ---------------------------------------------------------------------------


class Replay:
    """Replay a page's shell blocks and file contents in order."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.monkeypatch = monkeypatch
        self.runner = CliRunner()
        self.outputs: list[tuple[tuple[str, ...], str]] = []
        self.excerpts: list[Block] = []

    def block(self, block: Block) -> None:
        if block.lang in SHELL_LANGS:
            for tokens in shell_commands(block):
                self.command(block, tokens)
        elif block.file_path:
            target = Path(block.file_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(block.code, encoding="utf-8")
        elif block.lang in PYTHON_LANGS:
            self.excerpts.append(block)
        else:
            pytest.fail(f"{block.where}: no way to replay a {block.lang} block")

    def command(self, block: Block, tokens: list[str]) -> None:
        program, args = tokens[0], tokens[1:]
        if program == "iceberg-ops":
            self.cli(*args, where=block.where)
        elif program == "mkdir":
            Path(args[-1]).mkdir(parents=True)
        elif program == "cd":
            self.monkeypatch.chdir(args[0])
        elif program in {"pip", "python", "source"}:
            pass  # environment setup; the test already runs in one
        else:
            pytest.fail(f"{block.where}: unexpected command {tokens}")

    def cli(self, *args: str, where: str = "") -> str:
        result = self.runner.invoke(main, list(args))
        assert result.exit_code == 0, (where, args, result.output, result.exception)
        self.outputs.append((args, result.output))
        return result.output

    def output_of(self, subcommand: str) -> list[str]:
        return [out for args, out in self.outputs if subcommand in args]


def _section(page: Page, start: str, stop: str) -> list[Block]:
    """Blocks between the heading *start* and the heading *stop*."""
    begin = page.lines.index(start)
    end = page.lines.index(stop, begin)
    return [b for b in page.blocks if begin < b.line <= end]


def _table_columns() -> list[str]:
    from iceberg_ops.core.catalog import CatalogFactory

    catalog = CatalogFactory.build("sqlite", db_path="demo.db", warehouse_dir="warehouse")
    try:
        return [f.name for f in catalog.load_table("sales.orders").schema().fields]
    finally:
        catalog.close()


def test_readme_quickstart_replays(docs_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """README "Try it in 60 seconds": init, revision, migrate, autogenerate,
    plan, migrate, status and rollback, exactly as written."""
    page = load_page(README)
    blocks = _section(page, "## Try it in 60 seconds", "## Install")
    assert [b.lang for b in blocks] == ["bash", "python", "bash", "yaml", "bash"]
    replay = Replay(monkeypatch)
    for block in blocks:
        replay.block(block)

    demo = docs_env / "demo"
    assert Path.cwd().samefile(demo)
    assert (demo / "iceberg-ops.yaml").is_file()
    assert (demo / "migrations" / "0001_create_orders.py").is_file()
    assert (demo / "migrations" / "0002_rename_total_add_status.py").is_file()
    [status] = replay.output_of("status")
    assert "0001" in status and "0002" in status
    # The final rollback undid 0002 only: the rename is reversed and
    # the added column is gone.
    assert _table_columns() == ["order_id", "order_total", "created_at"]


def test_quickstart_page_replays(docs_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """docs/quickstart.rst from "Scaffold a project" to the config file."""
    page = load_page(QUICKSTART)
    blocks = _section(page, "Scaffold a project", "Run migrations -- Python API")
    replay = Replay(monkeypatch)
    for block in blocks:
        replay.block(block)

    demo = docs_env / "demo"
    generated = (demo / "migrations" / "0002_rename_total_add_status.py").read_text("utf-8")
    [excerpt] = replay.excerpts
    assert excerpt.code.strip() in generated, (
        f"{excerpt.where}: the generated migration no longer reads like the excerpt"
    )
    assert _table_columns() == ["order_id", "order_total", "created_at"]
    # "Then iceberg-ops migrate, iceberg-ops status ... need no options."
    status = replay.cli("status")
    assert "0001" in status and "0002" in status
    replay.cli("migrate")
    assert _table_columns() == ["order_id", "order_amount", "created_at", "status"]


def test_autogenerate_cli_example(docs_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """docs/autogenerate.rst "CLI usage": the contract and the command."""
    import pyarrow as pa

    from iceberg_ops.core.catalog import CatalogFactory

    page = load_page(ROOT / "docs" / "autogenerate.rst")
    contract, command = _section(page, "CLI usage", "Python API")[:2]
    replay = Replay(monkeypatch)
    replay.cli("init")
    # "Given an existing sales.orders table with columns order_id (long,
    # required) and amount (double)", in the catalog init configured.
    catalog = CatalogFactory.build("sqlite")
    try:
        catalog.create_namespace("sales")
        catalog.create_table(
            "sales.orders",
            pa.schema(
                [pa.field("order_id", pa.int64(), nullable=False), ("amount", pa.float64())]
            ),
        )
    finally:
        catalog.close()
    replay.block(contract)
    replay.block(command)
    [generated] = sorted((docs_env / "migrations").glob("0001_*.py"))
    text = generated.read_text("utf-8")
    assert "ctx.enforce_rename(target_table, 'amount', 'order_amount')" in text
    assert "ctx.enforce_rename(target_table, 'order_amount', 'amount')" in text


def test_schema_migration_examples_apply(docs_env: Path) -> None:
    """The quality-gate and legacy-archival examples do what the page says."""
    import pyarrow as pa

    from iceberg_ops.core.catalog import CatalogFactory
    from iceberg_ops.runner import MigrationRunner

    page = load_page(ROOT / "docs" / "schema_migrations.rst")
    files = [b for b in python_blocks(page) if b.file_path]
    assert [b.file_path for b in files] == [
        "migrations/0010_add_session_duration.py",
        "migrations/0042_retype_duration.py",
    ]
    for block in files:
        target = Path(block.file_path or "")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(block.code, encoding="utf-8")

    catalog = CatalogFactory.build("sqlite", db_path="catalog.db", warehouse_dir="warehouse")
    try:
        catalog.create_namespace("sales")
        events = catalog.create_table("sales.events", pa.schema([("duration", pa.int32())]))
        runner = MigrationRunner(catalog, namespace="sales", migrations_dir="migrations")
        # The pre_upgrade gate refuses an empty table.
        with pytest.raises(RuntimeError):
            runner.migrate()
        events.append(pa.table({"duration": pa.array([5], pa.int32())}))
        runner.migrate()

        schema = catalog.load_table("sales.events").schema()
        assert str(schema.find_field("session_duration").field_type) == "double"
        assert str(schema.find_field("duration").field_type) == "string"
        legacy = schema.find_field("duration__legacy_1")
        assert str(legacy.field_type) == "int"
        assert (legacy.doc or "").startswith("[ARCHIVED]")
        # The irreversible downgrade raises before changing anything.
        with pytest.raises(NotImplementedError):
            runner.rollback(steps=1)
        catalog.load_table("sales.events").schema().find_field("duration__legacy_1")
    finally:
        catalog.close()
