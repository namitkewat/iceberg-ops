"""Sphinx configuration for iceberg-ops documentation."""

from __future__ import annotations

import logging
import os
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

# Add project root to path so autodoc can find the package.
sys.path.insert(0, os.path.abspath(".."))

import iceberg_ops

_PYPROJECT = tomllib.loads(
    (Path(__file__).resolve().parent.parent / "pyproject.toml").read_text(encoding="utf-8")
)

project = "iceberg-ops"
copyright = "2026, Namit Kewat"
author = "Namit Kewat"
release = iceberg_ops.__version__
version = ".".join(release.split(".")[:2])

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    # sphinx_click must load before sphinx_autodoc_typehints on Sphinx 9
    # (click-contrib/sphinx-click#159).
    "sphinx_click",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
    "myst_parser",
    "sphinxext.opengraph",
    # Writes llms.txt and llms-full.txt: a plain-text index of the site and
    # the full text of every page, at the site root.
    "sphinx_llms_txt",
]

# Napoleon settings (NumPy-style docstrings).
napoleon_google_docstring = False
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_use_ivar = True

# Autodoc settings.
autodoc_member_order = "bysource"
autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "show-inheritance": True,
}

# Type hints.
always_document_param_types = True
typehints_defaults = "comma"

# Avoid duplicate descriptions for dataclass fields. Napoleon's
# Attributes section already documents them; without this autodoc
# would emit a second copy.
autodoc_class_content = "class"

# Suppress noisy warnings from sphinx-autodoc-typehints around
# forward refs and guarded imports inside lazy-loaded backends.
suppress_warnings = [
    "sphinx_autodoc_typehints.forward_reference",
    "sphinx_autodoc_typehints.guarded_import",
]

# Cross-project links. An inventory that cannot be fetched (offline
# build, docs host down) is reported as a message, not a warning, so a
# ``-W`` build still passes; links into that project then render as
# plain text. See ``_UnreachableInventoryFilter`` below.
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pyarrow": ("https://arrow.apache.org/docs/", None),
    "pyiceberg": ("https://py.iceberg.apache.org/", None),
    "click": ("https://click.palletsprojects.com/en/stable/", None),
    "requests": ("https://requests.readthedocs.io/en/latest/", None),
}
intersphinx_timeout = 10

# Canonical site URL: the Pages workflow passes the deployed URL; local
# builds fall back to the published site.
html_baseurl = os.environ.get("DOCS_BASE_URL") or "https://namitkewat.github.io/iceberg-ops/"
if not html_baseurl.endswith("/"):
    html_baseurl += "/"

# HTML theme.
html_theme = "furo"
html_title = f"iceberg-ops {release}"
html_static_path = ["_static"]
# _templates/sidebar/brand.html gives the sidebar logo an empty alt text:
# the "iceberg-ops <version>" text in the same link already names it.
templates_path = ["_templates"]
# The favicon follows the browser's light or dark theme on its own.
html_favicon = "_static/favicon.svg"
html_theme_options = {
    "source_repository": "https://github.com/namitkewat/iceberg-ops/",
    "source_branch": "main",
    "source_directory": "docs/",
    "light_logo": "logo-light.svg",
    "dark_logo": "logo-dark.svg",
}

# Open Graph tags for link previews.
ogp_site_url = html_baseurl
ogp_site_name = "iceberg-ops"
ogp_image = "_static/social-preview.png"
ogp_image_alt = "iceberg-ops: Alembic-style schema migrations for Apache Iceberg"
# Ask for a large preview card; keep the size in step with the PNG.
ogp_custom_meta_tags = [
    '<meta name="twitter:card" content="summary_large_image" />',
    '<meta property="og:image:width" content="1280" />',
    '<meta property="og:image:height" content="640" />',
]

# Copy buttons on code blocks: strip shell and Python REPL prompts.
copybutton_prompt_text = r"\$ |>>> |\.\.\. "
copybutton_prompt_is_regexp = True

# MyST (CHANGELOG.md is included through docs/changelog.rst).
myst_heading_anchors = 3

# llms.txt: the summary line is the package's PyPI summary.
llms_txt_summary = _PYPROJECT["project"]["description"]

# Source / build settings.
source_suffix = {".rst": "restructuredtext", ".md": "markdown"}
master_doc = "index"
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


class _UnreachableInventoryFilter(logging.Filter):
    """Report an unreachable intersphinx inventory as info, not a warning."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Downgrade the "failed to reach any of the inventories" warning.

        Parameters
        ----------
        record : logging.LogRecord
            A record from Sphinx's intersphinx logger.

        Returns
        -------
        bool
            Always True: the record is kept, at INFO level when it is
            the unreachable-inventory warning.
        """
        if record.levelno == logging.WARNING and str(record.msg).startswith(
            "failed to reach any of the inventories"
        ):
            record.levelno = logging.INFO
            record.levelname = "INFO"
        return True


# Click help text names options as plain words (--namespace). Parsed as
# reStructuredText, "--" would turn into an en dash, so the generated CLI
# reference shows option names as inline literals instead.
_CLI_OPTION_RE = re.compile(r"(?<![\w`-])--[a-z0-9][a-z0-9-]*(?:=[\w.-]*\w)?")
_RST_LITERAL_RE = re.compile(r"(``.+?``)")
_RST_END_FOLLOWERS = frozenset(".,;:!?)]}'\"/")


def _wrap_option(match: re.Match[str]) -> str:
    """Return the matched option as an inline literal.

    Parameters
    ----------
    match : re.Match
        A ``--option`` match inside one line of help text.

    Returns
    -------
    str
        The option in double backquotes, with escaped spaces where an
        adjacent character would stop docutils from seeing the literal.
    """
    text, start, end = match.string, match.start(), match.end()
    before = "\\ " if start > 0 and not text[start - 1].isspace() else ""
    after = ""
    if end < len(text) and not text[end].isspace() and text[end] not in _RST_END_FOLLOWERS:
        after = "\\ "
    return f"{before}``{match.group(0)}``{after}"


def _literal_cli_options(app: Any, ctx: Any, lines: list[str]) -> None:
    """Rewrite ``--option`` mentions in sphinx-click output as literals.

    Parameters
    ----------
    app : sphinx.application.Sphinx
        The Sphinx application (unused).
    ctx : click.Context
        Context of the command being documented (unused).
    lines : list of str
        reStructuredText lines; edited in place. Directive and field
        lines (``.. option::``, ``:default:``) and existing literals are
        left alone.
    """
    for index, line in enumerate(lines):
        if line.lstrip().startswith((".. ", ":")):
            continue
        parts = _RST_LITERAL_RE.split(line)
        parts[::2] = [_CLI_OPTION_RE.sub(_wrap_option, part) for part in parts[::2]]
        lines[index] = "".join(parts)


def setup(app: Any) -> None:
    """Register this project's build hooks.

    Parameters
    ----------
    app : sphinx.application.Sphinx
        The Sphinx application.
    """
    # Sphinx prefixes its logger names with "sphinx.".
    logging.getLogger("sphinx.sphinx.ext.intersphinx").addFilter(_UnreachableInventoryFilter())
    app.connect("sphinx-click-process-description", _literal_cli_options)
    app.connect("sphinx-click-process-options", _literal_cli_options)
