"""Sphinx configuration for the maplib documentation site.

The site is built from the Markdown guides in the repository (README.md, API_USAGE.md, CHANGES.md,
CONFIG_GRAPH.md, docs/release_summary.md), which are included with MyST, and the API reference,
which sphinx-autoapi reads from the type stubs in py_maplib/maplib (no Rust build is needed).

Build locally with:  sphinx-build -W --keep-going -b html docs/site docs/site/_build/html
"""

import os
import re
import tomllib
from pathlib import Path

SITE = Path(__file__).resolve().parent
REPO = SITE.parent.parent
GITHUB = "https://github.com/DataTreehouse/maplib"

with open(REPO / "py_maplib" / "Cargo.toml", "rb") as f:
    release = tomllib.load(f)["package"]["version"]
version = release

project = "maplib"
author = "Data Treehouse AS"
copyright = "Data Treehouse AS and Prediktor AS"
html_title = f"maplib {release}"

extensions = [
    "myst_parser",
    "autoapi.extension",
    "sphinx.ext.intersphinx",
]
exclude_patterns = ["_build"]

# Markdown: GitHub-style heading anchors, so links like API_USAGE.md#querying keep working.
myst_heading_anchors = 4
myst_enable_extensions = ["colon_fence"]
# storage.py and opcua import from the compiled extension (maplib.maplib), which only exists as stubs.
suppress_warnings = ["myst.header", "autoapi.python_import_resolution"]

# API reference from the type stubs. The .pyi files describe the compiled extension and take
# precedence over the .py files next to them.
autoapi_type = "python"
autoapi_dirs = [str(REPO / "py_maplib" / "maplib")]
autoapi_file_patterns = ["*.pyi", "*.py"]
autoapi_root = "api"
autoapi_add_toctree_entry = False
autoapi_keep_files = False
autoapi_member_order = "groupwise"
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    "imported-members",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "polars": ("https://docs.pola.rs/api/python/stable", None),
}

html_theme = "furo"
html_theme_options = {
    "source_repository": f"{GITHUB}/",
    "source_branch": "main",
    "source_directory": "docs/site/",
    "top_of_page_buttons": [],
    "footer_icons": [
        {
            "name": "GitHub",
            "url": GITHUB,
            "html": '<svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 16 16">'
                    '<path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17'
                    '.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68'
                    '-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64'
                    '-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27'
                    ' 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15'
                    ' 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013'
                    ' 8.013 0 0016 8c0-4.42-3.58-8-8-8z"></path></svg>',
            "class": "",
        },
    ],
}

# The guides are written for GitHub, where they link to each other as files. On the site, links to
# another guide go to its page, and links to other files in the repository go to GitHub.
PAGES = {
    "README.md": "index",
    "API_USAGE.md": "usage",
    "CHANGES.md": "changes",
    "CONFIG_GRAPH.md": "config_graph",
    "docs/release_summary.md": "whats_new",
}
LINK = re.compile(r"(?<!!)(\[[^\]\n]*\])\(([^)\s#]+)(#[^)\s]*)?\)")


def _rewrite_links(app, relative_path, parent_docname, content):
    source_dir = (SITE / relative_path).resolve().parent

    def replace(match):
        text, target, anchor = match.group(1), match.group(2), match.group(3) or ""
        if re.match(r"^[a-z][a-z0-9+.-]*:", target):  # http:, mailto:, ...
            return match.group(0)
        path = (source_dir / target).resolve()
        try:
            repo_path = path.relative_to(REPO).as_posix()
        except ValueError:
            return match.group(0)
        if repo_path in PAGES:
            return f"{text}({PAGES[repo_path]}.md{anchor})"
        if path.suffix in {".svg", ".png", ".jpg", ".gif"}:
            return match.group(0)
        kind = "tree" if path.is_dir() else "blob"
        return f"{text}({GITHUB}/{kind}/main/{repo_path}{anchor})"

    content[0] = LINK.sub(replace, content[0])
    included = (SITE / relative_path).resolve().relative_to(REPO).as_posix()
    if included in DEMOTE_HEADINGS:
        content[0] = _demote_headings(content[0])


# Guides with several top-level sections, one per release, that the site shows under one page title.
DEMOTE_HEADINGS = {"docs/release_summary.md"}


def _demote_headings(text):
    lines, fence = [], None
    for line in text.split("\n"):
        stripped = line.lstrip()
        if stripped.startswith(("```", "~~~")):
            marker = stripped[:3]
            fence = None if fence == marker else (fence or marker)
        elif fence is None and re.match(r"#{1,5} ", line):
            line = "#" + line
        lines.append(line)
    return "\n".join(lines)


# The old pdoc site lived at /maplib.html and /maplib/<module>.html; keep those URLs working.
REDIRECTS = {
    "maplib.html": "api/maplib/index.html",
    **{
        f"maplib/{m}.html": f"../api/maplib/{m}/index.html"
        for m in ["config", "outbound", "storage", "virtualization", "vq"]
    },
}


def _write_redirects(app, exception):
    if exception is not None or app.builder.format != "html":
        return
    for source, target in REDIRECTS.items():
        out = Path(app.outdir) / source
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            f'<!doctype html><meta charset="utf-8"><title>Moved</title>'
            f'<meta http-equiv="refresh" content="0; url={target}">'
            f'<link rel="canonical" href="{target}"><a href="{target}">Moved here</a>\n'
        )


def setup(app):
    app.connect("include-read", _rewrite_links)
    app.connect("build-finished", _write_redirects)
