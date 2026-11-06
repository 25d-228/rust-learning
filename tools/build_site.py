#!/usr/bin/env python3
"""Render the course's canonical Markdown as the shared Scholar book.
Run from a course package: python3 tools/build_site.py
Print an exact issue body: python3 tools/build_site.py --issue-body chapter-01
This program reads/writes local files only. It never accesses GitHub.
"""
from __future__ import annotations
import argparse
import html
import json
from pathlib import Path
import re
import shutil
import shlex
import sys
from urllib.parse import urlparse

from markdown_it import MarkdownIt
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import get_lexer_by_name, TextLexer
from pygments.util import ClassNotFound

MATHJAX_VERSION = "3.2.1"


def escaped(text: object) -> str:
    return html.escape(str(text), quote=True)


def safe_child(parent: Path, name: str) -> Path:
    path = (parent / name).resolve()
    if not path.is_relative_to(parent.resolve()):
        raise ValueError(f"Path leaves its documentation directory: {name}")
    return path


def math_html(source: str, display: bool) -> str:
    tag = "div" if display else "span"
    mode = "true" if display else "false"
    return (f'<{tag} class="math-input" data-display="{mode}">'
            f'{escaped(source)}</{tag}>')


def inline_math(state, silent: bool) -> bool:
    """The book uses $inline math$; literal currency dollars must be escaped."""
    start = state.pos
    if state.src[start] != "$" or start + 1 >= state.posMax:
        return False
    if state.src[start + 1] == "$" or state.src[start + 1].isspace():
        return False
    end = start + 1
    while end < state.posMax:
        ch = state.src[end]
        if ch == "\n":
            return False
        if ch == "\\":
            end += 2
            continue
        if ch == "$":
            if state.src[end - 1].isspace():
                return False
            if not silent:
                token = state.push("math_inline", "", 0)
                token.content = state.src[start + 1:end]
            state.pos = end + 1
            return True
        end += 1
    return False


def block_math(state, start_line: int, end_line: int, silent: bool) -> bool:
    """Display math uses $$ on separate lines, also supported by GitHub."""
    pos = state.bMarks[start_line] + state.tShift[start_line]
    if state.is_code_block(start_line):
        return False
    if state.src[pos:state.eMarks[start_line]].strip() != "$$":
        return False
    last = start_line + 1
    while last < end_line:
        p = state.bMarks[last] + state.tShift[last]
        line = state.src[p:state.eMarks[last]]
        if line and state.sCount[last] < state.blkIndent:
            return False
        if line.strip() == "$$":
            if silent:
                return True
            token = state.push("math_block", "", 0)
            token.block = True
            token.content = state.getLines(start_line + 1, last, state.blkIndent, False)
            token.map = [start_line, last + 1]
            state.line = last + 1
            return True
        last += 1
    return False


def code_fence(tokens, index, options, env) -> str:
    token = tokens[index]
    fields = shlex.split(token.info) if token.info.strip() else ["text"]
    language = fields[0]
    filename = next((v[6:] for v in fields[1:] if v.startswith("title=")), "")
    caption = filename or {"cpp": "C++", "rust": "Rust", "c": "C", "bash": "Terminal",
                            "sh": "Terminal", "text": "Output", "python": "Python"}.get(language, language)
    if language == "math":
        return math_html(token.content.strip(), True) + "\n"
    aliases = {"c++": "cpp", "shell": "bash", "sh": "bash", "console": "text"}
    try:
        lexer = get_lexer_by_name(aliases.get(language, language))
    except ClassNotFound:
        lexer = TextLexer()
    source = highlight(token.content, lexer, HtmlFormatter(nowrap=True))
    return (f'<figure class="code-block"><figcaption><span class="code-title">{escaped(caption)}</span>'
            '<button type="button" class="copy-code" aria-label="Copy code">Copy</button>'
            '</figcaption><pre><code>' + source + '</code></pre></figure>\n')


def parser() -> MarkdownIt:
    md = MarkdownIt("commonmark", {"html": False}).enable(["table", "strikethrough"])
    md.inline.add_terminator_char("$")
    md.inline.ruler.before("escape", "course_math_inline", inline_math)
    md.block.ruler.before("fence", "course_math_block", block_math,
                         {"alt": ["paragraph", "reference", "blockquote", "list"]})
    md.renderer.rules["math_inline"] = lambda ts, i, opts, env: math_html(ts[i].content, False)
    md.renderer.rules["math_block"] = lambda ts, i, opts, env: math_html(ts[i].content, True) + "\n"
    md.renderer.rules["fence"] = code_fence
    return md


def render_markdown(source: str) -> tuple[str, str]:
    md = parser()
    tokens = md.parse(source)
    used: dict[str, int] = {}
    links = []
    for i, token in enumerate(tokens):
        if token.type != "heading_open" or i + 1 >= len(tokens):
            continue
        inner = tokens[i + 1]
        label = "".join(t.content for t in (inner.children or [])
                        if t.type in {"text", "code_inline", "math_inline"}) or inner.content
        base = re.sub(r"[^\w\s-]", "", label.lower())
        base = re.sub(r"\s+", "-", base.strip()) or "section"
        n = used.get(base, 0)
        used[base] = n + 1
        slug = base if n == 0 else f"{base}-{n}"
        token.attrSet("id", slug)
        if token.tag in {"h2", "h3"}:
            links.append(f'<li class="toc-{token.tag}"><a href="#{escaped(slug)}">'
                         f'{escaped(label)}</a></li>')
    return md.renderer.render(tokens, md.options, {}), "<ul>" + "".join(links) + "</ul>"


def check_url(value: str, field: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme != "https" or not parsed.netloc:
        raise ValueError(f"{field} must be an absolute HTTPS URL")
    return value


def load_course(root: Path) -> dict:
    course = json.loads((root / "course.json").read_text(encoding="utf-8"))
    for key in ("title", "course_key", "repository", "pages_url", "pages"):
        if key not in course:
            raise ValueError(f"course.json is missing {key}")
    check_url(course["repository"], "repository")
    check_url(course["pages_url"], "pages_url")
    if not course["pages"]:
        raise ValueError("Write the roadmap and add its entry to course.json before building.")
    ids, outputs = set(), set()
    for page in course["pages"]:
        for key in ("id", "label", "title", "source", "output"):
            if not page.get(key):
                raise ValueError(f"Page entry is missing {key}")
        if not re.fullmatch(r"[a-z0-9-]+", page["id"]):
            raise ValueError(f"Invalid stable page ID: {page['id']}")
        if page["id"] in ids or page["output"] in outputs:
            raise ValueError("Duplicate page ID or output filename in course.json")
        if not re.fullmatch(r"[a-z0-9-]+\.html", page["output"]):
            raise ValueError("Use a simple lowercase HTML filename, such as chapter-01.html")
        ids.add(page["id"]); outputs.add(page["output"])
        source = safe_child(root / "book", page["source"])
        if source.suffix != ".md" or not source.is_file():
            raise ValueError(f"Missing canonical Markdown: {page['source']}")
        if page.get("issue_url"):
            check_url(page["issue_url"], "issue_url")
    if course["pages"][0]["id"] != "roadmap" or course["pages"][0]["output"] != "index.html":
        raise ValueError("The first entry must be roadmap, with output index.html")
    return course



def page_html(course: dict, page: dict, body: str, toc: str, index: int) -> str:
    pages = course["pages"]
    nav = ""
    for other in pages:
        current = ' aria-current="page"' if other["id"] == page["id"] else ""
        nav += (f'<li><a href="{escaped(other["output"])}"{current}>'
                f'<span class="chapter-label">{escaped(other["label"])}</span>'
                f'<span>{escaped(other["title"])}</span></a></li>')
    issue = (f'<a href="{escaped(page["issue_url"])}">Read on GitHub</a>'
             if page.get("issue_url") else "")
    pager = []
    for direction, target in (("Previous", index - 1), ("Next", index + 1)):
        if 0 <= target < len(pages):
            other = pages[target]
            pager.append(f'<a href="{escaped(other["output"])}"><small>{direction} · {escaped(other["label"])}</small>'
                         f'<span>{escaped(other["title"])}</span></a>')
    canonical = course["pages_url"].rstrip("/") + "/" + page["output"]
    first_chapter = pages[1]["output"] if len(pages) > 1 else "index.html"
    return f'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escaped(page['label'])} — {escaped(page['title'])} · {escaped(course['title'])}</title>
<meta name="description" content="{escaped(course.get('description',''))}">
<link rel="canonical" href="{escaped(canonical)}">
<link rel="stylesheet" href="assets/code-light.css">
<link rel="stylesheet" href="assets/course.css">
<script>window.MathJax = {{startup: {{typeset: false}}, options: {{enableMenu: false}}, svg: {{fontCache: "local"}}}};</script>
<script defer src="assets/vendor/tex-svg.js"></script>
<script defer src="assets/course.js"></script>
</head>
<body>
<a class="skip-link" href="#main-content">Skip to chapter</a>
<header class="topbar"><a class="brand" href="index.html"><svg viewBox="0 0 28 28" aria-hidden="true"><path d="M14 7C10 3 5 4 2 5v17c4-2 8-1 12 2m0-17c4-4 9-3 12-2v17c-4-2-8-1-12 2V7Z"/></svg><span>{escaped(course['title'])}</span></a>
<div class="header-actions"><a class="desktop-link" href="index.html">Overview</a><a class="desktop-link" href="{escaped(first_chapter)}">Chapters</a><a class="desktop-link" href="{escaped(course['repository'])}">Repository</a><button id="menu-toggle" type="button" aria-expanded="false" aria-controls="chapter-nav">Contents</button></div></header>
<div class="book-layout">
<aside class="sidebar" id="chapter-nav"><h2 class="sidebar-title">Contents</h2>
<label for="chapter-filter">Find a chapter</label><input id="chapter-filter" type="search" placeholder="Search chapter titles…" autocomplete="off">
<nav aria-label="Chapters"><ol>{nav}</ol></nav>
<p class="sidebar-note">{escaped(course.get('description', 'A practical technical book.'))}</p></aside>
<main id="main-content" tabindex="-1">
<div class="breadcrumb"><a href="index.html">{escaped(course['title'])}</a><span aria-hidden="true">/</span><span>{escaped(page['label'])}</span></div>
<div class="page-actions">{issue}<a href="source/{escaped(page['id'])}.md">Markdown</a></div>
<details class="mobile-toc"><summary>In this chapter</summary>{toc}</details>
<p id="math-status" hidden>Some mathematics could not be rendered. The original notation remains visible.</p>
<article>{body}</article>
<nav class="pager" aria-label="Previous and next chapters">{''.join(pager)}</nav>
<footer>{escaped(course['title'])} · {escaped(page['label'])}</footer>
</main>
<aside class="page-toc" aria-label="In this chapter"><div class="toc-title">In this chapter</div>{toc}</aside>
</div></body></html>
'''


def build(root: Path, course: dict, destination: Path | None = None) -> None:
    destination = (destination or root / "site").resolve()
    for protected in (root / "book", root / "tools", root / "private", root / "authoring"):
        if destination.is_relative_to(protected.resolve()):
            raise ValueError("Output must not overwrite source, tools, or private notes")
    assets, sources = destination / "assets", destination / "source"
    assets.mkdir(parents=True, exist_ok=True)
    sources.mkdir(parents=True, exist_ok=True)
    (destination / ".nojekyll").write_text("", encoding="utf-8")
    for name in ("course.css", "course.js"):
        shutil.copyfile(root / "tools/site_assets" / name, assets / name)
    shutil.copytree(root / "tools/site_assets/vendor", assets / "vendor", dirs_exist_ok=True)
    css = HtmlFormatter(style="friendly").get_style_defs(".code-block")
    (assets / "code-light.css").write_text(css + "\n", encoding="utf-8")
    if (root / "book/media").is_dir():
        shutil.copytree(root / "book/media", destination / "media", dirs_exist_ok=True)
    for index, page in enumerate(course["pages"]):
        source_file = safe_child(root / "book", page["source"])
        source_bytes = source_file.read_bytes()
        body, toc = render_markdown(source_bytes.decode("utf-8"))
        (destination / page["output"]).write_text(page_html(course, page, body, toc, index), encoding="utf-8")
        (sources / f"{page['id']}.md").write_bytes(source_bytes)
    print(f"Built {len(course['pages'])} local book pages in {destination}.")


def main() -> int:
    argp = argparse.ArgumentParser(description=__doc__)
    argp.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    argp.add_argument("--output", type=Path, help="default: site/; use --output . in a gh-pages checkout")
    argp.add_argument("--issue-body", metavar="ID", help="print one exact canonical Markdown body")
    args = argp.parse_args()
    try:
        root = args.root.resolve()
        course = load_course(root)
        if args.issue_body:
            page = next((p for p in course["pages"] if p["id"] == args.issue_body), None)
            if page is None:
                raise ValueError(f"Unknown page ID: {args.issue_body}")
            sys.stdout.buffer.write(safe_child(root / "book", page["source"]).read_bytes())
        else:
            build(root, course, args.output)
    except (OSError, UnicodeError, ValueError, KeyError, TypeError) as error:
        print(f"Site build: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
