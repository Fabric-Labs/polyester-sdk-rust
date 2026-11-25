#!/usr/bin/env python3
"""Fail if the public repository discloses internal references.

README.md and CHANGELOG.md are checked for every internal/QA disclosure
pattern in FORBIDDEN. Every other tracked file outside the generated tree
(GENERATED_PREFIXES) is checked for internal tracker references, in its path
and, unless it is binary, in its text, so they cannot come back. Markdown
prose is also checked for internal merge request numbers; source files are
not, as "!" and digits are code there. See markdown_prose for what counts as
Markdown code.
"""

from __future__ import annotations

import re
import subprocess
import sys
from collections.abc import Iterable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FILES = ("README.md", "CHANGELOG.md")
FORBIDDEN: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bPOLY-\d+\b"), "internal ticket ID"),
    (re.compile(r"\bF-\d+\b"), "internal finding ID"),
    (re.compile(r"\b(?:Yvan|Sergio)\b"), "internal person/reference"),
    (
        re.compile(r"\b(?:current )?(?:devnet|staging) (?:behavior|testing)\b", re.I),
        "environment-specific QA prose",
    ),
    (
        re.compile(r"\b(?:reserve corruption|backend bug|backend issue)\b", re.I),
        "backend incident prose",
    ),
    (re.compile(r"—"), "em dash"),
]

# An internal ticket reference: "POLY" or "Ticket", an optional hyphen or
# underscore, and digits, in any case, also inside an identifier such as a
# test name. The one legitimate term of that shape, the Poly1305
# authenticator (poly1305), is excluded by name.
TICKET_REFERENCE = re.compile(r"poly(?!1305(?!\d))[-_]?\d+|ticket[-_]?\d+", re.IGNORECASE)
# One of the tracker's other project keys, a hyphen and an issue number, in
# any case. A key joined to the word or hyphen before it, as in a fixture
# name such as depth-chain-00, is not a reference.
PROJECT_REFERENCE = re.compile(r"(?<![\w-])(?:admin|chain|huly|ody|zip)-\d+\b", re.IGNORECASE)
# An internal merge request number: "!" and at least three digits, not
# joined to a word before it. Only Markdown prose is checked for one.
MERGE_REQUEST = re.compile(r"(?<!\w)!\d{3,}\b")
PROSE_SUFFIXES = (".md",)
# Blockquote markers: ">" and an optional space, each after up to 3 spaces.
BLOCKQUOTE = re.compile(r"(?: {0,3}> ?)*")
# A code fence: up to 3 spaces, then three or more backticks or tildes. The
# closing fence is at least as long as the opening one.
CODE_FENCE = re.compile(r" {0,3}(`{3,}|~{3,})")
# An indented code line: 4 spaces, or a tab after up to 3 spaces.
INDENTED_CODE = re.compile(r" {4}| {0,3}\t")
# An inline code span on one line: a run of backticks, then text, then a
# run as long.
CODE_SPAN = re.compile(r"(?<!`)(`+)(?!`).*?(?<!`)\1(?!`)")
# Generated code, copied from the published contract, is not checked.
GENERATED_PREFIXES = ("src/gen/",)
# Git reads a file as binary when its first 8000 bytes hold a NUL byte.
BINARY_SNIFF_BYTES = 8000


def tracked_files(root: Path) -> list[str]:
    """Returns the repository's tracked files, relative to root."""
    out = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z"],
        check=True,
        capture_output=True,
    ).stdout
    return [name for name in out.decode("utf-8").split("\0") if name]


def is_binary(content: bytes) -> bool:
    """Reports whether content is binary as Git reads it: a NUL byte in its
    first 8000 bytes, or bytes that are not UTF-8 text."""
    if b"\0" in content[:BINARY_SNIFF_BYTES]:
        return True
    try:
        content.decode("utf-8")
    except UnicodeDecodeError:
        return True
    return False


def is_reference(text: str) -> bool:
    """Reports whether text holds an internal ticket or project reference."""
    return bool(TICKET_REFERENCE.search(text) or PROJECT_REFERENCE.search(text))


def markdown_prose(text: str) -> dict[int, str]:
    """Returns Markdown text's prose by line number, without its code.

    Code is a subset of CommonMark's: a fenced code block, whose fences are
    indented at most 3 spaces, inside blockquotes too (only a closing fence
    at the same quote depth closes it, and a fence opened in a blockquote
    ends with it); an indented code line, outside a fence; and an
    inline code span within one line. Everything else is prose, so a code
    span that spans lines is checked as prose, which fails closed."""
    prose: dict[int, str] = {}
    fence = ""
    fence_depth = 0
    for number, line in enumerate(text.splitlines(), start=1):
        quote = BLOCKQUOTE.match(line)
        assert quote is not None
        depth = quote.group().count(">")
        body = line[quote.end() :]
        marker = CODE_FENCE.match(body)
        if fence and depth >= fence_depth:
            # Only a closing fence at the fence's own quote depth closes it;
            # a deeper quoted one is code content.
            closes = marker and marker.group(1).startswith(fence)
            if depth == fence_depth and closes and not body[marker.end() :].strip():
                fence = ""
            continue
        fence = ""
        if marker:
            fence, fence_depth = marker.group(1), depth
            continue
        if INDENTED_CODE.match(body):
            continue
        prose[number] = CODE_SPAN.sub("", line)
    return prose


def ticket_references(root: Path, names: Iterable[str]) -> list[str]:
    """Names each file outside the generated tree whose path or text holds
    an internal tracker reference, with the first line that does. A binary
    file is checked by its path only."""
    failures: list[str] = []
    for name in sorted(names):
        if name.startswith(GENERATED_PREFIXES):
            continue
        if is_reference(name):
            failures.append(f"{name}: path contains an internal tracker reference")
            continue
        path = root / name
        if not path.is_file():
            continue
        content = path.read_bytes()
        if is_binary(content):
            continue
        text = content.decode("utf-8")
        prose = markdown_prose(text) if name.endswith(PROSE_SUFFIXES) else {}
        for number, line in enumerate(text.splitlines(), start=1):
            if is_reference(line) or MERGE_REQUEST.search(prose.get(number, "")):
                failures.append(f"{name}:{number}: contains an internal tracker reference")
                break
    return failures


def markdown_disclosures(root: Path) -> list[str]:
    """Names each FORBIDDEN pattern README.md or CHANGELOG.md holds."""
    failures: list[str] = []
    for name in FILES:
        path = root / name
        if not path.is_file():
            failures.append(f"{name}: missing")
            continue
        text = path.read_text(encoding="utf-8")
        for pattern, description in FORBIDDEN:
            if pattern.search(text):
                failures.append(f"{name}: contains {description}")
    return failures


def main() -> int:
    failures = markdown_disclosures(ROOT)
    failures.extend(ticket_references(ROOT, tracked_files(ROOT)))
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print(
        "ok: public disclosure checks passed for README.md, CHANGELOG.md and every"
        f" tracked file outside {', '.join(GENERATED_PREFIXES)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
