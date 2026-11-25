#!/usr/bin/env python3
"""Fail if the public repository discloses internal references.

README.md and CHANGELOG.md are checked for every internal/QA disclosure
pattern in FORBIDDEN. Every other tracked file outside the generated tree
(GENERATED_PREFIXES) is checked for internal tracker references, in its path
and, unless it is binary, in its text, so they cannot come back.
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
# A candidate internal merge request number: "!" and at least three digits,
# at the start of a line or after whitespace. It is a reference only in
# prose; see is_merge_request.
MERGE_REQUEST = re.compile(r"(?<!\S)!\d{3,}\b")
# Before a merge request number, nothing but a comment marker reads as prose.
COMMENT_PREFIX = re.compile(r"\s*(?:#+|//[/!]?)?")
# Rust keywords that take an expression, so a "!" after one is the unary
# operator, not a merge request.
EXPRESSION_KEYWORDS = frozenset({"break", "if", "match", "mut", "return", "while", "yield"})
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


def is_merge_request(text: str) -> bool:
    """Reports whether text holds a merge request number in prose: one that
    starts the line or a comment, or follows a word. After an operator, a
    bracket, punctuation or an expression keyword, "!" and digits are the
    unary operator of code such as Rust's, and are not a reference."""
    for match in MERGE_REQUEST.finditer(text):
        before = text[: match.start()].rstrip()
        if COMMENT_PREFIX.fullmatch(before):
            return True
        word = re.search(r"\w+$", before)
        if word and word.group() not in EXPRESSION_KEYWORDS:
            return True
    return False


def is_reference(text: str) -> bool:
    """Reports whether text holds an internal tracker reference."""
    return bool(
        TICKET_REFERENCE.search(text) or PROJECT_REFERENCE.search(text) or is_merge_request(text)
    )


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
        for number, line in enumerate(content.decode("utf-8").splitlines(), start=1):
            if is_reference(line):
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
