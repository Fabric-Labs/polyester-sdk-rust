#!/usr/bin/env python3
"""Fail if the public repository discloses internal references.

README.md and CHANGELOG.md are checked for every internal/QA disclosure
pattern in FORBIDDEN. Every other tracked file outside the generated src/gen/
tree is checked for internal tracker references, in its path and, unless it
is binary, in its text, so they cannot come back.
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
# The tracker's other project keys with a hyphen and an issue number, and
# an internal merge request number: "!" and at least three digits, at the
# start of a line or after a space, comma or opening bracket.
TRACKER_REFERENCE = re.compile(r"\b(?:ADMIN|CHAIN|HULY|ODY|ZIP)-\d+\b|(?<![^\s(\[,])!\d{3,}\b")
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
    """Reports whether text holds an internal tracker reference."""
    return bool(TICKET_REFERENCE.search(text) or TRACKER_REFERENCE.search(text))


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
        "ok: public disclosure checks passed for README.md, CHANGELOG.md"
        " and every tracked file outside src/gen/"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
