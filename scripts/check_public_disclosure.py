#!/usr/bin/env python3
"""Fail if public-facing markdown contains internal/QA disclosure patterns."""

from __future__ import annotations

import re
import sys
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


def main() -> int:
    failures: list[str] = []
    for name in FILES:
        path = ROOT / name
        if not path.is_file():
            failures.append(f"{name}: missing")
            continue
        text = path.read_text(encoding="utf-8")
        for pattern, description in FORBIDDEN:
            if pattern.search(text):
                failures.append(f"{name}: contains {description}")
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print("ok: public disclosure checks passed for README.md and CHANGELOG.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
