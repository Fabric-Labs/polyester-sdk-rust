"""Tests for scripts/check_public_disclosure.py.

Run with: python3 -m unittest discover -s scripts -p 'test_*.py'
"""

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location(
    "check_public_disclosure", SCRIPTS / "check_public_disclosure.py"
)
assert _spec and _spec.loader
cpd = importlib.util.module_from_spec(_spec)
sys.modules["check_public_disclosure"] = cpd
_spec.loader.exec_module(cpd)

# Built at run time, so this file holds no tracker reference itself.
TICKET = "POLY" + "-" + "1234"
MR = "!" + "4537"
SOURCE = "src/orders.txt"
FAILURE = f"{SOURCE}:1: contains an internal tracker reference"


class TicketReferenceTest(unittest.TestCase):
    def check(self, files: dict[str, bytes]) -> list[str]:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name, content in files.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            return cpd.ticket_references(root, files)

    def assert_refused(self, *lines: str) -> None:
        for line in lines:
            with self.subTest(line=line):
                self.assertEqual(self.check({SOURCE: (line + "\n").encode()}), [FAILURE])

    def assert_accepted(self, *lines: str) -> None:
        for line in lines:
            with self.subTest(line=line):
                self.assertEqual(self.check({SOURCE: (line + "\n").encode()}), [])

    def test_refuses_a_ticket_reference_in_a_source_file(self) -> None:
        failures = self.check({SOURCE: f"Orders.\n\n# {TICKET}: keep it.\n".encode()})
        self.assertEqual(failures, [f"{SOURCE}:3: contains an internal tracker reference"])

    def test_refuses_any_case(self) -> None:
        self.assert_refused(f'key = "{TICKET.lower()}-control"')

    def test_refuses_a_ticket_reference_in_a_path(self) -> None:
        name = f"tests/{TICKET.lower().replace('-', '_')}.py"
        failure = f"{name}: path contains an internal tracker reference"
        self.assertEqual(self.check({name: b"\n"}), [failure])

    def test_refuses_every_numbered_form(self) -> None:
        self.assert_refused(
            "def test_" + "poly" + "_8471_since_is_inclusive(): ...",
            "fn " + "ticket" + "_8583_zero_price() {}",
            'key = "' + "poly" + '4996-wd"',
            "POLY" + "_1234",
            # Short numbers are tickets too, hyphenated or not.
            "see " + "POLY" + "-12",
            "see " + "Ticket" + "_42",
            "Poly" + "7",
            # Only Poly1305 itself is excluded: other numbers that start
            # with 1305, and its hyphenated form, are tickets.
            "Poly" + "13050",
            "POLY" + "-1305",
        )

    def test_refuses_other_project_keys_in_any_case(self) -> None:
        self.assert_refused(
            "# see " + "CHAIN" + "-17",
            "# see " + "ODY" + "-204",
            "// see " + "ZIP" + "-9",
            "# see " + "HULY" + "-3",
            "# see " + "ADMIN" + "-55",
            "# fixed in " + "ody" + "-456",
            "// tracked as " + "huly" + "-789",
            "(" + "Chain" + "-17)",
            "/// " + "zip" + "-9: keep the memo",
        )

    def test_refuses_a_merge_request_in_markdown_prose(self) -> None:
        for line in (
            "Fixed by " + MR + ".",
            "Fixed by " + MR,
            "Fixed (" + MR + ").",
            "Fixed: " + MR,
            MR + " moved this check here.",
            "- " + MR + ": keep the order",
            "Reverted " + MR + " and " + "!" + "4538.",
            "| " + MR + " | merged |",
        ):
            with self.subTest(line=line):
                failures = self.check({"docs/guide.md": f"# Guide\n\n{line}\n".encode()})
                self.assertEqual(
                    failures, ["docs/guide.md:3: contains an internal tracker reference"]
                )

    def test_accepts_a_merge_request_number_in_markdown_code(self) -> None:
        guide = "\n".join(
            (
                "# Guide",
                "Use `!" + "4537` or ``a ` !" + "4537`` inline.",
                "```rust",
                "const COMPLEMENT: i64 = !" + "1024;",
                "Fixed (" + MR + ").",
                "```",
                "~~~~",
                "Fixed: " + MR,
                "```",
                "still code: " + MR,
                "~~~~",
                "   ```",
                "    !" + "1024, // clear this bit",
                "   ```",
                "Back in prose.",
            )
        )
        self.assertEqual(self.check({"docs/guide.md": (guide + "\n").encode()}), [])
        # Prose after a closed fence is checked again.
        after = guide + "\nFixed: " + MR + "\n"
        self.assertEqual(
            self.check({"docs/guide.md": after.encode()}),
            ["docs/guide.md:16: contains an internal tracker reference"],
        )

    def test_never_checks_source_for_a_merge_request_number(self) -> None:
        # "!" and digits are code in source files, comments included: only
        # Markdown prose is checked for a merge request number.
        lines = (
            "const MASKS: [i64; 2] = [",
            "    !" + "1024, // clear this bit",
            "    !" + "2048, // clear this bit",
            "];",
            "const COMPLEMENT: i64 = !" + "1024;",
            "let mask = !" + "1024 & bits;",
            "let mask = bits & !" + "1024;",
            "    | !" + "1024,",
            "call(a, !" + "1024)",
            "Foo { mask: !" + "1024 }",
            "match x { _ => !" + "1024 }",
            "    return !" + "1024;",
            "if !" + "1024 == x {}",
            "let r = &mut !" + "1024;",
            "/// let mask = !" + "1024;",
            "// " + MR + " moved this check here.",
        )
        for name in ("src/bits.rs", "src/bits.py", "bits.go", "Cargo.toml"):
            with self.subTest(name=name):
                content = ("\n".join(lines) + "\n").encode()
                self.assertEqual(self.check({name: content}), [])

    def test_ignores_generated_code_and_binary_files(self) -> None:
        generated = cpd.GENERATED_PREFIXES[0] + "orders/v1/orders.txt"
        failures = self.check(
            {
                generated: f"# {TICKET}\n".encode(),
                "testdata/blob.bin": b"\xff\xfe" + TICKET.encode(),
                # Valid UTF-8, but binary as Git reads it: it holds a NUL byte.
                "testdata/image.bin": b"\x00" + TICKET.encode() + b"\x00",
            }
        )
        self.assertEqual(failures, [])

    def test_a_binary_file_is_still_checked_by_its_path(self) -> None:
        name = "testdata/" + TICKET.lower() + ".bin"
        failure = f"{name}: path contains an internal tracker reference"
        self.assertEqual(self.check({name: b"\x00\x01"}), [failure])

    def test_a_nul_byte_past_the_sniffed_prefix_leaves_a_text_file(self) -> None:
        content = b"a" * cpd.BINARY_SNIFF_BYTES + b"\n# " + TICKET.encode() + b"\n\x00"
        failure = "big.txt:2: contains an internal tracker reference"
        self.assertEqual(self.check({"big.txt": content}), [failure])

    def test_accepts_text_without_a_tracker_reference(self) -> None:
        self.assert_accepted(
            "Polyester SDK, POLYESTER_API_URL, poly-market, POLY- alone, a setup ticket.",
            "HTTP 404, 429 and 503; status=400; UTF-8, EIP-712, ERC-20, SHA-256.",
            "CVE-2024-1234, GHSA-4v7x-pqxf-cx7m, RFC-5737, TEST-NET-1, LICENSE-2.0.",
            "Versions 0.1.0a23, 0.1.0-alpha.45, v1.90.0, py3-none-any, x86_64-linux-1.",
            "14-depth-chain-00.bin\tserver\tDepthSnapshot\tdepth-chain",
            "25-depth-levels-chain-00.bin\tdepth-levels-chain",
            "the chain-agnostic decoder, an admin-scoped key, gzip-9, mod_zip-2",
            "# ChaCha20-Poly1305, POLY1305 = 1, poly1305.Sum(&tag)",
            "if a != 100 and not b: pass",
            "![diagram](img.png) Hello!100 times? a!=123",
        )
        prose = "![diagram](img.png) Hello!100 times? a!=123, HTTP 404, GHSA-4v7x-pqxf-cx7m.\n"
        self.assertEqual(self.check({"README.md": prose.encode()}), [])

    def test_markdown_refuses_a_finding_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "README.md").write_text("ok\n", encoding="utf-8")
            (root / "CHANGELOG.md").write_text("- Tests for " + "F" + "-24.\n", encoding="utf-8")
            self.assertEqual(
                cpd.markdown_disclosures(root), ["CHANGELOG.md: contains internal finding ID"]
            )

    def test_the_repository_is_clean(self) -> None:
        root = SCRIPTS.parent
        self.assertEqual(cpd.ticket_references(root, cpd.tracked_files(root)), [])
        self.assertEqual(cpd.markdown_disclosures(root), [])


if __name__ == "__main__":
    unittest.main()
