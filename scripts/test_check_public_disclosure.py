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
SOURCE = "src/services/orders.rs"
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

    def test_refuses_a_ticket_reference_in_a_source_file(self) -> None:
        failures = self.check({SOURCE: f"//! Orders.\n\n// {TICKET}: keep it.\n".encode()})
        self.assertEqual(failures, [f"{SOURCE}:3: contains an internal tracker reference"])

    def test_refuses_any_case(self) -> None:
        failures = self.check({SOURCE: f'let key = "{TICKET.lower()}-control";\n'.encode()})
        self.assertEqual(failures, [FAILURE])

    def test_refuses_a_ticket_reference_in_a_path(self) -> None:
        name = f"tests/integration/{TICKET.lower().replace('-', '_')}.rs"
        self.assertEqual(
            self.check({name: b"\n"}), [f"{name}: path contains an internal tracker reference"]
        )

    def test_refuses_every_numbered_form(self) -> None:
        for text in (
            "fn " + "poly" + "_8471_since_is_inclusive() {}",
            "fn " + "ticket" + "_8583_zero_price() {}",
            'let key = "' + "poly" + '4996-wd";',
            "POLY" + "_1234",
            # Short numbers are tickets too, hyphenated or not.
            "see " + "POLY" + "-12",
            "see " + "Ticket" + "_42",
            "Poly" + "7",
            # Only Poly1305 itself is excluded: other numbers that start
            # with 1305, and its hyphenated form, are tickets.
            "Poly" + "13050",
            "POLY" + "-1305",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.check({SOURCE: (text + "\n").encode()}), [FAILURE])

    def test_refuses_other_tracker_keys_and_merge_requests(self) -> None:
        for text in (
            "// see " + "CHAIN" + "-17",
            "// see " + "ODY" + "-204",
            "// see " + "ZIP" + "-9",
            "// see " + "HULY" + "-3",
            "// see " + "ADMIN" + "-55",
            "// fixed by " + "!" + "4537",
            "// (" + "!" + "123)",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.check({SOURCE: (text + "\n").encode()}), [FAILURE])

    def test_ignores_generated_code_and_binary_files(self) -> None:
        failures = self.check(
            {
                "src/gen/buffa/orders.v1.rs": f"// {TICKET}\n".encode(),
                "testdata/blob.bin": b"\xff\xfe" + TICKET.encode(),
                # Valid UTF-8, but binary as Git reads it: it holds a NUL byte.
                "testdata/image.bin": b"\x00" + TICKET.encode() + b"\x00",
            }
        )
        self.assertEqual(failures, [])

    def test_a_binary_file_is_still_checked_by_its_path(self) -> None:
        name = "testdata/" + TICKET.lower() + ".bin"
        self.assertEqual(
            self.check({name: b"\x00\x01"}), [f"{name}: path contains an internal tracker reference"]
        )

    def test_a_nul_byte_past_the_sniffed_prefix_leaves_a_text_file(self) -> None:
        content = b"a" * cpd.BINARY_SNIFF_BYTES + b"\n// " + TICKET.encode() + b"\n\x00"
        self.assertEqual(
            self.check({"big.txt": content}), ["big.txt:2: contains an internal tracker reference"]
        )

    def test_accepts_text_without_a_tracker_reference(self) -> None:
        failures = self.check(
            {
                "README.md": b"Polyester SDK, POLYESTER_API_URL, poly-market, POLY- alone, "
                b"a setup ticket, HTTP 404, UTF-8, EIP-712, ERC-20, CVE-2024-1234, "
                b"RFC-5737, TEST-NET-1, SHA-256, LICENSE-2.0, 0.1.0a23.\n",
                "src/crypto.rs": b"// ChaCha20-Poly1305\nconst POLY1305: u8 = 1;\n"
                b"if a != 100 && !b { mask = !0u64 & !0; }\n",
                "docs/guide.md": b"![diagram](img.png) Hello!100 times? a!=123\n",
                "testdata/index.tsv": b"14-depth-chain-00.bin\tdepth-chain\n",
            }
        )
        self.assertEqual(failures, [])

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
