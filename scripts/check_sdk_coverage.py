#!/usr/bin/env python3
"""CI gate: every public Connect RPC is wrapped or allowlisted.

Discovers generated Connect procedures under the path in ``manifest.json``,
finds handwritten wrapper call sites for this SDK language, and applies
``sdk-coverage.toml``. Fails if any public RPC is neither wrapped nor
allowlisted.

Usage:
  python3 scripts/check_sdk_coverage.py         # CI / local gate (default)
  python3 scripts/check_sdk_coverage.py --json  # same gate + JSON on stdout
  python3 scripts/check_sdk_coverage.py --write # optional local scratch under .sdk-coverage/
  python3 scripts/check_sdk_coverage.py --write-capabilities
      # Refresh sdk-capabilities.json + README capability table

Keep this file identical across polyester-sdk-{go,python,rust}. Language behavior
is selected from ``manifest.json`` ``sdk``.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None  # type: ignore[assignment]

ROOT = Path(__file__).resolve().parents[1]
PROCEDURE_RE = re.compile(
    r"^/[a-z][a-z0-9_.]+\.[A-Za-z0-9_]+/[A-Za-z0-9_]+$"
)
# Local-only scratch output; must stay gitignored (not published with the SDK).
DEFAULT_WRITE_DIR = ".sdk-coverage"
# Public product capability snapshot (committed; consumed by docs automation).
CAPABILITIES_PATH = "sdk-capabilities.json"
README_PATH = "README.md"
README_CAP_START = "<!-- sdk-capabilities:start -->"
README_CAP_END = "<!-- sdk-capabilities:end -->"
MATRIX_URL = (
    "https://polyester.ai/docs/developer-docs/getting-started/sdk-capability-matrix"
)


def _parse_toml_subset(text: str) -> dict[str, Any]:
    """Minimal TOML reader for sdk-coverage.toml (stdlib-only fallback)."""
    data: dict[str, Any] = {"allowlist": []}
    section: str | None = None
    current: dict[str, str] | None = None

    def flush() -> None:
        nonlocal current
        if current is not None:
            data["allowlist"].append(current)
            current = None

    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if line == "[[allowlist]]":
            flush()
            section = "allowlist"
            current = {}
            continue
        if line.startswith("["):
            flush()
            section = None
            continue
        if "=" not in line:
            die(f"unsupported TOML line (use Python 3.11+ or simplify): {raw}")
        key, value = [p.strip() for p in line.split("=", 1)]
        if not (value.startswith('"') and value.endswith('"')):
            die(f"only double-quoted string values supported in fallback TOML: {raw}")
        value = value[1:-1]
        if section == "allowlist" and current is not None:
            current[key] = value
        else:
            die(f"TOML key outside [[allowlist]]: {raw}")
    flush()
    return data


def load_toml(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if tomllib is not None:
        return tomllib.loads(text)
    return _parse_toml_subset(text)


@dataclass(frozen=True)
class AllowEntry:
    procedure: str | None = None
    service: str | None = None
    reason: str = ""


@dataclass
class CoverageResult:
    sdk: str
    gen_root: str
    descriptor_sha256: str
    procedures: set[str]
    covered: set[str]
    allowlisted: set[str]
    unexpected: set[str]
    allow_reasons: dict[str, str] = field(default_factory=dict)

    @property
    def total(self) -> int:
        return len(self.procedures)

    @property
    def covered_count(self) -> int:
        return len(self.covered)

    @property
    def allowlisted_count(self) -> int:
        return len(self.allowlisted)

    @property
    def accounted(self) -> int:
        return self.covered_count + self.allowlisted_count

    @property
    def pct_wrapped(self) -> float:
        if not self.total:
            return 100.0
        return 100.0 * self.covered_count / self.total

    @property
    def pct_accounted(self) -> float:
        if not self.total:
            return 100.0
        return 100.0 * self.accounted / self.total


def die(msg: str, code: int = 2) -> None:
    print(f"error: {msg}", file=sys.stderr)
    raise SystemExit(code)


def load_manifest(root: Path) -> dict:
    path = root / "manifest.json"
    if not path.is_file():
        die(f"missing {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if "sdk" not in data or "paths" not in data or "gen" not in data["paths"]:
        die("manifest.json must include sdk and paths.gen")
    return data


def load_allowlist(root: Path) -> list[AllowEntry]:
    path = root / "sdk-coverage.toml"
    if not path.is_file():
        die(f"missing {path}")
    data = load_toml(path)
    entries: list[AllowEntry] = []
    for raw in data.get("allowlist") or []:
        proc = raw.get("procedure")
        svc = raw.get("service")
        reason = (raw.get("reason") or "").strip()
        if bool(proc) == bool(svc):
            die(
                "each [[allowlist]] entry needs exactly one of "
                f"`procedure` or `service` (got {raw!r})"
            )
        if not reason:
            die(f"allowlist entry missing reason: {raw!r}")
        if proc and not PROCEDURE_RE.match(proc):
            die(f"invalid allowlist procedure path: {proc}")
        entries.append(AllowEntry(procedure=proc, service=svc, reason=reason))
    return entries


def service_of(procedure: str) -> str:
    return procedure.lstrip("/").rsplit("/", 1)[0]


def method_of(procedure: str) -> str:
    return procedure.rsplit("/", 1)[-1]


def apply_allowlist(
    procedures: set[str],
    covered: set[str],
    entries: list[AllowEntry],
) -> tuple[set[str], set[str], dict[str, str]]:
    reasons: dict[str, str] = {}
    allowlisted: set[str] = set()
    for entry in entries:
        if entry.procedure:
            if entry.procedure not in procedures:
                die(
                    f"allowlist procedure not in public gen: {entry.procedure} "
                    "(remove stale allowlist entry or refresh gen)"
                )
            if entry.procedure in covered:
                die(
                    f"allowlist procedure is already wrapped: {entry.procedure} "
                    "(remove from sdk-coverage.toml)"
                )
            allowlisted.add(entry.procedure)
            reasons[entry.procedure] = entry.reason
            continue
        assert entry.service
        matched = {
            p for p in procedures if service_of(p) == entry.service and p not in covered
        }
        if not matched:
            still_in_gen = {p for p in procedures if service_of(p) == entry.service}
            if still_in_gen and still_in_gen <= covered:
                die(
                    f"allowlist service {entry.service} is fully wrapped; "
                    "remove the service allowlist entry"
                )
            if not still_in_gen:
                die(
                    f"allowlist service not in public gen: {entry.service} "
                    "(remove stale allowlist entry or refresh gen)"
                )
        for proc in matched:
            allowlisted.add(proc)
            reasons[proc] = entry.reason
    unexpected = procedures - covered - allowlisted
    return allowlisted, unexpected, reasons


# ---------------------------------------------------------------------------
# Gen extractors
# ---------------------------------------------------------------------------


def extract_gen_go(gen_root: Path) -> tuple[set[str], dict[str, str]]:
    """Return (procedures, shortServiceName -> FQDN)."""
    procedures: set[str] = set()
    service_names: dict[str, str] = {}
    proc_re = re.compile(r'(\w+)Procedure = "(/[^"]+)"')
    name_re = re.compile(r'(\w+)Name = "([^"]+)"')
    for path in gen_root.rglob("*.go"):
        if "connect" not in path.parts and not path.name.endswith(".connect.go"):
            continue
        text = path.read_text(encoding="utf-8")
        for m in name_re.finditer(text):
            key, value = m.group(1), m.group(2)
            if key.endswith("Service"):
                service_names[key] = value
        for m in proc_re.finditer(text):
            proc = m.group(2)
            if PROCEDURE_RE.match(proc):
                procedures.add(proc)
    return procedures, service_names


def extract_gen_python(
    gen_root: Path,
) -> tuple[set[str], dict[str, set[str]], dict[str, str]]:
    """Return (procedures, snake_method -> paths, ClientClass -> service FQDN)."""
    procedures: set[str] = set()
    snake_to_paths: dict[str, set[str]] = defaultdict(set)
    path_re = re.compile(
        r'"(/[a-z][a-z0-9_.]+\.[A-Za-z0-9_]+/[A-Za-z0-9_]+)"\s*:\s*Endpoint\.unary'
    )
    block_re = re.compile(
        r'"(/[^"]+)"\s*:\s*Endpoint\.unary\(\s*method=MethodInfo\(\s*'
        r'name="(?P<name>[^"]+)",\s*service_name="(?P<svc>[^"]+)",'
        r".*?function=svc\.(?P<snake>[a-z0-9_]+)",
        re.S,
    )
    client_to_service: dict[str, str] = {}
    for path in gen_root.rglob("*_connect.py"):
        text = path.read_text(encoding="utf-8")
        for m in path_re.finditer(text):
            proc = m.group(1)
            if PROCEDURE_RE.match(proc):
                procedures.add(proc)
        for m in block_re.finditer(text):
            proc = m.group(1)
            snake = m.group("snake")
            if PROCEDURE_RE.match(proc):
                snake_to_paths[snake].add(proc)
        for m in re.finditer(r'service_name="([^"]+)"', text):
            svc = m.group(1)
            short = svc.rsplit(".", 1)[-1] + "Client"
            client_to_service[short] = svc
            client_to_service["Async" + short] = svc
    return procedures, snake_to_paths, client_to_service


def extract_gen_rust(gen_root: Path) -> tuple[set[str], dict[str, set[str]]]:
    procedures: set[str] = set()
    snake_to_paths: dict[str, set[str]] = defaultdict(set)
    path_re = re.compile(r'"(/[a-z][a-z0-9_.]+\.[A-Za-z0-9_]+/[A-Za-z0-9_]+)"')
    connect_root = gen_root / "connect"
    if not connect_root.is_dir():
        die(f"rust gen connect dir missing: {connect_root}")
    for path in connect_root.glob("*.rs"):
        text = path.read_text(encoding="utf-8")
        for m in path_re.finditer(text):
            proc = m.group(1)
            if "Service/" not in proc or not PROCEDURE_RE.match(proc):
                continue
            procedures.add(proc)
            method = method_of(proc)
            snake = re.sub(r"(?<!^)(?=[A-Z])", "_", method).lower()
            snake_to_paths[snake].add(proc)
    return procedures, snake_to_paths


# ---------------------------------------------------------------------------
# Wrapper extractors
# ---------------------------------------------------------------------------


def extract_wrapped_go(services_root: Path, service_names: dict[str, str]) -> set[str]:
    covered: set[str] = set()
    call_re = re.compile(
        r"s\.(?P<acc>client|readClient|writeClient|viewClient)\(\)\.(?P<meth>[A-Za-z0-9_]+)"
    )
    def_re = re.compile(
        r"func \(s \*\w+\) (?P<acc>client|readClient|writeClient|viewClient)\(\)"
        r"[^{]*?(?P<svc>\w+)Client\b"
    )
    if not services_root.is_dir():
        die(f"services dir missing: {services_root}")
    for path in sorted(services_root.glob("*.go")):
        text = path.read_text(encoding="utf-8")
        local: dict[str, str] = {}
        for m in def_re.finditer(text):
            local[m.group("acc")] = m.group("svc")
        for m in call_re.finditer(text):
            acc, meth = m.group("acc"), m.group("meth")
            short = local.get(acc)
            if not short:
                continue
            fqdn = service_names.get(short)
            if not fqdn:
                continue
            covered.add(f"/{fqdn}/{meth}")
    return covered


def extract_wrapped_python(
    services_root: Path,
    snake_to_paths: dict[str, set[str]],
    client_to_service: dict[str, str],
) -> set[str]:
    covered: set[str] = set()
    call_re = re.compile(
        r"(?:unary_auth_decoded|unary_public_decoded|unary_auth|unary_public|"
        r"unary_auth_message|unary_public_message)\(\s*"
        r"[^,]+,\s*([A-Za-z]+ServiceClient)\s*,\s*"
        r"lambda\s+client\s*,\s*\w+\s*:\s*client\.([a-z][a-z0-9_]*)\(",
        re.S,
    )
    unbound_re = re.compile(r"([A-Za-z]+ServiceClient)\.([a-z][a-z0-9_]*)\b")

    def resolve(cls: str, snake: str) -> str | None:
        paths = snake_to_paths.get(snake, set())
        svc = client_to_service.get(cls)
        if svc:
            matched = [p for p in paths if p.startswith("/" + svc + "/")]
            if len(matched) == 1:
                return matched[0]
            if len(matched) > 1:
                return None
        if len(paths) == 1:
            return next(iter(paths))
        return None

    if not services_root.is_dir():
        die(f"services dir missing: {services_root}")
    for path in sorted(services_root.glob("*.py")):
        if path.name.startswith("_"):
            continue
        text = path.read_text(encoding="utf-8")
        for m in call_re.finditer(text):
            proc = resolve(m.group(1), m.group(2))
            if proc:
                covered.add(proc)
        for m in unbound_re.finditer(text):
            proc = resolve(m.group(1), m.group(2))
            if proc:
                covered.add(proc)
    return covered


_RUST_METHOD_NOISE = frozenset(
    {
        "await_auth",
        "await_public",
        "ok",
        "map_err",
        "clone",
        "as_ref",
        "into",
        "from",
        "new",
        "expect",
        "unwrap",
        "to_string",
        "as_str",
        "is_empty",
        "len",
        "push",
        "insert",
        "get",
        "or_else",
        "and_then",
        "unwrap_or",
        "unwrap_or_else",
        "unwrap_or_default",
        "context",
        "with_context",
        "collect",
        "iter",
        "into_iter",
        "filter",
        "map",
        "copied",
        "cloned",
        "flatten",
        "format",
        "write",
        "writeln",
        "print",
        "println",
        "eprintln",
        "dbg",
        "default",
        "parse",
        "try_into",
        "try_from",
        "to_owned",
        "to_vec",
        "as_bytes",
        "as_slice",
        "connect_client",
        "client",
        "factory",
        "require_credentials",
    }
)


def extract_wrapped_rust(
    services_root: Path,
    procedures: set[str],
    snake_to_paths: dict[str, set[str]],
) -> set[str]:
    covered: set[str] = set()
    path_re = re.compile(r'"(/[a-z][a-z0-9_.]+\.[A-Za-z0-9_]+/[A-Za-z0-9_]+)"')
    method_re = re.compile(r"\.([a-z][a-z0-9_]*)(?:_with_options)?\s*\(")
    if not services_root.is_dir():
        die(f"services dir missing: {services_root}")
    for path in sorted(services_root.rglob("*.rs")):
        text = path.read_text(encoding="utf-8")
        for m in path_re.finditer(text):
            proc = m.group(1)
            if proc in procedures:
                covered.add(proc)
        for m in method_re.finditer(text):
            snake = m.group(1)
            if snake in _RUST_METHOD_NOISE:
                continue
            paths = snake_to_paths.get(snake, set())
            if len(paths) == 1:
                covered.add(next(iter(paths)))
    return covered


def analyze(root: Path) -> CoverageResult:
    manifest = load_manifest(root)
    sdk = manifest["sdk"]
    gen_rel = manifest["paths"]["gen"]
    gen_root = root / gen_rel
    descriptor = manifest.get("publicDescriptorSha256") or ""
    allow_entries = load_allowlist(root)

    if sdk == "go":
        procedures, service_names = extract_gen_go(gen_root)
        covered = extract_wrapped_go(root / "services", service_names)
    elif sdk == "python":
        procedures, snake_to_paths, client_to_service = extract_gen_python(gen_root)
        covered = extract_wrapped_python(
            root / "src" / "polyester" / "services",
            snake_to_paths,
            client_to_service,
        )
    elif sdk == "rust":
        procedures, snake_to_paths = extract_gen_rust(gen_root)
        covered = extract_wrapped_rust(
            root / "src" / "services",
            procedures,
            snake_to_paths,
        )
    else:
        die(f"unsupported sdk in manifest.json: {sdk!r}")

    unknown = covered - procedures
    if unknown:
        sample = ", ".join(sorted(unknown)[:5])
        die(f"wrapper extractor produced paths not in gen ({len(unknown)}): {sample}")

    allowlisted, unexpected, reasons = apply_allowlist(
        procedures, covered, allow_entries
    )
    return CoverageResult(
        sdk=sdk,
        gen_root=gen_rel,
        descriptor_sha256=descriptor,
        procedures=procedures,
        covered=covered,
        allowlisted=allowlisted,
        unexpected=unexpected,
        allow_reasons=reasons,
    )


# ---------------------------------------------------------------------------
# Optional local scratch reporting (gitignored; not for publication)
# ---------------------------------------------------------------------------


def by_service(procedures: Iterable[str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = defaultdict(list)
    for proc in sorted(procedures):
        out[service_of(proc)].append(proc)
    return dict(out)


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)  # noqa: UP017
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def build_json(result: CoverageResult) -> dict:
    gaps_allowlisted = [
        {"procedure": p, "reason": result.allow_reasons[p]}
        for p in sorted(result.allowlisted)
    ]
    services = []
    for svc, procs in by_service(result.procedures).items():
        wrapped = sorted(p for p in procs if p in result.covered)
        allowed = sorted(p for p in procs if p in result.allowlisted)
        missing = sorted(p for p in procs if p in result.unexpected)
        services.append(
            {
                "service": svc,
                "total": len(procs),
                "wrapped": len(wrapped),
                "allowlisted": len(allowed),
                "unexpected": len(missing),
                "pct_wrapped": round(100.0 * len(wrapped) / len(procs), 2),
                "missing_unexpected": missing,
            }
        )
    return {
        "schemaVersion": 1,
        "sdk": result.sdk,
        "generatedAt": _utc_now_iso(),
        "publicDescriptorSha256": result.descriptor_sha256,
        "genRoot": result.gen_root,
        "summary": {
            "procedures": result.total,
            "wrapped": result.covered_count,
            "allowlisted": result.allowlisted_count,
            "unexpected": len(result.unexpected),
            "pctWrapped": round(result.pct_wrapped, 2),
            "pctAccounted": round(result.pct_accounted, 2),
        },
        "allowlistedGaps": gaps_allowlisted,
        "unexpectedGaps": sorted(result.unexpected),
        "services": services,
        "wrappedProcedures": sorted(result.covered),
    }


# Product-facing rows for the public SDK capability matrix.
# Detection uses wrapped Connect services (and a few filesystem heuristics).
# When a new public Connect service is wrapped but unmapped, CI fails so a row
# can be added intentionally (or folded into an existing row).
CAPABILITY_DEFS: list[dict[str, Any]] = [
    {
        "id": "market_data",
        "label": "Public market data (spot config, trades, candles)",
        "any_services": ["marketdata.v1.MarketDataService"],
    },
    {
        "id": "orderbook",
        "label": "Order book snapshot + realtime",
        "any_services": ["orderbook.v1.OrderbookService"],
    },
    {
        "id": "market_overview",
        "label": "Market overview (list + subscribe)",
        "any_services": ["marketoverview.v1.MarketOverviewService"],
    },
    {
        "id": "heatmap",
        "label": "Order book heatmap",
        "any_services": ["marketdata.v1.HeatmapService"],
    },
    {
        "id": "api_key_auth",
        "label": "API-key (Ed25519 signature) auth",
        "kind": "always_yes",
    },
    {
        "id": "wallet_login",
        "label": "Wallet / browser login",
        "kind": "always_no",
        "note": "API-key SDKs only; use the TypeScript browser client.",
    },
    {
        "id": "session_mfa",
        "label": "Session MFA enrollment and challenges",
        "kind": "always_no",
        "note": "API-key SDKs only; use the TypeScript browser client.",
    },
    {
        "id": "profile",
        "label": "Profile (identity subscribe)",
        "kind": "always_yes",
    },
    {
        "id": "api_keys",
        "label": "API keys (list/get/subscribe/local keypair generation)",
        "any_services": ["auth.v1.ApiKeyService"],
    },
    {
        "id": "subaccounts",
        "label": "Subaccounts (list/get/members/invites/activity/subscribe)",
        "any_services": [
            "auth.v1.SubaccountService",
            "auth.v1.SubaccountViewService",
        ],
    },
    {
        "id": "address_book",
        "label": "Address book (list/view/create/update/subscribe)",
        "any_services": ["auth.v1.AddressBookService"],
    },
    {
        "id": "policies",
        "label": "Policies (realtime subscribe)",
        "kind": "always_yes",
    },
    {
        "id": "guard_signer",
        "label": "Guard signer",
        "any_services": ["chain.guard.v1.GuardSignerService"],
    },
    {
        "id": "vip",
        "label": "VIP tiers + status",
        "any_services": ["vip.v1.VIPService"],
    },
    {
        "id": "spot_fees",
        "label": "Spot fee rates",
        "any_services": ["fees.v1.FeeService"],
    },
    {
        "id": "trading_rate_limits",
        "label": "Trading rate limits",
        "any_services": ["ratelimit.v1.RateLimitService"],
    },
    {
        "id": "balances",
        "label": "Balances, holds, equity history",
        "any_services": ["ledger.read.v1.LedgerReadService"],
        # LedgerRead also backs transfers; balances is yes if any ledger read RPC wrapped.
    },
    {
        "id": "orders",
        "label": "Orders (create, cancel, modify, batch, cancel-all)",
        "any_services": [
            "orders.v1.OrdersService",
            "orders.v1.OrdersReadService",
        ],
    },
    {
        "id": "user_trades",
        "label": "User trades",
        "any_services": ["orders.v1.OrdersReadService"],
        "any_procedures": ["/orders.v1.OrdersReadService/GetUserTrades"],
    },
    {
        "id": "triggers",
        "label": "Triggers",
        "any_services": ["triggers.v1.TriggersService"],
    },
    {
        "id": "internal_transfers",
        "label": "Internal transfers",
        "any_services": ["transfer.v1.InternalTransferService"],
    },
    {
        "id": "transfer_history",
        "label": "Transfer history",
        "any_procedures": ["/ledger.read.v1.LedgerReadService/ListTransfers"],
    },
    {
        "id": "deposit_addresses",
        "label": "Deposit addresses",
        "any_services": ["chain.deposit.v1.DepositAddressService"],
    },
    {
        "id": "withdraws",
        "label": "Trading / funding withdraws",
        "any_services": ["chain.withdraw.v1.WithdrawService"],
    },
    {
        "id": "zipper_config",
        "label": "Zipper deposit-withdraw config",
        "any_services": ["chain.zipper.v1.ZipperService"],
    },
    {
        "id": "chain_analytics",
        "label": "Chain analytics",
        "any_services": ["chain.analytics.v1.ChainAnalyticsService"],
    },
    {
        "id": "lifecycle",
        "label": "Lifecycle flows",
        "any_services": ["chain.lifecycle.v1.LifecycleReadService"],
    },
    {
        "id": "collab",
        "label": "Polychart / layout / whiteboard",
        "any_services": [
            "polychart.v1.PolychartService",
            "layout.v1.LayoutService",
            "collab.v1.WhiteboardService",
        ],
    },
    {
        "id": "realtime",
        "label": "Realtime account and market streams",
        "kind": "realtime",
    },
    {
        "id": "catalogs",
        "label": "Reference catalogs + wait-for-ready",
        "kind": "catalogs",
    },
    {
        "id": "qty_price",
        "label": "Qty / price decimal + scaled-int inputs",
        "kind": "qty_price",
    },
    {
        "id": "auth_me",
        "label": "Auth Me (API-key session)",
        "any_procedures": ["/auth.v1.AuthService/Me"],
        "hidden": True,  # covered by api_key_auth row; keep for unmapped checks
    },
    {
        "id": "social_verification",
        "label": "Social verification",
        "any_services": ["auth.v1.SocialVerificationService"],
    },
    {
        "id": "resolve",
        "label": "Account resolve / lookup",
        "any_services": ["auth.v1.ResolveService"],
    },
]


def _services_in_result(result: CoverageResult) -> set[str]:
    return {service_of(p) for p in result.procedures}


def _service_has_wrapper(result: CoverageResult, service: str) -> bool:
    return any(
        p in result.covered for p in result.procedures if service_of(p) == service
    )


def _procedure_wrapped(result: CoverageResult, procedure: str) -> bool:
    return procedure in result.covered


def _detect_qty_price(root: Path, sdk: str) -> bool:
    if sdk == "go":
        return (root / "codecs" / "scalars.go").is_file()
    if sdk == "python":
        return (root / "src" / "polyester" / "codecs" / "scalars.py").is_file()
    if sdk == "rust":
        return (root / "src" / "types" / "money.rs").is_file()
    return False


def _detect_catalogs(root: Path, sdk: str) -> bool:
    if sdk == "go":
        return (root / "catalogs").is_dir()
    if sdk == "python":
        return (root / "src" / "polyester" / "catalogs").is_dir()
    if sdk == "rust":
        return (root / "src" / "catalogs").is_dir() or (
            root / "src" / "catalog"
        ).is_dir()
    return False


def _detect_realtime(root: Path, sdk: str) -> bool:
    if sdk == "go":
        return (root / "realtime").is_dir()
    if sdk == "python":
        return (root / "src" / "polyester" / "realtime").is_dir()
    if sdk == "rust":
        return (root / "src" / "realtime").is_dir()
    return False


def build_capabilities(result: CoverageResult, root: Path) -> dict[str, Any]:
    """Public product capability snapshot derived from wrappers + heuristics."""
    rows: list[dict[str, Any]] = []
    mapped_services: set[str] = set()
    for cap in CAPABILITY_DEFS:
        for svc in cap.get("any_services") or []:
            mapped_services.add(svc)
        for proc in cap.get("any_procedures") or []:
            mapped_services.add(service_of(proc))
        kind = cap.get("kind")
        if kind == "always_yes":
            supported = True
        elif kind == "always_no":
            supported = False
        elif kind == "qty_price":
            supported = _detect_qty_price(root, result.sdk)
        elif kind == "catalogs":
            supported = _detect_catalogs(root, result.sdk)
        elif kind == "realtime":
            supported = _detect_realtime(root, result.sdk)
        elif cap.get("any_procedures"):
            supported = any(
                _procedure_wrapped(result, p) for p in cap["any_procedures"]
            )
        else:
            supported = any(
                _service_has_wrapper(result, s) for s in (cap.get("any_services") or [])
            )
        if cap.get("hidden"):
            continue
        row = {
            "id": cap["id"],
            "label": cap["label"],
            "supported": bool(supported),
        }
        if cap.get("note"):
            row["note"] = cap["note"]
        rows.append(row)

    # Services that appear in public gen with at least one wrapper, but no matrix row.
    unmapped = sorted(
        svc
        for svc in _services_in_result(result)
        if svc not in mapped_services
        and _service_has_wrapper(result, svc)
        and not svc.endswith("MFAService")
    )
    return {
        "schemaVersion": 1,
        "sdk": result.sdk,
        "generatedAt": _utc_now_iso(),
        "publicDescriptorSha256": result.descriptor_sha256,
        "capabilities": rows,
        "unmappedWrappedServices": unmapped,
    }


def capabilities_match(existing: Path, payload: dict) -> bool:
    if not existing.is_file():
        return False
    try:
        current = json.loads(existing.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    a = dict(current)
    b = dict(payload)
    a.pop("generatedAt", None)
    b.pop("generatedAt", None)
    return a == b


def build_readme_capabilities_inner(capabilities: dict[str, Any]) -> str:
    """Markdown between README capability markers (no marker lines)."""
    lines = [
        "<!-- Generated by scripts/check_sdk_coverage.py --write-capabilities. Do not edit. -->",
        "",
        "| Capability | Supported |",
        "| --- | --- |",
    ]
    for row in capabilities.get("capabilities") or []:
        label = str(row["label"]).replace("|", "\\|")
        cell = "Yes" if row.get("supported") else "No"
        lines.append(f"| {label} | {cell} |")
    lines += [
        "",
        "Rows marked **No** are intentional for API-key SDKs (use the TypeScript",
        "browser client for wallet login and session MFA).",
        "",
        "Full cross-language comparison:",
        f"[SDK capability matrix]({MATRIX_URL}).",
        "",
    ]
    return "\n".join(lines)


def build_readme_capabilities_block(capabilities: dict[str, Any]) -> str:
    return (
        f"{README_CAP_START}\n"
        f"{build_readme_capabilities_inner(capabilities)}"
        f"{README_CAP_END}\n"
    )


def _replace_supported_surface_body(readme: str, block: str) -> str | None:
    """Replace body under ## Supported surface through the next ## heading."""
    heading = "## Supported surface"
    start = readme.find(heading)
    if start < 0:
        return None
    body_start = start + len(heading)
    # Skip a single newline after the heading.
    if body_start < len(readme) and readme[body_start] == "\n":
        body_start += 1
    next_heading = re.search(r"\n## ", readme[body_start:])
    if next_heading is None:
        return readme[:body_start] + "\n" + block + (
            "" if block.endswith("\n") else "\n"
        )
    # Keep the newline that precedes the next ## heading.
    abs_next = body_start + next_heading.start()
    return readme[:body_start] + "\n" + block + readme[abs_next:]


def sync_readme_capabilities(readme_path: Path, capabilities: dict[str, Any]) -> bool:
    """Write capability table into README. Returns True if the file changed."""
    if not readme_path.is_file():
        raise FileNotFoundError(f"missing {readme_path}")
    block = build_readme_capabilities_block(capabilities)
    text = readme_path.read_text(encoding="utf-8")
    if README_CAP_START in text and README_CAP_END in text:
        pattern = re.compile(
            re.escape(README_CAP_START) + r".*?" + re.escape(README_CAP_END) + r"\n?",
            re.DOTALL,
        )
        new_text, n = pattern.subn(block, text, count=1)
        if n != 1:
            raise RuntimeError(f"failed to replace capability markers in {readme_path}")
    else:
        new_text = _replace_supported_surface_body(text, block)
        if new_text is None:
            raise RuntimeError(
                f"README missing '{README_CAP_START}' markers and no "
                f"'## Supported surface' section to seed: {readme_path}"
            )
    if new_text == text:
        return False
    readme_path.write_text(new_text, encoding="utf-8")
    return True


def readme_capabilities_match(readme_path: Path, capabilities: dict[str, Any]) -> bool:
    if not readme_path.is_file():
        return False
    text = readme_path.read_text(encoding="utf-8")
    if README_CAP_START not in text or README_CAP_END not in text:
        return False
    expected = build_readme_capabilities_block(capabilities)
    pattern = re.compile(
        re.escape(README_CAP_START) + r".*?" + re.escape(README_CAP_END) + r"\n?",
        re.DOTALL,
    )
    m = pattern.search(text)
    if not m:
        return False
    return m.group(0) == expected


def build_markdown(result: CoverageResult, payload: dict) -> str:
    summary = payload["summary"]
    lines = [
        f"# SDK Connect coverage ({result.sdk})",
        "",
        "Local scratch report from `scripts/check_sdk_coverage.py --write`.",
        "Do not commit this file — keep coverage dashboards out of the published SDK tree.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Public Connect procedures | {summary['procedures']} |",
        f"| Wrapped by handwritten SDK services | {summary['wrapped']} |",
        f"| Allowlisted intentional gaps | {summary['allowlisted']} |",
        f"| Unexpected gaps | {summary['unexpected']} |",
        f"| % wrapped | {summary['pctWrapped']:.2f}% |",
        f"| % accounted (wrapped + allowlisted) | {summary['pctAccounted']:.2f}% |",
        "",
        "## Intentional gaps (allowlisted)",
        "",
    ]
    if payload["allowlistedGaps"]:
        lines += ["| Procedure | Reason |", "| --- | --- |"]
        for gap in payload["allowlistedGaps"]:
            reason = gap["reason"].replace("|", "\\|")
            lines.append(f"| `{gap['procedure']}` | {reason} |")
    else:
        lines.append("_None._")
    lines += ["", "## Unexpected gaps", ""]
    if payload["unexpectedGaps"]:
        for proc in payload["unexpectedGaps"]:
            lines.append(f"- `{proc}`")
    else:
        lines.append("_None._")
    lines += ["", "## By service", ""]
    lines += [
        "| Service | Total | Wrapped | Allowlisted | Unexpected | % wrapped |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for svc in payload["services"]:
        lines.append(
            f"| `{svc['service']}` | {svc['total']} | {svc['wrapped']} | "
            f"{svc['allowlisted']} | {svc['unexpected']} | {svc['pct_wrapped']:.1f}% |"
        )
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write",
        action="store_true",
        help=f"Write local scratch reports under {DEFAULT_WRITE_DIR}/ (gitignored)",
    )
    parser.add_argument(
        "--write-capabilities",
        action="store_true",
        help=(
            f"Write committed {CAPABILITIES_PATH} and refresh the README "
            "capability table between sdk-capabilities markers"
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the coverage JSON report to stdout (still enforces gates)",
    )
    parser.add_argument(
        "--capabilities-json",
        action="store_true",
        help="Print capabilities JSON to stdout (still enforces gates)",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help="SDK repository root (default: parent of scripts/)",
    )
    args = parser.parse_args(argv)

    root = args.root.resolve()
    result = analyze(root)
    payload = build_json(result)
    capabilities = build_capabilities(result, root)
    cap_path = root / CAPABILITIES_PATH
    readme_path = root / README_PATH

    if args.capabilities_json:
        json.dump(capabilities, sys.stdout, indent=2, sort_keys=False)
        sys.stdout.write("\n")
    elif args.json:
        payload = dict(payload)
        payload["capabilities"] = capabilities
        json.dump(payload, sys.stdout, indent=2, sort_keys=False)
        sys.stdout.write("\n")

    if args.write:
        out_dir = root / DEFAULT_WRITE_DIR
        out_dir.mkdir(parents=True, exist_ok=True)
        md_path = out_dir / "sdk-coverage.md"
        json_path = out_dir / "sdk-coverage.json"
        md_path.write_text(build_markdown(result, payload), encoding="utf-8")
        json_path.write_text(
            json.dumps(payload, indent=2, sort_keys=False) + "\n",
            encoding="utf-8",
        )
        print(
            f"wrote {md_path.relative_to(root)} (local scratch; do not commit)",
            file=sys.stderr,
        )
        print(
            f"wrote {json_path.relative_to(root)} (local scratch; do not commit)",
            file=sys.stderr,
        )

    if args.write_capabilities:
        if not capabilities_match(cap_path, capabilities):
            cap_path.write_text(
                json.dumps(capabilities, indent=2, sort_keys=False) + "\n",
                encoding="utf-8",
            )
            print(f"wrote {cap_path.relative_to(root)}")
        else:
            print(f"ok: {cap_path.relative_to(root)} unchanged")
        try:
            changed = sync_readme_capabilities(readme_path, capabilities)
        except (FileNotFoundError, RuntimeError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        if changed:
            print(f"wrote {readme_path.relative_to(root)} capability table")
        else:
            print(f"ok: {readme_path.relative_to(root)} capability table unchanged")

    # Keep the one-line summary off stdout when emitting JSON (for piping).
    summary_line = (
        f"{result.sdk}: {result.covered_count}/{result.total} wrapped "
        f"({result.pct_wrapped:.1f}%), "
        f"{result.allowlisted_count} allowlisted, "
        f"{len(result.unexpected)} unexpected"
    )
    print(
        summary_line,
        file=sys.stderr if (args.json or args.capabilities_json) else sys.stdout,
    )

    exit_code = 0
    if result.unexpected:
        print("unexpected gaps:", file=sys.stderr)
        for proc in sorted(result.unexpected):
            print(f"  {proc}", file=sys.stderr)
        print(
            "Wrap these RPCs or add them to sdk-coverage.toml with a reason.",
            file=sys.stderr,
        )
        exit_code = 1

    if capabilities.get("unmappedWrappedServices"):
        print("unmapped wrapped Connect services:", file=sys.stderr)
        for svc in capabilities["unmappedWrappedServices"]:
            print(f"  {svc}", file=sys.stderr)
        print(
            "Add a CAPABILITY_DEFS row (or fold into an existing row) in "
            "scripts/check_sdk_coverage.py, then re-run with --write-capabilities.",
            file=sys.stderr,
        )
        exit_code = 1

    if not args.write_capabilities and not capabilities_match(cap_path, capabilities):
        print(
            f"error: committed {CAPABILITIES_PATH} is missing or stale\n"
            "  run: python3 scripts/check_sdk_coverage.py --write-capabilities",
            file=sys.stderr,
        )
        exit_code = 1
    elif not args.write_capabilities:
        print(f"ok: {CAPABILITIES_PATH} matches wrappers")

    if not args.write_capabilities and not readme_capabilities_match(
        readme_path, capabilities
    ):
        print(
            f"error: {README_PATH} capability table is missing or stale\n"
            "  run: python3 scripts/check_sdk_coverage.py --write-capabilities",
            file=sys.stderr,
        )
        exit_code = 1
    elif not args.write_capabilities:
        print(f"ok: {README_PATH} capability table matches wrappers")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
