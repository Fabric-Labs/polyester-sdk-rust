#!/usr/bin/env python3
"""Release channels for the Polyester SDK: stable tags and devnet pre-releases.

Stable releases are cut by hand. CHANGELOG, README and the package version are
bumped in a pull request, and pushing an annotated ``vX.Y.Z`` tag runs the
stable publish path. Every push to ``main`` whose CHANGELOG has entries under
``## Unreleased`` publishes a devnet pre-release of the next version instead:

    Go      tag v<target>-devnet.<ts>     go get resolves @latest to stable
    Rust    crate <target>-devnet.<ts>    Cargo skips pre-releases by default
    Python  wheel <target>.dev<ts>        pip skips pre-releases without --pre

``<ts>`` is the commit time as ``YYYYMMDDHHmmss`` UTC, so a version only grows
along ``main`` and a re-run of a commit computes the same version. The target
is the next minor after the last stable tag when Unreleased has a
``**Breaking:**`` entry (the next major from 1.0 on), and the next patch
otherwise.

The workflows run each step through this script so that the routing, the
version and the artefacts can be tested and dry-run without publishing:

    route    which path a workflow run takes: stable, devnet or none
    plan     the devnet version of a commit, or skip when Unreleased is empty
    stamp    write a devnet version into the package manifest (build only)
    verify   check the built artefact carries the version (no publishing)
    exists   whether the registry already has the version
    check-registry  after publishing: the version resolves, default stays stable
    release  move the rolling GitHub pre-release to this snapshot

Only the Python standard library is used (Python 3.11 or later).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib
import urllib.error
import urllib.request
import zipfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

ECOSYSTEMS = ("go", "python", "rust")

STABLE_TAG = re.compile(r"^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
# A tag carrying "-devnet" belongs to the devnet channel, never to the stable
# path. The workflow's tag filter excludes the same pattern ("!v*-devnet*").
DEVNET_MARK = "-devnet"
BREAKING = re.compile(r"\*\*Breaking\b")
ENTRY = re.compile(r"^\s*[-*+]\s+\S")
UNRELEASED_HEADING = re.compile(r"^##\s+Unreleased\s*$", re.IGNORECASE)
TIMESTAMP = re.compile(r"(\d{14})$")

# GitHub release tags this channel owns. Go: one immutable tag per snapshot,
# which is also the module version. Rust and Python: one rolling tag per
# target version, moved to each snapshot like the TypeScript SDK's.
RELEASE_TAG = {
    "go": re.compile(r"^v(\d+\.\d+\.\d+)-devnet\.\d{14}$"),
    "python": re.compile(r"^v(\d+\.\d+\.\d+)-devnet$"),
    "rust": re.compile(r"^v(\d+\.\d+\.\d+)-devnet$"),
}

# GitHub refuses a release body longer than this many characters (HTTP 422,
# "body is too long"). Notes are built to stay under RELEASE_NOTES_BUDGET, a
# margin below it; longer Unreleased text is linked, never cut.
GITHUB_RELEASE_BODY_LIMIT = 125_000
RELEASE_NOTES_BUDGET = 120_000
# owner/name of a GitHub repository, as GITHUB_REPOSITORY names it or as the
# end of an origin URL (https://github.com/o/r.git, git@github.com:o/r.git).
REPOSITORY = re.compile(r"[A-Za-z0-9-]+/[A-Za-z0-9._-]+")
GITHUB_REMOTE = re.compile(r"github\.com[:/]([A-Za-z0-9-]+/[A-Za-z0-9._-]+?)(?:\.git)?/?$")

USER_AGENT = "polyester-sdk-release-check"
GO_MODULE_LINE = re.compile(r"^module\s+(\S+)\s*$", re.MULTILINE)

# The checks parse what cargo, pip and go print, so every tool runs with color
# off whatever the runner exports (dtolnay/rust-toolchain sets
# CARGO_TERM_COLOR=always), and parsed text has escape sequences stripped.
NO_COLOR_ENV = {"NO_COLOR": "1", "CARGO_TERM_COLOR": "never", "PIP_NO_COLOR": "1"}
FORCE_COLOR_ENV = ("FORCE_COLOR", "CLICOLOR_FORCE")
# CSI sequences (colors, cursor) and OSC sequences (hyperlinks).
ANSI_ESCAPE = re.compile(r"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07\x1b]*(?:\x07|\x1b\\))")


class ReleaseError(Exception):
    """A release step cannot continue; the message says why."""


Version = tuple[int, int, int]


def fmt(version: Version) -> str:
    return ".".join(str(part) for part in version)


# --------------------------------------------------------------------------- route


@dataclass(frozen=True)
class Route:
    channel: str  # "stable", "devnet" or "none"
    dry_run: bool
    reason: str


def is_devnet_tag(name: str) -> bool:
    return DEVNET_MARK in name


def parse_bool(value: str | None) -> bool:
    text = (value or "").strip().lower()
    if text in ("", "false", "0", "no"):
        return False
    if text in ("true", "1", "yes"):
        return True
    raise ReleaseError(f"not a boolean: {value!r}")


def route(event: str, ref: str, dry_run: bool) -> Route:
    """Pick the publish path for a workflow run.

    A pushed ``v*`` tag takes the stable path unless it is a devnet tag. A push
    to main takes the devnet path. A manual run takes the devnet path, and
    publishes only from main with dry_run off.
    """
    if event == "push":
        if ref.startswith("refs/tags/"):
            name = ref.removeprefix("refs/tags/")
            if is_devnet_tag(name):
                return Route("none", False, f"{name} is a devnet tag; it never publishes")
            if name.startswith("v"):
                return Route("stable", False, f"{name} is a release tag")
            return Route("none", False, f"{name} is not a release tag")
        if ref == "refs/heads/main":
            return Route("devnet", False, "push to main")
        return Route("none", False, f"push to {ref} publishes nothing")
    if event == "workflow_dispatch":
        if dry_run:
            return Route("devnet", True, f"manual dry run on {ref}")
        if ref == "refs/heads/main":
            return Route("devnet", False, "manual run on main")
        raise ReleaseError(f"a publishing manual run must start from main, not {ref}; set dry_run")
    return Route("none", False, f"{event} events publish nothing")


# --------------------------------------------------------------------------- plan


def git(root: Path, *args: str) -> str:
    result = subprocess.run(["git", *args], cwd=root, capture_output=True, text=True)
    if result.returncode != 0:
        raise ReleaseError(f"git {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def stable_versions(tags: Sequence[str]) -> list[Version]:
    versions = []
    for tag in tags:
        match = STABLE_TAG.match(tag.strip())
        if match:
            versions.append((int(match[1]), int(match[2]), int(match[3])))
    return sorted(versions)


def last_stable(root: Path, rev: str) -> Version:
    tags = git(root, "tag", "--merged", rev, "--list", "v*").splitlines()
    versions = stable_versions(tags)
    if not versions:
        raise ReleaseError(
            f"no vX.Y.Z tag is reachable from {rev}; check out with tags (fetch-depth: 0)"
        )
    return versions[-1]


def unreleased_section(changelog: str) -> list[str]:
    """Lines under ``## Unreleased`` up to the next ``## `` heading."""
    lines: list[str] = []
    inside = False
    for line in changelog.splitlines():
        if UNRELEASED_HEADING.match(line):
            inside = True
            continue
        if inside and line.startswith("## "):
            break
        if inside:
            lines.append(line)
    return lines


def next_version(base: Version, breaking: bool) -> Version:
    major, minor, patch = base
    if breaking:
        return (0, minor + 1, 0) if major == 0 else (major + 1, 0, 0)
    return (major, minor, patch + 1)


def commit_timestamp(root: Path, rev: str) -> str:
    seconds = int(git(root, "show", "-s", "--format=%ct", rev))
    return datetime.fromtimestamp(seconds, UTC).strftime("%Y%m%d%H%M%S")


def package_version(target: str, timestamp: str, ecosystem: str) -> str:
    if ecosystem == "python":
        return f"{target}.dev{timestamp}"  # PEP 440 developmental release
    return f"{target}-devnet.{timestamp}"  # SemVer pre-release


def release_tag(target: str, version: str, ecosystem: str) -> str:
    return f"v{version}" if ecosystem == "go" else f"v{target}-devnet"


@dataclass(frozen=True)
class Plan:
    ecosystem: str
    sha: str
    skip: bool
    reason: str
    base: str = ""
    target: str = ""
    breaking: bool = False
    timestamp: str = ""
    version: str = ""
    tag: str = ""
    title: str = ""
    entries: tuple[str, ...] = ()

    def outputs(self) -> dict[str, str]:
        return {
            "skip": str(self.skip).lower(),
            "sha": self.sha,
            "base": self.base,
            "target": self.target,
            "breaking": str(self.breaking).lower(),
            "timestamp": self.timestamp,
            "version": self.version,
            "tag": self.tag,
            "title": self.title,
        }


def changelog_at(root: Path, sha: str) -> str:
    """CHANGELOG.md as the commit ``sha`` holds it, whatever the working tree
    holds: the plan and the notes of a commit both read it from there."""
    return git(root, "show", f"{sha}:CHANGELOG.md")


def plan(root: Path, ecosystem: str, rev: str = "HEAD") -> Plan:
    sha = git(root, "rev-parse", f"{rev}^{{commit}}")
    section = unreleased_section(changelog_at(root, sha))
    entries = tuple(line for line in section if ENTRY.match(line))
    if not entries:
        return Plan(ecosystem, sha, True, "CHANGELOG has no entries under ## Unreleased")
    base = last_stable(root, sha)
    breaking = any(BREAKING.search(line) for line in section)
    target = fmt(next_version(base, breaking))
    timestamp = commit_timestamp(root, sha)
    version = package_version(target, timestamp, ecosystem)
    reason = f"{len(entries)} Unreleased entries" + (", breaking" if breaking else "")
    return Plan(
        ecosystem=ecosystem,
        sha=sha,
        skip=False,
        reason=reason,
        base=fmt(base),
        target=target,
        breaking=breaking,
        timestamp=timestamp,
        version=version,
        tag=release_tag(target, version, ecosystem),
        title=f"v{version}",
        entries=entries,
    )


def install_hint(root: Path, ecosystem: str, version: str) -> str:
    if ecosystem == "go":
        return f"go get {go_module(root)}@v{version}"
    if ecosystem == "python":
        return f"pip install {python_project(root)}=={version}"
    return f"cargo add {crate_name(root)}@={version}"


def github_repository(root: Path) -> str:
    """owner/name of the repository: GITHUB_REPOSITORY in a workflow run, else
    the GitHub origin remote of ``root``."""
    named = os.environ.get("GITHUB_REPOSITORY", "").strip()
    if REPOSITORY.fullmatch(named):
        return named
    match = GITHUB_REMOTE.search(git(root, "remote", "get-url", "origin"))
    if match is None:
        raise ReleaseError("cannot tell the GitHub repository: set GITHUB_REPOSITORY")
    return match[1]


def release_notes(root: Path, p: Plan, repository: str | None = None) -> str:
    """The devnet pre-release body: the whole ``## Unreleased`` section when
    the body stays within RELEASE_NOTES_BUDGET characters, else a link to
    CHANGELOG.md at the snapshot's commit. The section is never cut, so no
    entry is ever left half written."""
    section = "\n".join(unreleased_section(changelog_at(root, p.sha)))
    head = (
        f"Devnet pre-release `{p.version}`, built from {p.sha} on main. "
        f"It is not the latest release: the default install stays on the stable "
        f"release, which tracks testnet.\n\n"
        f"```\n{install_hint(root, p.ecosystem, p.version)}\n```\n\n"
        f"## Changes since v{p.base}\n\n"
    )
    notes = f"{head}{section.strip()}\n"
    if len(notes) <= RELEASE_NOTES_BUDGET:
        return notes
    changelog = (
        f"https://github.com/{repository or github_repository(root)}/blob/{p.sha}/CHANGELOG.md"
    )
    return (
        f"{head}The {len(p.entries)} entries under `## Unreleased` are longer than a "
        f"release body holds. Read them in [CHANGELOG.md at {p.sha}]({changelog}).\n"
    )


def write_notes(path: Path, notes: str) -> None:
    """Write ``notes`` exactly, with no newline translation, so the file gh
    sends is the text that was measured."""
    path.write_text(notes, encoding="utf-8", newline="")


def check_notes(notes: Path) -> None:
    """Refuse a body GitHub would refuse, before any release is changed: Rust
    and Python delete the previous pre-release before creating this one.

    gh sends the file as it is, so its characters are counted as sent: the
    bytes decoded as UTF-8 with no newline translation (a CRLF is two)."""
    try:
        length = len(notes.read_bytes().decode("utf-8"))
    except UnicodeDecodeError as error:
        raise ReleaseError(f"{notes} is not UTF-8: {error}") from None
    if length > GITHUB_RELEASE_BODY_LIMIT:
        raise ReleaseError(
            f"{notes} has {length} characters; a GitHub release body holds at most "
            f"{GITHUB_RELEASE_BODY_LIMIT}. Build it with the plan command."
        )


# --------------------------------------------------------------------------- manifests


def go_module(root: Path) -> str:
    match = GO_MODULE_LINE.search((root / "go.mod").read_text(encoding="utf-8"))
    if not match:
        raise ReleaseError("go.mod has no module line")
    return match[1]


def python_project(root: Path) -> str:
    with (root / "pyproject.toml").open("rb") as handle:
        return str(tomllib.load(handle)["project"]["name"])


def crate_name(root: Path) -> str:
    with (root / "Cargo.toml").open("rb") as handle:
        return str(tomllib.load(handle)["package"]["name"])


def replace_table_key(text: str, table: str, key: str, value: str) -> str:
    """Set ``key = "value"`` in a TOML table, keeping every other line as is."""
    out: list[str] = []
    current = ""
    replaced = 0
    pattern = re.compile(rf'^{re.escape(key)}\s*=\s*"[^"]*"\s*$')
    for line in text.splitlines(keepends=True):
        header = re.match(r"^\[([^\[\]]+)\]\s*$", line)
        if header:
            current = header[1].strip()
        if current == table and pattern.match(line.rstrip("\r\n")):
            newline = line[len(line.rstrip("\r\n")) :]
            line = f'{key} = "{value}"{newline}'
            replaced += 1
        out.append(line)
    if replaced != 1:
        raise ReleaseError(f"expected one {key} in [{table}], found {replaced}")
    return "".join(out)


def python_package_dir(root: Path) -> str:
    """The wheel's package directory, such as ``src/polyester``."""
    with (root / "pyproject.toml").open("rb") as handle:
        packages = tomllib.load(handle)["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"]
    if len(packages) != 1:
        raise ReleaseError(f"expected one wheel package in pyproject.toml, found {packages}")
    return str(packages[0])


def stamp_python(root: Path, version: str) -> list[Path]:
    pyproject = root / "pyproject.toml"
    pyproject.write_text(
        replace_table_key(pyproject.read_text(encoding="utf-8"), "project", "version", version),
        encoding="utf-8",
    )
    package = root / python_package_dir(root) / "_version.py"
    text = package.read_text(encoding="utf-8")
    stamped, count = re.subn(
        r'^__version__\s*=\s*"[^"]*"\s*$', f'__version__ = "{version}"', text, flags=re.MULTILINE
    )
    if count != 1:
        raise ReleaseError(f"expected one __version__ in {package}, found {count}")
    package.write_text(stamped, encoding="utf-8")
    stamped_paths = [pyproject, package]
    # The single-file request builders (pure.py) carry the version too, as an
    # indented _VERSION inside their factory function; keep that indent.
    pure = package.with_name("pure.py")
    if pure.exists():
        stamped, count = re.subn(
            r'^([ \t]*)_VERSION\s*=\s*"[^"]*"[ \t]*$',
            lambda match: f'{match.group(1)}_VERSION = "{version}"',
            pure.read_text(encoding="utf-8"),
            flags=re.MULTILINE,
        )
        if count != 1:
            raise ReleaseError(f"expected one _VERSION in {pure}, found {count}")
        pure.write_text(stamped, encoding="utf-8")
        stamped_paths.append(pure)
    with pyproject.open("rb") as handle:
        if tomllib.load(handle)["project"]["version"] != version:
            raise ReleaseError("pyproject.toml does not read back the stamped version")
    return stamped_paths


def stamp_rust(root: Path, version: str) -> list[Path]:
    manifest = root / "Cargo.toml"
    name = crate_name(root)
    manifest.write_text(
        replace_table_key(manifest.read_text(encoding="utf-8"), "package", "version", version),
        encoding="utf-8",
    )
    lock = root / "Cargo.lock"
    # The crate's own lock entry carries its version; `cargo publish --locked`
    # refuses a lock file that disagrees with Cargo.toml. Registry packages
    # have a source line; the workspace package does not.
    blocks = re.split(r"(?m)^(?=\[\[package\]\])", lock.read_text(encoding="utf-8"))
    hits = 0
    for index, block in enumerate(blocks):
        if re.search(rf'(?m)^name = "{re.escape(name)}"$', block) and "\nsource = " not in block:
            blocks[index] = re.sub(
                r'(?m)^version = "[^"]*"$', f'version = "{version}"', block, count=1
            )
            hits += 1
    if hits != 1:
        raise ReleaseError(f"expected one local {name} entry in Cargo.lock, found {hits}")
    lock.write_text("".join(blocks), encoding="utf-8")
    with manifest.open("rb") as handle:
        if tomllib.load(handle)["package"]["version"] != version:
            raise ReleaseError("Cargo.toml does not read back the stamped version")
    return [manifest, lock]


# --------------------------------------------------------------------------- verify


def run(
    args: Sequence[str],
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    quiet = dict(os.environ if env is None else env)
    for key in FORCE_COLOR_ENV:
        quiet.pop(key, None)
    quiet.update(NO_COLOR_ENV)
    result = subprocess.run(list(args), cwd=cwd, env=quiet, capture_output=True, text=True)
    if result.returncode != 0:
        raise ReleaseError(
            f"{' '.join(args)} failed ({result.returncode}):\n{result.stdout}{result.stderr}"
        )
    return result


def metadata_version(text: str) -> str:
    match = re.search(r"(?m)^Version: (\S+)$", text)
    if not match:
        raise ReleaseError("package metadata has no Version field")
    return match[1]


def verify_python(root: Path, version: str, dist: Path) -> list[str]:
    stem = re.sub(r"[-_.]+", "_", python_project(root)).lower()
    wheel = dist / f"{stem}-{version}-py3-none-any.whl"
    sdist = dist / f"{stem}-{version}.tar.gz"
    found = sorted(path.name for path in dist.iterdir())
    if not wheel.is_file() or not sdist.is_file():
        raise ReleaseError(f"expected {wheel.name} and {sdist.name} in {dist}, found {found}")
    package = Path(python_package_dir(root)).name
    with zipfile.ZipFile(wheel) as archive:
        wheel_meta = metadata_version(archive.read(f"{stem}-{version}.dist-info/METADATA").decode())
        module = archive.read(f"{package}/_version.py").decode()
    with tarfile.open(sdist) as archive:
        member = archive.extractfile(f"{stem}-{version}/PKG-INFO")
        if member is None:
            raise ReleaseError(f"{sdist.name} has no PKG-INFO")
        sdist_meta = metadata_version(member.read().decode())
    if {wheel_meta, sdist_meta} != {version} or f'__version__ = "{version}"' not in module:
        raise ReleaseError(
            f"artefacts disagree with {version}: wheel {wheel_meta}, sdist {sdist_meta}"
        )
    return [
        f"{wheel.name}: METADATA Version {wheel_meta}, _version.py {version}",
        f"{sdist.name}: PKG-INFO Version {sdist_meta}",
    ]


def pip_resolve(links: Path, requirement: str, pre: bool) -> str:
    """The version pip installs for a requirement from a local directory only."""
    report = links / "report.json"
    args = [sys.executable, "-m", "pip", "install", "--dry-run", "--ignore-installed"]
    args += ["--no-deps", "--no-index", "--find-links", str(links), "--quiet"]
    args += ["--report", str(report), *(["--pre"] if pre else []), requirement]
    run(args)
    data = json.loads(report.read_text(encoding="utf-8"))
    return str(data["install"][0]["metadata"]["version"])


def pip_default_resolution(root: Path, version: str, dist: Path) -> list[str]:
    """Prove pip keeps installing stable once this devnet wheel is published.

    The latest stable wheel comes from the index the way a plain
    ``pip download`` finds it; next to this devnet wheel, pip without --pre
    still picks stable, and --pre picks the devnet wheel.
    """
    name = python_project(root)
    stem = re.sub(r"[-_.]+", "_", name).lower()
    with tempfile.TemporaryDirectory() as tmp:
        links = Path(tmp)
        run(
            [
                sys.executable,
                "-m",
                "pip",
                "download",
                name,
                "--no-deps",
                "--only-binary=:all:",
                "--no-cache-dir",
                "--quiet",
                "--dest",
                str(links),
            ]
        )
        stable = [p.name for p in links.glob(f"{stem}-*.whl")]
        if len(stable) != 1 or ".dev" in stable[0]:
            raise ReleaseError(f"pip download {name} did not fetch one stable wheel: {stable}")
        shutil.copy(dist / f"{stem}-{version}-py3-none-any.whl", links)
        default = pip_resolve(links, name, pre=False)
        pre = pip_resolve(links, name, pre=True)
    if ".dev" in default or "-" in default:
        raise ReleaseError(f"pip install {name} picks pre-release {default}")
    if pre != version:
        raise ReleaseError(f"pip install --pre {name} picks {pre}, not {version}")
    return [
        f"index default: {stable[0]}",
        f"pip install {name} -> {default} (stable)",
        f"pip install --pre {name} -> {pre} (devnet)",
    ]


def verify_rust(root: Path, version: str) -> list[str]:
    name = crate_name(root)
    crate = root / "target" / "package" / f"{name}-{version}.crate"
    if not crate.is_file():
        raise ReleaseError(f"{crate} is missing; run cargo package first")
    with tarfile.open(crate) as archive:
        member = archive.extractfile(f"{name}-{version}/Cargo.toml")
        if member is None:
            raise ReleaseError(f"{crate.name} has no Cargo.toml")
        packaged = tomllib.loads(member.read().decode())["package"]["version"]
    if packaged != version:
        raise ReleaseError(f"{crate.name} carries version {packaged}, not {version}")
    return [f"{crate.name}: Cargo.toml version {packaged}"]


def go_env(home: Path, extra: dict[str, str]) -> dict[str, str]:
    env = {
        key: value for key, value in os.environ.items() if not key.startswith(("GO", "GIT_CONFIG"))
    }
    env.update(
        GOPATH=str(home / "gopath"),
        GOMODCACHE=str(home / "gopath" / "pkg" / "mod"),
        GOCACHE=str(home / "gocache"),
        GOTOOLCHAIN=os.environ.get("GOTOOLCHAIN", "local"),
        **extra,
    )
    return env


def go_query(home: Path, env: dict[str, str], query: str) -> str:
    probe = home / "probe"
    probe.mkdir(exist_ok=True)
    result = run(["go", "list", "-m", "-json", query], cwd=probe, env=env)
    return go_module_version(result.stdout, query)


def go_module_version(stdout: str, query: str) -> str:
    """The Version in the module document `go list -m -json <query>` printed.

    -json output is a stream of JSON values, one per listed module. The go on
    PATH can be a launcher (a version-manager shim) that writes its own JSON
    log lines to the same stdout before it runs the toolchain, so this decodes
    the whole stream and keeps the one document for the queried module path.
    Output that is not a JSON stream still fails. Errors here are ValueError,
    not ReleaseError, which published() reads as "this version is not there".
    """
    decoder = json.JSONDecoder()
    values: list[object] = []
    rest = stdout.lstrip()
    while rest:
        value, end = decoder.raw_decode(rest)
        values.append(value)
        rest = rest[end:].lstrip()
    path = query.partition("@")[0]
    modules = [value for value in values if isinstance(value, dict) and value.get("Path") == path]
    if len(modules) != 1:
        raise ValueError(
            f"go list -m -json {query} printed {len(modules)} documents for {path}:\n{stdout}"
        )
    return str(modules[0]["Version"])


def verify_go(root: Path, version: str, sha: str) -> list[str]:
    """Resolve the devnet tag with the go command, without pushing it.

    A bare clone of this repository gets the tag, and git redirects the module
    path to that clone, so `go list -m` resolves versions the way it would
    from GitHub: the tag names a valid module version, and @latest stays on
    the newest stable tag.
    """
    module = go_module(root)
    tag = f"v{version}"
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp)
        mirror = home / "mirror.git"
        gitconfig = home / "gitconfig"
        # Every git here, the go command's included, reads only this config.
        # A fetch otherwise starts `git maintenance run --auto`, which detaches
        # by default and can still be repacking the go command's VCS cache
        # after go exits, so removing this directory fails with "Directory not
        # empty". Auto maintenance stays off, and in the foreground if any runs.
        gitconfig.write_text(
            f'[url "file://{mirror}"]\n\tinsteadOf = https://{module}\n'
            '[protocol "file"]\n\tallow = always\n'
            "[gc]\n\tauto = 0\n\tautoDetach = false\n"
            "[maintenance]\n\tauto = false\n\tautoDetach = false\n",
            encoding="utf-8",
        )
        env = go_env(
            home,
            {
                "GOPROXY": "direct",
                "GOSUMDB": "off",
                "GOPRIVATE": module,
                "GIT_CONFIG_GLOBAL": str(gitconfig),
                "GIT_CONFIG_NOSYSTEM": "1",
            },
        )
        run(["git", "clone", "--quiet", "--bare", "--no-local", str(root), str(mirror)], env=env)
        # After a publish the checkout already has the tag (fetch-depth: 0), so
        # a re-run reuses it when it names this commit.
        existing = subprocess.run(
            [
                "git",
                "--git-dir",
                str(mirror),
                "rev-parse",
                "--verify",
                "--quiet",
                f"refs/tags/{tag}^{{commit}}",
            ],
            env=env,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if not existing:
            run(["git", "--git-dir", str(mirror), "tag", tag, sha], env=env)
        elif existing != sha:
            raise ReleaseError(f"{tag} already exists at {existing}, not {sha}")
        resolved = go_query(home, env, f"{module}@{tag}")
        latest = go_query(home, env, f"{module}@latest")
    if resolved != tag:
        raise ReleaseError(f"go resolves {tag} as {resolved}")
    if not STABLE_TAG.match(latest):
        raise ReleaseError(f"go resolves @latest to {latest}, not a stable tag")
    return [f"go list -m {module}@{tag} -> {resolved}", f"go list -m {module}@latest -> {latest}"]


# --------------------------------------------------------------------------- registry


def http_status(url: str) -> tuple[int, bytes]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, b""


def crates_index_versions(name: str) -> list[dict[str, object]]:
    lower = name.lower()
    if len(lower) <= 2:
        path = f"{len(lower)}/{lower}"
    elif len(lower) == 3:
        path = f"3/{lower[0]}/{lower}"
    else:
        path = f"{lower[:2]}/{lower[2:4]}/{lower}"
    status, body = http_status(f"https://index.crates.io/{path}")
    if status == 404:
        return []
    if status != 200:
        raise ReleaseError(f"crates.io index answered {status} for {name}")
    return [json.loads(line) for line in body.decode().splitlines() if line.strip()]


def exists(root: Path, ecosystem: str, version: str, sha: str) -> bool:
    if ecosystem == "python":
        status, _ = http_status(f"https://pypi.org/pypi/{python_project(root)}/{version}/json")
        if status not in (200, 404):
            raise ReleaseError(f"PyPI answered {status} for {version}")
        return status == 200
    if ecosystem == "rust":
        return any(
            entry.get("vers") == version for entry in crates_index_versions(crate_name(root))
        )
    tag = f"v{version}"
    remote = git(root, "ls-remote", "--tags", "origin", f"refs/tags/{tag}")
    if not remote:
        return False
    pointed = remote.split()[0]
    if pointed != sha:
        raise ReleaseError(f"{tag} already exists at {pointed}, not {sha}")
    return True


def is_prerelease(version: str) -> bool:
    return bool(re.search(r"(-|\.dev|a\d|b\d|rc\d)", version))


def strip_ansi(text: str) -> str:
    return ANSI_ESCAPE.sub("", text)


def cargo_added_version(name: str, output: str) -> str:
    """The version ``cargo add --dry-run <name>`` picked, from what it printed."""
    text = strip_ansi(output)
    match = re.search(rf"(?m)^\s*Adding\s+{re.escape(name)}\s+v(\S+)", text)
    if not match:
        raise ReleaseError(f"cargo add printed no version for {name}:\n{text}")
    return match[1]


def default_resolution(root: Path, ecosystem: str) -> str:
    """The version a plain install picks from the public registry today."""
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp)
        if ecosystem == "python":
            name = python_project(root)
            report = home / "report.json"
            run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "--dry-run",
                    "--ignore-installed",
                    "--no-deps",
                    "--no-cache-dir",
                    "--quiet",
                    "--report",
                    str(report),
                    name,
                ]
            )
            data = json.loads(report.read_text(encoding="utf-8"))
            return str(data["install"][0]["metadata"]["version"])
        if ecosystem == "rust":
            name = crate_name(root)
            run(["cargo", "new", "--lib", "--quiet", "--name", "probe", str(home / "probe")])
            added = run(["cargo", "add", "--dry-run", "--color", "never", name], cwd=home / "probe")
            return cargo_added_version(name, added.stderr + added.stdout)
        env = go_env(home, {"GOPROXY": "https://proxy.golang.org"})
        return go_query(home, env, f"{go_module(root)}@latest")


def published(root: Path, ecosystem: str, version: str) -> bool:
    if ecosystem == "python":
        status, _ = http_status(f"https://pypi.org/pypi/{python_project(root)}/{version}/json")
        return status == 200
    if ecosystem == "rust":
        return any(
            entry.get("vers") == version for entry in crates_index_versions(crate_name(root))
        )
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp)
        env = go_env(home, {"GOPROXY": "https://proxy.golang.org"})
        try:
            return go_query(home, env, f"{go_module(root)}@v{version}") == f"v{version}"
        except ReleaseError:
            return False


def check_registry(
    root: Path, ecosystem: str, version: str, attempts: int, delay: float
) -> list[str]:
    for attempt in range(1, attempts + 1):
        if published(root, ecosystem, version):
            break
        if attempt == attempts:
            raise ReleaseError(f"{version} is not visible in the registry after {attempts} tries")
        time.sleep(delay)
    default = default_resolution(root, ecosystem)
    if is_prerelease(default):
        raise ReleaseError(f"the default install resolves to pre-release {default}")
    return [f"{version} resolves from the registry", f"default install -> {default} (stable)"]


# --------------------------------------------------------------------------- release

Runner = Callable[[Sequence[str]], str]


def gh_runner(args: Sequence[str]) -> str:
    return run(args).stdout


def snapshot_key(name: str) -> tuple[int, ...]:
    match = TIMESTAMP.search(name)
    return (int(match[1]),) if match else ()


def sync_release(
    ecosystem: str,
    p: Plan,
    notes: Path,
    stable_tags: set[str],
    gh: Runner,
    dry_run: bool,
) -> list[str]:
    """Point the one devnet pre-release of the target at this snapshot.

    The pre-release is never marked latest, so the latest GitHub release stays
    the stable one. Snapshot timestamps only grow along main, so a devnet
    pre-release newer than this snapshot, of any target, means this run is
    stale (a re-run of an old commit, or runs that started out of order) and
    it changes nothing. A re-run of the same commit creates nothing again.
    Otherwise the older pre-release of the target is replaced, and older
    pre-releases of a target that never shipped and is no longer the target
    (Unreleased gained a breaking entry) are removed; those of shipped targets
    stay. Go keeps every snapshot tag, since tags are module versions; Rust
    and Python move their rolling tag.
    """
    pattern = RELEASE_TAG[ecosystem]
    listed = json.loads(gh(["gh", "release", "list", "--limit", "100", "--json", "tagName,name"]))
    cleanup = [] if ecosystem == "go" else ["--cleanup-tag"]
    ours = snapshot_key(p.title)
    devnet = []
    for release in listed:
        tag, name = release["tagName"], release.get("name") or release["tagName"]
        match = pattern.match(tag)
        if match:
            devnet.append((tag, name, match[1], snapshot_key(name)))
    newer = [(tag, name) for tag, name, _, key in devnet if key > ours]
    if newer:
        tag, name = newer[0]
        return [f"{tag} holds {name}, newer than {p.title}; this run is stale, nothing to do"]
    log: list[str] = []
    replaced: list[list[str]] = []
    orphaned: list[list[str]] = []
    current = False
    for tag, name, target, key in devnet:
        if target == p.target:
            if key == ours:
                current = True
                log.append(f"{tag} already holds {p.title}")
            else:
                log.append(f"replace {tag} ({name})")
                replaced.append(["gh", "release", "delete", tag, "--yes", *cleanup])
        elif f"v{target}" not in stable_tags:
            log.append(f"remove {tag}: {target} never shipped and is no longer the target")
            orphaned.append(["gh", "release", "delete", tag, "--yes", *cleanup])
    # When this snapshot's release already exists (a re-run), the older
    # releases of the target still go: a Go run creates its release before it
    # deletes the previous one, and that deletion may have failed. Rust and
    # Python hold one release per target tag, so they have none left over.
    planned: list[list[str]] = [*replaced]
    if not current:
        create = ["gh", "release", "create", p.tag, "--prerelease", "--latest=false"]
        create += ["--title", p.title, "--notes-file", str(notes)]
        create += ["--verify-tag"] if ecosystem == "go" else ["--target", p.sha]
        log.append(f"create {p.tag} titled {p.title} at {p.sha}, pre-release, not latest")
        # Rust and Python reuse the tag name, so the old release goes first. A
        # Go snapshot has a tag of its own: create it first and leave no gap.
        planned = [create, *replaced] if ecosystem == "go" else [*replaced, create]
    planned += orphaned
    if not planned:
        log.append("nothing to do")
    for command in planned:
        log.append(("would run: " if dry_run else "run: ") + " ".join(command))
        if not dry_run:
            gh(command)
    return log


# --------------------------------------------------------------------------- cli


def detect(root: Path) -> str:
    for marker, ecosystem in (
        ("go.mod", "go"),
        ("Cargo.toml", "rust"),
        ("pyproject.toml", "python"),
    ):
        if (root / marker).is_file():
            return ecosystem
    raise ReleaseError(f"{root} has no go.mod, Cargo.toml or pyproject.toml")


def emit(values: dict[str, str]) -> None:
    for key, value in values.items():
        print(f"{key}={value}")
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as handle:
            for key, value in values.items():
                handle.write(f"{key}={value}\n")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--ecosystem", choices=ECOSYSTEMS)
    sub = parser.add_subparsers(dest="command", required=True)

    p_route = sub.add_parser("route")
    p_route.add_argument("--event", required=True)
    p_route.add_argument("--ref", required=True)
    p_route.add_argument("--dry-run", default="")

    p_plan = sub.add_parser("plan")
    p_plan.add_argument("--rev", default="HEAD")
    p_plan.add_argument("--notes", type=Path)

    for name in ("stamp", "exists", "check-registry"):
        p_cmd = sub.add_parser(name)
        p_cmd.add_argument("--version", required=True)
        p_cmd.add_argument("--sha", default="HEAD")
        if name == "check-registry":
            p_cmd.add_argument("--attempts", type=int, default=20)
            p_cmd.add_argument("--delay", type=float, default=30.0)

    p_verify = sub.add_parser("verify")
    p_verify.add_argument("--version", required=True)
    p_verify.add_argument("--sha", default="HEAD")
    p_verify.add_argument("--dist", type=Path, default=Path("dist"))

    p_release = sub.add_parser("release")
    p_release.add_argument("--rev", default="HEAD")
    p_release.add_argument("--notes", type=Path, required=True)
    p_release.add_argument("--dry-run", default="")

    args = parser.parse_args(argv)
    root: Path = args.root.resolve()
    try:
        if args.command == "route":
            r = route(args.event, args.ref, parse_bool(args.dry_run))
            print(f"route: {r.reason}", file=sys.stderr)
            emit({"channel": r.channel, "dry_run": str(r.dry_run).lower()})
            return 0
        ecosystem = args.ecosystem or detect(root)
        if args.command == "plan":
            result = plan(root, ecosystem, args.rev)
            print(f"plan: {result.reason}", file=sys.stderr)
            if args.notes and not result.skip:
                write_notes(args.notes, release_notes(root, result))
            emit(result.outputs())
            return 0
        if args.command == "stamp":
            if ecosystem == "go":
                raise ReleaseError("Go versions are tags; there is nothing to stamp")
            stamper = stamp_python if ecosystem == "python" else stamp_rust
            for path in stamper(root, args.version):
                print(f"stamped {path.relative_to(root)} with {args.version}")
            return 0
        if args.command in ("verify", "exists"):
            # Only Go reads git here: its version is a tag on this commit.
            sha = git(root, "rev-parse", f"{args.sha}^{{commit}}") if ecosystem == "go" else ""
        if args.command == "verify":
            if ecosystem == "python":
                lines = verify_python(root, args.version, args.dist.resolve())
                lines += pip_default_resolution(root, args.version, args.dist.resolve())
            elif ecosystem == "rust":
                lines = verify_rust(root, args.version)
            else:
                lines = verify_go(root, args.version, sha)
            print("\n".join(f"verified: {line}" for line in lines))
            return 0
        if args.command == "exists":
            found = exists(root, ecosystem, args.version, sha)
            print(
                f"exists: {args.version} {'is' if found else 'is not'} in the registry",
                file=sys.stderr,
            )
            emit({"exists": str(found).lower()})
            return 0
        if args.command == "check-registry":
            lines = check_registry(root, ecosystem, args.version, args.attempts, args.delay)
            print("\n".join(f"registry: {line}" for line in lines))
            return 0
        if args.command == "release":
            result = plan(root, ecosystem, args.rev)
            if result.skip:
                print(f"release: {result.reason}; nothing to release")
                return 0
            check_notes(args.notes)
            stable = {
                t for t in git(root, "tag", "--list", "v*").splitlines() if STABLE_TAG.match(t)
            }
            lines = sync_release(
                ecosystem, result, args.notes, stable, gh_runner, parse_bool(args.dry_run)
            )
            print("\n".join(f"release: {line}" for line in lines))
            return 0
    except ReleaseError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
