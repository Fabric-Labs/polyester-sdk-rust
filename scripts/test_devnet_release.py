"""Tests for scripts/devnet_release.py and the workflow that runs it.

Run with: python3 -m unittest discover -s scripts -p 'test_devnet_release.py'
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest
from collections.abc import Sequence
from pathlib import Path
from unittest import mock

SCRIPTS = Path(__file__).resolve().parent
ROOT = SCRIPTS.parent
_spec = importlib.util.spec_from_file_location("devnet_release", SCRIPTS / "devnet_release.py")
assert _spec and _spec.loader
dr = importlib.util.module_from_spec(_spec)
sys.modules["devnet_release"] = dr
_spec.loader.exec_module(dr)

ECOSYSTEM = dr.detect(ROOT)
WORKFLOW = ROOT / ".github" / "workflows" / ("devnet.yml" if ECOSYSTEM == "go" else "publish.yml")

STABLE_TAGS = ("v0.34.0", "v1.2.3", "v0.33.1")
DEVNET_TAGS = (
    "v0.35.0-devnet",
    "v0.35.0-devnet.20261117093000",
    "v1.0.0-devnet.20270101000000",
)
# PEP 440 public version (canonical form) and SemVer 2.0.0, from their specs.
PEP440 = re.compile(
    r"^([1-9][0-9]*!)?(0|[1-9][0-9]*)(\.(0|[1-9][0-9]*))*((a|b|rc)(0|[1-9][0-9]*))?"
    r"(\.post(0|[1-9][0-9]*))?(\.dev(0|[1-9][0-9]*))?$"
)
SEMVER = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)


def semver_key(version: str) -> tuple[object, ...]:
    """SemVer 2.0.0 precedence (section 11)."""
    match = SEMVER.match(version.removeprefix("v"))
    assert match, version
    core = tuple(int(match[i]) for i in (1, 2, 3))
    if match[4] is None:
        return (core, 1, ())
    ids = tuple((0, int(p), "") if p.isdigit() else (1, 0, p) for p in match[4].split("."))
    return (core, 0, ids)


def run_git(cwd: Path, *args: str, date: str | None = None) -> str:
    env = dict(
        os.environ,
        GIT_AUTHOR_NAME="t",
        GIT_AUTHOR_EMAIL="t@example.com",
        GIT_COMMITTER_NAME="t",
        GIT_COMMITTER_EMAIL="t@example.com",
    )
    env.pop("GIT_DIR", None)
    if date:
        env.update(GIT_AUTHOR_DATE=date, GIT_COMMITTER_DATE=date)
    return subprocess.run(
        ["git", *args], cwd=cwd, env=env, check=True, capture_output=True, text=True
    ).stdout.strip()


CHANGELOG = """# Changelog

## Unreleased

{entries}
## 0.34.0

- **Breaking:** an old breaking entry that must not count.
"""


class RouteTest(unittest.TestCase):
    def test_stable_tag_takes_the_stable_path_only(self) -> None:
        for tag in STABLE_TAGS:
            with self.subTest(tag=tag):
                self.assertEqual(dr.route("push", f"refs/tags/{tag}", False).channel, "stable")

    def test_devnet_tag_takes_no_path(self) -> None:
        for tag in DEVNET_TAGS:
            with self.subTest(tag=tag):
                self.assertEqual(dr.route("push", f"refs/tags/{tag}", False).channel, "none")

    def test_main_takes_the_devnet_path(self) -> None:
        r = dr.route("push", "refs/heads/main", False)
        self.assertEqual((r.channel, r.dry_run), ("devnet", False))

    def test_other_branches_and_events_take_no_path(self) -> None:
        self.assertEqual(dr.route("push", "refs/heads/feature", False).channel, "none")
        self.assertEqual(dr.route("pull_request", "refs/pull/1/merge", False).channel, "none")
        self.assertEqual(dr.route("push", "refs/tags/release-1", False).channel, "none")

    def test_manual_run_dry_runs_anywhere_and_publishes_only_from_main(self) -> None:
        r = dr.route("workflow_dispatch", "refs/heads/feature", True)
        self.assertEqual((r.channel, r.dry_run), ("devnet", True))
        r = dr.route("workflow_dispatch", "refs/heads/main", False)
        self.assertEqual((r.channel, r.dry_run), ("devnet", False))
        with self.assertRaises(dr.ReleaseError):
            dr.route("workflow_dispatch", "refs/heads/feature", False)
        r = dr.route("workflow_dispatch", "refs/tags/v0.34.0", True)
        self.assertEqual((r.channel, r.dry_run), ("devnet", True))

    def test_parse_bool(self) -> None:
        self.assertFalse(dr.parse_bool(""))
        self.assertFalse(dr.parse_bool("false"))
        self.assertTrue(dr.parse_bool("true"))
        with self.assertRaises(dr.ReleaseError):
            dr.parse_bool("maybe")


class VersionTest(unittest.TestCase):
    def test_next_version(self) -> None:
        self.assertEqual(dr.next_version((0, 34, 0), breaking=True), (0, 35, 0))
        self.assertEqual(dr.next_version((0, 34, 2), breaking=False), (0, 34, 3))
        self.assertEqual(dr.next_version((1, 4, 2), breaking=True), (2, 0, 0))
        self.assertEqual(dr.next_version((1, 4, 2), breaking=False), (1, 4, 3))

    def test_stable_versions_ignore_devnet_and_old_prerelease_tags(self) -> None:
        tags = ["v0.9.0", "v0.10.0", "v0.10.1-devnet.20261117093000", "v0.1.0a44", "v0.11.0-devnet"]
        self.assertEqual(dr.stable_versions(tags)[-1], (0, 10, 0))

    def test_unreleased_section(self) -> None:
        text = CHANGELOG.format(entries="- one\n- **Breaking:** two\n")
        self.assertEqual(
            [x for x in dr.unreleased_section(text) if x], ["- one", "- **Breaking:** two"]
        )
        self.assertEqual(dr.unreleased_section("# Changelog\n\n## 0.1.0\n- x\n"), [])

    def test_versions_are_valid_and_order_between_stable_releases(self) -> None:
        for ecosystem in dr.ECOSYSTEMS:
            version = dr.package_version("0.35.0", "20261117093000", ecosystem)
            if ecosystem == "python":
                self.assertEqual(version, "0.35.0.dev20261117093000")
                self.assertRegex(version, PEP440)
                continue
            self.assertEqual(version, "0.35.0-devnet.20261117093000")
            key = semver_key(version)
            self.assertLess(semver_key("0.34.0"), key)
            self.assertLess(key, semver_key("0.35.0"))
            self.assertLess(semver_key("0.35.0-devnet.20261117092959"), key)

    def test_release_tags(self) -> None:
        self.assertEqual(
            dr.release_tag("0.35.0", "0.35.0-devnet.20261117093000", "go"),
            "v0.35.0-devnet.20261117093000",
        )
        self.assertEqual(
            dr.release_tag("0.35.0", "0.35.0.dev20261117093000", "python"), "v0.35.0-devnet"
        )
        self.assertEqual(
            dr.release_tag("0.35.0", "0.35.0-devnet.20261117093000", "rust"), "v0.35.0-devnet"
        )


class GitRepoCase(unittest.TestCase):
    """A throwaway git repository with the stable tag v0.34.0."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self.tmp.name)
        run_git(self.repo, "init", "-q", "-b", "main")
        (self.repo / "go.mod").write_text("module example.com/sdk\n\ngo 1.22\n")
        self.commit(CHANGELOG.format(entries=""), "2026-11-01T10:00:00+00:00")
        run_git(self.repo, "tag", "-a", "v0.34.0", "-m", "v0.34.0")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def commit(self, changelog: str, date: str) -> str:
        (self.repo / "CHANGELOG.md").write_text(changelog)
        run_git(self.repo, "add", "-A")
        run_git(self.repo, "commit", "-q", "-m", "change", date=date)
        return run_git(self.repo, "rev-parse", "HEAD")


class PlanTest(GitRepoCase):
    def test_empty_unreleased_skips(self) -> None:
        self.commit(CHANGELOG.format(entries="\n"), "2026-11-02T10:00:00+00:00")
        p = dr.plan(self.repo, "go")
        self.assertTrue(p.skip)
        self.assertEqual(p.outputs()["skip"], "true")

    def test_patch_target_from_commit_time_in_utc(self) -> None:
        sha = self.commit(CHANGELOG.format(entries="- a fix\n"), "2026-11-17T10:30:05+02:00")
        p = dr.plan(self.repo, "python")
        self.assertFalse(p.skip)
        self.assertEqual((p.base, p.target, p.timestamp), ("0.34.0", "0.34.1", "20261117083005"))
        self.assertEqual(p.version, "0.34.1.dev20261117083005")
        self.assertEqual(
            (p.tag, p.title, p.sha), ("v0.34.1-devnet", "v0.34.1.dev20261117083005", sha)
        )

    def test_breaking_entry_targets_next_minor(self) -> None:
        self.commit(
            CHANGELOG.format(entries="- a\n- **Breaking:** b\n"), "2026-11-17T09:30:00+00:00"
        )
        p = dr.plan(self.repo, "go")
        self.assertEqual(
            (p.target, p.version, p.tag),
            ("0.35.0", "0.35.0-devnet.20261117093000", "v0.35.0-devnet.20261117093000"),
        )
        self.assertTrue(p.breaking)

    def test_rerun_of_the_same_commit_is_reproducible(self) -> None:
        self.commit(CHANGELOG.format(entries="- a\n"), "2026-11-17T09:30:00+00:00")
        first = dr.plan(self.repo, "rust")
        run_git(
            self.repo,
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            "later",
            date="2026-11-17T11:00:00+00:00",
        )
        self.assertEqual(dr.plan(self.repo, "rust", "HEAD~1"), first)
        self.assertGreater(dr.plan(self.repo, "rust").version, first.version)

    def test_devnet_tags_and_unmerged_tags_are_not_the_base(self) -> None:
        self.commit(CHANGELOG.format(entries="- a\n"), "2026-11-17T09:30:00+00:00")
        run_git(self.repo, "tag", "v0.34.1-devnet.20261117093000")
        run_git(self.repo, "checkout", "-q", "-b", "side")
        self.commit(CHANGELOG.format(entries="- side\n"), "2026-11-17T09:40:00+00:00")
        run_git(self.repo, "tag", "-a", "v0.40.0", "-m", "v0.40.0")
        run_git(self.repo, "checkout", "-q", "main")
        self.assertEqual(dr.plan(self.repo, "go").base, "0.34.0")

    def test_no_stable_tag_is_an_error(self) -> None:
        run_git(self.repo, "tag", "-d", "v0.34.0")
        self.commit(CHANGELOG.format(entries="- a\n"), "2026-11-17T09:30:00+00:00")
        with self.assertRaises(dr.ReleaseError):
            dr.plan(self.repo, "go")

    def test_release_notes_hold_the_unreleased_section(self) -> None:
        self.commit(CHANGELOG.format(entries="- a change\n"), "2026-11-17T09:30:00+00:00")
        notes = dr.release_notes(self.repo, dr.plan(self.repo, "go"))
        self.assertIn("go get example.com/sdk@v0.34.1-devnet.20261117093000", notes)
        self.assertIn("- a change", notes)
        self.assertNotIn("old breaking entry", notes)


class CliTest(GitRepoCase):
    """main() for each subcommand that needs no network, as the workflows call it."""

    def main(self, *args: str) -> tuple[int, str]:
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            code = dr.main(["--root", str(self.repo), *args])
        return code, out.getvalue()

    def test_route_plan_and_release_dry_run(self) -> None:
        self.commit(CHANGELOG.format(entries="- a\n"), "2026-11-17T09:30:00+00:00")
        code, out = self.main("route", "--event", "push", "--ref", "refs/tags/v0.35.0-devnet")
        self.assertEqual((code, out), (0, "channel=none\ndry_run=false\n"))
        notes = self.repo / "notes.md"
        code, out = self.main("plan", "--notes", str(notes))
        self.assertEqual(code, 0)
        self.assertIn("version=0.34.1-devnet.20261117093000\n", out)
        self.assertTrue(notes.is_file())
        gh = FakeGh([])
        with mock.patch.object(dr, "gh_runner", gh):
            code, out = self.main("release", "--notes", str(notes), "--dry-run", "true")
        self.assertEqual(code, 0)
        self.assertIn("would run: gh release create v0.34.1-devnet.20261117093000", out)
        self.assertEqual(gh.calls, [])

    def test_errors_exit_non_zero(self) -> None:
        code, _ = self.main("route", "--event", "workflow_dispatch", "--ref", "refs/heads/x")
        self.assertEqual(code, 1)
        code, _ = self.main("stamp", "--version", "0.34.1-devnet.20261117093000")
        self.assertEqual(code, 1)


class StampTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    @unittest.skipUnless(ECOSYSTEM == "python", "Python package manifest")
    def test_stamp_python_changes_only_the_version(self) -> None:
        shutil.copy(ROOT / "pyproject.toml", self.dir)
        package = self.dir / dr.python_package_dir(ROOT)
        package.mkdir(parents=True)
        shutil.copy(ROOT / dr.python_package_dir(ROOT) / "_version.py", package)
        before = (self.dir / "pyproject.toml").read_text()
        dr.stamp_python(self.dir, "0.35.0.dev20261117093000")
        after = (self.dir / "pyproject.toml").read_text()
        with (self.dir / "pyproject.toml").open("rb") as handle:
            self.assertEqual(tomllib.load(handle)["project"]["version"], "0.35.0.dev20261117093000")
        self.assertEqual(len(before.splitlines()), len(after.splitlines()))
        self.assertEqual(
            sum(a != b for a, b in zip(before.splitlines(), after.splitlines(), strict=True)), 1
        )
        self.assertIn(
            '__version__ = "0.35.0.dev20261117093000"', (package / "_version.py").read_text()
        )

    @unittest.skipUnless(ECOSYSTEM == "rust", "Rust package manifest")
    def test_stamp_rust_changes_manifest_and_own_lock_entry(self) -> None:
        shutil.copy(ROOT / "Cargo.toml", self.dir)
        shutil.copy(ROOT / "Cargo.lock", self.dir)
        lock_before = (self.dir / "Cargo.lock").read_text().splitlines()
        dr.stamp_rust(self.dir, "0.35.0-devnet.20261117093000")
        with (self.dir / "Cargo.toml").open("rb") as handle:
            manifest = tomllib.load(handle)
        self.assertEqual(manifest["package"]["version"], "0.35.0-devnet.20261117093000")
        lock_after = (self.dir / "Cargo.lock").read_text().splitlines()
        changed = [b for a, b in zip(lock_before, lock_after, strict=True) if a != b]
        self.assertEqual(changed, ['version = "0.35.0-devnet.20261117093000"'])
        with (self.dir / "Cargo.lock").open("rb") as handle:
            own = [
                p
                for p in tomllib.load(handle)["package"]
                if p["name"] == manifest["package"]["name"]
            ]
        self.assertEqual([p["version"] for p in own], ["0.35.0-devnet.20261117093000"])

    def test_replace_table_key_refuses_ambiguity(self) -> None:
        text = '[package]\nversion = "1"\n[dependencies]\nversion = "2"\n'
        self.assertIn(
            '[package]\nversion = "3"\n[dependencies]\nversion = "2"',
            dr.replace_table_key(text, "package", "version", "3"),
        )
        with self.assertRaises(dr.ReleaseError):
            dr.replace_table_key('[package]\nname = "x"\n', "package", "version", "3")


class FakeGh:
    def __init__(self, releases: list[dict[str, str]]) -> None:
        self.releases = releases
        self.calls: list[list[str]] = []

    def __call__(self, args: Sequence[str]) -> str:
        if list(args[:3]) == ["gh", "release", "list"]:
            return json.dumps(self.releases)
        self.calls.append(list(args))
        return ""


def snapshot(ecosystem: str, target: str, ts: str) -> object:
    version = dr.package_version(target, ts, ecosystem)
    return dr.Plan(
        ecosystem=ecosystem,
        sha="abc123",
        skip=False,
        reason="",
        base="0.34.0",
        target=target,
        timestamp=ts,
        version=version,
        tag=dr.release_tag(target, version, ecosystem),
        title=f"v{version}",
    )


class ReleaseTest(unittest.TestCase):
    notes = Path("notes.md")

    def test_rolling_release_moves_to_the_new_snapshot(self) -> None:
        for ecosystem in ("python", "rust"):
            with self.subTest(ecosystem=ecosystem):
                p = snapshot(ecosystem, "0.35.0", "20261117093000")
                old = f"v{dr.package_version('0.35.0', '20261117080000', ecosystem)}"
                gh = FakeGh(
                    [
                        {"tagName": "v0.35.0-devnet", "name": old},
                        {"tagName": "v0.34.0", "name": "v0.34.0"},
                    ]
                )
                dr.sync_release(ecosystem, p, self.notes, {"v0.34.0"}, gh, dry_run=False)
                self.assertEqual(
                    gh.calls[0],
                    ["gh", "release", "delete", "v0.35.0-devnet", "--yes", "--cleanup-tag"],
                )
                self.assertEqual(
                    gh.calls[1][:6],
                    ["gh", "release", "create", "v0.35.0-devnet", "--prerelease", "--latest=false"],
                )
                self.assertEqual(gh.calls[1][-2:], ["--target", "abc123"])
                self.assertEqual(len(gh.calls), 2)

    def test_go_keeps_snapshot_tags(self) -> None:
        p = snapshot("go", "0.35.0", "20261117093000")
        gh = FakeGh(
            [{"tagName": "v0.35.0-devnet.20261117080000", "name": "v0.35.0-devnet.20261117080000"}]
        )
        dr.sync_release("go", p, self.notes, set(), gh, dry_run=False)
        self.assertEqual(
            gh.calls[0][:4], ["gh", "release", "create", "v0.35.0-devnet.20261117093000"]
        )
        self.assertIn("--verify-tag", gh.calls[0])
        self.assertEqual(
            gh.calls[1], ["gh", "release", "delete", "v0.35.0-devnet.20261117080000", "--yes"]
        )

    def test_never_marked_latest_and_always_prerelease(self) -> None:
        for ecosystem in dr.ECOSYSTEMS:
            gh = FakeGh([])
            dr.sync_release(
                ecosystem,
                snapshot(ecosystem, "0.35.0", "20261117093000"),
                self.notes,
                set(),
                gh,
                dry_run=False,
            )
            (create,) = gh.calls
            self.assertIn("--prerelease", create)
            self.assertIn("--latest=false", create)

    def test_rerun_and_older_snapshot_change_nothing(self) -> None:
        for ts in ("20261117093000", "20261117100000"):
            gh = FakeGh([{"tagName": "v0.35.0-devnet", "name": f"v0.35.0-devnet.{ts}"}])
            dr.sync_release(
                "rust",
                snapshot("rust", "0.35.0", "20261117093000"),
                self.notes,
                set(),
                gh,
                dry_run=False,
            )
            self.assertEqual(gh.calls, [])

    def test_unshipped_old_target_is_removed_and_shipped_one_kept(self) -> None:
        gh = FakeGh(
            [
                {"tagName": "v0.34.1-devnet", "name": "v0.34.1.dev20261116000000"},
                {"tagName": "v0.34.0-devnet", "name": "v0.34.0.dev20261110000000"},
            ]
        )
        dr.sync_release(
            "python",
            snapshot("python", "0.35.0", "20261117093000"),
            self.notes,
            {"v0.34.0"},
            gh,
            dry_run=False,
        )
        self.assertEqual(
            gh.calls[-1], ["gh", "release", "delete", "v0.34.1-devnet", "--yes", "--cleanup-tag"]
        )
        self.assertNotIn("v0.34.0-devnet", [c[3] for c in gh.calls])

    def test_dry_run_only_lists(self) -> None:
        gh = FakeGh([{"tagName": "v0.35.0-devnet", "name": "v0.35.0-devnet.20261117080000"}])
        log = dr.sync_release(
            "rust",
            snapshot("rust", "0.35.0", "20261117093000"),
            self.notes,
            set(),
            gh,
            dry_run=True,
        )
        self.assertEqual(gh.calls, [])
        self.assertTrue(any(line.startswith("would run: gh release create") for line in log))


# --------------------------------------------------------------------------- workflow


def github_filter(patterns: Sequence[str], name: str) -> bool:
    """GitHub Actions branch/tag filter semantics: the last matching pattern
    wins, `!` negates, `*` matches any character but `/`, `**` any character,
    `?` and `+` repeat the previous character, and [..] is a character class.
    """
    matched = False
    for raw in patterns:
        negate = raw.startswith("!")
        pattern = raw[1:] if negate else raw
        regex, i = "", 0
        while i < len(pattern):
            if pattern.startswith("**", i):
                regex, i = regex + ".*", i + 2
            elif pattern[i] == "*":
                regex, i = regex + "[^/]*", i + 1
            elif pattern[i] in "?+":
                regex, i = regex + pattern[i], i + 1
            elif pattern[i] == "[":
                end = pattern.index("]", i)
                regex, i = regex + pattern[i : end + 1], end + 1
            else:
                regex, i = regex + re.escape(pattern[i]), i + 1
        if re.fullmatch(regex, name):
            matched = not negate
    return matched


def top_level_block(text: str, key: str) -> list[str]:
    lines = text.splitlines()
    start = lines.index(f"{key}:")
    block = []
    for line in lines[start + 1 :]:
        if line and not line.startswith((" ", "#")):
            break
        block.append(line)
    return block


def push_filters(text: str) -> dict[str, list[str]]:
    filters: dict[str, list[str]] = {}
    current = None
    for line in top_level_block(text, "on"):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key = re.match(r"^    (branches|tags):\s*(\[.*\])?\s*$", line)
        if key:
            current = key[1]
            inline = key[2][1:-1].split(",") if key[2] else []
            filters[current] = [x.strip().strip("\"'") for x in inline]
            continue
        item = re.match(r"^      - [\"']?([^\"']+)[\"']?\s*$", line)
        if current and item:
            filters[current].append(item[1])
            continue
        current = None
    return filters


def jobs(text: str) -> dict[str, dict[str, object]]:
    found: dict[str, dict[str, object]] = {}
    name = None
    for line in top_level_block(text, "jobs"):
        header = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", line)
        if header:
            name = header[1]
            found[name] = {"if": "", "needs": [], "body": []}
            continue
        if name is None:
            continue
        job = found[name]
        job["body"].append(line)  # type: ignore[union-attr]
        cond = re.match(r"^    if: (.*)$", line)
        if cond:
            job["if"] = cond[1].strip().removeprefix("${{").removesuffix("}}").strip()
        needs = re.match(r"^    needs: (.*)$", line)
        if needs:
            job["needs"] = [n.strip() for n in needs[1].strip("[] ").split(",")]
    return found


def channel_of(all_jobs: dict[str, dict[str, object]], name: str) -> set[str]:
    """The channels a job can run on: its own route gate, else its needs'."""
    job = all_jobs[name]
    cond = str(job["if"])
    gates = set(re.findall(r"needs\.route\.outputs\.channel == '(\w+)'", cond))
    if gates:
        return gates
    if re.search(r"always\(\)|cancelled\(\)|failure\(\)", cond):
        raise AssertionError(f"{name} overrides skipped needs without its own channel gate")
    needs = job["needs"]
    assert isinstance(needs, list)
    if not needs:
        return {"stable", "devnet", "none"}
    return set().union(*(channel_of(all_jobs, n) for n in needs))


class WorkflowTest(unittest.TestCase):
    text = WORKFLOW.read_text(encoding="utf-8")

    def test_filter_emulation(self) -> None:
        self.assertTrue(github_filter(["v*"], "v0.34.0"))
        self.assertFalse(github_filter(["v*", "!v*-devnet*"], "v0.35.0-devnet"))
        self.assertTrue(github_filter(["!v*-devnet*", "v*"], "v0.35.0-devnet"))
        self.assertFalse(github_filter(["release/*"], "release/a/b"))
        self.assertTrue(github_filter(["v[0-9]+.[0-9]+.[0-9]+"], "v10.2.3"))

    def test_triggers(self) -> None:
        filters = push_filters(self.text)
        self.assertEqual(filters["branches"], ["main"])
        self.assertIn("workflow_dispatch:", self.text)
        self.assertRegex(self.text, r"dry_run:\n(.*\n)*?\s+default: true")
        if ECOSYSTEM == "go":
            # Go's stable channel is the tag alone; this workflow never runs for tags.
            self.assertNotIn("tags", filters)
            return
        for tag in STABLE_TAGS:
            self.assertTrue(github_filter(filters["tags"], tag), tag)
        for tag in DEVNET_TAGS:
            self.assertFalse(github_filter(filters["tags"], tag), tag)

    def test_every_job_runs_on_exactly_one_channel(self) -> None:
        all_jobs = jobs(self.text)
        self.assertIn("route", all_jobs)
        seen = {}
        for name in all_jobs:
            if name == "route":
                continue
            channels = channel_of(all_jobs, name)
            self.assertEqual(len(channels), 1, f"{name} runs on {channels}")
            seen[name] = channels.pop()
        devnet = [n for n, c in seen.items() if c == "devnet"]
        stable = [n for n, c in seen.items() if c == "stable"]
        self.assertTrue(devnet)
        self.assertTrue(all(n.startswith("devnet-") for n in devnet), devnet)
        if ECOSYSTEM == "go":
            self.assertEqual(stable, [])
        else:
            self.assertTrue(stable)
            self.assertFalse(any(n.startswith("devnet-") for n in stable), stable)

    def test_devnet_jobs_that_can_publish_never_run_on_a_dry_run(self) -> None:
        all_jobs = jobs(self.text)
        for name, job in all_jobs.items():
            body = "\n".join(job["body"])  # type: ignore[arg-type]
            writes = re.search(r"id-token: write|contents: write|environment:", body)
            if not name.startswith("devnet-") or not writes:
                continue
            with self.subTest(job=name):
                self.assertIn("needs.route.outputs.dry_run == 'false'", job["if"])
                self.assertIn("needs.route.outputs.channel == 'devnet'", job["if"])

    def test_one_publish_at_a_time(self) -> None:
        block = "\n".join(top_level_block(self.text, "concurrency"))
        self.assertIn("cancel-in-progress: false", block)
        self.assertIn("github.ref", block)


if __name__ == "__main__":
    unittest.main()
