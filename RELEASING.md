# Releasing

`polyester-sdk` is published to crates.io on two channels. Both run in
`.github/workflows/publish.yml`, because crates.io trusted publishing trusts
that file and its `crates-io` environment; no registry token is stored.

| Channel | Tracks  | Version                              | Published when                                     | Selected by                          |
|---------|---------|--------------------------------------|----------------------------------------------------|--------------------------------------|
| Stable  | testnet | `X.Y.Z`                              | a maintainer pushes the tag `vX.Y.Z`               | `cargo add polyester-sdk`, `"0.33"`  |
| Devnet  | devnet  | `<next>-devnet.<YYYYMMDDHHmmss>`     | a push to `main` while `## Unreleased` has entries | a requirement naming it, `"=<ver>"`  |

This SDK's version line is its own; the Go, Python and TypeScript SDKs number
their releases independently.

## Stable releases

Stable releases are cut only when testnet moves.

1. In a pull request, move the `## Unreleased` entries of `CHANGELOG.md` under
   `## X.Y.Z`, set `X.Y.Z` in `Cargo.toml` (and `Cargo.lock`), and update the
   versions in the README install snippets.
2. After it merges, push an annotated tag on that commit of `main`:
   `git tag -a vX.Y.Z -m vX.Y.Z && git push origin vX.Y.Z`.
3. The workflow runs CI, checks that the tag matches `Cargo.toml` and is on
   `main`, and publishes to crates.io.
4. Create the GitHub release with notes that name the backend release:
   `python3 scripts/devnet_release.py stable-notes --version X.Y.Z --notes notes.md`
   then `gh release create vX.Y.Z --verify-tag --title vX.Y.Z --notes-file notes.md`.
   The notes open with `Targets Polyester backend <release>`, read from
   `manifest.json` `backendRelease` at the tag, then the install line and a
   link to the version's `CHANGELOG.md` section. The command fails when the tag,
   that section or the backend release is missing.

## Devnet pre-releases

Every push to `main` runs the devnet path. When `## Unreleased` has no entries
(right after a stable release) it stops there.

- **Version.** `scripts/devnet_release.py plan` takes the newest `vX.Y.Z` tag
  reachable from the commit. If `## Unreleased` has a `**Breaking:**` entry the
  target is the next minor (the next major from 1.0 on), otherwise the next
  patch. The pre-release `-devnet.<YYYYMMDDHHmmss>` is the commit time in UTC,
  so the version only grows along `main` and a re-run computes the same
  version. By SemVer precedence it sorts before the stable `<next>` and after
  every earlier release.
- **Build.** The version is written into `Cargo.toml` and the crate's own
  `Cargo.lock` entry in the runner only, never committed, so the crate is
  packaged with `--allow-dirty`. The packaged `.crate` is checked to carry it.
- **Publish.** After CI passes, the crate is published through the same
  trusted publisher and `crates-io` environment as stable releases, then the
  workflow waits for the crates.io index to list it and checks that
  `cargo add polyester-sdk` still picks a stable release.
- **GitHub release.** One pre-release per target version, tagged
  `v<next>-devnet`, is moved to each new build, with the `## Unreleased` entries
  as its notes,
  after the line `Targets Polyester backend <release>` from `manifest.json`
  `backendRelease` at the snapshot's commit (a commit without it fails). When they would make the notes longer than 120,000 characters
  (GitHub refuses a release body over 125,000), the notes link `CHANGELOG.md` at
  the snapshot's commit instead; the entries are never cut. It is never marked
  latest. When a target is abandoned (a breaking entry moves `0.32.1` to
  `0.33.0`) its pre-release is removed; pre-releases of versions that shipped
  stay.
- **One at a time.** Runs on `main` queue behind each other (`queue: max`),
  so every push is published in its turn. A re-run of a
  commit whose version crates.io already has skips the upload, and a GitHub
  pre-release is never moved back to an older build: a run older than any
  devnet pre-release, of any target, changes nothing.

Tags containing `-devnet` never take the stable path: the tag filter excludes
`v*-devnet*`, and the `route` job sends them nowhere.

The `crates-io` environment must allow deployments from the `main` branch as
well as `v*` tags, so the devnet job can use it.

### Dry run

Run the **Publish** workflow by hand (`dry_run` is on by default). It plans
the version, packages and checks the crate, runs CI and prints the GitHub
release changes it would make, without publishing anything. A run with
`dry_run` off publishes, and only from `main`.

The same checks run locally:

```bash
python3 scripts/devnet_release.py plan --notes notes.md   # version, tag, skip
python3 -m unittest discover -s scripts -p 'test_devnet_release.py'
# In a scratch copy, never in a checkout you commit from:
python3 scripts/devnet_release.py stamp --version <version>
cargo package --locked --allow-dirty
python3 scripts/devnet_release.py verify --version <version>
python3 scripts/devnet_release.py release --notes notes.md --dry-run true
```
