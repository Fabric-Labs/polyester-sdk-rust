#!/usr/bin/env bash
# Generates the tradews codecs (src/tradews/sbe) from the public SBE
# schema with Real Logic's sbe-tool, pinned to schema id 7 and
# the version the schema file declares.
#
#   scripts/generate_tradews_sbe.sh --proto <polyester-proto checkout>
#       Reads the published schema: manifest.json's tradeWebSocketSbe
#       block and paths.sbe. The file's sha256, schema id and
#       version must match the manifest. The schema is copied byte for byte to
#       sbe/tradews-v<version>.xml.
#   scripts/generate_tradews_sbe.sh --schema <file>
#       Generates from a schema file that is not published yet. The lock
#       records the source as unpublished.
#   scripts/generate_tradews_sbe.sh --check
#       Regenerates from the vendored schema into a temporary directory and
#       fails if the committed codecs or lock differ.
#
# Environment: JAVA (default: java on PATH), SBE_JAR (default: sbe-all
# 1.35.6 from Maven Central, cached and checked against its sha256).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCHEMA_ID=7
MANIFEST_BLOCK="tradeWebSocketSbe"
MANIFEST_PATH_KEY="sbe"
SBE_VERSION="1.35.6"
SBE_SHA256="1b85c37264866bcb83cf82d9a8adcf0d7c5faad2e080e75c0813dc9f13c15783"
OUT_REL="src/tradews/sbe"
LOCK_REL="sbe/tradews.lock.json"

usage() {
  sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
  exit 2
}

mode=""
arg=""
case "${1:-}" in
  --proto | --schema)
    [[ $# -eq 2 ]] || usage
    mode="${1#--}"
    arg="$2"
    ;;
  --check)
    [[ $# -eq 1 ]] || usage
    mode="check"
    ;;
  *) usage ;;
esac

JAVA="${JAVA:-java}"
command -v rustfmt >/dev/null 2>&1 || { echo "error: rustfmt not found" >&2; exit 127; }
command -v "$JAVA" >/dev/null 2>&1 || { echo "error: java not found; set JAVA or install a JRE (17 or later)" >&2; exit 127; }

sha256() { sha256sum "$1" | cut -d' ' -f1; }

jar="${SBE_JAR:-}"
if [[ -z "$jar" ]]; then
  cache="${XDG_CACHE_HOME:-$HOME/.cache}/polyester-sdk"
  jar="$cache/sbe-all-$SBE_VERSION.jar"
  if [[ ! -f "$jar" ]]; then
    mkdir -p "$cache"
    curl -fsSL -o "$jar.part" "https://repo1.maven.org/maven2/uk/co/real-logic/sbe-all/$SBE_VERSION/sbe-all-$SBE_VERSION.jar"
    mv "$jar.part" "$jar"
  fi
fi
if [[ "$(sha256 "$jar")" != "$SBE_SHA256" ]]; then
  echo "error: $jar is not sbe-all $SBE_VERSION (sha256 mismatch)" >&2
  exit 1
fi

# schema_attr FILE ATTR prints the messageSchema root attribute.
schema_attr() {
  python3 -I -c 'import sys, xml.etree.ElementTree as ET; print(ET.parse(sys.argv[1]).getroot().attrib[sys.argv[2]])' "$1" "$2"
}

source_kind=""
schema=""
case "$mode" in
  proto)
    manifest="$arg/manifest.json"
    [[ -f "$manifest" ]] || { echo "error: $manifest not found" >&2; exit 1; }
    read -r rel want_sha want_id want_version < <(python3 -I - "$manifest" "$MANIFEST_BLOCK" "$MANIFEST_PATH_KEY" <<'PY'
import json, sys
manifest = json.load(open(sys.argv[1], encoding="utf-8"))
block = manifest.get(sys.argv[2])
path = manifest.get("paths", {}).get(sys.argv[3])
if not block or not path:
    sys.exit(f"manifest has no {sys.argv[2]} block or paths.{sys.argv[3]}: the schema is not published")
print(path, block["sha256"], block["schemaId"], block["schemaVersion"])
PY
    )
    schema="$arg/$rel"
    [[ -f "$schema" ]] || { echo "error: $schema not found" >&2; exit 1; }
    [[ "$(sha256 "$schema")" == "$want_sha" ]] || { echo "error: $schema does not match the manifest's sha256" >&2; exit 1; }
    [[ "$want_id" == "$SCHEMA_ID" ]] || { echo "error: manifest schemaId $want_id, want $SCHEMA_ID" >&2; exit 1; }
    [[ "$(schema_attr "$schema" version)" == "$want_version" ]] || { echo "error: $schema version differs from the manifest" >&2; exit 1; }
    source_kind="polyester-proto"
    ;;
  schema)
    schema="$arg"
    [[ -f "$schema" ]] || { echo "error: $schema not found" >&2; exit 1; }
    source_kind="unpublished"
    ;;
  check)
    schema="$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1]))["schemaFile"])' "$ROOT/$LOCK_REL")"
    schema="$ROOT/$schema"
    source_kind="$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1]))["source"])' "$ROOT/$LOCK_REL")"
    ;;
esac

id="$(schema_attr "$schema" id)"
version="$(schema_attr "$schema" version)"
[[ "$id" == "$SCHEMA_ID" ]] || { echo "error: $schema has schema id $id, want $SCHEMA_ID" >&2; exit 1; }
[[ "$(schema_attr "$schema" package)" == "tradews" ]] || { echo "error: $schema package is not tradews" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
schema_name="tradews-v$version.xml"
cp "$schema" "$work/$schema_name"

"$JAVA" --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
  -Dsbe.target.language=Rust \
  -Dsbe.output.dir="$work/crate" \
  -jar "$jar" "$work/$schema_name" >/dev/null
# sbe-tool writes a crate; the SDK carries it as the module
# tradews::sbe. Its root becomes mod.rs and paths into the crate root
# become paths into that module. Nothing else is changed.
mkdir -p "$work/out/tradews"
for f in "$work/crate/tradews/src"/*.rs; do
  name="$(basename "$f")"
  [[ "$name" == "lib.rs" ]] && name="mod.rs"
  sed -e 's/\bcrate::/super::/g' "$f" >"$work/out/tradews/$name"
done
rustfmt --edition 2024 --config-path "$ROOT/rustfmt.toml" "$work/out/tradews/mod.rs"
# rustfmt keeps trailing whitespace inside comments; sbe-tool writes some.
sed -i -E 's/[[:space:]]+$//' "$work/out/tradews"/*.rs

python3 -I - "$work/lock.json" "$schema_name" "$id" "$version" "$(sha256 "$work/$schema_name")" "$source_kind" "$SBE_VERSION" "$SBE_SHA256" "$MANIFEST_BLOCK" <<'PY'
import json, sys
out, name, schema_id, version, digest, source, tool_version, tool_sha, block = sys.argv[1:]
lock = {
    "schemaFile": f"sbe/{name}",
    "schemaId": int(schema_id),
    "schemaVersion": int(version),
    "sha256": digest,
    "source": source,
    "manifestBlock": block,
    "generator": {"tool": "sbe-all", "version": tool_version, "sha256": tool_sha},
}
with open(out, "w", encoding="utf-8") as f:
    json.dump(lock, f, indent=2)
    f.write("\n")
PY

if [[ "$mode" == "check" ]]; then
  status=0
  diff -r "$work/out/tradews" "$ROOT/$OUT_REL" >/dev/null || { echo "error: $OUT_REL differs from a fresh generation" >&2; status=1; }
  diff "$work/lock.json" "$ROOT/$LOCK_REL" >/dev/null || { echo "error: $LOCK_REL differs from a fresh generation" >&2; status=1; }
  [[ $status -eq 0 ]] && echo "ok: tradews codecs match schema $id version $version"
  exit $status
fi

rm -rf "${ROOT:?}/$OUT_REL"
mkdir -p "$(dirname "$ROOT/$OUT_REL")"
cp -r "$work/out/tradews" "$ROOT/$OUT_REL"
if [[ "$ROOT/sbe/$schema_name" != "$(cd "$(dirname "$schema")" && pwd)/$(basename "$schema")" ]]; then
  find "$ROOT/sbe" -maxdepth 1 -name 'tradews-v*.xml' -delete
  cp "$work/$schema_name" "$ROOT/sbe/$schema_name"
fi
cp "$work/lock.json" "$ROOT/$LOCK_REL"
echo "generated $OUT_REL from schema $id version $version ($source_kind)"
if [[ "$source_kind" != "polyester-proto" ]]; then
  echo "warning: generated from an unpublished schema; regenerate with --proto before release" >&2
fi
