//! The generated codecs, the vendored schema and its lock agree: the lock
//! pins the schema file's digest, and the schema's id and version are the
//! codecs'. `scripts/generate_tradews_sbe.sh --check` regenerates them.

use sha2::{Digest, Sha256};

use crate::tradews::{SCHEMA_ID, SCHEMA_VERSION};

fn root_attr(xml: &str, name: &str) -> String {
    let start = xml.find("<messageSchema").expect("a messageSchema root");
    let tag = &xml[start..start + xml[start..].find('>').unwrap()];
    let key = format!(" {name}=\"");
    let at = tag
        .find(&key)
        .unwrap_or_else(|| panic!("no {name} attribute"))
        + key.len();
    tag[at..at + tag[at..].find('"').unwrap()].to_owned()
}

#[test]
fn codecs_are_pinned_to_the_vendored_schema() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let lock: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("sbe/tradews.lock.json")).unwrap())
            .unwrap();
    let file = lock["schemaFile"].as_str().unwrap();
    let schema = std::fs::read(root.join(file)).unwrap();
    assert_eq!(
        hex::encode(Sha256::digest(&schema)),
        lock["sha256"].as_str().unwrap(),
        "{file} differs from its lock"
    );
    let xml = String::from_utf8(schema).unwrap();
    let (id, version): (u16, u16) = (
        root_attr(&xml, "id").parse().unwrap(),
        root_attr(&xml, "version").parse().unwrap(),
    );
    assert_eq!(root_attr(&xml, "package"), "tradews");
    assert_eq!((id, version), (7, SCHEMA_VERSION));
    assert_eq!(
        (SCHEMA_ID, u64::from(SCHEMA_VERSION)),
        (7, lock["schemaVersion"].as_u64().unwrap())
    );
    assert_eq!(lock["schemaId"].as_u64(), Some(7));
    assert_eq!(file, format!("sbe/tradews-v{version}.xml"));
    assert_eq!(lock["generator"]["tool"].as_str(), Some("sbe-all"));
    assert_eq!(lock["generator"]["version"].as_str(), Some("1.35.6"));
    assert_eq!(lock["manifestBlock"].as_str(), Some("tradeWebSocketSbe"));
    let source = lock["source"].as_str().unwrap();
    assert!(
        source == "polyester-proto" || source == "unpublished",
        "source {source}"
    );
    if std::env::var_os("POLYESTER_RELEASE").is_some() {
        assert_eq!(
            source, "polyester-proto",
            "a release must generate the codecs from the schema published in polyester-proto"
        );
    }
}
