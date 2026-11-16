//! Every ID field of every reply in the contract is classified.
//!
//! `testdata/reply_id_inventory.tsv`, shared with the Go and Python SDKs,
//! lists each non-repeated scalar field named `id` or ending in `_id` of every
//! RPC reply, recursively, as `required` (its decoder refuses 0 or an empty
//! string) or `not_required` with a reason. This test reads the generated
//! messages and Connect clients under `src/gen` and fails when the contract
//! gains an ID field the inventory does not list, or loses one it lists.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// A message or oneof enum: its fields as (proto field name, Rust type), and
/// the module path its type references resolve from.
struct Item {
    full_name: String,
    scope: Vec<String>,
    fields: Vec<(String, String)>,
}

/// The generated messages and oneofs of every package, keyed by package and
/// module path plus type name, and the reply message of every RPC.
struct Contract {
    items: BTreeMap<(String, Vec<String>, String), Item>,
    replies: BTreeSet<(String, String)>,
    packages: BTreeSet<String>,
}

fn snake(camel: &str) -> String {
    let mut out = String::new();
    for (i, c) in camel.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// `text` without whitespace, and without the trailing comma rustfmt leaves in
/// a generic argument list split over lines.
fn compact(text: &str) -> String {
    let out: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    out.replace(",>", ">")
}

fn parse_messages(package: &str, source: &str, contract: &mut Contract) {
    // (module name, brace depth at which it was opened)
    let mut modules: Vec<(String, usize)> = Vec::new();
    let mut depth = 0usize;
    let lines: Vec<&str> = source.lines().collect();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i].trim();
        let path: Vec<String> = modules.iter().map(|(name, _)| name.clone()).collect();
        // `__buffa::view` holds the borrowed views, not messages. A message
        // named `View` has a module `view` of its own elsewhere.
        let in_view = path.len() >= 2 && path[0] == "__buffa" && path[1] == "view";
        if let Some(name) = line
            .strip_prefix("pub mod ")
            .and_then(|rest| rest.strip_suffix(" {"))
        {
            modules.push((name.to_owned(), depth));
            depth += 1;
            i += 1;
            continue;
        }
        let is_struct =
            line.starts_with("pub struct ") && line.ends_with(" {") && !line.contains('<');
        let is_oneof = line.starts_with("pub enum ")
            && line.ends_with(" {")
            && !line.contains('<')
            && path.first().map(String::as_str) == Some("__buffa")
            && path.get(1).map(String::as_str) == Some("oneof");
        if !in_view && (is_struct || is_oneof) {
            let name = line
                .trim_start_matches("pub struct ")
                .trim_start_matches("pub enum ")
                .trim_end_matches(" {")
                .to_owned();
            let mut fields = Vec::new();
            let mut pending: Option<String> = None;
            let mut j = i + 1;
            let mut body_depth = 1usize;
            while body_depth > 0 {
                let raw = lines[j];
                let text = raw.trim();
                if let Some(rest) = text.strip_prefix("/// Field ")
                    && let Some(start) = rest.find('`')
                {
                    let end = rest[start + 1..].find('`').unwrap() + start + 1;
                    pending = Some(rest[start + 1..end].to_owned());
                }
                let at_member =
                    body_depth == 1 && !text.starts_with('#') && !text.starts_with("//");
                if at_member && is_struct && text.starts_with("pub ") && text.contains(':') {
                    // `pub name: Type,`, the type possibly spanning lines.
                    let mut decl = text.to_owned();
                    while !(decl.trim_end().ends_with(',')
                        && decl.matches('<').count() == decl.matches('>').count())
                    {
                        j += 1;
                        decl.push_str(lines[j].trim());
                    }
                    let (ident, ty) = decl.split_once(':').unwrap();
                    // A oneof field has no field number of its own.
                    let field = pending.take().unwrap_or_else(|| {
                        ident
                            .trim_start_matches("pub ")
                            .trim_start_matches("r#")
                            .to_owned()
                    });
                    fields.push((field, compact(ty.trim_end_matches(','))));
                } else if at_member
                    && is_oneof
                    && text.chars().next().is_some_and(|c| c.is_ascii_uppercase())
                {
                    // `Variant(Type),`, the type possibly spanning lines.
                    let mut decl = text.to_owned();
                    while !decl.trim_end().ends_with("),") {
                        j += 1;
                        decl.push_str(lines[j].trim());
                    }
                    let (variant, ty) = decl.split_once('(').unwrap();
                    let ty = compact(ty.trim_end_matches("),"));
                    fields.push((snake(variant), ty.trim_end_matches(',').to_owned()));
                }
                body_depth += raw.matches('{').count();
                body_depth -= raw.matches('}').count();
                j += 1;
            }
            // A oneof is keyed by the message module it belongs to, as its
            // re-export outside `__buffa::oneof` names it.
            let key_path: Vec<String> = if is_oneof {
                path[2..].to_vec()
            } else {
                path.clone()
            };
            contract.items.insert(
                (package.to_owned(), key_path.clone(), name.clone()),
                Item {
                    full_name: String::new(),
                    scope: path.clone(),
                    fields,
                },
            );
            i = j;
            continue;
        }
        depth += lines[i].matches('{').count();
        depth -= lines[i].matches('}').count();
        while modules.last().is_some_and(|(_, opened)| depth <= *opened) {
            modules.pop();
        }
        i += 1;
    }
}

fn parse_replies(source: &str, contract: &mut Contract) {
    let text = compact(source);
    let marker = "__buffa::view::";
    let mut rest = text.as_str();
    while let Some(at) = rest.find("crate::proto::") {
        let tail = &rest[at + "crate::proto::".len()..];
        if let Some(view) = tail.find(marker)
            && let Some(close) = tail[view..].find("View<")
        {
            let package = tail[..view].trim_end_matches("::").replace("::", ".");
            let name = &tail[view + marker.len()..view + close];
            if name.ends_with("Response") && !name.contains(':') {
                contract.replies.insert((package, name.to_owned()));
            }
        }
        rest = &rest[at + 1..];
    }
}

/// The message or oneof a Rust type names, resolved from `scope` in `package`.
fn resolve(
    contract: &Contract,
    package: &str,
    scope: &[String],
    ty: &str,
) -> Option<(String, Vec<String>, String)> {
    let mut ty = ty;
    for wrapper in [
        "::buffa::MessageField<",
        "::core::option::Option<",
        "::buffa::alloc::boxed::Box<",
        "::buffa::alloc::vec::Vec<",
    ] {
        while let Some(inner) = ty.strip_prefix(wrapper) {
            ty = inner.strip_suffix('>')?;
        }
    }
    if ty.starts_with("::") || !ty.chars().any(|c| c.is_ascii_uppercase()) {
        return None; // scalars, enums and well-known types
    }
    // The absolute module path under `crate::proto` the type names: a
    // reference to another package climbs out of this one with `super`.
    let segments: Vec<&str> = ty.split("::").collect();
    let mut absolute: Vec<String>;
    let rest: &[&str];
    if segments[0] == "crate" {
        absolute = Vec::new();
        rest = segments.get(2..)?;
    } else {
        absolute = package.split('.').map(str::to_owned).collect();
        absolute.extend(scope.iter().cloned());
        let supers = segments.iter().take_while(|s| **s == "super").count();
        absolute.truncate(absolute.len().checked_sub(supers)?);
        rest = &segments[supers..];
    }
    let (name, modules) = rest.split_last()?;
    absolute.extend(modules.iter().map(|m| (*m).to_owned()));
    // Split off the longest known package; what follows is the module path.
    let split = (1..=absolute.len())
        .rev()
        .find(|end| contract.packages.contains(&absolute[..*end].join(".")))?;
    let package = absolute[..split].join(".");
    let mut path = absolute[split..].to_vec();
    // A oneof is keyed by its message module, without `__buffa::oneof`.
    if path.len() >= 2 && path[0] == "__buffa" && path[1] == "oneof" {
        path.drain(..2);
    }
    let key = (package, path, (*name).to_owned());
    contract.items.contains_key(&key).then_some(key)
}

fn load() -> Contract {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/gen");
    let mut contract = Contract {
        items: BTreeMap::new(),
        replies: BTreeSet::new(),
        packages: BTreeSet::new(),
    };
    let mut sources = Vec::new();
    for entry in std::fs::read_dir(root.join("buffa")).unwrap() {
        let path = entry.unwrap().path();
        let package = path.file_stem().unwrap().to_string_lossy().into_owned();
        if package.starts_with("google.")
            || package.starts_with("buf.")
            || package.starts_with("gnostic.")
        {
            continue;
        }
        contract.packages.insert(package.clone());
        sources.push((package, std::fs::read_to_string(&path).unwrap()));
    }
    for (package, source) in &sources {
        parse_messages(package, source, &mut contract);
    }
    for entry in std::fs::read_dir(root.join("connect")).unwrap() {
        parse_replies(
            &std::fs::read_to_string(entry.unwrap().path()).unwrap(),
            &mut contract,
        );
    }
    // Proto full names: a nested message's module is its parent's snake name.
    let keys: Vec<_> = contract.items.keys().cloned().collect();
    for key in keys {
        let (package, path, name) = &key;
        let mut full = package.clone();
        for (depth, module) in path.iter().enumerate() {
            let parent = contract
                .items
                .keys()
                .find(|(p, m, n)| p == package && m[..] == path[..depth] && snake(n) == *module)
                .map(|(_, _, n)| n.clone())
                .unwrap_or_else(|| module.clone());
            full.push('.');
            full.push_str(&parent);
        }
        full.push('.');
        full.push_str(name);
        contract.items.get_mut(&key).unwrap().full_name = full;
    }
    contract
}

fn reply_id_fields(contract: &Contract) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut seen = BTreeSet::new();
    let mut stack: Vec<(String, Vec<String>, String)> = contract
        .replies
        .iter()
        .map(|(package, name)| (package.clone(), Vec::new(), name.clone()))
        .collect();
    while let Some(key) = stack.pop() {
        if !seen.insert(key.clone()) {
            continue;
        }
        let Some(item) = contract.items.get(&key) else {
            continue;
        };
        for (field, ty) in &item.fields {
            if let Some(next) = resolve(contract, &key.0, &item.scope, ty) {
                stack.push(next);
                continue;
            }
            let repeated = ty.starts_with("::buffa::alloc::vec::Vec<");
            let enum_value = ty.contains("EnumValue<");
            let message_like = ty.contains("MessageField<") || ty.contains("google::protobuf");
            if !repeated
                && !enum_value
                && !message_like
                && (field == "id" || field.ends_with("_id"))
            {
                // A oneof variant belongs to the message the oneof is in.
                let owner = if item.scope.first().map(String::as_str) == Some("__buffa") {
                    item.full_name.rsplit_once('.').unwrap().0.to_owned()
                } else {
                    item.full_name.clone()
                };
                out.insert(format!("{owner}.{field}"));
            }
        }
    }
    out
}

#[test]
fn inventory_lists_every_reply_id_field() {
    let inventory = include_str!("../testdata/reply_id_inventory.tsv");
    let mut listed = BTreeSet::new();
    for line in inventory.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        assert_eq!(parts.len(), 3, "malformed inventory line {line:?}");
        assert!(
            parts[1] == "required" || (parts[1] == "not_required" && !parts[2].is_empty()),
            "malformed inventory line {line:?}"
        );
        assert!(
            listed.insert(parts[0].to_owned()),
            "{} is listed twice",
            parts[0]
        );
    }
    let contract = load();
    assert!(
        contract.replies.len() > 100,
        "found {} replies",
        contract.replies.len()
    );
    let walked = reply_id_fields(&contract);
    let unlisted: Vec<_> = walked.difference(&listed).collect();
    let gone: Vec<_> = listed.difference(&walked).collect();
    assert!(
        unlisted.is_empty() && gone.is_empty(),
        "reply ID inventory out of date:\nunlisted: {unlisted:?}\nlisted but not in any reply: {gone:?}"
    );
}

/// The messages of the 0.93.3 and 0.93.4 contract changes whose every field the SDK
/// sets from a parameter or exposes on a model, unless the inventory says
/// why not.
const FIELD_INVENTORY_MESSAGES: [&str; 37] = [
    "ledger.read.v1.CreateStatementExportRequest",
    "ledger.read.v1.GetStatementExportRequest",
    "ledger.read.v1.ListStatementExportsRequest",
    "ledger.read.v1.StatementExport",
    "ledger.read.v1.CreateStatementExportResponse",
    "ledger.read.v1.GetStatementExportResponse",
    "ledger.read.v1.ListStatementExportsResponse",
    "ledger.read.v1.ExportStatementResponse",
    "ledger.read.v1.BalanceSeries",
    "ledger.read.v1.GetBalanceHistoryResponse",
    "transfer.v1.BatchCreateInternalTransfersRequest",
    "transfer.v1.InternalTransferBatchLeg",
    "transfer.v1.BatchCreateInternalTransfersResponse",
    "transfer.v1.InternalTransferBatchLegResult",
    "transfer.v1.ResolvedDestination",
    "transfer.v1.ListDustBalancesRequest",
    "transfer.v1.QuoteDustConversionRequest",
    "transfer.v1.CreateDustConversionRequest",
    "transfer.v1.GetDustConversionRequest",
    "transfer.v1.ListDustConversionsRequest",
    "transfer.v1.DustBalance",
    "transfer.v1.DustAssetPrice",
    "transfer.v1.DustSource",
    "transfer.v1.DustConversion",
    "transfer.v1.ListDustBalancesResponse",
    "transfer.v1.QuoteDustConversionResponse",
    "transfer.v1.CreateDustConversionResponse",
    "transfer.v1.GetDustConversionResponse",
    "transfer.v1.ListDustConversionsResponse",
    "chain.lifecycle.v1.ListAccountFlowsRequest",
    "chain.lifecycle.v1.GetAccountFlowRequest",
    "chain.lifecycle.v1.ListAccountFlowsResponse",
    "chain.lifecycle.v1.GetAccountFlowResponse",
    "orders.v1.ErrorDetail",
    "auth.v1.AuthErrorDetail",
    "orderbook.v1.GetOrderBookRequest",
    "orderbook.v1.GetOrderBookResponse",
];

/// Every proto field of `message` as `<message>.<field>`, a oneof's members
/// in place of the oneof. `None` when the contract has no such message.
fn message_fields(contract: &Contract, message: &str) -> Option<BTreeSet<String>> {
    let (package, name) = message.rsplit_once('.')?;
    let key = (package.to_owned(), Vec::new(), name.to_owned());
    let item = contract.items.get(&key)?;
    let mut out = BTreeSet::new();
    for (field, ty) in &item.fields {
        if field.starts_with("__buffa") {
            continue;
        }
        match resolve(contract, package, &item.scope, ty) {
            Some(oneof)
                if contract.items[&oneof].scope.first().map(String::as_str) == Some("__buffa") =>
            {
                for (member, _) in &contract.items[&oneof].fields {
                    out.insert(format!("{message}.{member}"));
                }
            }
            _ => {
                out.insert(format!("{message}.{field}"));
            }
        }
    }
    Some(out)
}

#[test]
fn contract_field_inventory_lists_every_field() {
    let inventory = include_str!("../testdata/contract_field_inventory.tsv");
    let mut listed = BTreeSet::new();
    for line in inventory.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        assert_eq!(parts.len(), 3, "malformed inventory line {line:?}");
        assert!(
            matches!(parts[1], "covered" | "omitted") && !parts[2].is_empty(),
            "malformed inventory line {line:?}"
        );
        assert!(
            listed.insert(parts[0].to_owned()),
            "{} is listed twice",
            parts[0]
        );
    }
    let contract = load();
    let mut walked = BTreeSet::new();
    let mut missing = Vec::new();
    for message in FIELD_INVENTORY_MESSAGES {
        match message_fields(&contract, message) {
            Some(fields) => walked.extend(fields),
            None => missing.push(message),
        }
    }
    let unlisted: Vec<_> = walked.difference(&listed).collect();
    let gone: Vec<_> = listed.difference(&walked).collect();
    assert!(
        missing.is_empty() && unlisted.is_empty() && gone.is_empty(),
        "contract field inventory out of date:\nmessages not in the contract: {missing:?}\nunlisted: {unlisted:?}\nlisted but gone: {gone:?}"
    );
}
