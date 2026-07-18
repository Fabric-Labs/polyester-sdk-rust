# Versioning

Each language SDK versions **independently**. Bump this repo only when Rust SDK changes ship; do not mirror Go/Python release numbers.

Shared **format** only (each SDK’s own `N`):

| SDK | Declared version | Git tag |
|-----|------------------|---------|
| Python | `0.1.0aN` | `v0.1.0aN` |
| Go | *(pin the tag)* | `v0.1.0aN` |
| Rust | `0.1.0-alpha.N` (Cargo) | `v0.1.0aN` |

This crate declares `0.1.0-alpha.N` because Cargo rejects PEP 440-style `0.1.0aN`. The git tag is still `v0.1.0aN` for this crate’s own `N`.

See also shared context: `fabric-context/backend/sdk-versioning.md`.
