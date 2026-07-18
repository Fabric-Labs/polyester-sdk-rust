# Versioning

Go, Python, and Rust SDKs share one alpha release train.

| | Declared version | Git tag |
|---|------------------|---------|
| **Python** | `0.1.0aN` | `v0.1.0aN` |
| **Go** | *(pin the tag)* | `v0.1.0aN` |
| **Rust** | `0.1.0-alpha.N` (Cargo) | `v0.1.0aN` |

Tags always match across repos (`v0.1.0aN`). This crate declares `0.1.0-alpha.N` because Cargo rejects PEP 440-style `0.1.0aN`.

See also shared context: `fabric-context/backend/sdk-versioning.md`.
