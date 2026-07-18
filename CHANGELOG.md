# Changelog

## Unreleased

## 0.1.0a2

Package version: `0.1.0-alpha.2`. Git tag: `v0.1.0a2` (see [VERSIONING.md](VERSIONING.md)).

### Breaking
- Authoritative freshness (POLY-3564): `Order.state_revision` → `Order.version`; balance `trading_version` / `funding_version` / `reserved_version` → `trading_updated_at_ns` / `funding_updated_at_ns` / `reserved_updated_at_ns`; subaccount and API-key `updated_at` are configuration timestamps; API-key `last_used_at` stays independent activity time
- Dual-path qty/price typing and broader Go/Python API parity (POLY-3253)

### Features
- Generated reconciliation and policy types exposed in the public SDK surface
- Internal transfer amounts use U128 wire types end-to-end

### Docs
- Added [VERSIONING.md](VERSIONING.md): independent per-SDK versions, shared tag format

## 0.1.0-alpha.1

Initial alpha tag (`v0.1.0-alpha.1`). Later tags use `v0.1.0aN` while the crate version stays `0.1.0-alpha.N`.
