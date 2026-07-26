# Decoder fuzzing

Run the coverage-guided realtime publication decoder target:

```bash
cargo +nightly fuzz run realtime_decoders
```

The target exercises every distinct decoder used by the typed protobuf
subscription APIs. Keep `tests/hardening.rs` as the deterministic CI gate; use
this target for longer local or scheduled campaigns.
