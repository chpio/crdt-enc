# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

An encrypted storage layer for CRDTs (Conflict-free Replicated Data Types), meant to let CRDT-based
apps sync their data through plain file-sync tools like Syncthing without exposing plaintext to the
sync transport/storage. Changes are persisted as either CRDT ops or full-state snapshots; written files
are immutable (content-addressed by hash, never mutated after creation, only deleted during compaction)
so they sync safely over simple replicating filesystems.

`Core` itself is deliberately agnostic about *how* content gets protected — it only knows about a single
`Protector` trait (`encrypt`/`decrypt` on opaque byte blobs). The LUKS-like design (a random, rotatable
content-encryption key, itself protected by a swappable secondary mechanism such as a password or GPG
recipients, so the key can be rotated or the password changed without re-encrypting historical files) is
implemented as *one* `Protector` — [crdt-enc-envelope](crdt-enc-envelope/) — not baked into `Core`. See
[README.md](README.md) for the original design notes that motivated this envelope-encryption approach.

## Workspace layout

This is a Cargo workspace (resolver "2", edition 2024) with these crates:

- [crdt-enc/](crdt-enc/) — the core, storage/crypto-agnostic. Defines the `Core<S, ST, P>` engine and
  the two traits other crates implement:
  - [storage.rs](crdt-enc/src/storage.rs) — `Storage` trait: where/how encrypted blobs (local meta,
    remote meta, states, ops) are persisted and listed.
  - [protector.rs](crdt-enc/src/protector.rs) — `Protector` trait: `encrypt`/`decrypt` on opaque byte
    blobs, plus `init`/`set_remote_meta` lifecycle hooks. `Core` has no concept of "keys" at all — it's
    entirely up to the `Protector` implementation whether/how it manages key material.
  - [keys.rs](crdt-enc/src/keys.rs) — `Keys`/`Key`: reusable CRDT machinery (an `Orswot` of `Key`s plus
    a `latest_key_id` `MVReg`, converging via `Key: Ord` when multiple actors concurrently create the
    first key before ever syncing) for `Protector` implementations that want key rotation. Not used by
    `Core` itself — only by [crdt-enc-envelope](crdt-enc-envelope/).
  - [utils/](crdt-enc/src/utils/) — `VersionBytes`/`VersionBytesRef`/`VersionBytesBuf` (a UUID version
    tag prepended to a byte blob, used everywhere data is serialized so formats can evolve safely),
    `LockBox` (a sync-`Mutex` wrapper used to guard mutable state without holding a lock across
    `.await`), and `decode_version_bytes_mvreg_custom_phf`/`encode_version_bytes_mvreg_custom` (generic
    helpers for merging/writing an encrypted `T: CvRDT` value into a synced `MVReg<VersionBytes, Uuid>`
    register — the extension point where a `Protector` plugs in its actual encrypt/decrypt).
- [crdt-enc-envelope/](crdt-enc-envelope/) — a `Protector` implementing envelope encryption: manages its
  own rotating `Keys` CRDT (bootstrapping a new random content key if none exists yet, converging via
  `Keys::latest_key()`'s min-by-id if multiple devices bootstrap concurrently) and encrypts content
  directly with XChaCha20-Poly1305 using the current key. Protecting that one content key is delegated to
  a generic `KeySlotProtector` sub-trait (`wrap_key`/`unwrap_key` on a raw key blob, no CRDT/sync
  concerns) — `EnvelopeProtector<KS>` is generic over it.
- [crdt-enc-tokio/](crdt-enc-tokio/) — a `Storage` implementation backed by the local filesystem via
  Tokio, using two directories: a `local_path` (device-local meta) and a `remote_path` (the
  syncthing-shared tree, subdirectories `meta/`, `states/`, `ops/<actor-uuid>/<version>`). Locks
  `local_path` for exclusive use by this process (via `fs4`/`flock`, lazily on first local-meta access)
  so the same actor's local storage can't be opened by two processes at once.
- [examples/test/](examples/test/) — a minimal binary wiring the tokio storage + an
  `EnvelopeProtector<PasswordKeySlot>` together against a CRDT `MVReg<u64, Uuid>` state, useful as the
  reference for how the pieces fit together end to end.

## Core architecture (`crdt-enc/src/lib.rs`)

`Core<S, ST, P>` is generic over the CRDT state type `S` and two trait impls (`ST: Storage`,
`P: Protector`). Important flow to understand before changing this file:

- `Core::open` loads/creates `LocalMeta` (which holds a per-device random `local_actor_id`), then calls
  `init` on storage/protector concurrently, then does an initial `read_remote_meta_` pass. Unlike before
  the `Protector` refactor, `Core::open` does **not** generate or manage any encryption key itself —
  that's entirely the protector's job (see `EnvelopeProtector::set_remote_meta` for where the "bootstrap
  a key if none exists yet" logic now lives).
- All mutable in-process state lives in `CoreMutData` behind `Core.data: LockBox<..>` — never hold the
  lock across an `.await`; pull needed values out inside `data.with(...)`/`data.try_with(...)` closures
  first, then `.await` outside.
- The `remote_meta` field is itself a small CRDT (`RemoteMeta`, a `CvRDT` composed of two `MVReg`s: one
  for storage, one for the protector) used to let storage/protector gossip their own out-of-band metadata
  between devices the same way the app data syncs — via `set_remote_meta_storage`/
  `set_remote_meta_protector` / `CoreSubHandle`.
- `CoreSubHandle` is the object-safe, `dyn`-compatible handle (`Arc<Core<..>>` implements it) passed
  into `Storage::init`/`Protector::init` and stored by implementors (e.g.
  [crdt-enc-envelope](crdt-enc-envelope/src/lib.rs) clones it via `dyn_clone` to call back into `Core`
  later for `set_remote_meta_protector`).
- `read_and_apply` atomically reads state, builds a batch of CRDT ops from it, encrypts, and writes them
  via `Storage::store_ops` — all under a single `state_lock` (also taken by the remote-merge paths in
  `read_remote_states`/`read_remote_ops`) so a local apply and a remote merge can never interleave between
  a `read_and_apply` closure's read and its write.
- `compact` snapshots the current state into a new encrypted full-state file, then removes the
  now-redundant state/op files it superseded.
- Every persisted blob is versioned with a `Uuid` via `VersionBytes` and checked against a
  `phf::Set`/sorted `Vec` of supported version UUIDs before being deserialized (`ensure_versions_phf`,
  `ensure_versions`) — when introducing a new on-disk format, add a new UUID constant, extend the
  supported-versions set, and keep old versions readable rather than bumping in place.

## Common commands

```sh
cargo build --workspace          # build everything
cargo check --workspace          # fast type-check
cargo test --workspace           # run all tests
cargo test -p crdt-enc           # test just the core crate
cargo test --test version_box_buf   # run the doc/integration tests in crdt-enc/tests/version_box_buf.rs
cargo fmt                        # uses rustfmt.toml (imports_granularity = "Crate")
cargo run -p example-test        # run the example binary (writes/reads under ./data, see below)

cargo cov                        # run all tests + print a per-file coverage table
cargo cov-html                   # same, but render an HTML report and open it
cargo cov-lcov                   # same, but write target/llvm-cov/lcov.info (editor gutters)
```

The three `cov` aliases live in [.cargo/config.toml](.cargo/config.toml) and wrap
[`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov) — Cargo itself has no coverage
subcommand; the alias drives rustc's `-C instrument-coverage` plus the `llvm-tools` rustup component.
One-time setup on a fresh machine: `rustup component add llvm-tools-preview` and
`cargo install cargo-llvm-cov --locked`. They cover the whole workspace except `example-test`, and
build into a separate `target/llvm-cov-target/` directory, so switching between `cargo test` and
`cargo cov` does not invalidate the normal build cache.

The example binary ([examples/test/src/main.rs](examples/test/src/main.rs)) reads/writes real files
under `./data/local` and `./data/remote` relative to the workspace root — the untracked `data/`
directory in this repo is its output, safe to inspect or delete.

There is no CI config and no linter beyond `cargo fmt`/`cargo check` in this repo currently.

## Notes on current state

[docs/todo.md](docs/todo.md) tracks known rough edges the maintainer is aware of — check it before assuming an
`unwrap()`, missing validation, or stubbed encryption is an oversight to silently fix rather than
known, intentionally-deferred work.
