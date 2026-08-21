# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

An encrypted storage layer for CRDTs (Conflict-free Replicated Data Types), meant to let CRDT-based
apps sync their data through plain file-sync tools like Syncthing without exposing plaintext to the
sync transport/storage. Changes are persisted as either CRDT ops or full-state snapshots; written files
are immutable (content-addressed by hash, never mutated after creation, only deleted during compaction)
so they sync safely over simple replicating filesystems.

The on-disk header/key scheme is modeled on LUKS: the actual data-encryption key is never derived
directly from a password, so keys can be rotated (via multiple stored `Key` entries with a "latest key"
CRDT register) without needing to re-encrypt every historical file. See [README.md](README.md) for the
original design notes.

## Workspace layout

This is a Cargo workspace (resolver "2", edition 2024) with these crates:

- [crdt-enc/](crdt-enc/) — the core, storage/transport-agnostic. Defines the `Core<S, ST, C, KC>`
  engine and the three traits other crates implement:
  - [storage.rs](crdt-enc/src/storage.rs) — `Storage` trait: where/how encrypted blobs (local meta,
    remote meta, states, ops) are persisted and listed.
  - [cryptor.rs](crdt-enc/src/cryptor.rs) — `Cryptor` trait: how data blobs are encrypted/decrypted
    with a symmetric key (`gen_key`/`encrypt`/`decrypt`).
  - [key_cryptor.rs](crdt-enc/src/key_cryptor.rs) — `KeyCryptor` trait: how the symmetric `Keys`
    CRDT (an `Orswot` of `Key`s plus a `latest_key_id` `MVReg`) itself gets protected/shared, e.g. via
    per-recipient asymmetric encryption.
  - [utils/](crdt-enc/src/utils/) — `VersionBytes`/`VersionBytesRef`/`VersionBytesBuf` (a UUID version
    tag prepended to a byte blob, used everywhere data is serialized so formats can evolve safely) and
    `LockBox` (a sync-`Mutex` wrapper used to guard `Core`'s in-memory state without holding a lock
    across `.await`).
- [crdt-enc-tokio/](crdt-enc-tokio/) — a `Storage` implementation backed by the local filesystem via
  Tokio, using two directories: a `local_path` (device-local meta) and a `remote_path` (the
  syncthing-shared tree, subdirectories `meta/`, `states/`, `ops/<actor-uuid>/<version>`).
- [crdt-enc-xchacha20poly1305/](crdt-enc-xchacha20poly1305/) — a `Cryptor` implementation using
  XChaCha20-Poly1305 (via the `chacha20poly1305` crate) for symmetric encryption of data blobs.
- [crdt-enc-gpgme/](crdt-enc-gpgme/) — a `KeyCryptor` implementation intended to protect the `Keys`
  CRDT with GPG/OpenPGP (via `gpgme`); the actual encrypt/decrypt calls are still stubbed as TODOs, see
  [crdt-enc-gpgme/src/lib.rs](crdt-enc-gpgme/src/lib.rs). Requires the system `gpgme` library to build.
- [examples/test/](examples/test/) — a minimal binary wiring the tokio storage + xchacha20poly1305
  cryptor + gpgme key-cryptor together against a CRDT `MVReg<u64, Uuid>` state, useful as the
  reference for how the pieces fit together end to end.

`Cargo.toml` at the workspace root also patches `agnostik` (the async-runtime-agnostic `spawn_blocking`
helper used by the xchacha20poly1305 crate) to a git branch — don't "helpfully" remove that patch.

## Core architecture (`crdt-enc/src/lib.rs`)

`Core<S, ST, C, KC>` is generic over the CRDT state type `S` and the three trait impls (`ST: Storage`,
`C: Cryptor`, `KC: KeyCryptor`). Important flow to understand before changing this file:

- `Core::open` loads/creates `LocalMeta` (which holds a per-device random `local_actor_id`), then calls
  `init` on storage/cryptor/key_cryptor concurrently, then does an initial `read_remote_meta_` pass, and
  finally generates a new data-encryption key if none exists yet.
- All mutable in-process state lives in `CoreMutData` behind `Core.data: LockBox<..>` — never hold the
  lock across an `.await`; pull needed values out inside `data.with(...)`/`data.try_with(...)` closures
  first, then `.await` outside.
- The `remote_meta` field is itself a small CRDT (`RemoteMeta`, a `CvRDT` composed of three `MVReg`s)
  used to let storage/cryptor/key_cryptor gossip their own out-of-band metadata (e.g. GPG recipient
  fingerprints) between devices the same way the app data syncs — via `set_remote_meta_*` /
  `CoreSubHandle`.
- `CoreSubHandle` is the object-safe, `dyn`-compatible handle (`Arc<Core<..>>` implements it) passed
  into `Storage::init`/`Cryptor::init`/`KeyCryptor::init` and stored by implementors (e.g.
  [crdt-enc-gpgme](crdt-enc-gpgme/src/lib.rs) clones it via `dyn_clone` to call back into `Core` later
  for `set_keys`/`set_remote_meta_key_cryptor`).
- `apply_ops` serializes+encrypts a batch of CRDT ops and writes them via `Storage::store_ops`, guarded
  by `apply_ops_lock` so ops from this actor are never interleaved/misordered.
- `compact` snapshots the current state into a new encrypted full-state file, then removes the
  now-redundant state/op files it superseded.
- Every persisted blob is versioned with a `Uuid` via `VersionBytes` and checked against a
  `phf::Set`/sorted `Vec` of supported version UUIDs before being deserialized (`ensure_versions_phf`,
  `ensure_versions`) — when introducing a new on-disk format, add a new UUID constant, extend the
  supported-versions set, and keep old versions readable rather than bumping in place.

## Common commands

```sh
cargo build --workspace          # build everything (requires system gpgme library)
cargo check --workspace          # fast type-check
cargo test --workspace           # run all tests
cargo test -p crdt-enc           # test just the core crate
cargo test --test version_box_buf   # run the doc/integration tests in crdt-enc/tests/version_box_buf.rs
cargo fmt                        # uses rustfmt.toml (imports_granularity = "Crate")
cargo run -p example-test        # run the example binary (writes/reads under ./data, see below)
```

The example binary ([examples/test/src/main.rs](examples/test/src/main.rs)) reads/writes real files
under `./data/local` and `./data/remote` relative to the workspace root — the untracked `data/`
directory in this repo is its output, safe to inspect or delete.

There is no CI config and no linter beyond `cargo fmt`/`cargo check` in this repo currently.

## Notes on current state

[todo.md](todo.md) tracks known rough edges the maintainer is aware of — check it before assuming an
`unwrap()`, missing validation, or stubbed encryption (e.g. the TODO comments in
[crdt-enc-gpgme/src/lib.rs](crdt-enc-gpgme/src/lib.rs)) is an oversight to silently fix rather than
known, intentionally-deferred work.
