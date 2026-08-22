# crdt-enc-envelope

A [`Protector`](../crdt-enc/src/protector.rs) implementation for `crdt-enc` that does envelope
encryption: content is encrypted with a random, rotatable content key, and that one key is in turn
protected by a pluggable `KeySlotProtector` (a password, GPG recipients, ...). This is the LUKS-like
design described in the root [README](../README.md) -- the content key isn't derived from a password, so
changing the password/recipients later doesn't require re-encrypting historical data.

`crdt-enc` itself has no concept of "keys" at all; it only calls `Protector::encrypt`/`decrypt` on opaque
bytes. Everything about managing, rotating, and protecting a key lives in this crate instead.

## How it fits together

```
EnvelopeProtector<KS>             implements crdt_enc::protector::Protector
  keys: Keys                      rotating content-key CRDT (Orswot + latest_key_id MVReg)
  key_slot: KS                    implements KeySlotProtector
    KS::wrap_key(key)   -> bytes  protect the one content key
    KS::unwrap_key(bytes) -> key  unprotect it again
```

- `encrypt`/`decrypt` always use the current `keys.latest_key()`, via XChaCha20Poly1305.
- `set_remote_meta` decodes the synced `Keys` CRDT (each device's wrapped copy of the content key) and
  merges it in. If nobody has created a content key yet, it generates one, wraps it via
  `key_slot.wrap_key`, and syncs it back. If multiple devices independently bootstrap a key before ever
  syncing with each other, they converge on one canonical key afterwards via `Keys::latest_key()`'s
  min-by-id tiebreak -- the same pattern `Core::open` used before this crate existed.

## Implementing a `KeySlotProtector`

Only two methods, no CRDT or sync bookkeeping required -- that part is entirely `EnvelopeProtector`'s job:

```rust
#[async_trait]
impl KeySlotProtector for MyKeySlot {
    async fn wrap_key(&self, key: &[u8]) -> Result<Vec<u8>> {
        // protect `key` (e.g. encrypt it with a password-derived key, or for GPG recipients)
    }

    async fn unwrap_key(&self, wrapped: &[u8]) -> Result<Vec<u8>> {
        // reverse of wrap_key
    }
}
```

[`crdt-enc-gpgme`](../crdt-enc-gpgme/) is the only implementation right now, and it's a passthrough
stub (no real GPG encryption yet).

## Usage

```rust
let protector = EnvelopeProtector::new(my_key_slot);
let core = crdt_enc::Core::open(crdt_enc::OpenOptions {
    storage,
    protector,
    create: true,
    supported_data_versions,
    current_data_version,
}).await?;
```

See [examples/test](../examples/test/) for a complete runnable example.

## Current limitations

- **No key rotation API yet.** `Keys::insert_latest_key` is only ever called once, to bootstrap the very
  first content key -- there's no way to intentionally roll to a new one later.
- **Encrypted blobs aren't tagged with the id of the key that encrypted them**, so `decrypt` always
  assumes `latest_key()`. Rotating today (even if an API existed) would make all previously-encrypted
  data unreadable, since old keys stay in the `Keys` CRDT (`Keys::get_key` can look them up) but nothing
  records which key a given blob needs.

Both are tracked in the root [todo.md](../todo.md).
