* replace agnostik (higher priority now): every `cargo build`/`update` prints a future-incompatibility
  warning from its `cfg_aliases = "0.1.1"` build-dependency (trailing-semicolon-in-macro, will become
  a hard error in a future Rust release, see rust-lang/rust#79813). Not fixable via `[patch]` --
  agnostik's manifest pins `cfg_aliases` to `^0.1.1`, and Cargo refuses to substitute the fixed 0.2.x
  release (or even our own git-patched cfg_aliases) because that falls outside the semver range;
  tried and confirmed this doesn't work. Only real fixes are forking agnostik to bump its
  `cfg_aliases` dependency, or dropping agnostik for something else entirely (find an alternative
  `spawn_blocking` helper, or drop the async-runtime-agnostic requirement).
* change crate names
* switch pgp to https://crates.io/crates/sequoia-openpgp
* harden the Storage trait contract for load_ops -- document and test that it must return ops
  contiguously/strictly ascending per actor (currently only true by convention in crdt-enc-tokio)
* queue & flush ops?
* crdt validation
* filter filenames in storage-tokio
* ensure concurrently deleted files (between storage::list and storage::load) do not crash core
* harden key material protection for the content-key bytes held in the Keys CRDT itself
  (crdt-enc-envelope::keys::Key/VersionBytes) -- done for PasswordKeySlot's password/derived keys
  (Zeroizing) already. Use https://crates.io/crates/secrets (mlock + guard pages + underflow
  canary + zeroize-on-drop, ~7 transitive deps, no required system library) rather than
  sequoia-openpgp's crypto::mem::Protected/Encrypted -- pulling in sequoia just for that is way
  oversized (~490 transitive deps even with minimal features)
* https://github.com/BurntSushi/quickcheck
* PasswordKeySlot must keep the cleartext password in memory for its entire lifetime (only
  Zeroizing-protected on drop), because unwrap_key must be able to handle a not-yet-seen salt at
  any time (e.g. a new device joining sync later with its own independently-bootstrapped entry) --
  there's no point at which it's safe to assume no more unknown salts will show up. Salt
  convergence (own_salt_kek preferring an already-known salt over minting a fresh one) makes this
  the rare case rather than the norm, but doesn't eliminate it. Add an opt-in `forget_password()`
  the app can call once it decides no new devices will join -- after that, only already-cached
  salts keep working until the password is provided again. (Own instance-lifetime commit, separate
  from everything else.)
