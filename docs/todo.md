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
* https://github.com/BurntSushi/quickcheck
* crdt-enc-envelope::keys::Keys.keys (an Orswot<Key,Uuid>) grows unbounded and is never
  compacted/garbage-collected -- by design, old keys stay around forever so content encrypted with
  them (after a rotation) remains decryptable. Unlike the MVReg wrappers around it
  (latest_key_id, remote_meta.protector), which do shrink back to a single value after a
  conflict-resolving write (crdts::MVReg::apply really prunes dominated entries via Vec::retain,
  not just a read-time interpretation), the Orswot itself has no such mechanism, so every rotation
  ever performed permanently grows the serialized Keys payload. Core::compact() doesn't help --
  Keys lives entirely inside the protector's own remote_meta, separate from the app-level CRDT
  state that compact() snapshots. Would need something like: once every device is known to be able
  to decrypt content with the latest key (or an app-level policy decides old keys are no longer
  needed), prune superseded Key entries from the Orswot.
* PasswordKeySlot must keep the password resident for its entire lifetime (encrypted at rest via
  `AtRest` since the crdt-enc-envelope::at_rest work, but still resident), because unwrap_key must
  be able to handle a not-yet-seen salt at any time (e.g. a new device joining sync later with its
  own independently-bootstrapped entry) -- there's no point at which it's safe to assume no more
  unknown salts will show up. Salt convergence (own_salt_kek preferring an already-known salt over
  minting a fresh one) makes this the rare case rather than the norm, but doesn't eliminate it. Add
  an opt-in `forget_password()` the app can call once it decides no new devices will join -- after
  that, only already-cached salts keep working until the password is provided again. (Own
  instance-lifetime commit, separate from everything else.)
