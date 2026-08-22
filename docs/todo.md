* change crate names
* switch pgp to https://crates.io/crates/sequoia-openpgp
* harden the Storage trait contract for load_ops -- document and test that it must return ops
  contiguously/strictly ascending per actor (currently only true by convention in crdt-enc-tokio)
* queue & flush ops?
* crdt validation
* filter filenames in storage-tokio
* ensure concurrently deleted files (between storage::list and storage::load) do not crash core
* zero-on-drop for the actual key material in crdt-enc-envelope::keys::Key/VersionBytes -- done for
  PasswordKeySlot's password/derived keys (Zeroizing), not yet for the content-key bytes held in
  the Keys CRDT itself
  * https://docs.rs/sequoia-openpgp/1.1.0/sequoia_openpgp/crypto/mem/struct.Protected.html
  * https://docs.rs/sequoia-openpgp/1.1.0/sequoia_openpgp/crypto/mem/struct.Encrypted.html
* https://github.com/BurntSushi/quickcheck
* ist agnostik noch aktuell/wird es noch geupdatet? Alternativen?
* wird async_trait noch benötigt? sollte doch jetzt auch nativ gehen in rust
* PasswordKeySlot caches its own (salt, derived kek) pair for its whole lifetime
  (crdt-enc-password/src/lib.rs, `own` field) and reuses it for every wrap_key call, including
  rotation-triggered ones (EnvelopeProtector::rotate_key now exists). That's fine cryptographically
  as long as the password itself hasn't changed (same key derivable from it either way), but decide
  explicitly whether rotation should force a fresh salt/kek anyway (e.g. as defense-in-depth if a
  past kek leaked without the password itself leaking) or intentionally keep reusing the cached one
* PasswordKeySlot must keep the cleartext password in memory for its entire lifetime (only
  Zeroizing-protected on drop), because unwrap_key must be able to handle a not-yet-seen salt at
  any time (e.g. a new device joining sync later with its own independently-bootstrapped entry) --
  there's no point at which it's safe to assume no more unknown salts will show up. Two ways to
  reduce this exposure, neither implemented yet:
  1. an opt-in `forget_password()` the app can call once it decides no new devices will join --
     after that, only already-cached salts keep working until the password is provided again
  2. redesign to one canonical synced salt per repo (analogous to how the content key itself
     converges via `Keys::latest_key()`), so the password is only needed briefly at startup and
     can be dropped immediately after deriving the single resulting kek
