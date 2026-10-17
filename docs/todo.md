Each list is ordered by priority, most important first.

## Bugs

* crdt-enc: race in `Core::store_remote_meta`. It serializes `remote_meta`, `.await`s the
  write, then drains and deletes *every* name in `read_remote_metas` -- including names merged in by
  a concurrent `read_remote_meta_` during that `.await`, whose content is not in the blob just
  written. Two concurrent calls (e.g. `rotate_key` racing `read_remote_meta`) can likewise delete
  each other's newer blob, keeping the older, smaller one. The protector's wrapped keys live there,
  so this can lose key material on disk. Fix: an async lock around merge + serialize + store +
  delete, and only delete the names known at snapshot time.
* crdt-enc-password: Argon2 params (`m_cost`/`t_cost`/`p_cost`) are taken from the envelope
  unchecked, before anything is authenticated -- anyone who can write to the sync folder can plant
  a meta file with `m_cost = u32::MAX` KiB (~4 TiB) and hang/OOM every device on `open`. Clamp to
  sane upper bounds in `unwrap_key`.
* crdt-enc-tokio: writes are not crash-safe. `write_file_inner` writes straight to the final
  name, so a crash mid-write leaves a truncated file that then gets synced to every device: a
  truncated op makes `store_ops` fail with `AlreadyExists` at that version forever (this device can
  never write ops again), a truncated content-addressed file makes the next identical write
  silently "succeed". Fix: write to a temp file, fsync, then hard-link/rename into place (+ fsync
  the directory).
* filter filenames in storage-tokio -- concretely: Syncthing downloads into `.syncthing.<name>.tmp`
  (`~syncthing~<name>.tmp` on Windows) in the same directory, so `list_state_names`/
  `list_remote_meta_names` pick up half-downloaded files and `read_remote` fails for as long as a
  download is in flight. Same filter needed for our own temp files once writes go through one.
* ensure concurrently deleted files (between storage::list and storage::load) do not crash core
* `Core::open` should `ensure!` that `current_data_version` is in `supported_data_versions` --
  otherwise `Core` writes data it can't read back itself. Also `OpenOptions` docs ask for a sorted
  list, but `open` sorts it anyway.
* `Keys::latest_key` panics if `latest_key_id` names an unknown key; return a `Result` instead. Only
  reachable with a valid KEK, so low priority.

## Security hardening

* bind ciphertexts to their slot: `EnvelopeProtector::encrypt` uses no associated data, so anyone
  with write access to the sync folder can copy op files between actors/versions or replay old ones
  and they still decrypt fine. Give `Protector::encrypt`/`decrypt` a context parameter (e.g.
  `op/<actor>/<version>`, `state`) and pass it as AAD. Format change -> new version UUID.
* PasswordKeySlot must keep the password resident for its entire lifetime (encrypted at rest via
  `AtRest` since the crdt-enc-envelope::at_rest work, but still resident), because unwrap_key must
  be able to handle a not-yet-seen salt at any time (e.g. a new device joining sync later with its
  own independently-bootstrapped entry) -- there's no point at which it's safe to assume no more
  unknown salts will show up. Salt convergence (own_salt_kek preferring an already-known salt over
  minting a fresh one) makes this the rare case rather than the norm, but doesn't eliminate it. Add
  an opt-in `forget_password()` the app can call once it decides no new devices will join -- after
  that, only already-cached salts keep working until the password is provided again. (Own
  instance-lifetime commit, separate from everything else.)
* the CRDT state itself is the largest remaining plaintext exposure in memory, and `SecretBytes` at
  the `Protector` boundary does not touch it: `CoreMutData::state` holds `S` in the clear for the
  whole process lifetime, and `S` is the caller's type, so `Core` cannot zeroize it. The
  serialization buffers around it are now `SecretBytes`, but that only shortens the window, it does
  not close it. Two smaller gaps in the same area: `rmp_serde::to_vec_named` grows its `Vec`, so any
  buffer it reallocates away is freed uncleared before the final one gets wrapped, and
  `rmp_serde::from_slice` copies the decrypted plaintext straight into `S`/`Vec<S::Op>` on the way
  back in. Fixing either properly needs a serializer that writes into a pre-sized zeroizing buffer.

## Features

* `Core::read_remote` has a `TODO: notify app of state changes`; at least return `bool` so the app
  knows whether anything changed.
* implement a GPG/OpenPGP KeySlotProtector using https://crates.io/crates/sequoia-openpgp (the
  earlier gpgme-based stub crate, crdt-enc-gpgme, was removed since it never had real
  wrap_key/unwrap_key logic)
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
* crdt validation
* queue & flush ops?

## Tests & CI

* harden the Storage trait contract for load_ops -- document and test that it must return ops
  contiguously/strictly ascending per actor (currently only true by convention in crdt-enc-tokio)
* CI: add `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` to guard the intra-doc links
  (passes today).
* https://github.com/BurntSushi/quickcheck

## Cleanup

* remove the commented-out `TODO: add key id` / `Block` in `Core::read_and_apply` -- obsolete, the
  envelope tags each ciphertext with its key id.
* `decode_version_bytes_mvreg_custom` and `decode_version_bytes_mvreg_custom_phf` are near-identical;
  unify via a small trait over the supported-versions set (`&[Uuid]` / `&phf::Set<u128>`).
* typo `actor_last_verions` in `Storage::remove_ops`.
* `examples/test` is still on edition 2021, everything else on 2024.
* change crate names
