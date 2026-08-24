//! A [`crdt_enc_envelope::KeySlotProtector`] intended to protect the content-encryption key for
//! GPG/OpenPGP recipients (via the `gpgme` crate). **Not implemented yet**: `wrap_key`/`unwrap_key`
//! are still stubs that pass the key through unprotected -- see [`KeyHandler`].
#![warn(missing_docs)]

use ::anyhow::Result;
use ::crdt_enc_envelope::KeySlotProtector;
use ::zeroize::Zeroizing;

/// Initializes the underlying `gpgme` library. Must be called once before using [`KeyHandler`].
pub fn init() {
    gpgme::init();
}

/// A [`KeySlotProtector`] intended to wrap/unwrap the content-encryption key for one or more GPG
/// recipients. Currently a stub: `wrap_key`/`unwrap_key` pass the key through unprotected, so this
/// provides **no actual encryption** yet -- see the TODOs on those methods.
#[derive(Debug)]
pub struct KeyHandler;

impl KeyHandler {
    /// Creates a new `KeyHandler`.
    pub fn new() -> KeyHandler {
        KeyHandler
    }
}

impl Default for KeyHandler {
    /// Same as `new`.
    fn default() -> Self {
        Self::new()
    }
}

impl KeySlotProtector for KeyHandler {
    /// **Stub, not yet implemented**: currently returns `key` unmodified instead of encrypting it
    /// for any GPG recipients.
    async fn wrap_key(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // TODO: encrypt for GPG recipients
        Ok(key)
    }

    /// **Stub, not yet implemented**: currently returns `wrapped` unmodified instead of decrypting
    /// it via GPG.
    async fn unwrap_key(&self, wrapped: Vec<u8>) -> Result<Zeroizing<Vec<u8>>> {
        // TODO: decrypt via GPG
        Ok(Zeroizing::new(wrapped))
    }
}
