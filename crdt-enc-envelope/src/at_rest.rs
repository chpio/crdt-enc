use ::chacha20poly1305::{Key as AeadKey, KeyInit, XChaCha20Poly1305, XNonce, aead::Aead};
use ::rand::{TryRng, rng};
use ::std::{
    fmt::{self, Debug},
    sync::LazyLock,
};
use ::zeroize::Zeroizing;

/// The byte length of an XChaCha20Poly1305 key.
const AEAD_KEY_LEN: usize = 32;
/// The byte length of an XChaCha20Poly1305 nonce.
const AEAD_NONCE_LEN: usize = 24;

/// Ephemeral, process-local key used only to encrypt secrets while they sit idle in memory
/// ("encrypted at rest") -- see [`AtRest`]. Regenerated fresh on every process start, never
/// persisted or synced.
static REST_KEY: LazyLock<[u8; AEAD_KEY_LEN]> = LazyLock::new(|| {
    let mut key = [0u8; AEAD_KEY_LEN];
    rng()
        .try_fill_bytes(&mut key)
        .expect("Unable to get random data for at-rest key");
    key
});

/// Encrypts an arbitrary byte secret under the process-local [`REST_KEY`] so it doesn't sit in
/// plaintext in memory while idle, on top of whatever protection its own wire format already gets
/// (e.g. a [`KeySlotProtector`](crate::KeySlotProtector)). Decrypted only for the brief moment the
/// real bytes are actually needed, via `decrypt` -- the returned buffer is zeroized once dropped.
///
/// This is a reusable, generic primitive: it doesn't care what the secret bytes mean (a raw
/// content-encryption key, a cleartext password, ...), only that they exist and shouldn't sit
/// around in plaintext. Reduces the number of places raw secret bytes are visible in this
/// process's memory to one well-known location (`REST_KEY` itself), rather than scattered across
/// every clone of the protected value. Same principle as `sequoia-openpgp`'s
/// `crypto::mem::Encrypted`, built on the `chacha20poly1305` dependency this crate already uses
/// for content encryption instead of pulling in a new crate for it.
#[derive(Clone)]
pub struct AtRest {
    /// The random nonce this ciphertext was encrypted with.
    nonce: [u8; AEAD_NONCE_LEN],
    /// The encrypted secret bytes.
    ciphertext: Vec<u8>,
}

impl Debug for AtRest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AtRest([ENCRYPTED])")
    }
}

impl AtRest {
    /// Encrypts `plaintext` under `REST_KEY` with a fresh random nonce.
    pub fn encrypt(plaintext: impl AsRef<[u8]>) -> AtRest {
        let mut nonce = [0u8; AEAD_NONCE_LEN];
        rng()
            .try_fill_bytes(&mut nonce)
            .expect("Unable to get random data for at-rest nonce");

        let aead = XChaCha20Poly1305::new(&AeadKey::from(*REST_KEY));
        let ciphertext = aead
            .encrypt(&XNonce::from(nonce), plaintext.as_ref())
            .expect("encrypting at-rest secret failed");

        AtRest { nonce, ciphertext }
    }

    /// Reverses `encrypt`, returning the original plaintext bytes as [`Exposed`]. Only fails if
    /// `REST_KEY` or the ciphertext were somehow corrupted, which would be a bug in this module,
    /// not a normal runtime error -- panics rather than returning a `Result`.
    pub fn decrypt(&self) -> Exposed {
        let aead = XChaCha20Poly1305::new(&AeadKey::from(*REST_KEY));
        let plaintext = aead
            .decrypt(&XNonce::from(self.nonce), self.ciphertext.as_ref())
            .expect("decrypting at-rest secret failed");

        Exposed(Zeroizing::new(plaintext))
    }
}

/// The plaintext returned by [`AtRest::decrypt`]: zeroized once dropped, and deliberately redacted
/// by `Debug` so accidentally logging/printing it (e.g. via `dbg!` or an error message) doesn't
/// leak the secret -- unlike a bare `Zeroizing<Vec<u8>>`, whose `Debug` impl just forwards to the
/// inner `Vec<u8>` and would print the raw bytes. See `as_bytes` for actual use.
pub struct Exposed(Zeroizing<Vec<u8>>);

impl Debug for Exposed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Exposed([REDACTED])")
    }
}

impl Exposed {
    /// Exposes the plaintext bytes. Deliberately a named method rather than a `Deref` impl: a
    /// `Deref` would let this secret flow anywhere a `&[u8]` is expected via silent autoderef
    /// coercion, with no trace at the call site that something sensitive is being touched. Calling
    /// `as_bytes()` explicitly keeps every actual use of the secret grep-able and visible in review.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
