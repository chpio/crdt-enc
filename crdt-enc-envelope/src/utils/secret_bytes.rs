use ::std::fmt::{self, Debug};
use ::zeroize::Zeroizing;

/// The plaintext returned by [`AtRest::decrypt`](crate::utils::AtRest::decrypt): zeroized once
/// dropped, and deliberately redacted by `Debug` so accidentally logging/printing it (e.g. via
/// `dbg!` or an error message) doesn't leak the secret -- unlike a bare `Zeroizing<Vec<u8>>`, whose
/// `Debug` impl just forwards to the inner `Vec<u8>` and would print the raw bytes. See
/// `expose_secret` for actual use.
pub struct SecretBytes(Zeroizing<Vec<u8>>);

impl Debug for SecretBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SecretBytes([REDACTED])")
    }
}

impl SecretBytes {
    /// Takes ownership of bytes that already exist in the clear (freshly generated key material, a
    /// just-decrypted buffer, ...), so they get zeroized and `Debug`-redacted from here on.
    pub fn new(bytes: Vec<u8>) -> SecretBytes {
        SecretBytes(Zeroizing::new(bytes))
    }

    /// Hands out the raw secret in the clear -- named to say exactly that at every call site.
    /// Deliberately a named method rather than a `Deref` impl: a `Deref` would let this secret flow
    /// anywhere a `&[u8]` is expected via silent autoderef coercion, with no trace at the call site
    /// that something sensitive is being touched. Spelling out `expose_secret()` keeps every actual
    /// use of the secret grep-able and visible in review.
    pub fn expose_secret(&self) -> &[u8] {
        &self.0
    }
}
