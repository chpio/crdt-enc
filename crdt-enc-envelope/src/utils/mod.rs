/// [`AtRest`]: a reusable, generic "encrypt this secret while it sits idle in memory" primitive.
mod at_rest;
/// [`SecretBytes`]: plaintext secret bytes that zeroize on drop and redact themselves in `Debug`,
/// for the brief windows where a secret has to be readable.
mod secret_bytes;

pub use at_rest::*;
pub use secret_bytes::*;
