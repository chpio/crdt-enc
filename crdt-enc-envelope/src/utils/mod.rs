/// [`AtRest`]: a reusable, generic "encrypt this secret while it sits idle in memory" primitive.
mod at_rest;

/// Re-exported because this crate's own public API names it ([`crate::KeySlotProtector`],
/// [`AtRest::decrypt`]), while it lives in `crdt-enc` so [`crdt_enc::protector::Protector`] can
/// name it too. That keeps a crate implementing only [`crate::KeySlotProtector`] free of a
/// `crdt-enc` dependency it would otherwise need for this one type, and -- once these are published
/// separately -- stops such a crate from pairing a [`SecretBytes`] from one `crdt-enc` version with
/// a trait expecting another.
pub use ::crdt_enc::utils::SecretBytes;
pub use at_rest::*;
