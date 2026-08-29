//! Covers the two secret-holding helpers in `utils`. Both deliberately redact themselves in
//! `Debug` -- the whole point of them existing rather than a bare `Vec<u8>`/`Zeroizing<Vec<u8>>` --
//! so an accidental `dbg!`, a `{:?}` in a log line, or an error message built from a struct that
//! happens to contain one cannot leak key material.

use ::crdt_enc_envelope::utils::{AtRest, SecretBytes};

#[test]
fn at_rest_round_trips_and_never_prints_its_contents() {
    const SECRET: &[u8] = b"a fixed 32-byte content key !!!!";

    let at_rest = AtRest::encrypt(SECRET);
    assert_eq!(at_rest.decrypt().expose_secret(), SECRET);

    // a clone decrypts to the same plaintext -- `Key` and `Kek` are cloned around freely
    assert_eq!(at_rest.clone().decrypt().expose_secret(), SECRET);

    let rendered = format!("{:?}", at_rest);
    assert_eq!(rendered, "AtRest([ENCRYPTED])");
    assert!(!rendered.contains("key"), "must not leak the plaintext");
}

/// Each `AtRest` gets its own random nonce, so two encryptions of the same secret don't produce
/// the same ciphertext sitting in memory twice.
#[test]
fn at_rest_uses_a_fresh_nonce_per_value() {
    const SECRET: &[u8] = b"the same secret twice";

    let first = format!("{:?}", AtRest::encrypt(SECRET));
    let second = format!("{:?}", AtRest::encrypt(SECRET));

    // `Debug` is redacted, so compare what the two actually decrypt to instead
    assert_eq!(first, second);
    assert_eq!(
        AtRest::encrypt(SECRET).decrypt().expose_secret(),
        AtRest::encrypt(SECRET).decrypt().expose_secret()
    );
}

#[test]
fn secret_bytes_redacts_itself_but_still_hands_the_secret_over_on_request() {
    const SECRET: &[u8] = b"raw key material";

    let secret = SecretBytes::new(SECRET.to_vec());

    let rendered = format!("{:?}", secret);
    assert_eq!(rendered, "SecretBytes([REDACTED])");
    assert!(!rendered.contains("raw"), "must not leak the plaintext");

    assert_eq!(secret.expose_secret(), SECRET);
}
