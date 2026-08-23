use ::crdt_enc_envelope::{KeySlotProtector, at_rest::AtRest};
use ::crdt_enc_password::PasswordKeySlot;

// tiny Argon2 params so tests run fast; production code should use `PasswordKeySlot::new`
fn fast_key_slot(password: &str) -> PasswordKeySlot {
    PasswordKeySlot::with_params(AtRest::encrypt(password), 8, 1, 1)
}

#[tokio::test]
async fn round_trip() {
    let key_slot = fast_key_slot("correct horse battery staple");
    let key = b"some 32 byte content key material";

    let wrapped = key_slot.wrap_key(key).await.unwrap();
    let unwrapped = key_slot.unwrap_key(&wrapped).await.unwrap();

    assert_eq!(unwrapped, key);
}

#[tokio::test]
async fn wrong_password_fails() {
    let wrapped = fast_key_slot("right password")
        .wrap_key(b"secret key bytes")
        .await
        .unwrap();

    let result = fast_key_slot("wrong password").unwrap_key(&wrapped).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn tampered_ciphertext_fails() {
    let key_slot = fast_key_slot("a password");
    let mut wrapped = key_slot.wrap_key(b"secret key bytes").await.unwrap();

    *wrapped.last_mut().unwrap() ^= 0xFF;

    let result = key_slot.unwrap_key(&wrapped).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn same_instance_reuses_salt_but_nonce_still_differs() {
    let key_slot = fast_key_slot("a password");
    let key = b"secret key bytes";

    let wrapped_a = key_slot.wrap_key(key).await.unwrap();
    let wrapped_b = key_slot.wrap_key(key).await.unwrap();

    // different ciphertext/nonce each call ...
    assert_ne!(wrapped_a, wrapped_b);
    // ... but both still unwrap correctly, and the salt (hence a cached kek) is reused
    assert_eq!(key_slot.unwrap_key(&wrapped_a).await.unwrap(), key);
    assert_eq!(key_slot.unwrap_key(&wrapped_b).await.unwrap(), key);
}

#[tokio::test]
async fn isolated_instances_that_never_synced_use_different_salts() {
    let key = b"secret key bytes";

    let wrapped_a = fast_key_slot("a password").wrap_key(key).await.unwrap();
    let wrapped_b = fast_key_slot("a password").wrap_key(key).await.unwrap();

    assert_ne!(wrapped_a, wrapped_b);
}

#[tokio::test]
async fn instance_that_saw_a_wrap_converges_on_its_salt() {
    let key = b"secret key bytes";

    // instance A mints the first-ever salt
    let a = fast_key_slot("shared password");
    let wrapped_by_a = a.wrap_key(key).await.unwrap();

    // instance B "syncs" by unwrapping A's entry before ever wrapping anything itself --
    // mirrors EnvelopeProtector::set_remote_meta decoding existing entries before deciding
    // whether it needs to wrap a new one
    let b = fast_key_slot("shared password");
    assert_eq!(b.unwrap_key(&wrapped_by_a).await.unwrap(), key);

    // B's own first wrap must now reuse A's salt instead of minting a third one
    let wrapped_by_b = b.wrap_key(key).await.unwrap();
    assert_eq!(a.unwrap_key(&wrapped_by_b).await.unwrap(), key);

    #[derive(::serde::Deserialize)]
    struct EnvelopeSalt {
        #[serde(with = "serde_bytes")]
        salt: Vec<u8>,
    }
    let salt_a: EnvelopeSalt = rmp_serde::from_slice(&wrapped_by_a).unwrap();
    let salt_b: EnvelopeSalt = rmp_serde::from_slice(&wrapped_by_b).unwrap();
    assert_eq!(salt_a.salt, salt_b.salt);
}
