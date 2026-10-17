use ::crdt_enc_envelope::{
    KeySlotProtector,
    utils::{AtRest, SecretBytes},
};
use ::crdt_enc_password::PasswordKeySlot;

// tiny Argon2 params so tests run fast; production code should use `PasswordKeySlot::new`
fn fast_key_slot(password: &str) -> PasswordKeySlot {
    PasswordKeySlot::with_params(AtRest::encrypt(password), 8, 1, 1)
}

/// Known-answer test against a stored envelope, rather than a round trip within one run: every
/// other test here wraps and unwraps with the same code, so all of them would still pass if the
/// derivation or the encoding changed -- they'd just agree with each other on something new, while
/// silently locking every user out of the keys they already have on disk.
///
/// This one fails instead. It pins the whole chain end to end: the `ENVELOPE_VERSION` tag, the
/// MessagePack field layout, the Argon2id derivation (salt and parameters are read back out of the
/// envelope, so the KEK must come out bit-identical or the AEAD tag won't verify), and
/// XChaCha20Poly1305 decryption. It caught nothing when argon2 was bumped 0.5 -> 0.6, which is the
/// point -- that bump was verified by hand, and this test is that check made permanent.
#[tokio::test]
async fn unwraps_a_frozen_envelope() {
    // captured once from `fast_key_slot("correct horse battery staple").wrap_key(KEY)`, then frozen
    const ENVELOPE: &[u8] = &[
        0x3d, 0xd6, 0x96, 0x16, 0x48, 0x92, 0x40, 0x88, 0x91, 0x43, 0xc4, 0x00, 0x25, 0xe6, 0xe1,
        0x1e, 0x86, 0xa4, 0x73, 0x61, 0x6c, 0x74, 0xc4, 0x10, 0xd9, 0x74, 0xdc, 0x71, 0x96, 0xb6,
        0x5a, 0xd7, 0x9c, 0xab, 0x91, 0x86, 0x99, 0xa9, 0x82, 0x74, 0xa6, 0x6d, 0x5f, 0x63, 0x6f,
        0x73, 0x74, 0x08, 0xa6, 0x74, 0x5f, 0x63, 0x6f, 0x73, 0x74, 0x01, 0xa6, 0x70, 0x5f, 0x63,
        0x6f, 0x73, 0x74, 0x01, 0xa5, 0x6e, 0x6f, 0x6e, 0x63, 0x65, 0xc4, 0x18, 0x23, 0x90, 0x6f,
        0xe0, 0x33, 0x5c, 0x80, 0x91, 0xf1, 0xc1, 0x08, 0x86, 0x6c, 0x74, 0x91, 0x57, 0xb7, 0xb8,
        0x23, 0xd1, 0xd0, 0xb3, 0xfe, 0xdf, 0xaa, 0x63, 0x69, 0x70, 0x68, 0x65, 0x72, 0x74, 0x65,
        0x78, 0x74, 0xc4, 0x30, 0x44, 0x66, 0xa2, 0x14, 0x4e, 0x96, 0xda, 0xf3, 0xef, 0x39, 0xb8,
        0xe2, 0x5a, 0xd2, 0x24, 0xd7, 0xfb, 0x49, 0xee, 0xa4, 0x9d, 0xa3, 0xb8, 0x90, 0x33, 0x74,
        0x9b, 0x11, 0xa6, 0xb2, 0x6c, 0xa3, 0xd2, 0x49, 0x0a, 0xe2, 0x31, 0x22, 0x65, 0x3e, 0x7a,
        0x51, 0xcc, 0x22, 0x19, 0x96, 0xb7, 0x8c,
    ];
    const KEY: &[u8] = b"a fixed 32-byte content key !!!!";

    let unwrapped = fast_key_slot("correct horse battery staple")
        .unwrap_key(ENVELOPE.to_vec())
        .await
        .unwrap();

    assert_eq!(unwrapped.expose_secret(), KEY);
}

#[tokio::test]
async fn round_trip() {
    let key_slot = fast_key_slot("correct horse battery staple");
    let key = b"some 32 byte content key material";

    let wrapped = key_slot
        .wrap_key(SecretBytes::new(key.to_vec()))
        .await
        .unwrap();
    let unwrapped = key_slot.unwrap_key(wrapped).await.unwrap();

    assert_eq!(unwrapped.expose_secret(), key);
}

#[tokio::test]
async fn wrong_password_fails() {
    let wrapped = fast_key_slot("right password")
        .wrap_key(SecretBytes::new(b"secret key bytes".to_vec()))
        .await
        .unwrap();

    let result = fast_key_slot("wrong password").unwrap_key(wrapped).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn tampered_ciphertext_fails() {
    let key_slot = fast_key_slot("a password");
    let mut wrapped = key_slot
        .wrap_key(SecretBytes::new(b"secret key bytes".to_vec()))
        .await
        .unwrap();

    *wrapped.last_mut().unwrap() ^= 0xFF;

    let result = key_slot.unwrap_key(wrapped).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn same_instance_reuses_salt_but_nonce_still_differs() {
    let key_slot = fast_key_slot("a password");
    let key = b"secret key bytes";

    let wrapped_a = key_slot
        .wrap_key(SecretBytes::new(key.to_vec()))
        .await
        .unwrap();
    let wrapped_b = key_slot
        .wrap_key(SecretBytes::new(key.to_vec()))
        .await
        .unwrap();

    // different ciphertext/nonce each call ...
    assert_ne!(wrapped_a, wrapped_b);
    // ... but both still unwrap correctly, and the salt (hence a cached kek) is reused
    assert_eq!(
        key_slot
            .unwrap_key(wrapped_a)
            .await
            .unwrap()
            .expose_secret(),
        key
    );
    assert_eq!(
        key_slot
            .unwrap_key(wrapped_b)
            .await
            .unwrap()
            .expose_secret(),
        key
    );
}

#[tokio::test]
async fn isolated_instances_that_never_synced_use_different_salts() {
    let key = b"secret key bytes";

    let wrapped_a = fast_key_slot("a password")
        .wrap_key(SecretBytes::new(key.to_vec()))
        .await
        .unwrap();
    let wrapped_b = fast_key_slot("a password")
        .wrap_key(SecretBytes::new(key.to_vec()))
        .await
        .unwrap();

    assert_ne!(wrapped_a, wrapped_b);
}

#[tokio::test]
async fn instance_that_saw_a_wrap_converges_on_its_salt() {
    let key = b"secret key bytes";

    // instance A mints the first-ever salt
    let a = fast_key_slot("shared password");
    let wrapped_by_a = a.wrap_key(SecretBytes::new(key.to_vec())).await.unwrap();

    // instance B "syncs" by unwrapping A's entry before ever wrapping anything itself --
    // mirrors EnvelopeProtector::set_remote_meta decoding existing entries before deciding
    // whether it needs to wrap a new one
    let b = fast_key_slot("shared password");
    assert_eq!(
        b.unwrap_key(wrapped_by_a.clone())
            .await
            .unwrap()
            .expose_secret(),
        key
    );

    // B's own first wrap must now reuse A's salt instead of minting a third one
    let wrapped_by_b = b.wrap_key(SecretBytes::new(key.to_vec())).await.unwrap();
    assert_eq!(
        a.unwrap_key(wrapped_by_b.clone())
            .await
            .unwrap()
            .expose_secret(),
        key
    );

    #[derive(::serde::Deserialize)]
    struct EnvelopeSalt {
        #[serde(with = "serde_bytes")]
        salt: Vec<u8>,
    }
    let version_box_a = ::crdt_enc::utils::VersionBytesRef::deserialize(&wrapped_by_a).unwrap();
    let version_box_b = ::crdt_enc::utils::VersionBytesRef::deserialize(&wrapped_by_b).unwrap();
    let salt_a: EnvelopeSalt = rmp_serde::from_slice(version_box_a.as_ref()).unwrap();
    let salt_b: EnvelopeSalt = rmp_serde::from_slice(version_box_b.as_ref()).unwrap();
    assert_eq!(salt_a.salt, salt_b.salt);
}

/// With several salts known under its own parameters, an instance wraps with the smallest one.
/// That is the tiebreak every device applies, so they all end up on the same salt regardless of
/// the order they saw the others in -- picking, say, the first one seen would let each device
/// stick with a different salt forever.
#[tokio::test]
async fn instance_that_saw_several_salts_converges_on_the_smallest() {
    #[derive(::serde::Deserialize)]
    struct EnvelopeSalt {
        #[serde(with = "serde_bytes")]
        salt: Vec<u8>,
    }

    fn salt_of(wrapped: &[u8]) -> Vec<u8> {
        let version_box = ::crdt_enc::utils::VersionBytesRef::deserialize(wrapped).unwrap();
        let envelope: EnvelopeSalt = rmp_serde::from_slice(version_box.as_ref()).unwrap();
        envelope.salt
    }

    let key = b"secret key bytes";

    // three instances that never synced, each minting its own salt
    let mut seen = Vec::new();
    for _ in 0..3 {
        let isolated = fast_key_slot("shared password");
        seen.push(
            isolated
                .wrap_key(SecretBytes::new(key.to_vec()))
                .await
                .unwrap(),
        );
    }

    let observer = fast_key_slot("shared password");
    for wrapped in &seen {
        observer.unwrap_key(wrapped.clone()).await.unwrap();
    }
    let own = observer
        .wrap_key(SecretBytes::new(key.to_vec()))
        .await
        .unwrap();

    let smallest = seen.iter().map(|wrapped| salt_of(wrapped)).min().unwrap();
    assert_eq!(salt_of(&own), smallest);
}

/// Salt convergence must stay within one set of Argon2 parameters. A device that adopted a salt
/// seen under *other* parameters used to reuse that device's cached key while labelling its own
/// envelope with its own parameters -- an envelope nobody could unwrap without that exact cache
/// entry, not even the same device after a restart.
#[tokio::test]
async fn instance_with_other_params_does_not_adopt_a_foreign_salt() {
    let key = b"secret key bytes";

    let a = PasswordKeySlot::with_params(AtRest::encrypt("shared password"), 8, 1, 1);
    let wrapped_by_a = a.wrap_key(SecretBytes::new(key.to_vec())).await.unwrap();

    // B runs with different params and sees A's entry before wrapping its own
    let b = PasswordKeySlot::with_params(AtRest::encrypt("shared password"), 16, 1, 1);
    b.unwrap_key(wrapped_by_a).await.unwrap();
    let wrapped_by_b = b.wrap_key(SecretBytes::new(key.to_vec())).await.unwrap();

    // a fresh instance has no cache to fall back on, so this only works if B's envelope is
    // self-consistent
    let fresh = fast_key_slot("shared password");
    assert_eq!(
        fresh
            .unwrap_key(wrapped_by_b)
            .await
            .unwrap()
            .expose_secret(),
        key
    );
}

/// `PasswordKeySlot::new` is what applications actually call -- `with_params` only exists so tests
/// (and anyone with a considered reason) can pick cheaper Argon2 settings. This is the one test
/// that pays the real OWASP-minimum derivation cost, to make sure the defaults it hard-codes are
/// still accepted by `Params::new` and produce a working slot.
#[tokio::test]
async fn default_owasp_parameters_produce_a_working_slot() {
    let key_slot = PasswordKeySlot::new(AtRest::encrypt("correct horse battery staple"));
    let key = b"a fixed 32-byte content key !!!!";

    let wrapped = key_slot
        .wrap_key(SecretBytes::new(key.to_vec()))
        .await
        .unwrap();
    let unwrapped = key_slot.unwrap_key(wrapped).await.unwrap();

    assert_eq!(unwrapped.expose_secret(), key);
}

/// Argon2 rejects parameters outside its valid ranges (here: a memory cost below the minimum), and
/// that has to surface as an error rather than a panic deep inside a `spawn_blocking`.
#[tokio::test]
async fn invalid_argon2_parameters_are_reported() {
    let key_slot = PasswordKeySlot::with_params(AtRest::encrypt("a password"), 0, 0, 0);

    key_slot
        .wrap_key(SecretBytes::new(b"secret key bytes".to_vec()))
        .await
        .unwrap_err();
}

/// An envelope's Argon2 parameters come from synced data and are read before anything is
/// authenticated, so they must be capped -- otherwise a planted envelope with an enormous memory
/// cost makes every device hang or run out of memory on open. Each case sits just one step above
/// its limit, and the error has to name the limit: a merely failed decryption would also come
/// back as an error, after paying for the derivation.
#[tokio::test]
async fn argon2_parameters_above_the_limits_are_refused() {
    // a mirror of the private `Envelope`, to rewrite the parameters of a genuine one
    #[derive(::serde::Serialize, ::serde::Deserialize)]
    struct Envelope {
        #[serde(with = "serde_bytes")]
        salt: Vec<u8>,
        m_cost: u32,
        t_cost: u32,
        p_cost: u32,
        #[serde(with = "serde_bytes")]
        nonce: Vec<u8>,
        #[serde(with = "serde_bytes")]
        ciphertext: Vec<u8>,
    }

    let key_slot = fast_key_slot("a password");
    let wrapped = key_slot
        .wrap_key(SecretBytes::new(b"secret key bytes".to_vec()))
        .await
        .unwrap();
    let version_box = ::crdt_enc::utils::VersionBytesRef::deserialize(&wrapped).unwrap();

    let oversized: [fn(&mut Envelope); 3] = [
        |envelope| envelope.m_cost = 1024 * 1024 + 1,
        |envelope| envelope.t_cost = 11,
        |envelope| envelope.p_cost = 17,
    ];
    for oversize in oversized {
        let mut envelope: Envelope = rmp_serde::from_slice(version_box.as_ref()).unwrap();
        oversize(&mut envelope);
        let tampered = ::crdt_enc::utils::VersionBytes::new(
            version_box.version(),
            rmp_serde::to_vec_named(&envelope).unwrap(),
        )
        .serialize();

        // a fresh instance, so nothing is served from the cache
        let err = fast_key_slot("a password")
            .unwrap_key(tampered)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("exceed the supported maximum"),
            "{err}"
        );
    }

    // configuring an instance above the limits fails at wrap time rather than producing envelopes
    // no instance would unwrap
    PasswordKeySlot::with_params(AtRest::encrypt("a password"), 8, 11, 1)
        .wrap_key(SecretBytes::new(b"secret key bytes".to_vec()))
        .await
        .unwrap_err();
}

/// Anything that isn't one of this crate's envelopes has to be refused rather than parsed
/// half-way. All three shapes are what a device would actually meet in the wild: a truncated file,
/// a blob written by a future envelope format, and a corrupted body.
#[tokio::test]
async fn blobs_that_are_not_this_envelope_format_are_refused() {
    // the envelope's own version tag, so a "wrong version" case can be built around it
    const ENVELOPE_VERSION: ::uuid::Uuid =
        ::uuid::Uuid::from_u128(0x_3dd69616_4892_4088_9143_c40025e6e11e);
    const OTHER_VERSION: ::uuid::Uuid =
        ::uuid::Uuid::from_u128(0x_00000000_0000_4000_8000_000000000000);

    let key_slot = fast_key_slot("a password");

    // too short to even hold a version tag
    key_slot.unwrap_key(vec![0; 8]).await.unwrap_err();

    // a version tag this build doesn't know
    let wrong_version = ::crdt_enc::utils::VersionBytes::new(OTHER_VERSION, vec![0x80]).serialize();
    key_slot.unwrap_key(wrong_version).await.unwrap_err();

    // the right tag, but a body that isn't an `Envelope` (0xc1 is msgpack's "never used" byte)
    let bad_body = ::crdt_enc::utils::VersionBytes::new(ENVELOPE_VERSION, vec![0xc1]).serialize();
    key_slot.unwrap_key(bad_body).await.unwrap_err();
}
