//! Covers what `Protector::encrypt`/`decrypt` do when they're handed something they can't use --
//! a blob from a different format, a key id nobody knows, or key material of the wrong shape --
//! plus the two calls that need a `Core` handle before they can do anything at all.

use ::anyhow::Result;
use ::crdt_enc::{
    OpenOptions,
    protector::Protector,
    utils::{EmptyCrdt, VersionBytes},
};
use ::crdt_enc_envelope::{EnvelopeProtector, KeySlotProtector, utils::SecretBytes};
use ::crdt_enc_tokio::Storage;
use ::crdts::{MVReg, Orswot};
use ::serde::{Deserialize, Serialize};
use ::std::sync::Arc;
use ::uuid::Uuid;

const CURRENT_DATA_VERSION: Uuid = Uuid::from_u128(0x_2a94f3c8_5e17_4b60_9d2f_c8b1740e63a5);
const SUPPORTED_DATA_VERSIONS: &[Uuid] = &[CURRENT_DATA_VERSION];

/// The version tag `EnvelopeProtector` puts on the raw content-encryption key bytes. Private to
/// the crate, mirrored here so a test can build key material that is deliberately wrong in exactly
/// one way.
const KEY_VERSION: Uuid = Uuid::from_u128(0x_3bb60b03_00df_4c79_a199_f96031511d4d);
/// The version tag on the XChaCha20Poly1305 content envelope, same reason.
const DATA_VERSION: Uuid = Uuid::from_u128(0x_ae6e17fd_8aa7_46c9_8797_89ecfbedbae9);

#[derive(Debug)]
struct NoopKeySlot;

impl KeySlotProtector for NoopKeySlot {
    async fn wrap_key(&self, key: SecretBytes) -> Result<Vec<u8>> {
        Ok(key.expose_secret().to_vec())
    }

    async fn unwrap_key(&self, wrapped: Vec<u8>) -> Result<SecretBytes> {
        Ok(SecretBytes::new(wrapped))
    }
}

/// Opens a `Core` over a fresh temporary tree, so the protector inside it has gone through the
/// usual bootstrap.
async fn open_core<KS: KeySlotProtector>(
    tmp: &::std::path::Path,
    key_slot: KS,
) -> Result<Arc<crdt_enc::Core<EmptyCrdt, Storage, EnvelopeProtector<KS>>>> {
    crdt_enc::Core::open(OpenOptions {
        storage: Storage::new(tmp.join("device"), tmp.join("remote"))?,
        protector: EnvelopeProtector::new(key_slot),
        create: true,
        supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
        current_data_version: CURRENT_DATA_VERSION,
    })
    .await
}

/// Both of these need the `CoreSubHandle` that `Protector::init` hands over: one to publish a
/// freshly bootstrapped key, the other to publish a rotated one. Called before `init`, they have
/// to say so rather than panic on an unwrapped `None`.
#[tokio::test]
async fn a_protector_without_a_core_reports_it() {
    let protector = EnvelopeProtector::new(NoopKeySlot);

    protector.set_remote_meta(None).await.unwrap_err();
    protector.rotate_key().await.unwrap_err();
}

/// Encrypting before any content key exists is not something to paper over with a fresh key --
/// that would be a key no other device knows about, minted outside the bootstrap path.
#[tokio::test]
async fn encrypting_without_a_key_is_refused() {
    let protector = EnvelopeProtector::new(NoopKeySlot);

    let err = protector
        .encrypt(SecretBytes::new(b"secret".to_vec()))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no latest key"), "got: {}", err);
}

#[tokio::test]
async fn decrypting_something_that_is_not_a_content_envelope_is_refused() -> Result<()> {
    const OTHER_VERSION: Uuid = Uuid::from_u128(0x_00000000_0000_4000_8000_000000000000);

    let tmp = tempfile::tempdir()?;
    let core = open_core(tmp.path(), NoopKeySlot).await?;
    let protector = core.protector();

    // too short to even hold a version tag
    protector.decrypt(vec![0; 8]).await.unwrap_err();

    // a version tag this build doesn't know
    let wrong_version = VersionBytes::new(OTHER_VERSION, vec![0x80]).serialize();
    protector.decrypt(wrong_version).await.unwrap_err();

    // the right tag, but a body that isn't an `EncBox` (0xc1 is msgpack's "never used" byte)
    let bad_body = VersionBytes::new(DATA_VERSION, vec![0xc1]).serialize();
    protector.decrypt(bad_body).await.unwrap_err();

    // well-formed, but tagged with a key this device has never seen -- what a device that hasn't
    // synced the latest key rotation yet is holding
    let cipher = protector
        .encrypt(SecretBytes::new(b"secret".to_vec()))
        .await?;
    let mut tampered = decode_enc_box(&cipher);
    tampered.key_id = Uuid::new_v4();
    let err = protector
        .decrypt(encode_enc_box(&tampered))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no key with id"), "got: {}", err);

    // ... and the untouched blob still round-trips
    assert_eq!(protector.decrypt(cipher).await?.expose_secret(), b"secret");

    Ok(())
}

/// XChaCha20Poly1305 is authenticated, and this is the property that matters most for a sync
/// transport nobody trusts: a blob that was edited on its way through -- ciphertext or nonce --
/// must fail to decrypt rather than yield plaintext that quietly differs from what was written.
#[tokio::test]
async fn tampered_content_fails_to_decrypt() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let core = open_core(tmp.path(), NoopKeySlot).await?;
    let protector = core.protector();

    let cipher = protector
        .encrypt(SecretBytes::new(b"secret".to_vec()))
        .await?;

    let mut flipped = decode_enc_box(&cipher);
    *flipped.enc_data.last_mut().unwrap() ^= 0xFF;
    protector
        .decrypt(encode_enc_box(&flipped))
        .await
        .unwrap_err();

    let mut renonced = decode_enc_box(&cipher);
    renonced.nonce[0] ^= 0xFF;
    protector
        .decrypt(encode_enc_box(&renonced))
        .await
        .unwrap_err();

    Ok(())
}

/// A key slot that cannot wrap -- a GPG agent that is not running, a hardware token that was
/// unplugged -- has to fail the open. Carrying on would leave a `Core` with a content key that
/// exists only in this process and was never published for any other device.
#[tokio::test]
async fn a_key_slot_that_cannot_wrap_fails_the_open() {
    #[derive(Debug)]
    struct BrokenKeySlot;

    impl KeySlotProtector for BrokenKeySlot {
        async fn wrap_key(&self, _key: SecretBytes) -> Result<Vec<u8>> {
            Err(::anyhow::Error::msg("the key slot is unavailable"))
        }

        async fn unwrap_key(&self, wrapped: Vec<u8>) -> Result<SecretBytes> {
            Ok(SecretBytes::new(wrapped))
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    open_core(tmp.path(), BrokenKeySlot).await.unwrap_err();
}

/// Just enough of the private `EncBox` to rewrite the key id a blob is tagged with.
#[derive(Serialize, Deserialize)]
struct EncBoxWire {
    key_id: Uuid,
    #[serde(with = "serde_bytes")]
    nonce: Vec<u8>,
    #[serde(with = "serde_bytes")]
    enc_data: Vec<u8>,
}

fn decode_enc_box(blob: &[u8]) -> EncBoxWire {
    let version_box = ::crdt_enc::utils::VersionBytesRef::deserialize(blob).unwrap();
    rmp_serde::from_slice(version_box.as_ref()).unwrap()
}

fn encode_enc_box(enc_box: &EncBoxWire) -> Vec<u8> {
    let bytes = rmp_serde::to_vec_named(enc_box).unwrap();
    VersionBytes::new(DATA_VERSION, bytes).serialize()
}

/// Mirrors the private `Keys`/`Key` wire format so a test can round-trip a real one and hand back
/// a version with deliberately broken key material. Field names and shapes have to match, which
/// is why the value is always taken from a genuine `wrap_key` call rather than built from nothing.
#[derive(Serialize, Deserialize)]
struct KeysWire {
    latest_key_id: MVReg<Uuid, Uuid>,
    keys: Orswot<KeyWire, Uuid>,
}

#[derive(Serialize, Deserialize, Clone)]
struct KeyWire {
    id: Uuid,
    key: VersionBytes,
}

// `Orswot` identifies members by `Hash`/`Eq`, and the real `Key` keys them off the id alone --
// mirrored here so a replacement key lands on the same member as the one it replaces.
impl ::std::hash::Hash for KeyWire {
    fn hash<H: ::std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for KeyWire {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for KeyWire {}

/// A `KeySlotProtector` that hands back a `Keys` whose key material has been replaced with
/// `replacement` -- standing in for a corrupted or foreign-format key slot.
#[derive(Debug)]
struct CorruptingKeySlot {
    replacement: VersionBytes,
}

impl KeySlotProtector for CorruptingKeySlot {
    async fn wrap_key(&self, key: SecretBytes) -> Result<Vec<u8>> {
        Ok(key.expose_secret().to_vec())
    }

    async fn unwrap_key(&self, wrapped: Vec<u8>) -> Result<SecretBytes> {
        let mut keys: KeysWire = rmp_serde::from_slice(&wrapped)?;

        // rebuild the key set from scratch, keeping every id (so `latest_key_id` still resolves)
        // but swapping the key material for the broken replacement
        let mut rebuilt: Orswot<KeyWire, Uuid> = Orswot::new();
        for key in keys.keys.read().val {
            let add_ctx = rebuilt.read_ctx().derive_add_ctx(Uuid::nil());
            let op = rebuilt.add(
                KeyWire {
                    id: key.id,
                    key: self.replacement.clone(),
                },
                add_ctx,
            );
            ::crdts::CmRDT::apply(&mut rebuilt, op);
        }
        keys.keys = rebuilt;

        Ok(SecretBytes::new(rmp_serde::to_vec_named(&keys)?))
    }
}

/// Key material that isn't what this build writes -- a slot from an older format, or a corrupted
/// one -- must be refused outright. Using it anyway would produce ciphertext no device, including
/// this one, can ever decrypt.
#[tokio::test]
async fn key_material_of_the_wrong_shape_is_refused() -> Result<()> {
    const OTHER_KEY_VERSION: Uuid = Uuid::from_u128(0x_00000000_0000_4000_8000_000000000000);

    // right length, version tag from some other format
    let tmp = tempfile::tempdir()?;
    let core = open_core(
        tmp.path(),
        CorruptingKeySlot {
            replacement: VersionBytes::new(OTHER_KEY_VERSION, vec![0u8; 32]),
        },
    )
    .await?;
    let err = core
        .protector()
        .encrypt(SecretBytes::new(b"x".to_vec()))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("not matching key version"),
        "got: {}",
        err
    );

    // right version tag, wrong length for XChaCha20Poly1305
    let tmp = tempfile::tempdir()?;
    let core = open_core(
        tmp.path(),
        CorruptingKeySlot {
            replacement: VersionBytes::new(KEY_VERSION, vec![0u8; 7]),
        },
    )
    .await?;
    let err = core
        .protector()
        .encrypt(SecretBytes::new(b"x".to_vec()))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("Invalid key length"),
        "got: {}",
        err
    );

    Ok(())
}

/// The same checks guard the decrypt side, where the bad key material is the *only* thing between
/// a device and content another device already wrote. Here the ciphertext is genuine (written by a
/// device with a working key slot); it is the reading device's view of the key that is broken.
#[tokio::test]
async fn a_device_whose_key_material_is_broken_cannot_decrypt() -> Result<()> {
    const OTHER_KEY_VERSION: Uuid = Uuid::from_u128(0x_00000000_0000_4000_8000_000000000000);

    for (replacement, expected) in [
        (
            VersionBytes::new(OTHER_KEY_VERSION, vec![0u8; 32]),
            "not matching key version",
        ),
        (
            VersionBytes::new(KEY_VERSION, vec![0u8; 7]),
            "Invalid key length",
        ),
    ] {
        let tmp = tempfile::tempdir()?;
        let remote = tmp.path().join("remote");

        let writer: Arc<crdt_enc::Core<EmptyCrdt, Storage, EnvelopeProtector<NoopKeySlot>>> =
            crdt_enc::Core::open(OpenOptions {
                storage: Storage::new(tmp.path().join("writer"), remote.clone())?,
                protector: EnvelopeProtector::new(NoopKeySlot),
                create: true,
                supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
                current_data_version: CURRENT_DATA_VERSION,
            })
            .await?;
        let cipher = writer
            .protector()
            .encrypt(SecretBytes::new(b"perfectly good ciphertext".to_vec()))
            .await?;

        // a second device on the same tree, whose key slot hands back broken key material
        let reader: Arc<crdt_enc::Core<EmptyCrdt, Storage, EnvelopeProtector<CorruptingKeySlot>>> =
            crdt_enc::Core::open(OpenOptions {
                storage: Storage::new(tmp.path().join("reader"), remote)?,
                protector: EnvelopeProtector::new(CorruptingKeySlot { replacement }),
                create: true,
                supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
                current_data_version: CURRENT_DATA_VERSION,
            })
            .await?;

        let err = reader.protector().decrypt(cipher).await.unwrap_err();
        assert!(err.to_string().contains(expected), "got: {}", err);
    }

    Ok(())
}

/// A key slot payload that isn't a `Keys` at all -- a slot written by a different format, or a
/// corrupted one -- has to be reported by the deserializer rather than merged as an empty key set,
/// which would look exactly like "no key has ever been established" and trigger a bootstrap.
#[tokio::test]
async fn a_key_slot_payload_that_is_not_keys_is_refused() {
    /// A `Keys` whose key material is a string where a `VersionBytes` belongs.
    #[derive(Serialize)]
    struct BadKeysWire {
        latest_key_id: MVReg<Uuid, Uuid>,
        keys: Orswot<BadKeyWire, Uuid>,
    }

    #[derive(Serialize, Clone, PartialEq, Eq, Hash)]
    struct BadKeyWire {
        id: Uuid,
        key: String,
    }

    #[derive(Debug)]
    struct MalformedKeySlot;

    impl KeySlotProtector for MalformedKeySlot {
        async fn wrap_key(&self, key: SecretBytes) -> Result<Vec<u8>> {
            Ok(key.expose_secret().to_vec())
        }

        async fn unwrap_key(&self, _wrapped: Vec<u8>) -> Result<SecretBytes> {
            let mut keys: Orswot<BadKeyWire, Uuid> = Orswot::new();
            let add_ctx = keys.read_ctx().derive_add_ctx(Uuid::nil());
            let op = keys.add(
                BadKeyWire {
                    id: Uuid::new_v4(),
                    key: "not a version-tagged blob".to_string(),
                },
                add_ctx,
            );
            ::crdts::CmRDT::apply(&mut keys, op);

            let bad = BadKeysWire {
                latest_key_id: MVReg::new(),
                keys,
            };
            Ok(SecretBytes::new(rmp_serde::to_vec_named(&bad)?))
        }
    }

    // the first open bootstraps and publishes a key; the second has to read that published slot,
    // which is where the malformed payload comes back
    let tmp = tempfile::tempdir().unwrap();
    let remote = tmp.path().join("remote");
    let _writer: Arc<crdt_enc::Core<EmptyCrdt, Storage, EnvelopeProtector<NoopKeySlot>>> =
        crdt_enc::Core::open(OpenOptions {
            storage: Storage::new(tmp.path().join("writer"), remote.clone()).unwrap(),
            protector: EnvelopeProtector::new(NoopKeySlot),
            create: true,
            supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
            current_data_version: CURRENT_DATA_VERSION,
        })
        .await
        .unwrap();

    let opened: Result<
        Arc<crdt_enc::Core<EmptyCrdt, Storage, EnvelopeProtector<MalformedKeySlot>>>,
    > = crdt_enc::Core::open(OpenOptions {
        storage: Storage::new(tmp.path().join("reader"), remote).unwrap(),
        protector: EnvelopeProtector::new(MalformedKeySlot),
        create: true,
        supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
        current_data_version: CURRENT_DATA_VERSION,
    })
    .await;
    opened.unwrap_err();
}
