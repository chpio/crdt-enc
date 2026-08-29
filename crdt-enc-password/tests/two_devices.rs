use ::anyhow::Result;
use ::crdt_enc::OpenOptions;
use ::crdt_enc_envelope::{EnvelopeProtector, utils::AtRest};
use ::crdt_enc_password::PasswordKeySlot;
use ::crdt_enc_tokio::Storage;
use ::crdts::MVReg;
use ::uuid::Uuid;

const CURRENT_DATA_VERSION: Uuid = Uuid::from_u128(0x_2b6f9e4a_1d3c_4a7f_9e0b_5c8a6f2d3e91);
const SUPPORTED_DATA_VERSIONS: &[Uuid] = &[CURRENT_DATA_VERSION];

// tiny Argon2 params so tests run fast; production code should use `PasswordKeySlot::new`
fn fast_key_slot(password: &str) -> PasswordKeySlot {
    PasswordKeySlot::with_params(AtRest::encrypt(password), 8, 1, 1)
}

/// Two independent `Core`s sharing a password via `PasswordKeySlot`: verifies device B can
/// actually decrypt the content-encryption key device A bootstrapped, and therefore read A's
/// state, end to end through real Argon2id + XChaCha20Poly1305 (not just a no-op stub).
#[tokio::test]
async fn two_devices_converge_with_shared_password() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let remote_path = tmp.path().join("remote");

    let storage_a = Storage::new(tmp.path().join("device-a"), remote_path.clone())?;
    let protector_a = EnvelopeProtector::new(fast_key_slot("shared-password"));
    let core_a = crdt_enc::Core::open(OpenOptions {
        storage: storage_a,
        protector: protector_a,
        create: true,
        supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
        current_data_version: CURRENT_DATA_VERSION,
    })
    .await?;
    let info_a = core_a.info();

    core_a
        .read_and_apply(|s: &MVReg<u64, Uuid>| {
            let read_ctx = s.read();
            let op = s.write(7, read_ctx.derive_add_ctx(info_a.actor()));
            Ok(vec![op])
        })
        .await?;

    let storage_b = Storage::new(tmp.path().join("device-b"), remote_path)?;
    let protector_b = EnvelopeProtector::new(fast_key_slot("shared-password"));
    let core_b = crdt_enc::Core::open(OpenOptions {
        storage: storage_b,
        protector: protector_b,
        create: true,
        supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
        current_data_version: CURRENT_DATA_VERSION,
    })
    .await?;

    core_b.read_remote().await?;

    let values = core_b.with_state(|s: &MVReg<u64, Uuid>| Ok(s.read().val))?;

    assert_eq!(values, vec![7]);

    Ok(())
}

/// A device opening with the wrong password must fail loudly (via `Core::open`'s
/// `read_remote_meta_` propagating the decrypt error) rather than silently proceeding with no
/// content key or corrupting the shared remote state.
#[tokio::test]
async fn wrong_password_fails_to_open() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let remote_path = tmp.path().join("remote");

    let storage_a = Storage::new(tmp.path().join("device-a"), remote_path.clone())?;
    let protector_a = EnvelopeProtector::new(fast_key_slot("correct-password"));
    crdt_enc::Core::<MVReg<u64, Uuid>, _, _>::open(OpenOptions {
        storage: storage_a,
        protector: protector_a,
        create: true,
        supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
        current_data_version: CURRENT_DATA_VERSION,
    })
    .await?;

    let storage_b = Storage::new(tmp.path().join("device-b"), remote_path)?;
    let protector_b = EnvelopeProtector::new(fast_key_slot("wrong-password"));
    let result = crdt_enc::Core::<MVReg<u64, Uuid>, _, _>::open(OpenOptions {
        storage: storage_b,
        protector: protector_b,
        create: true,
        supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
        current_data_version: CURRENT_DATA_VERSION,
    })
    .await;

    assert!(result.is_err());

    Ok(())
}
