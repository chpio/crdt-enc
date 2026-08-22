use ::anyhow::Result;
use ::async_trait::async_trait;
use ::crdt_enc::OpenOptions;
use ::crdt_enc_envelope::{EnvelopeProtector, KeySlotProtector};
use ::crdt_enc_tokio::Storage;
use ::crdts::MVReg;
use ::uuid::Uuid;

const CURRENT_DATA_VERSION: Uuid = Uuid::from_u128(0x_7c3e2a58_9b14_4f6d_8a02_1e5f9c3b7d4a);
const SUPPORTED_DATA_VERSIONS: &[Uuid] = &[CURRENT_DATA_VERSION];

/// A `KeySlotProtector` that doesn't protect anything -- good enough to exercise
/// `EnvelopeProtector`'s rotation logic without depending on a real implementation (see
/// `two_devices.rs` for why).
#[derive(Debug)]
struct NoopKeySlot;

#[async_trait]
impl KeySlotProtector for NoopKeySlot {
    async fn wrap_key(&self, key: &[u8]) -> Result<Vec<u8>> {
        Ok(key.to_vec())
    }

    async fn unwrap_key(&self, wrapped: &[u8]) -> Result<Vec<u8>> {
        Ok(wrapped.to_vec())
    }
}

/// Writes op1 with the original (bootstrapped) key, rotates, then writes op2 with the new key. A
/// second, independently-opened `Core` on the same remote directory must be able to decrypt
/// *both* ops via `read_remote()` -- if `decrypt` still always assumed `latest_key()` instead of
/// looking up each blob's tagged `key_id`, op1 (encrypted with the now-superseded key) would fail
/// to decrypt and `read_remote()` would return an `Err`.
#[tokio::test]
async fn rotation_keeps_previously_encrypted_data_readable() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let remote_path = tmp.path().join("remote");

    let storage_a = Storage::new(tmp.path().join("device-a"), remote_path.clone())?;
    let core_a = crdt_enc::Core::open(OpenOptions {
        storage: storage_a,
        protector: EnvelopeProtector::new(NoopKeySlot),
        create: true,
        supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
        current_data_version: CURRENT_DATA_VERSION,
    })
    .await?;
    let actor_a = core_a.info().actor();

    core_a
        .read_and_apply(|s: &MVReg<u64, Uuid>| {
            let read_ctx = s.read();
            let op = s.write(1, read_ctx.derive_add_ctx(actor_a));
            Ok(vec![op])
        })
        .await?;

    core_a.protector().rotate_key().await?;

    core_a
        .read_and_apply(|s: &MVReg<u64, Uuid>| {
            let read_ctx = s.read();
            let op = s.write(2, read_ctx.derive_add_ctx(actor_a));
            Ok(vec![op])
        })
        .await?;

    let storage_b = Storage::new(tmp.path().join("device-b"), remote_path)?;
    let core_b = crdt_enc::Core::open(OpenOptions {
        storage: storage_b,
        protector: EnvelopeProtector::new(NoopKeySlot),
        create: true,
        supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
        current_data_version: CURRENT_DATA_VERSION,
    })
    .await?;

    // would return an `Err` if the op encrypted before rotation couldn't be decrypted anymore
    core_b.read_remote().await?;

    let values = core_b.with_state(|s: &MVReg<u64, Uuid>| Ok(s.read().val))?;
    assert_eq!(values, vec![2]);

    Ok(())
}

/// Two overlapping `rotate_key()` calls on the same `EnvelopeProtector` must not corrupt the
/// `Keys` CRDT (e.g. via colliding CRDT dots for the same actor, which would silently drop one of
/// the two new keys or trigger a panic in `Keys::latest_key`). Both calls must succeed, and the
/// protector must remain fully usable afterwards.
#[tokio::test]
async fn concurrent_rotations_do_not_corrupt_keys() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let storage = Storage::new(tmp.path().join("device"), tmp.path().join("remote"))?;
    let core = crdt_enc::Core::open(OpenOptions {
        storage,
        protector: EnvelopeProtector::new(NoopKeySlot),
        create: true,
        supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
        current_data_version: CURRENT_DATA_VERSION,
    })
    .await?;
    let actor = core.info().actor();
    let protector = core.protector();

    let (r1, r2) = ::tokio::join!(protector.rotate_key(), protector.rotate_key());
    r1?;
    r2?;

    // still fully usable afterwards -- would panic (via `Keys::latest_key`) if the CRDT got
    // corrupted by the two rotations above
    core.read_and_apply(|s: &MVReg<u64, Uuid>| {
        let read_ctx = s.read();
        let op = s.write(1, read_ctx.derive_add_ctx(actor));
        Ok(vec![op])
    })
    .await?;

    let values = core.with_state(|s: &MVReg<u64, Uuid>| Ok(s.read().val))?;
    assert_eq!(values, vec![1]);

    Ok(())
}
