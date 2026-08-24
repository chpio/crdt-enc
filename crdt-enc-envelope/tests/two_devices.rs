use ::anyhow::Result;
use ::crdt_enc::OpenOptions;
use ::crdt_enc_envelope::{EnvelopeProtector, KeySlotProtector};
use ::crdt_enc_tokio::Storage;
use ::crdts::MVReg;
use ::uuid::Uuid;
use ::zeroize::Zeroizing;

const CURRENT_DATA_VERSION: Uuid = Uuid::from_u128(0x_4045914a_f630_4859_a6bf_e3d0fa427b54);
const SUPPORTED_DATA_VERSIONS: &[Uuid] = &[CURRENT_DATA_VERSION];

/// A `KeySlotProtector` that doesn't protect anything — good enough to exercise
/// `EnvelopeProtector`'s key rotation/convergence logic without depending on a real
/// implementation (e.g. `crdt-enc-password`, which would add unrelated Argon2/password setup for
/// no benefit here).
#[derive(Debug)]
struct NoopKeySlot;

impl KeySlotProtector for NoopKeySlot {
    async fn wrap_key(&self, key: Zeroizing<Vec<u8>>) -> Result<Vec<u8>> {
        Ok(key.to_vec())
    }

    async fn unwrap_key(&self, wrapped: Vec<u8>) -> Result<Zeroizing<Vec<u8>>> {
        Ok(Zeroizing::new(wrapped))
    }
}

/// Two independent `Core`s, backed by two separate `EnvelopeProtector`s, sharing only a remote
/// directory: verifies that the content-encryption key `EnvelopeProtector` bootstraps on device A is
/// picked up by device B via `read_remote()` (rather than each device generating its own, forever
/// undecryptable-to-the-other key), and that device B can therefore actually read device A's state.
#[tokio::test]
async fn two_devices_converge_on_one_key_and_share_state() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let remote_path = tmp.path().join("remote");

    let storage_a = Storage::new(tmp.path().join("device-a"), remote_path.clone())?;
    let protector_a = EnvelopeProtector::new(NoopKeySlot);
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
            let op = s.write(42, read_ctx.derive_add_ctx(info_a.actor()));
            Ok(vec![op])
        })
        .await?;

    let storage_b = Storage::new(tmp.path().join("device-b"), remote_path)?;
    let protector_b = EnvelopeProtector::new(NoopKeySlot);
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

    assert_eq!(values, vec![42]);

    Ok(())
}
