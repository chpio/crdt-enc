//! The one test here that needs a whole `Core` on top of this backend, rather than the backend on
//! its own: `Core` writes remote meta *unencrypted* and straight from a serialized `RemoteMeta`, so
//! publishing a register that merges to something it already published produces byte-identical
//! content -- and content-addressed storage then has to accept that as a no-op rather than a name
//! collision. Nothing else in the workspace pairs a `Core` with the filesystem backend, so the
//! regression has no other home.

use ::anyhow::Result;
use ::crdt_enc::{
    CoreSubHandle, OpenOptions,
    protector::Protector,
    utils::{EmptyCrdt, VersionBytes},
};
use ::crdt_enc_tokio::Storage;
use ::crdts::{CmRDT, MVReg};
use ::std::sync::Arc;
use ::uuid::Uuid;

const DATA_VERSION: Uuid = Uuid::from_u128(0x_6d3f81a0_47c2_4e95_b1d8_0f27ea5c9b34);

/// `Core` requires a protector, but this test is about storage, so this one protects nothing.
#[derive(Debug)]
struct PassThrough;

impl Protector for PassThrough {
    async fn encrypt(&self, clear_text: Vec<u8>) -> Result<Vec<u8>> {
        Ok(clear_text)
    }

    async fn decrypt(&self, enc_data: Vec<u8>) -> Result<Vec<u8>> {
        Ok(enc_data)
    }
}

#[tokio::test]
async fn republishing_unchanged_remote_meta_is_not_a_name_collision() -> Result<()> {
    let tmp = tempfile::tempdir()?;

    let core: Arc<crdt_enc::Core<EmptyCrdt, Storage, PassThrough>> =
        crdt_enc::Core::open(OpenOptions {
            storage: Storage::new(tmp.path().join("local"), tmp.path().join("remote"))?,
            protector: PassThrough,
            create: true,
            supported_data_versions: vec![DATA_VERSION],
            current_data_version: DATA_VERSION,
        })
        .await?;

    let actor = core.info().actor();
    let mut reg: MVReg<VersionBytes, Uuid> = MVReg::new();
    let read_ctx = reg.read();
    let op = reg.write(
        VersionBytes::new(DATA_VERSION, b"payload".to_vec()),
        read_ctx.derive_add_ctx(actor),
    );
    reg.apply(op);

    // the second publish merges to exactly what the first one already wrote
    CoreSubHandle::set_remote_meta_protector(&core, reg.clone()).await?;
    CoreSubHandle::set_remote_meta_protector(&core, reg.clone()).await?;

    // the storage side is unchanged too: still one blob, still readable
    CoreSubHandle::set_remote_meta_storage(&core, reg).await?;
    core.read_remote_meta().await?;

    Ok(())
}
