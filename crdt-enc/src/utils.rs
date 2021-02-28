mod version_bytes;

pub use version_bytes::*;

use ::anyhow::{Context, Result};
use ::crdts::{ctx::ReadCtx, CmRDT, CvRDT, MVReg};
use ::serde::{de::DeserializeOwned, Deserialize, Serialize};
use ::std::convert::Infallible;
use ::uuid::Uuid;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmptyCrdt;

impl CmRDT for EmptyCrdt {
    type Op = ();

    type Validation = Infallible;

    fn validate_op(&self, _op: &Self::Op) -> Result<(), Infallible> {
        Ok(())
    }

    fn apply(&mut self, _op: Self::Op) {}
}

impl CvRDT for EmptyCrdt {
    type Validation = Infallible;

    fn validate_merge(&self, _other: &Self) -> Result<(), Infallible> {
        Ok(())
    }

    fn merge(&mut self, _other: Self) {}
}

pub fn decode_version_bytes_mvreg<T: DeserializeOwned + CvRDT + Default>(
    reg: &MVReg<VersionBytes, Uuid>,
    supported_versions: &[Uuid],
) -> Result<ReadCtx<T, Uuid>> {
    let (vals, read_ctx) = reg.read().split();
    let val = vals
        .into_iter()
        .try_fold(T::default(), |mut acc, vb| -> Result<T> {
            vb.ensure_versions(supported_versions)?;
            let keys = rmp_serde::from_read_ref(&vb).context("Could not parse msgpack value")?;
            acc.merge(keys);
            Ok(acc)
        })
        .context("Could not process mvreg value")?;
    Ok(ReadCtx {
        add_clock: read_ctx.add_clock,
        rm_clock: read_ctx.rm_clock,
        val,
    })
}

pub fn encode_version_bytes_mvreg<T: Serialize>(
    reg: &mut MVReg<VersionBytes, Uuid>,
    val: ReadCtx<T, Uuid>,
    actor: Uuid,
    version: Uuid,
) -> Result<()> {
    let (val, read_ctx) = val.split();
    let vb = VersionBytes::new(
        version,
        rmp_serde::to_vec_named(&val).context("Could not serialize value to msgpack")?,
    );
    let op = reg.write(vb, read_ctx.derive_add_ctx(actor));
    reg.apply(op);
    Ok(())
}
