mod version_bytes;

pub use version_bytes::*;

use ::anyhow::{Context, Result};
use ::crdts::{ctx::ReadCtx, CmRDT, CvRDT, MVReg};
use ::futures::{stream, Future, FutureExt, StreamExt, TryStreamExt};
use ::serde::{de::DeserializeOwned, Deserialize, Serialize};
use ::std::{convert::Infallible, fmt::Debug, sync::Mutex as SyncMutex};
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
            let keys =
                rmp_serde::from_slice(vb.as_ref()).context("Could not parse msgpack value")?;
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

/// `supported_versions` needs to be sorted
pub async fn decode_version_bytes_mvreg_custom<T, M, Fut>(
    reg: &MVReg<VersionBytes, Uuid>,
    supported_versions: &[Uuid],
    mut buf_decode: M,
) -> Result<ReadCtx<T, Uuid>>
where
    T: DeserializeOwned + CvRDT + Default,
    M: FnMut(Vec<u8>) -> Fut,
    Fut: Future<Output = Result<Vec<u8>>>,
{
    let (vals, read_ctx) = reg.read().split();
    let val = stream::iter(vals)
        .map(|vb| {
            vb.ensure_versions(supported_versions)?;
            Ok(vb.into())
        })
        .map_ok(|buf| {
            buf_decode(buf).map(|res| res.context("Custom buffer decode function failed"))
        })
        .try_buffer_unordered(16)
        .try_fold(T::default(), |mut acc, buf| async move {
            let keys = rmp_serde::from_slice(&buf).context("Could not parse msgpack value")?;
            acc.merge(keys);
            Ok(acc)
        })
        .await
        .context("Could not process mvreg value")?;
    Ok(ReadCtx {
        add_clock: read_ctx.add_clock,
        rm_clock: read_ctx.rm_clock,
        val,
    })
}

pub async fn decode_version_bytes_mvreg_custom_phf<T, M, Fut>(
    reg: &MVReg<VersionBytes, Uuid>,
    supported_versions: &phf::Set<u128>,
    mut buf_decode: M,
) -> Result<ReadCtx<T, Uuid>>
where
    T: DeserializeOwned + CvRDT + Default,
    M: FnMut(Vec<u8>) -> Fut,
    Fut: Future<Output = Result<Vec<u8>>>,
{
    let (vals, read_ctx) = reg.read().split();
    let val = stream::iter(vals)
        .map(|vb| {
            vb.ensure_versions_phf(supported_versions)?;
            Ok(vb.into())
        })
        .map_ok(|buf| {
            buf_decode(buf).map(|res| res.context("Custom buffer decode function failed"))
        })
        .try_buffer_unordered(16)
        .try_fold(T::default(), |mut acc, buf| async move {
            let keys = rmp_serde::from_slice(&buf).context("Could not parse msgpack value")?;
            acc.merge(keys);
            Ok(acc)
        })
        .await
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
    let buf = rmp_serde::to_vec_named(&val).context("Could not serialize value to msgpack")?;
    let vb = VersionBytes::new(version, buf);
    let op = reg.write(vb, read_ctx.derive_add_ctx(actor));
    reg.apply(op);
    Ok(())
}

pub async fn encode_version_bytes_mvreg_custom<T, M, Fut>(
    reg: &mut MVReg<VersionBytes, Uuid>,
    val: ReadCtx<T, Uuid>,
    actor: Uuid,
    version: Uuid,
    mut buf_encode: M,
) -> Result<()>
where
    T: Serialize,
    M: FnMut(Vec<u8>) -> Fut,
    Fut: Future<Output = Result<Vec<u8>>>,
{
    let (val, read_ctx) = val.split();
    let buf = rmp_serde::to_vec_named(&val).context("Could not serialize value to msgpack")?;
    let buf = buf_encode(buf)
        .await
        .context("Custom buffer encode function failed")?;
    let vb = VersionBytes::new(version, buf);
    let op = reg.write(vb, read_ctx.derive_add_ctx(actor));
    reg.apply(op);
    Ok(())
}

/// Uses sync `std::sync::Mutex` because it has less overhead than async mutex. Its intended use is
/// for short data accesses. Prevents `await`s while the lock is held. Awaiting could cause
/// deadlocking.
#[derive(Debug)]
pub struct LockBox<T> {
    inner: SyncMutex<T>,
}

impl<T> LockBox<T> {
    pub fn new(val: T) -> LockBox<T> {
        LockBox {
            inner: SyncMutex::new(val),
        }
    }

    pub fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut data = self.inner.lock().expect("Unable to lock LockBox");
        f(&mut *data)
    }

    /// Utility `LockBox::with` function, that enforces a `anyhow::Result` return type.
    pub fn try_with<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut T) -> Result<R>,
    {
        self.with(f)
    }
}
