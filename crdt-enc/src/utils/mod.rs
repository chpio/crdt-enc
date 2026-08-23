/// The `VersionBytes`/`VersionBytesRef`/`VersionBytesBuf` family: a UUID version tag prepended to a
/// byte blob, used everywhere data is serialized so formats can evolve safely.
mod version_bytes;

pub use version_bytes::*;

use ::anyhow::{Context, Result};
use ::crdts::{CmRDT, CvRDT, MVReg, ctx::ReadCtx};
use ::futures::{Future, FutureExt, StreamExt, TryStreamExt, stream};
use ::serde::{Deserialize, Serialize, de::DeserializeOwned};
use ::std::{convert::Infallible, fmt::Debug, sync::Mutex as SyncMutex};
use ::uuid::Uuid;

/// A `CvRDT`/`CmRDT` with no state and no ops -- everything about it is a no-op. Useful as a
/// placeholder `S` for `Core<S, ST, P>` in contexts that only exercise storage/protector behavior
/// and don't need real app data.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmptyCrdt;

impl CmRDT for EmptyCrdt {
    /// No operation carries any data.
    type Op = ();

    /// Applying an op can never fail.
    type Validation = Infallible;

    /// Always succeeds; see `Validation`.
    fn validate_op(&self, _op: &Self::Op) -> Result<(), Infallible> {
        Ok(())
    }

    /// No-op.
    fn apply(&mut self, _op: Self::Op) {}
}

impl CvRDT for EmptyCrdt {
    /// Merging can never fail.
    type Validation = Infallible;

    /// Always succeeds; see `Validation`.
    fn validate_merge(&self, _other: &Self) -> Result<(), Infallible> {
        Ok(())
    }

    /// No-op.
    fn merge(&mut self, _other: Self) {}
}

/// Decodes and merges every concurrent `VersionBytes` value in an `MVReg<VersionBytes, Uuid>`
/// register into a single `T`, version-checking each value against `supported_versions` first (must
/// be sorted). This is the plaintext counterpart of `decode_version_bytes_mvreg_custom`, for
/// registers that were never protector-encrypted in the first place.
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

/// Identical to `decode_version_bytes_mvreg_custom`, but checks each value's version against a
/// `phf::Set` (e.g. a crate's `SUPPORTED_VERSIONS` constant) instead of a sorted slice.
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

/// Serializes `val` (with its causal `ReadCtx`) as a new `VersionBytes` entry and writes it into the
/// register as one op from `actor`, immediately applying that op locally. This is the plaintext
/// counterpart of `encode_version_bytes_mvreg_custom`, for registers that don't need
/// protector-encryption.
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

/// Like `encode_version_bytes_mvreg`, but runs the serialized msgpack bytes through `buf_encode`
/// (e.g. a `Protector::encrypt` call) before wrapping them in `VersionBytes` and writing the op --
/// the extension point where a `Protector` plugs in its actual encryption when merging an encrypted
/// value into a synced `MVReg<VersionBytes, Uuid>` register.
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
    /// Wraps `val` in a new `LockBox`.
    pub fn new(val: T) -> LockBox<T> {
        LockBox {
            inner: SyncMutex::new(val),
        }
    }

    /// Locks the box and runs `f` against the guarded value, returning its result. Do not `.await`
    /// inside `f` -- the lock is a sync `std::sync::Mutex`, so holding it across an await point
    /// risks deadlocking the executor.
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
