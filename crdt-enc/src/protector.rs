use crate::{
    CoreSubHandle,
    utils::{SecretBytes, VersionBytes},
};
use ::anyhow::Result;
use ::crdts::MVReg;
use ::std::{fmt::Debug, future::Future};
use ::uuid::Uuid;

/// How [`crate::Core`] protects the opaque byte blobs it persists (states, ops, metas) before
/// handing them to [`crate::storage::Storage`], and unprotects them again on read. [`crate::Core`]
/// has no concept of "keys" at all -- it's entirely up to the implementation whether/how it manages
/// key material; see [`crdt-enc-envelope`](https://docs.rs/crdt-enc-envelope) for a LUKS-style
/// envelope-encryption implementation of this trait.
pub trait Protector
where
    Self: 'static + Debug + Send + Sync + Sized,
{
    /// Called once by [`crate::Core::open`], concurrently with
    /// [`crate::storage::Storage::init`], before the initial remote-meta read. `core` is a
    /// `dyn`-compatible handle back into the owning [`crate::Core`] (implementations that need to
    /// call back later, e.g. to publish protector metadata, should `dyn_clone` it and store it).
    /// The default no-op is enough for protectors that don't need any setup.
    fn init(&self, _core: &dyn CoreSubHandle) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Called whenever the protector's slice of `RemoteMeta` (gossiped alongside the app data via
    /// [`crate::Core`]) changes, with the latest merged register -- `None` if no protector metadata
    /// has ever been written yet. Implementations that don't need any out-of-band metadata (e.g. a
    /// passphrase known out-of-band to every device) can rely on the default no-op.
    fn set_remote_meta(
        &self,
        _data: Option<MVReg<VersionBytes, Uuid>>,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Protects one opaque blob before it's handed to [`crate::storage::Storage`] for persistence.
    /// Takes [`SecretBytes`] because this is the application's plaintext -- the very thing this
    /// crate exists to keep off the sync transport -- so it is zeroized once dropped and redacted
    /// in `Debug` rather than sitting in a bare `Vec<u8>`. The ciphertext coming back needs no such
    /// treatment, which is why the return type is a plain `Vec<u8>`.
    fn encrypt(&self, clear_text: SecretBytes) -> impl Future<Output = Result<Vec<u8>>> + Send;

    /// Reverses [`Self::encrypt`], recovering the original blob from what
    /// [`crate::storage::Storage`] returned. Mirrors [`Self::encrypt`]: ciphertext in as a plain
    /// `Vec<u8>`, plaintext out as [`SecretBytes`].
    fn decrypt(&self, enc_data: Vec<u8>) -> impl Future<Output = Result<SecretBytes>> + Send;
}
