use crate::{CoreSubHandle, utils::VersionBytes};
use ::anyhow::Result;
use ::crdts::MVReg;
use ::std::{fmt::Debug, future::Future};
use ::uuid::Uuid;

/// Where/how [`crate::Core`] persists and lists the (already-protected) byte blobs it produces:
/// local per-device metadata, remote metadata gossiped between devices, full-state snapshots, and
/// per-actor op logs. Every persisted blob is a [`VersionBytes`] and, once written, immutable --
/// implementations only ever create or delete files, never mutate one in place, so the data syncs
/// safely over simple replicating filesystems (e.g. Syncthing). See
/// [`crdt-enc-tokio`](https://docs.rs/crdt-enc-tokio) for a local-filesystem implementation.
pub trait Storage
where
    Self: 'static + Debug + Send + Sync + Sized,
{
    /// Called once by [`crate::Core::open`], concurrently with
    /// [`crate::protector::Protector::init`], before the initial remote-meta read. `core` is a
    /// `dyn`-compatible handle back into the owning [`crate::Core`]. The default no-op is enough for
    /// storage backends that don't need any setup.
    fn init(&self, _core: &dyn CoreSubHandle) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Called whenever the storage's slice of `RemoteMeta` (gossiped alongside the app data via
    /// [`crate::Core`]) changes, with the latest merged register -- `None` if no storage metadata
    /// has ever been written yet. Implementations that don't need any out-of-band metadata can rely
    /// on the default no-op.
    fn set_remote_meta(
        &self,
        _data: Option<MVReg<VersionBytes, Uuid>>,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Loads this device's own local metadata (e.g. [`crate::LocalMeta`]), if any has been stored
    /// yet. Unlike remote meta/states/ops, this is device-local and never synced.
    fn load_local_meta(&self) -> impl Future<Output = Result<Option<VersionBytes>>> + Send;

    /// Overwrites this device's own local metadata.
    fn store_local_meta(&self, data: VersionBytes) -> impl Future<Output = Result<()>> + Send;

    /// Lists the names of all currently-stored remote-meta blobs (one per device that has ever
    /// published one), for [`Self::load_remote_metas`] to load and [`crate::Core`] to merge.
    fn list_remote_meta_names(&self) -> impl Future<Output = Result<Vec<String>>> + Send;

    /// Loads the given remote-meta blobs by name, paired with the name each was loaded under.
    fn load_remote_metas(
        &self,
        names: Vec<String>,
    ) -> impl Future<Output = Result<Vec<(String, VersionBytes)>>> + Send;

    /// Writes a new immutable remote-meta blob (this device's merged view of `RemoteMeta`) and
    /// returns the name it was stored under.
    fn store_remote_meta(&self, data: VersionBytes) -> impl Future<Output = Result<String>> + Send;

    /// Deletes the given remote-meta blobs by name, e.g. ones superseded by a newer merged write
    /// from the same device.
    fn remove_remote_metas(&self, names: Vec<String>) -> impl Future<Output = Result<()>> + Send;

    /// Lists the names of all currently-stored full-state snapshots.
    fn list_state_names(&self) -> impl Future<Output = Result<Vec<String>>> + Send;

    /// Loads the given full-state snapshots by name, paired with the name each was loaded under.
    fn load_states(
        &self,
        names: Vec<String>,
    ) -> impl Future<Output = Result<Vec<(String, VersionBytes)>>> + Send;

    /// Writes a new immutable full-state snapshot (produced by [`crate::Core::compact`]) and returns
    /// the name it was stored under.
    fn store_state(&self, data: VersionBytes) -> impl Future<Output = Result<String>> + Send;

    /// Deletes the given full-state snapshots by name, e.g. ones superseded by a newer compaction.
    fn remove_states(&self, names: Vec<String>)
    -> impl Future<Output = Result<Vec<String>>> + Send;

    /// Lists the actor ids that have at least one op stored.
    fn list_op_actors(&self) -> impl Future<Output = Result<Vec<Uuid>>> + Send;

    /// For each `(actor, first_version)` pair, loads all ops of that actor at or after
    /// `first_version`. Implementations must return, per actor, ops ordered strictly ascending by
    /// version with no gaps -- [`crate::Core`] relies on this contiguous ordering to replay ops
    /// correctly.
    fn load_ops(
        &self,
        actor_first_versions: Vec<(Uuid, u64)>,
    ) -> impl Future<Output = Result<Vec<(Uuid, u64, VersionBytes)>>> + Send;

    /// Writes a new immutable op blob for `actor` at `version`. Callers (i.e. [`crate::Core`], for
    /// the local actor) are responsible for calling this with strictly ascending, contiguous
    /// versions per actor.
    fn store_ops(
        &self,
        actor: Uuid,
        version: u64,
        data: VersionBytes,
    ) -> impl Future<Output = Result<()>> + Send;

    /// For each `(actor, last_version)` pair, deletes all ops of that actor at or before
    /// `last_version`, e.g. ones superseded by a newer compaction.
    fn remove_ops(
        &self,
        actor_last_verions: Vec<(Uuid, u64)>,
    ) -> impl Future<Output = Result<()>> + Send;
}
