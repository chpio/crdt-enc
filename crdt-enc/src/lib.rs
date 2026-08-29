//! The storage/crypto-agnostic core of `crdt-enc`: [`Core<S, ST, P>`] persists a CRDT's ops and
//! full-state snapshots as immutable, content-addressed blobs, entirely agnostic about *where*
//! they're stored (`ST: Storage`) and *how* they're protected (`P: Protector`) -- those concerns are
//! implemented by other crates that plug into the two traits this crate defines.
#![warn(missing_docs)]

/// The `Protector` trait and its default no-op lifecycle hooks -- how `Core` encrypts/decrypts the
/// opaque blobs it persists.
pub mod protector;
/// The `Storage` trait and its default no-op lifecycle hooks -- where/how `Core` persists and
/// lists blobs.
pub mod storage;
/// Reusable helpers shared across this crate and its implementors: [`VersionBytes`]-family
/// UUID-tagged byte-blob versioning, [`LockBox`], and the `MVReg<VersionBytes, _>`
/// encode/decode/merge helpers protector/storage implementations plug their crypto into.
pub mod utils;

use crate::{
    protector::Protector,
    storage::Storage,
    utils::{LockBox, SecretBytes, VersionBytes, VersionBytesRef},
};
use ::anyhow::{Context, Error, Result};
use ::async_trait::async_trait;
use ::crdts::{CmRDT, CvRDT, MVReg, VClock};
use ::dyn_clone::DynClone;
use ::futures::{
    lock::Mutex as AsyncMutex,
    stream::{self, StreamExt, TryStreamExt},
};
use ::serde::{Deserialize, Serialize, de::DeserializeOwned};
use ::std::{
    collections::HashSet, convert::Infallible, default::Default, fmt::Debug, mem, sync::Arc,
};
use ::uuid::Uuid;

/// The version tag `crdt-enc` itself stamps onto the outer [`VersionBytes`] wrapper it puts around
/// every protector-encrypted blob (local meta, remote meta, states, ops) -- distinct from the
/// caller-controlled `current_data_version`/`supported_data_versions` used for the *inner*,
/// already-decrypted payload.
const CURRENT_VERSION: Uuid = Uuid::from_u128(0xe834d789_101b_4634_9823_9de990a9051f);

/// The set of `CURRENT_VERSION`-style outer-wrapper versions this build can still read; checked via
/// [`VersionBytes::ensure_versions_phf`] before deserializing. Extend when introducing a new outer
/// wrapper format, keeping old versions readable rather than bumping in place.
static SUPPORTED_VERSIONS: phf::Set<u128> = phf::phf_set! {
    // current
    0x_e834d789_101b_4634_9823_9de990a9051f_u128,
};

/// The object-safe, `dyn`-compatible handle into a `Core` passed to `Storage::init`/`Protector::init`
/// and stored by implementations that need to call back into `Core` later (e.g.
/// `EnvelopeProtector` clones it via `dyn_clone` to publish protector metadata after a key
/// rotation). `Arc<Core<S, ST, P>>` implements this for any concrete `S`/`ST`/`P`, erasing those
/// type parameters behind `dyn CoreSubHandle`.
#[async_trait]
pub trait CoreSubHandle
where
    Self: 'static + Debug + Send + Sync + DynClone,
{
    /// This device's own actor/session info, e.g. its `local_actor_id`.
    fn info(&self) -> Info;

    /// See [`Core::compact`].
    async fn compact(&self) -> Result<()>;
    /// See [`Core::read_remote`].
    async fn read_remote(&self) -> Result<()>;
    /// Re-reads and merges any remote-meta entries this device hasn't seen yet, notifying
    /// storage/protector of the result even if nothing new was found (unlike the initial read done
    /// by `Core::open`, which always notifies).
    async fn read_remote_meta(&self) -> Result<()>;

    /// Merges `remote_meta` into this `Core`'s storage-owned slice of `RemoteMeta` and persists the
    /// result, notifying `Storage::set_remote_meta` of the merged value. Called by a `Storage`
    /// implementation to publish its own out-of-band metadata for other devices to pick up.
    async fn set_remote_meta_storage(&self, remote_meta: MVReg<VersionBytes, Uuid>) -> Result<()>;
    /// The protector-owned counterpart to `set_remote_meta_storage`, notifying
    /// `Protector::set_remote_meta` instead.
    async fn set_remote_meta_protector(&self, remote_meta: MVReg<VersionBytes, Uuid>)
    -> Result<()>;
}

/// Delegates each method to the identically-named inherent `Core` method, erasing `S`/`ST`/`P`
/// behind `dyn CoreSubHandle`.
#[async_trait]
impl<S, ST, P> CoreSubHandle for Arc<Core<S, ST, P>>
where
    S: 'static
        + CmRDT
        + CvRDT
        + Default
        + Serialize
        + DeserializeOwned
        + Clone
        + Debug
        + Send
        + Sync,
    <S as CmRDT>::Op: 'static + Serialize + DeserializeOwned + Clone + Send,
    ST: Storage,
    P: Protector,
{
    fn info(&self) -> Info {
        self.info()
    }

    async fn compact(&self) -> Result<()> {
        self.compact().await
    }

    async fn read_remote(&self) -> Result<()> {
        self.read_remote().await
    }

    async fn read_remote_meta(&self) -> Result<()> {
        self.read_remote_meta().await
    }

    async fn set_remote_meta_storage(&self, remote_meta: MVReg<VersionBytes, Uuid>) -> Result<()> {
        self.set_remote_meta_storage(remote_meta).await
    }

    async fn set_remote_meta_protector(
        &self,
        remote_meta: MVReg<VersionBytes, Uuid>,
    ) -> Result<()> {
        self.set_remote_meta_protector(remote_meta).await
    }
}

/// The storage/crypto-agnostic engine at the heart of this crate: generic over the CRDT state type
/// `S` and the two trait implementations that give it somewhere to persist data (`ST: Storage`) and
/// something to protect it with (`P: Protector`). Construct via [`Core::open`]; all further
/// interaction goes through its inherent methods (`with_state`, `read_and_apply`, `compact`,
/// `read_remote`, ...) on the returned `Arc<Core<..>>`.
#[derive(Debug)]
pub struct Core<S, ST, P> {
    /// Where blobs are persisted/listed.
    storage: ST,
    /// How blobs are protected before being handed to `storage`.
    protector: P,
    /// All mutable in-process state, behind a sync mutex -- never held across an `.await`; values
    /// needed across an await point are pulled out inside a `data.with(...)`/`data.try_with(...)`
    /// closure first.
    data: LockBox<CoreMutData<S>>,
    /// The sorted set of inner-payload data versions this `Core` can still read, checked before
    /// deserializing a decrypted state/op payload.
    supported_data_versions: Vec<Uuid>,
    /// The inner-payload data version newly-written state/op payloads are stamped with.
    current_data_version: Uuid,
    /// Guards every section that reads-then-mutates `data.state` (local op application and remote
    /// state/op merges), so a `read_and_apply` op is never built from a causal context that a
    /// concurrent apply or remote merge invalidates before it lands.
    state_lock: AsyncMutex<()>,
}

/// All of `Core`'s mutable in-process state, guarded together by `Core::data` so it's always
/// mutated/read as one consistent snapshot.
#[derive(Debug)]
struct CoreMutData<S> {
    /// This device's own local metadata, `None` only until `Core::open` finishes loading/creating
    /// it.
    local_meta: Option<LocalMeta>,
    /// This device's merged view of the storage/protector out-of-band gossip metadata.
    remote_meta: RemoteMeta,
    /// The current CRDT state plus the per-actor op versions already folded into it.
    state: StateWrapper<S>,
    /// Names of full-state snapshots already merged into `state`, so `read_remote_states` doesn't
    /// re-load/re-merge them.
    read_states: HashSet<String>,
    /// Names of remote-meta blobs already merged into `remote_meta`, so `read_remote_meta_` doesn't
    /// re-load/re-merge them.
    read_remote_metas: HashSet<String>,
}

impl<S, ST, P> Core<S, ST, P>
where
    S: 'static
        + CmRDT
        + CvRDT
        + Default
        + Serialize
        + DeserializeOwned
        + Clone
        + Debug
        + Send
        + Sync,
    <S as CmRDT>::Op: 'static + Serialize + DeserializeOwned + Clone + Send,
    ST: Storage,
    P: Protector,
{
    /// Opens (creating if `options.create` and no local meta exists yet) a `Core` backed by the
    /// given storage/protector. Loads/creates this device's `LocalMeta`, runs `Storage::init` and
    /// `Protector::init` concurrently, then does an initial remote-meta read (always notifying
    /// storage/protector of the result, even if nothing was found). Does not generate or manage any
    /// encryption key itself -- that's entirely the protector's job.
    pub async fn open(options: OpenOptions<ST, P>) -> Result<Arc<Self>> {
        let mut supported_data_versions = options.supported_data_versions;
        supported_data_versions.sort_unstable();

        let core = Arc::new(Core {
            storage: options.storage,
            protector: options.protector,
            supported_data_versions,
            current_data_version: options.current_data_version,
            data: LockBox::new(CoreMutData {
                local_meta: None,
                remote_meta: RemoteMeta::default(),
                state: StateWrapper {
                    next_op_versions: Default::default(),
                    state: Default::default(),
                },
                read_states: HashSet::new(),
                read_remote_metas: HashSet::new(),
            }),
            state_lock: AsyncMutex::new(()),
        });

        let local_meta = core
            .storage
            .load_local_meta()
            .await
            .context("failed getting local meta")?;
        let local_meta: LocalMeta = match local_meta {
            Some(local_meta) => {
                local_meta.ensure_versions_phf(&SUPPORTED_VERSIONS)?;
                rmp_serde::from_slice(local_meta.as_ref())?
            }
            None => {
                if !options.create {
                    return Err(Error::msg(
                        "local meta does not exist, and `create` option is not set",
                    ));
                }
                let local_meta = LocalMeta {
                    local_actor_id: Uuid::new_v4(),
                };
                let vbox =
                    VersionBytes::new(CURRENT_VERSION, rmp_serde::to_vec_named(&local_meta)?);

                core.storage
                    .store_local_meta(vbox)
                    .await
                    .context("failed storing local meta")?;
                local_meta
            }
        };

        core.data.with(|data| {
            data.local_meta = Some(local_meta);
        });

        futures::try_join![core.storage.init(&core), core.protector.init(&core),]?;

        core.read_remote_meta_(true).await?;

        Ok(core)
    }

    /// This device's own actor/session info, e.g. its `local_actor_id`. Panics if called before
    /// `Core::open` has finished loading/creating local meta.
    pub fn info(self: &Arc<Self>) -> Info {
        self.data.with(|data| {
            let actor = data
                .local_meta
                .as_ref()
                .expect("info not set, yet. Do not call this fn in the init phase")
                .local_actor_id;
            Info { actor }
        })
    }

    /// Gives access to the concrete `Protector` this `Core` was opened with, for
    /// implementation-specific functionality (e.g. `EnvelopeProtector::rotate_key`) that isn't
    /// part of the generic `Protector` trait Core itself relies on.
    pub fn protector(self: &Arc<Self>) -> &P {
        &self.protector
    }

    /// Runs `f` against the current CRDT state under `Core`'s data lock. Do not call this
    /// recursively (e.g. from inside another `with_state`/`read_and_apply` closure) -- the
    /// underlying lock is not reentrant.
    pub fn with_state<F, R>(self: &Arc<Self>, f: F) -> Result<R>
    where
        F: FnOnce(&S) -> Result<R>,
    {
        self.data.with(|data| f(&data.state.state))
    }

    /// Atomically reads the current state, builds ops from it via `f`, and applies them — unlike
    /// separately reading via `with_state` and then applying, no local apply or remote merge can land
    /// between the read and the apply, so `f` never builds an op from a causal context that's gone
    /// stale by the time it's applied. To apply already-built ops without reading state, pass a
    /// closure that ignores its argument, e.g. `read_and_apply(|_| Ok(ops))`.
    pub async fn read_and_apply<F>(self: &Arc<Self>, f: F) -> Result<()>
    where
        F: FnOnce(&S) -> Result<Vec<S::Op>>,
    {
        let state_lock = self.state_lock.lock().await;

        let ops = self.data.try_with(|data| f(&data.state.state))?;

        let clear_text = rmp_serde::to_vec_named(&ops)?;
        let clear_text = VersionBytes::new(self.current_data_version, clear_text);

        let data_enc = self
            .protector
            .encrypt(SecretBytes::new(clear_text.serialize()))
            .await?;

        // TODO: add key id
        // let block = Block {
        //     data_version: self.current_data_version,
        //     key_id: Uuid::nil(),
        //     data_enc,
        // };

        let data_enc = VersionBytes::new(CURRENT_VERSION, data_enc);

        let (actor, version) = self.data.try_with(|data| {
            let actor = data
                .local_meta
                .as_ref()
                .ok_or_else(|| Error::msg("local meta not loaded"))?
                .local_actor_id;
            let version = data.state.next_op_versions.get(&actor);
            Ok((actor, version))
        })?;

        self.storage.store_ops(actor, version, data_enc).await?;

        self.data.with(|data| {
            for op in ops {
                data.state.state.apply(op);
            }

            let version_inc = data.state.next_op_versions.inc(actor);
            data.state.next_op_versions.apply(version_inc);
        });

        // release lock by hand to prevent an early release by accident
        mem::drop(state_lock);

        Ok(())
    }

    /// Snapshots the current (fully up-to-date, after an internal `read_remote`) state into a new
    /// encrypted full-state file, then removes the now-redundant state/op files it superseded.
    pub async fn compact(self: &Arc<Self>) -> Result<()> {
        self.read_remote().await?;

        let (clear_text, states_to_remove, ops_to_remove) = self.data.try_with(|data| {
            let clear_text = rmp_serde::to_vec_named(&data.state)?;
            let clear_text = VersionBytes::new(self.current_data_version, clear_text);

            let states_to_remove = data.read_states.iter().cloned().collect();

            let ops_to_remove = data
                .state
                .next_op_versions
                .iter()
                .map(|dot| (dot.actor.clone(), dot.counter - 1))
                .collect();

            Ok((clear_text, states_to_remove, ops_to_remove))
        })?;

        let data_enc = self
            .protector
            .encrypt(SecretBytes::new(clear_text.serialize()))
            .await?;

        let enc_data = VersionBytes::new(CURRENT_VERSION, data_enc);

        // first store new state
        let new_state_name = self.storage.store_state(enc_data).await?;

        // then remove old states and ops
        let (removed_states, _) = futures::try_join![
            self.storage.remove_states(states_to_remove),
            self.storage.remove_ops(ops_to_remove),
        ]?;

        self.data.with(|data| {
            for removed_state in removed_states {
                data.read_states.remove(&removed_state);
            }

            data.read_states.insert(new_state_name);
        });

        Ok(())
    }

    /// Merges in any full-state snapshots and ops this device hasn't seen yet, from storage.
    pub async fn read_remote(self: &Arc<Self>) -> Result<()> {
        let states_read = self.read_remote_states().await?;
        let ops_read = self.read_remote_ops().await?;

        if states_read || ops_read {
            // TODO: notify app of state changes
        }

        Ok(())
    }

    /// Loads, decrypts and merges every not-yet-seen full-state snapshot listed by `Storage`.
    /// Returns whether any were actually merged. Takes `state_lock` only around the merge itself,
    /// after the (slow) decrypt/deserialize work.
    async fn read_remote_states(self: &Arc<Self>) -> Result<bool> {
        let names = self
            .storage
            .list_state_names()
            .await
            .context("failed getting state entry names while reading remote states")?;

        let states_to_read = self.data.with(|data| {
            let states_to_read: Vec<_> = names
                .into_iter()
                .filter(|name| !data.read_states.contains(name))
                .collect();
            states_to_read
        });

        let new_states = self
            .storage
            .load_states(states_to_read)
            .await
            .context("failed loading state content while reading remote states")?;

        let new_states: Vec<_> = stream::iter(new_states)
            .map(|(name, state)| async move {
                state.ensure_versions_phf(&SUPPORTED_VERSIONS)?;

                let clear_text = self
                    .protector
                    .decrypt(state.into())
                    .await
                    .with_context(|| format!("failed decrypting remote state {}", name))?;

                let clear_text = VersionBytesRef::deserialize(clear_text.expose_secret())?;
                clear_text.ensure_versions(&self.supported_data_versions)?;

                let state_wrapper: StateWrapper<S> = rmp_serde::from_slice(clear_text.as_ref())?;

                Result::<_>::Ok((name, state_wrapper))
            })
            .buffer_unordered(16)
            .try_collect()
            .await?;

        let states_read = !new_states.is_empty();

        let state_lock = self.state_lock.lock().await;

        self.data.with(|data| {
            for (name, state_wrapper) in new_states {
                data.state.state.merge(state_wrapper.state);
                data.state
                    .next_op_versions
                    .merge(state_wrapper.next_op_versions);
                data.read_states.insert(name);
            }
        });

        mem::drop(state_lock);

        Ok(states_read)
    }

    /// Loads, decrypts and applies every not-yet-seen op of every actor listed by `Storage`.
    /// Returns whether any were actually applied. Requires each actor's ops to arrive in
    /// contiguous, strictly ascending order (per the `Storage::load_ops` contract); a gap is
    /// treated as a storage bug and returns an error, while an already-applied version is silently
    /// skipped (harmless race with a concurrent call to this function).
    async fn read_remote_ops(self: &Arc<Self>) -> Result<bool> {
        let actors = self
            .storage
            .list_op_actors()
            .await
            .context("failed getting op actor entries while reading remote ops")?;

        let ops_to_read = self.data.with(|data| {
            let ops_to_read: Vec<_> = actors
                .into_iter()
                .map(|actor| (actor, data.state.next_op_versions.get(&actor)))
                .collect();
            ops_to_read
        });

        let new_ops = self.storage.load_ops(ops_to_read).await?;

        let new_ops: Vec<_> = stream::iter(new_ops)
            .map(|(actor, version, data)| async move {
                data.ensure_versions_phf(&SUPPORTED_VERSIONS)?;
                let clear_text = self.protector.decrypt(data.into()).await?;

                let clear_text = VersionBytesRef::deserialize(clear_text.expose_secret())?;
                clear_text.ensure_versions(&self.supported_data_versions)?;

                let ops: Vec<_> = rmp_serde::from_slice(clear_text.as_ref())?;

                Result::<_, Error>::Ok((actor, version, ops))
            })
            .buffered(16)
            .try_collect()
            .await?;

        let state_lock = self.state_lock.lock().await;

        let ops_read = self.data.with(|data| {
            let mut ops_read = false;
            for (actor, version, ops) in new_ops {
                let expected_version = data.state.next_op_versions.get(&actor);

                if version < expected_version {
                    // already read that version (concurrent call to this fn between us reading
                    // the ops and processing them)
                    continue;
                }

                if expected_version < version {
                    return Err(Error::msg(
                        "Unexpected op version. Got ops in the wrong order? Bug in storage?",
                    ));
                }

                for op in ops {
                    data.state.state.apply(op);
                }

                let version_inc = data.state.next_op_versions.inc(actor);
                data.state.next_op_versions.apply(version_inc);

                ops_read = true;
            }

            Ok(ops_read)
        })?;

        mem::drop(state_lock);

        Ok(ops_read)
    }

    /// The `CoreSubHandle::read_remote_meta` implementation -- see there.
    async fn read_remote_meta(self: &Arc<Self>) -> Result<()> {
        self.read_remote_meta_(false).await
    }

    /// Loads and merges every not-yet-seen remote-meta blob, then notifies `Storage`/`Protector` of
    /// the merged result via `set_remote_meta` -- but only if something new was actually merged,
    /// unless `force_notify` is set (used by `Core::open`'s initial call, so implementations always
    /// get told the current state even if it's empty).
    async fn read_remote_meta_(self: &Arc<Self>, force_notify: bool) -> Result<()> {
        let names = self
            .storage
            .list_remote_meta_names()
            .await
            .context("failed getting remote meta entry names while reading remote metas")?;

        let remote_metas_to_read = self.data.with(|data| {
            let remote_metas_to_read: Vec<_> = names
                .into_iter()
                .filter(|name| !data.read_remote_metas.contains(name))
                .collect();
            remote_metas_to_read
        });

        let remote_metas = self
            .storage
            .load_remote_metas(remote_metas_to_read)
            .await
            .context("failed loading remote meta while reading remote metas")?
            .into_iter()
            .map(|(name, vbox)| {
                vbox.ensure_versions_phf(&SUPPORTED_VERSIONS)?;

                let remote_meta: RemoteMeta = rmp_serde::from_slice(vbox.as_ref())?;

                Ok((name, remote_meta))
            })
            .collect::<Result<Vec<_>>>()?;

        let remote_meta = if !remote_metas.is_empty() {
            self.data.with(|data| {
                for (name, meta) in remote_metas {
                    data.remote_meta.merge(meta);
                    data.read_remote_metas.insert(name);
                }

                Some(data.remote_meta.clone())
            })
        } else {
            None
        };

        if let Some(remote_meta) = remote_meta {
            futures::try_join![
                self.storage.set_remote_meta(Some(remote_meta.storage)),
                self.protector.set_remote_meta(Some(remote_meta.protector)),
            ]?;
        } else if force_notify {
            futures::try_join![
                self.storage.set_remote_meta(None),
                self.protector.set_remote_meta(None),
            ]?;
        }

        Ok(())
    }

    /// The `CoreSubHandle::set_remote_meta_storage` implementation -- see there.
    async fn set_remote_meta_storage(
        self: &Arc<Self>,
        remote_meta: MVReg<VersionBytes, Uuid>,
    ) -> Result<()> {
        self.data.with(|data| {
            data.remote_meta.storage.merge(remote_meta);
        });

        self.store_remote_meta().await
    }

    /// The `CoreSubHandle::set_remote_meta_protector` implementation -- see there.
    async fn set_remote_meta_protector(
        self: &Arc<Self>,
        remote_meta: MVReg<VersionBytes, Uuid>,
    ) -> Result<()> {
        self.data.with(|data| {
            data.remote_meta.protector.merge(remote_meta);
        });

        self.store_remote_meta().await
    }

    /// Serializes the current merged `remote_meta` and writes it as a new immutable blob, then
    /// removes every previously-written remote-meta blob from this device (they're all superseded
    /// by the one just written, which already reflects everything they contained).
    async fn store_remote_meta(self: &Arc<Self>) -> Result<()> {
        let vbox = self.data.try_with(|data| {
            let bytes = rmp_serde::to_vec_named(&data.remote_meta)?;
            Ok(VersionBytes::new(CURRENT_VERSION, bytes))
        })?;

        let new_name = self.storage.store_remote_meta(vbox).await?;

        let names_to_remove = self.data.with(|data| {
            let names_to_remove = data.read_remote_metas.drain().collect();
            data.read_remote_metas.insert(new_name);
            names_to_remove
        });

        self.storage.remove_remote_metas(names_to_remove).await?;

        Ok(())
    }
}

/// Arguments to [`Core::open`].
pub struct OpenOptions<ST, P> {
    /// The `Storage` implementation to use.
    pub storage: ST,
    /// The `Protector` implementation to use.
    pub protector: P,
    /// Whether to create local meta (and thus a new device identity) if none exists yet, instead of
    /// failing.
    pub create: bool,
    /// The sorted set of inner-payload data versions this `Core` can read. Must include
    /// `current_data_version`.
    pub supported_data_versions: Vec<Uuid>,
    /// The inner-payload data version newly-written state/op payloads are stamped with.
    pub current_data_version: Uuid,
}

/// This device's own local metadata: never synced, loaded/created once by `Core::open` and kept in
/// `CoreMutData::local_meta` for the rest of the process's lifetime.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LocalMeta {
    /// This device's randomly-generated actor id, used to attribute every op it creates.
    pub(crate) local_actor_id: Uuid,
}

/// The full persisted/synced unit of CRDT state: the state itself plus the per-actor op versions
/// already folded into it, so a reader knows which ops it still needs to separately apply on top of
/// a loaded snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StateWrapper<S> {
    /// For each actor, the version of the next op from that actor not yet reflected in `state`.
    pub(crate) next_op_versions: VClock<Uuid>,
    /// The CRDT state itself.
    pub(crate) state: S,
}

/// A small `CvRDT` composed of two `MVReg`s, used to let `Storage`/`Protector` gossip their own
/// out-of-band metadata between devices the same way the app data syncs.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct RemoteMeta {
    /// The storage implementation's slice, round-tripped through `Storage::set_remote_meta`.
    storage: MVReg<VersionBytes, Uuid>,
    /// The protector implementation's slice, round-tripped through `Protector::set_remote_meta`.
    protector: MVReg<VersionBytes, Uuid>,
}

impl CvRDT for RemoteMeta {
    /// Merging two `RemoteMeta`s can never fail -- each field is itself an infallible `MVReg` merge.
    type Validation = Infallible;

    /// Always succeeds; see `Validation`.
    fn validate_merge(&self, _other: &Self) -> Result<(), Infallible> {
        Ok(())
    }

    /// Merges each field's `MVReg` independently.
    fn merge(&mut self, other: Self) {
        self.storage.merge(other.storage);
        self.protector.merge(other.protector);
    }
}

/// This device's own actor/session info, returned by `Core::info`/`CoreSubHandle::info`.
#[derive(Debug, Clone)]
pub struct Info {
    /// This device's actor id (same as `LocalMeta::local_actor_id`).
    actor: Uuid,
}

impl Info {
    /// This device's actor id.
    pub fn actor(&self) -> Uuid {
        self.actor
    }
}
