pub mod cryptor;
pub mod key_cryptor;
pub mod storage;
pub mod task;
pub mod utils;

use crate::{
    cryptor::Cryptor,
    key_cryptor::{Key, KeyCryptor, Keys},
    storage::Storage,
    utils::{VersionBytes, VersionBytesRef},
};
use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use crdts::{CmRDT, CvRDT, MVReg, VClock};
use dyn_clone::DynClone;
use futures::{
    lock::Mutex as AsyncMutex,
    stream::{self, StreamExt, TryStreamExt},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::HashSet,
    convert::Infallible,
    default::Default,
    fmt::Debug,
    mem,
    sync::{Arc, Mutex as SyncMutex},
};
use uuid::Uuid;

const CURRENT_VERSION: Uuid = Uuid::from_u128(0xe834d789_101b_4634_9823_9de990a9051f);

// needs to be sorted!
const SUPPORTED_VERSIONS: [Uuid; 1] = [
    Uuid::from_u128(0xe834d789_101b_4634_9823_9de990a9051f), // current
];

#[async_trait]
pub trait CoreSubHandle
where
    Self: 'static + Debug + Send + Sync + DynClone,
{
    async fn compact(&self) -> Result<()>;
    async fn read_remote(&self) -> Result<()>;
    async fn read_remote_meta(&self) -> Result<()>;

    async fn set_keys(&self, keys: Keys) -> Result<()>;

    async fn set_remote_meta_storage(&self, remote_meta: MVReg<VersionBytes, Uuid>) -> Result<()>;
    async fn set_remote_meta_cryptor(&self, remote_meta: MVReg<VersionBytes, Uuid>) -> Result<()>;
    async fn set_remote_meta_key_cryptor(
        &self,
        remote_meta: MVReg<VersionBytes, Uuid>,
    ) -> Result<()>;
}

#[async_trait]
impl<S, ST, C, KC> CoreSubHandle for Arc<Core<S, ST, C, KC>>
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
    C: Cryptor,
    KC: KeyCryptor,
{
    async fn compact(&self) -> Result<()> {
        self.compact().await
    }

    async fn read_remote(&self) -> Result<()> {
        self.read_remote().await
    }

    async fn read_remote_meta(&self) -> Result<()> {
        self.read_remote_meta().await
    }

    async fn set_keys(&self, keys: Keys) -> Result<()> {
        self.set_keys(keys).await
    }

    async fn set_remote_meta_storage(&self, remote_meta: MVReg<VersionBytes, Uuid>) -> Result<()> {
        self.set_remote_meta_storage(remote_meta).await
    }

    async fn set_remote_meta_cryptor(&self, remote_meta: MVReg<VersionBytes, Uuid>) -> Result<()> {
        self.set_remote_meta_cryptor(remote_meta).await
    }

    async fn set_remote_meta_key_cryptor(
        &self,
        remote_meta: MVReg<VersionBytes, Uuid>,
    ) -> Result<()> {
        self.set_remote_meta_key_cryptor(remote_meta).await
    }
}

// #[async_trait]
// pub trait CoreTrait
// where
//     Self: 'static + Debug + Send + Sync + Clone,
//     <Self::State as CmRDT>::Op: 'static + Serialize + DeserializeOwned + Clone + Send,
// {
//     type State: 'static
//         + CmRDT
//         + CvRDT
//         + Default
//         + Serialize
//         + DeserializeOwned
//         + Clone
//         + Debug
//         + Send
//         + Sync;

//     async fn compact(&self) -> Result<()>;
//     async fn read_remote(&self) -> Result<()>;
//     async fn read_remote_meta(&self) -> Result<()>;
//     async fn apply_ops(&self, ops: Vec<<Self::State as CmRDT>::Op>) -> Result<()>;

//     async fn set_remote_meta_storage(&self, remote_meta: MVReg<VersionBytes, Uuid>) -> Result<()>;
//     async fn set_remote_meta_cryptor(&self, remote_meta: MVReg<VersionBytes, Uuid>) -> Result<()>;
//     async fn set_remote_meta_key_cryptor(&self, remote_meta: MVReg<VersionBytes, Uuid>)
//         -> Result<()>;
// }

// #[async_trait]
// impl<S, ST, C, KC> CoreTrait for Arc<Core<S, ST, C, KC>>
// where
//     S: 'static
//         + CmRDT
//         + CvRDT
//         + Default
//         + Serialize
//         + DeserializeOwned
//         + Clone
//         + Debug
//         + Send
//         + Sync,
//     <S as CmRDT>::Op: 'static + Serialize + DeserializeOwned + Clone + Send,
//     ST: Storage<Self>,
//     C: Cryptor<Self>,
//     KC: KeyCryptor<Self>,
// {
//     type State = S;

//     async fn compact(&self) -> Result<()> {
//         self.compact_().await
//     }

//     async fn read_remote(&self) -> Result<()> {
//         self.read_remote_().await
//     }

//     async fn read_remote_meta(&self) -> Result<()> {
//         self.read_remote_meta_(false).await
//     }

//     async fn apply_ops(&self, ops: Vec<<Self::State as CmRDT>::Op>) -> Result<()> {
//         self.apply_ops_(ops).await
//     }

//     async fn set_remote_meta_storage(&self, remote_meta: MVReg<VersionBytes, Uuid>) -> Result<()> {
//         self.set_remote_meta_storage_(remote_meta).await
//     }

//     async fn set_remote_meta_cryptor(&self, remote_meta: MVReg<VersionBytes, Uuid>) -> Result<()> {
//         self.set_remote_meta_cryptor_(remote_meta).await
//     }

//     async fn set_remote_meta_key_cryptor(
//         &self,
//         remote_meta: MVReg<VersionBytes, Uuid>,
//     ) -> Result<()> {
//         self.set_remote_meta_key_cryptor_(remote_meta).await
//     }
// }

#[derive(Debug)]
pub struct Core<S, ST, C, KC> {
    storage: ST,
    cryptor: C,
    key_cryptor: KC,
    // use sync `std::sync::Mutex` here because it has less overhead than async mutex, we are
    // holding it for a very shot time and do not `.await` while the lock is held.
    data: SyncMutex<CoreMutData<S>>,
    // task_mgr: task::TaskMgr,
    supported_data_versions: Vec<Uuid>,
    current_data_version: Uuid,
    apply_ops_lock: AsyncMutex<()>,
}

#[derive(Debug)]
struct CoreMutData<S> {
    local_meta: Option<LocalMeta>,
    remote_meta: RemoteMeta,
    keys: Keys,
    state: StateWrapper<S>,
    read_states: HashSet<String>,
    read_remote_metas: HashSet<String>,
}

impl<S, ST, C, KC> Core<S, ST, C, KC>
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
    C: Cryptor,
    KC: KeyCryptor,
{
    pub async fn open(options: OpenOptions<ST, C, KC>) -> Result<(Arc<Self>, Info)> {
        let core_data = SyncMutex::new(CoreMutData {
            local_meta: None,
            remote_meta: RemoteMeta::default(),
            keys: Keys::default(),
            state: StateWrapper {
                next_op_versions: Default::default(),
                state: Default::default(),
            },
            read_states: HashSet::new(),
            read_remote_metas: HashSet::new(),
        });

        let mut supported_data_versions = options.supported_data_versions;
        supported_data_versions.sort_unstable();

        let core = Arc::new(Core {
            storage: options.storage,
            cryptor: options.cryptor,
            key_cryptor: options.key_cryptor,
            supported_data_versions,
            current_data_version: options.current_data_version,
            data: core_data,
            apply_ops_lock: AsyncMutex::new(()),
        });

        futures::try_join![
            core.storage.init(&core),
            core.cryptor.init(&core),
            core.key_cryptor.init(&core),
        ]?;

        let local_meta = core
            .storage
            .load_local_meta()
            .await
            .context("failed getting local meta")?;
        let local_meta: LocalMeta = match local_meta {
            Some(local_meta) => {
                local_meta.ensure_versions(&SUPPORTED_VERSIONS)?;
                rmp_serde::from_read_ref(&local_meta)?
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

        let info = Info {
            actor: local_meta.local_actor_id,
        };

        core.with_mut_data(|data| {
            data.local_meta = Some(local_meta);
            Ok(())
        })?;

        futures::try_join![
            core.storage.set_info(&info),
            core.cryptor.set_info(&info),
            core.key_cryptor.set_info(&info),
        ]?;

        core.read_remote_meta_(true).await?;

        let insert_new_key = core.with_mut_data(|data| Ok(data.keys.latest_key().is_none()))?;

        if insert_new_key {
            let new_key = core.cryptor.gen_key().await?;

            let keys = core.with_mut_data(|data| {
                data.keys.insert_latest_key(info.actor(), Key::new(new_key));
                Ok(data.keys.clone())
            })?;

            core.key_cryptor.set_keys(keys).await?;
        }

        Ok((core, info))
    }

    fn with_mut_data<F, R>(self: &Arc<Self>, f: F) -> Result<R>
    where
        F: FnOnce(&mut CoreMutData<S>) -> Result<R>,
    {
        let mut data = self
            .data
            .lock()
            .map_err(|err| Error::msg(format!("unable to lock `CoreMutData`: {}", err)))?;

        f(&mut *data)
    }

    /// Locks cores data, do not call recursivl
    pub fn with_state<F, R>(self: &Arc<Self>, f: F) -> Result<R>
    where
        F: FnOnce(&S) -> Result<R>,
    {
        self.with_mut_data(|data| f(&data.state.state))
    }

    pub async fn compact(self: &Arc<Self>) -> Result<()> {
        self.read_remote().await?;

        let (clear_text, states_to_remove, ops_to_remove, key) = self.with_mut_data(|data| {
            let clear_text = rmp_serde::to_vec_named(&data.state)?;

            let states_to_remove = data.read_states.iter().cloned().collect();

            let ops_to_remove = data
                .state
                .next_op_versions
                .iter()
                .map(|dot| (dot.actor.clone(), dot.counter - 1))
                .collect();

            let key = data.keys.latest_key().context("no latest key")?;

            Ok((clear_text, states_to_remove, ops_to_remove, key))
        })?;

        let data_enc = self.cryptor.encrypt(key.key(), &clear_text).await.unwrap();

        let enc_data = VersionBytes::new(self.current_data_version, data_enc);

        // first store new state
        let new_state_name = self.storage.store_state(enc_data).await?;

        // then remove old states and ops
        let (removed_states, _) = futures::try_join![
            self.storage.remove_states(states_to_remove),
            self.storage.remove_ops(ops_to_remove),
        ]?;

        self.with_mut_data(|data| {
            for removed_state in removed_states {
                data.read_states.remove(&removed_state);
            }

            data.read_states.insert(new_state_name);
            Ok(())
        })?;

        Ok(())
    }

    async fn set_keys(self: &Arc<Self>, keys: Keys) -> Result<()> {
        self.with_mut_data(|data| {
            data.keys.merge(keys);
            Ok(())
        })?;

        Ok(())
    }

    pub async fn read_remote(self: &Arc<Self>) -> Result<()> {
        let states_read = self.read_remote_states().await?;
        let ops_read = self.read_remote_ops().await?;

        if states_read || ops_read {
            // TODO: notify app of state changes
        }

        Ok(())
    }

    async fn read_remote_states(self: &Arc<Self>) -> Result<bool> {
        let names = self
            .storage
            .list_state_names()
            .await
            .context("failed getting state entry names while reading remote states")?;

        let (states_to_read, key) = self.with_mut_data(|data| {
            let states_to_read: Vec<_> = names
                .into_iter()
                .filter(|name| !data.read_states.contains(name))
                .collect();

            let key = data.keys.latest_key().context("no latest key")?;

            Ok((states_to_read, key))
        })?;

        let new_states = self
            .storage
            .load_states(states_to_read)
            .await
            .context("failed loading state content while reading remote states")?;

        let new_states: Vec<_> = stream::iter(new_states)
            .map(|(name, state)| {
                let key = key.clone();
                async move {
                    state.ensure_versions(&SUPPORTED_VERSIONS)?;

                    let clear_text = self
                        .cryptor
                        .decrypt(key.key(), state.as_ref())
                        .await
                        .with_context(|| format!("failed decrypting remote state {}", name))?;

                    let clear_text = VersionBytesRef::from_slice(&clear_text)?;
                    clear_text.ensure_versions(&self.supported_data_versions)?;

                    let state_wrapper: StateWrapper<S> = rmp_serde::from_read_ref(&clear_text)?;

                    Result::<_>::Ok((name, state_wrapper))
                }
            })
            .buffer_unordered(16)
            .try_collect()
            .await?;

        let states_read = !new_states.is_empty();

        self.with_mut_data(|data| {
            for (name, state_wrapper) in new_states {
                data.state.state.merge(state_wrapper.state);
                data.state
                    .next_op_versions
                    .merge(state_wrapper.next_op_versions);
                data.read_states.insert(name);
            }
            Ok(())
        })?;

        Ok(states_read)
    }

    async fn read_remote_ops(self: &Arc<Self>) -> Result<bool> {
        let actors = self
            .storage
            .list_op_actors()
            .await
            .context("failed getting op actor entries while reading remote ops")?;

        let (ops_to_read, key) = self.with_mut_data(|data| {
            let ops_to_read: Vec<_> = actors
                .into_iter()
                .map(|actor| (actor, data.state.next_op_versions.get(&actor)))
                .collect();

            let key = data.keys.latest_key().context("no latest key")?;

            Ok((ops_to_read, key))
        })?;

        let new_ops = self.storage.load_ops(ops_to_read).await?;

        let new_ops: Vec<_> = stream::iter(new_ops)
            .map(|(actor, version, data)| {
                let key = key.clone();
                async move {
                    data.ensure_versions(&SUPPORTED_VERSIONS)?;
                    let clear_text = self
                        .cryptor
                        .decrypt(key.key(), data.as_ref())
                        .await
                        .unwrap();

                    let clear_text = VersionBytesRef::from_slice(&clear_text)?;
                    clear_text.ensure_versions(&self.supported_data_versions)?;

                    let ops: Vec<_> = rmp_serde::from_read_ref(&clear_text)?;

                    Result::<_, Error>::Ok((actor, version, ops))
                }
            })
            .buffered(16)
            .try_collect()
            .await?;

        let ops_read = self.with_mut_data(|data| {
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

        Ok(ops_read)
    }

    async fn read_remote_meta(self: &Arc<Self>) -> Result<()> {
        self.read_remote_meta_(false).await
    }

    async fn read_remote_meta_(self: &Arc<Self>, force_notify: bool) -> Result<()> {
        let names = self
            .storage
            .list_remote_meta_names()
            .await
            .context("failed getting remote meta entry names while reading remote metas")?;

        let remote_metas_to_read = self.with_mut_data(|data| {
            let remote_metas_to_read: Vec<_> = names
                .into_iter()
                .filter(|name| !data.read_remote_metas.contains(name))
                .collect();
            Ok(remote_metas_to_read)
        })?;

        let remote_metas = self
            .storage
            .load_remote_metas(remote_metas_to_read)
            .await
            .context("failed loading remote meta while reading remote metas")?
            .into_iter()
            .map(|(name, vbox)| {
                vbox.ensure_versions(&SUPPORTED_VERSIONS)?;

                let remote_meta: RemoteMeta = rmp_serde::from_read_ref(&vbox)?;

                Ok((name, remote_meta))
            })
            .collect::<Result<Vec<_>>>()?;

        let remote_meta = if !remote_metas.is_empty() {
            self.with_mut_data(|data| {
                for (name, meta) in remote_metas {
                    data.remote_meta.merge(meta);
                    data.read_remote_metas.insert(name);
                }

                Ok(Some(data.remote_meta.clone()))
            })?
        } else {
            None
        };

        if let Some(remote_meta) = remote_meta {
            futures::try_join![
                self.storage.set_remote_meta(Some(remote_meta.storage)),
                self.cryptor.set_remote_meta(Some(remote_meta.cryptor)),
                self.key_cryptor
                    .set_remote_meta(Some(remote_meta.key_cryptor)),
            ]?;
        } else if force_notify {
            futures::try_join![
                self.storage.set_remote_meta(None),
                self.cryptor.set_remote_meta(None),
                self.key_cryptor.set_remote_meta(None),
            ]?;
        }

        Ok(())
    }

    async fn set_remote_meta_storage(
        self: &Arc<Self>,
        remote_meta: MVReg<VersionBytes, Uuid>,
    ) -> Result<()> {
        self.with_mut_data(|data| {
            data.remote_meta.storage.merge(remote_meta);
            Ok(())
        })?;

        self.store_remote_meta().await
    }

    async fn set_remote_meta_cryptor(
        self: &Arc<Self>,
        remote_meta: MVReg<VersionBytes, Uuid>,
    ) -> Result<()> {
        self.with_mut_data(|data| {
            data.remote_meta.cryptor.merge(remote_meta);
            Ok(())
        })?;

        self.store_remote_meta().await
    }

    async fn set_remote_meta_key_cryptor(
        self: &Arc<Self>,
        remote_meta: MVReg<VersionBytes, Uuid>,
    ) -> Result<()> {
        self.with_mut_data(|data| {
            data.remote_meta.key_cryptor.merge(remote_meta);
            Ok(())
        })?;

        self.store_remote_meta().await
    }

    async fn store_remote_meta(self: &Arc<Self>) -> Result<()> {
        let vbox = self.with_mut_data(|data| {
            let bytes = rmp_serde::to_vec_named(&data.remote_meta)?;
            Ok(VersionBytes::new(CURRENT_VERSION, bytes))
        })?;

        let new_name = self.storage.store_remote_meta(vbox).await?;

        let names_to_remove = self.with_mut_data(|data| {
            let names_to_remove = data.read_remote_metas.drain().collect();
            data.read_remote_metas.insert(new_name);
            Ok(names_to_remove)
        })?;

        self.storage.remove_remote_metas(names_to_remove).await?;

        Ok(())
    }

    pub async fn apply_ops(self: &Arc<Self>, ops: Vec<S::Op>) -> Result<()> {
        // don't allow concurrent op applies
        let apply_ops_lock = self.apply_ops_lock.lock().await;

        let clear_text = rmp_serde::to_vec_named(&ops)?;
        let clear_text = VersionBytes::new(self.current_data_version, clear_text);

        let key = self.with_mut_data(|data| data.keys.latest_key().context("no latest key"))?;

        let data_enc = self
            .cryptor
            .encrypt(key.key(), &clear_text.to_vec())
            .await
            .unwrap();

        // TODO: add key id
        // let block = Block {
        //     data_version: self.current_data_version,
        //     key_id: Uuid::nil(),
        //     data_enc,
        // };

        let data_enc = VersionBytes::new(CURRENT_VERSION, data_enc);

        let (actor, version) = self.with_mut_data(|data| {
            let actor = data
                .local_meta
                .as_ref()
                .ok_or_else(|| Error::msg("local meta not loaded"))?
                .local_actor_id;
            let version = data.state.next_op_versions.get(&actor);
            Ok((actor, version))
        })?;

        self.storage.store_ops(actor, version, data_enc).await?;

        self.with_mut_data(|data| {
            for op in ops {
                data.state.state.apply(op);
            }

            let version_inc = data.state.next_op_versions.inc(actor);
            data.state.next_op_versions.apply(version_inc);
            Ok(())
        })?;

        // release lock by hand to prevent an early release by accident
        mem::drop(apply_ops_lock);

        Ok(())
    }
}

pub struct OpenOptions<ST, C, KC> {
    pub storage: ST,
    pub cryptor: C,
    pub key_cryptor: KC,
    pub create: bool,
    pub supported_data_versions: Vec<Uuid>,
    pub current_data_version: Uuid,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LocalMeta {
    pub(crate) local_actor_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StateWrapper<S> {
    pub(crate) next_op_versions: VClock<Uuid>,
    pub(crate) state: S,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct RemoteMeta {
    storage: MVReg<VersionBytes, Uuid>,
    cryptor: MVReg<VersionBytes, Uuid>,
    key_cryptor: MVReg<VersionBytes, Uuid>,
}

impl CvRDT for RemoteMeta {
    type Validation = Infallible;

    fn validate_merge(&self, _other: &Self) -> Result<(), Infallible> {
        Ok(())
    }

    fn merge(&mut self, other: Self) {
        self.storage.merge(other.storage);
        self.cryptor.merge(other.cryptor);
        self.key_cryptor.merge(other.key_cryptor);
    }
}

#[derive(Debug, Clone)]
pub struct Info {
    actor: Uuid,
}

impl Info {
    pub fn actor(&self) -> Uuid {
        self.actor
    }
}
