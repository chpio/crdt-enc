use crate::{CoreSubHandle, utils::VersionBytes};
use ::anyhow::Result;
use ::crdts::MVReg;
use ::std::{fmt::Debug, future::Future};
use ::uuid::Uuid;

pub trait Storage
where
    Self: 'static + Debug + Send + Sync + Sized,
{
    fn init(&self, _core: &dyn CoreSubHandle) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    fn set_remote_meta(
        &self,
        _data: Option<MVReg<VersionBytes, Uuid>>,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    fn load_local_meta(&self) -> impl Future<Output = Result<Option<VersionBytes>>> + Send;
    fn store_local_meta(&self, data: VersionBytes) -> impl Future<Output = Result<()>> + Send;

    fn list_remote_meta_names(&self) -> impl Future<Output = Result<Vec<String>>> + Send;
    fn load_remote_metas(
        &self,
        names: Vec<String>,
    ) -> impl Future<Output = Result<Vec<(String, VersionBytes)>>> + Send;
    fn store_remote_meta(&self, data: VersionBytes) -> impl Future<Output = Result<String>> + Send;
    fn remove_remote_metas(&self, names: Vec<String>) -> impl Future<Output = Result<()>> + Send;

    fn list_state_names(&self) -> impl Future<Output = Result<Vec<String>>> + Send;
    fn load_states(
        &self,
        names: Vec<String>,
    ) -> impl Future<Output = Result<Vec<(String, VersionBytes)>>> + Send;
    fn store_state(&self, data: VersionBytes) -> impl Future<Output = Result<String>> + Send;
    fn remove_states(&self, names: Vec<String>)
    -> impl Future<Output = Result<Vec<String>>> + Send;

    fn list_op_actors(&self) -> impl Future<Output = Result<Vec<Uuid>>> + Send;

    /// needs to return the ops ordered by version of that actor
    fn load_ops(
        &self,
        actor_first_versions: Vec<(Uuid, u64)>,
    ) -> impl Future<Output = Result<Vec<(Uuid, u64, VersionBytes)>>> + Send;
    fn store_ops(
        &self,
        actor: Uuid,
        version: u64,
        data: VersionBytes,
    ) -> impl Future<Output = Result<()>> + Send;
    fn remove_ops(
        &self,
        actor_last_verions: Vec<(Uuid, u64)>,
    ) -> impl Future<Output = Result<()>> + Send;
}
