use crate::{utils::VersionBytes, CoreSubHandle};
use ::anyhow::Result;
use ::async_trait::async_trait;
use ::crdts::MVReg;
use ::std::fmt::Debug;
use ::uuid::Uuid;

#[async_trait]
pub trait Storage
where
    Self: 'static + Debug + Send + Sync + Sized,
{
    async fn init(&self, _core: &dyn CoreSubHandle) -> Result<()> {
        Ok(())
    }

    async fn set_remote_meta(&self, _data: Option<MVReg<VersionBytes, Uuid>>) -> Result<()> {
        Ok(())
    }

    async fn load_local_meta(&self) -> Result<Option<VersionBytes>>;
    async fn store_local_meta(&self, data: VersionBytes) -> Result<()>;

    async fn list_remote_meta_names(&self) -> Result<Vec<String>>;
    async fn load_remote_metas(&self, names: Vec<String>) -> Result<Vec<(String, VersionBytes)>>;
    async fn store_remote_meta(&self, data: VersionBytes) -> Result<String>;
    async fn remove_remote_metas(&self, names: Vec<String>) -> Result<()>;

    async fn list_state_names(&self) -> Result<Vec<String>>;
    async fn load_states(&self, names: Vec<String>) -> Result<Vec<(String, VersionBytes)>>;
    async fn store_state(&self, data: VersionBytes) -> Result<String>;
    async fn remove_states(&self, names: Vec<String>) -> Result<Vec<String>>;

    async fn list_op_actors(&self) -> Result<Vec<Uuid>>;

    /// needs to return the ops ordered by version of that actor
    async fn load_ops(
        &self,
        actor_first_versions: Vec<(Uuid, u64)>,
    ) -> Result<Vec<(Uuid, u64, VersionBytes)>>;
    async fn store_ops(&self, actor: Uuid, version: u64, data: VersionBytes) -> Result<()>;
    async fn remove_ops(&self, actor_last_verions: Vec<(Uuid, u64)>) -> Result<()>;
}
