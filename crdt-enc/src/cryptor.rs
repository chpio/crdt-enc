use crate::{
    utils::{VersionBytes, VersionBytesRef},
    CoreSubHandle, Info,
};
use ::anyhow::Result;
use ::async_trait::async_trait;
use ::crdts::MVReg;
use ::std::fmt::Debug;
use ::uuid::Uuid;

#[async_trait]
pub trait Cryptor
where
    Self: 'static + Debug + Send + Sync + Sized,
{
    async fn init(&self, _core: &dyn CoreSubHandle) -> Result<()> {
        Ok(())
    }

    async fn set_info(&self, _info: &Info) -> Result<()> {
        Ok(())
    }

    async fn set_remote_meta(&self, _data: Option<MVReg<VersionBytes, Uuid>>) -> Result<()> {
        Ok(())
    }

    async fn gen_key(&self) -> Result<VersionBytes>;
    async fn encrypt(&self, key: VersionBytesRef<'_>, clear_text: &[u8]) -> Result<Vec<u8>>;
    async fn decrypt(&self, key: VersionBytesRef<'_>, enc_data: &[u8]) -> Result<Vec<u8>>;
}
