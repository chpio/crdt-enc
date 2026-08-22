use crate::{CoreSubHandle, utils::VersionBytes};
use ::anyhow::Result;
use ::async_trait::async_trait;
use ::crdts::MVReg;
use ::std::fmt::Debug;
use ::uuid::Uuid;

#[async_trait]
pub trait Protector
where
    Self: 'static + Debug + Send + Sync + Sized,
{
    async fn init(&self, _core: &dyn CoreSubHandle) -> Result<()> {
        Ok(())
    }

    async fn set_remote_meta(&self, _data: Option<MVReg<VersionBytes, Uuid>>) -> Result<()> {
        Ok(())
    }

    async fn encrypt(&self, clear_text: Vec<u8>) -> Result<Vec<u8>>;
    async fn decrypt(&self, enc_data: Vec<u8>) -> Result<Vec<u8>>;
}
