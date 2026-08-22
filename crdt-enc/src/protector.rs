use crate::{CoreSubHandle, utils::VersionBytes};
use ::anyhow::Result;
use ::crdts::MVReg;
use ::std::{fmt::Debug, future::Future};
use ::uuid::Uuid;

pub trait Protector
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

    fn encrypt(&self, clear_text: Vec<u8>) -> impl Future<Output = Result<Vec<u8>>> + Send;
    fn decrypt(&self, enc_data: Vec<u8>) -> impl Future<Output = Result<Vec<u8>>> + Send;
}
