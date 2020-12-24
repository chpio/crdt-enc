use crate::{
    utils::{VersionBytes, VersionBytesRef},
    CoreSubHandle, Info, KeyCryptor, Storage,
};
use anyhow::Result;
use async_trait::async_trait;
use crdts::{CmRDT, CvRDT, MVReg};
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, sync::Arc};
use uuid::Uuid;

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
