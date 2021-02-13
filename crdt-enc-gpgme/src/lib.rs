use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use crdt_enc::{key_cryptor::Keys, utils::VersionBytes, CoreSubHandle, Info};
use crdts::{CmRDT, CvRDT, MVReg, Orswot};
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, fmt::Debug, sync::Mutex as SyncMutex};
use uuid::Uuid;

pub fn init() {
    gpgme::init();
}

#[derive(Debug)]
struct MutData {
    info: Option<Info>,
    core: Option<Box<dyn CoreSubHandle>>,
    remote_meta: MVReg<VersionBytes, Uuid>,
}

#[derive(Debug)]
pub struct KeyHandler {
    data: SyncMutex<MutData>,
}

impl KeyHandler {
    pub fn new() -> KeyHandler {
        let data = MutData {
            info: None,
            core: None,
            remote_meta: MVReg::new(),
        };

        KeyHandler {
            data: SyncMutex::new(data),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Meta {
    key_fps: Orswot<serde_bytes::ByteBuf, Uuid>,
}

impl CvRDT for Meta {
    type Validation = Infallible;

    fn validate_merge(&self, _other: &Self) -> Result<(), Infallible> {
        Ok(())
    }

    fn merge(&mut self, other: Self) {
        self.key_fps.merge(other.key_fps);
    }
}

#[async_trait]
impl crdt_enc::key_cryptor::KeyCryptor for KeyHandler {
    async fn init(&self, core: &dyn CoreSubHandle) -> Result<()> {
        let mut data = self
            .data
            .lock()
            .map_err(|err| Error::msg(err.to_string()))?;
        data.core = Some(dyn_clone::clone_box(core));
        Ok(())
    }

    async fn set_info(&self, info: &Info) -> Result<()> {
        let mut data = self
            .data
            .lock()
            .map_err(|err| Error::msg(err.to_string()))?;
        data.info = Some(info.clone());
        Ok(())
    }

    async fn set_remote_meta(
        &self,
        new_remote_meta: Option<MVReg<VersionBytes, Uuid>>,
    ) -> Result<()> {
        let (keys, core) = {
            let mut data = self
                .data
                .lock()
                .map_err(|err| Error::msg(err.to_string()))?;

            if let Some(new_remote_meta) = new_remote_meta {
                data.remote_meta.merge(new_remote_meta);
            }

            let keys = data.remote_meta.read().val.into_iter().try_fold(
                Keys::default(),
                |mut acc, vb| {
                    // TODO: check version
                    // TODO: decrypt key
                    let keys = rmp_serde::from_read_ref(&vb).context("")?;
                    acc.merge(keys);
                    Result::<_, Error>::Ok(acc)
                },
            )?;

            let core = dyn_clone::clone_box(&**data.core.as_ref().context("core is none")?);

            (keys, core)
        };

        core.set_keys(keys).await?;

        Ok(())
    }

    async fn set_keys(&self, new_keys: Keys) -> Result<()> {
        let (rm, core) = {
            let mut data = self
                .data
                .lock()
                .map_err(|err| Error::msg(err.to_string()))?;

            let read_ctx = data.remote_meta.read();

            let mut keys = read_ctx
                .val
                .iter()
                .try_fold(Keys::default(), |mut acc, vb| {
                    // TODO: check version
                    // TODO: decrypt key
                    let keys = rmp_serde::from_read_ref(&vb).context("")?;
                    acc.merge(keys);
                    Result::<_, Error>::Ok(acc)
                })?;

            keys.merge(new_keys);

            let actor = data.info.as_ref().context("info is none")?.actor();
            let write_ctx = read_ctx.derive_add_ctx(actor);

            let op = data.remote_meta.write(
                VersionBytes::new(
                    // TODO
                    Uuid::nil(),
                    rmp_serde::to_vec_named(&keys)?,
                ),
                write_ctx,
            );
            data.remote_meta.apply(op);

            let core = dyn_clone::clone_box(&**data.core.as_ref().context("core is none")?);

            (data.remote_meta.clone(), core)
        };

        core.set_remote_meta_key_cryptor(rm).await?;

        Ok(())
    }

    // encrypt:
    // let mut pgp_ctx = gpgme::Context::from_protocol(gpgme::Protocol::OpenPgp)
    //     .context("gpgme init fail TODO")?;

    // let recp_pgp_keys = meta
    //     .key_fps
    //     .read()
    //     .val
    //     .into_iter()
    //     .map(|fp| pgp_ctx.get_key(fp.as_ref()).context("TODO gpgme get key"))
    //     .collect::<Result<Vec<_>>>()?;

    // let meta_keys = MetaKeys {
    //     meta: meta.clone(),
    //     keys: Cow::Borrowed(keys),
    // };

    // let meta_keys = rmp_serde::to_vec_named(&meta_keys).context("")?;

    // let mut enc = Vec::new();

    // // TODO: check enc_res
    // let _enc_res = pgp_ctx
    //     .encrypt(&recp_pgp_keys, &meta_keys, &mut enc)
    //     .context("TODO gpgme enc")?;
    // }

    // async fn decrypt(&self) -> Result<Keys> {
    //     // let mut pgp_ctx = gpgme::Context::from_protocol(gpgme::Protocol::OpenPgp)
    //     //     .context("gpgme init fail TODO")?;

    //     // let mut clear_text = Vec::new();

    //     // // TODO: check dec_res
    //     // let _dec_res = pgp_ctx
    //     //     .decrypt(enc_meta_keys, &mut clear_text)
    //     //     .context("TODO gpgme dec")?;

    //     // let meta_keys: MetaKeys = rmp_serde::from_read_ref(&clear_text).context("")?;

    //     // Ok((meta_keys.meta, meta_keys.keys.into()))

    //     Ok(Keys::default())
    // }
}
