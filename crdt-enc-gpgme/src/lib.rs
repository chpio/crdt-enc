use ::anyhow::{Context, Result};
use ::async_trait::async_trait;
use ::crdt_enc::{
    key_cryptor::Keys,
    utils::{
        decode_version_bytes_mvreg_custom, encode_version_bytes_mvreg_custom, LockBox, VersionBytes,
    },
    CoreSubHandle, Info,
};
use ::crdts::{ctx::ReadCtx, CvRDT, MVReg, Orswot};
use ::serde::{Deserialize, Serialize};
use ::std::{convert::Infallible, fmt::Debug};
use ::uuid::Uuid;

const CURRENT_VERSION: Uuid = Uuid::from_u128(0xe69cb68e_7fbb_41aa_8d22_87eace7a04c9);

// needs to be sorted!
const SUPPORTED_VERSIONS: &[Uuid] = &[
    Uuid::from_u128(0xe69cb68e_7fbb_41aa_8d22_87eace7a04c9), // current
];

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
    data: LockBox<MutData>,
}

impl KeyHandler {
    pub fn new() -> KeyHandler {
        KeyHandler {
            data: LockBox::new(MutData {
                info: None,
                core: None,
                remote_meta: MVReg::new(),
            }),
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
        self.data.with(|data| {
            data.info = Some(core.info());
            data.core = Some(dyn_clone::clone_box(core));
        });

        Ok(())
    }

    async fn set_remote_meta(
        &self,
        new_remote_meta: Option<MVReg<VersionBytes, Uuid>>,
    ) -> Result<()> {
        let (remote_meta, core) = self.data.try_with(|data| {
            if let Some(new_remote_meta) = new_remote_meta {
                data.remote_meta.merge(new_remote_meta);
            }

            let core = dyn_clone::clone_box(&**data.core.as_ref().context("core is none")?);
            Ok((data.remote_meta.clone(), core))
        })?;

        let keys_ctx =
            decode_version_bytes_mvreg_custom(&remote_meta, SUPPORTED_VERSIONS, |buf| async move {
                // TODO: decrypt key
                Ok(buf)
            })
            .await?;

        core.set_keys(keys_ctx).await?;

        Ok(())
    }

    async fn set_keys(&self, new_keys: ReadCtx<Keys, Uuid>) -> Result<()> {
        let (mut rm, core) = self.data.try_with(|data| {
            let core = dyn_clone::clone_box(&**data.core.as_ref().context("core is none")?);
            Ok((data.remote_meta.clone(), core))
        })?;

        encode_version_bytes_mvreg_custom(
            &mut rm,
            new_keys,
            core.info().actor(),
            CURRENT_VERSION,
            |buf| async move {
                // TODO: encrypt key
                Ok(buf)
            },
        )
        .await?;

        self.set_remote_meta(Some(rm.clone())).await?;
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
