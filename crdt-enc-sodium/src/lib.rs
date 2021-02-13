use ::anyhow::{Context, Error, Result};
use ::async_trait::async_trait;
use ::crdt_enc::utils::{VersionBytes, VersionBytesRef};
use ::serde::{Deserialize, Serialize};
use ::sodiumoxide::crypto::secretbox;
use ::std::{borrow::Cow, fmt::Debug};
use ::uuid::Uuid;

const DATA_VERSION: Uuid = Uuid::from_u128(0xc7f269be_0ff5_4a77_99c3_7c23c96d5cb4);

const KEY_VERSION: Uuid = Uuid::from_u128(0x5df28591_439a_4cef_8ca6_8433276cc9ed);

pub fn init() {
    sodiumoxide::init().expect("sodium init failed");
}

#[derive(Debug)]
pub struct EncHandler;

impl EncHandler {
    pub fn new() -> EncHandler {
        EncHandler
    }
}

#[async_trait]
impl crdt_enc::cryptor::Cryptor for EncHandler {
    async fn gen_key(&self) -> Result<VersionBytes> {
        let key = secretbox::gen_key();
        Ok(VersionBytes::new(KEY_VERSION, key.as_ref().into()))
    }

    async fn encrypt(&self, key: VersionBytesRef<'_>, clear_text: &[u8]) -> Result<Vec<u8>> {
        key.ensure_version(KEY_VERSION)
            .context("not matching key version")?;
        let key = secretbox::Key::from_slice(key.as_ref()).context("invalid key length")?;

        let nonce = secretbox::gen_nonce();
        let enc_data = secretbox::seal(clear_text, &nonce, &key);
        let enc_box = EncBox {
            nonce,
            enc_data: enc_data.into(),
        };
        let enc_box_bytes =
            rmp_serde::to_vec_named(&enc_box).context("failed to encode encryption box")?;
        let version_box = VersionBytesRef::new(DATA_VERSION, enc_box_bytes.as_ref());
        let version_box_bytes =
            rmp_serde::to_vec_named(&version_box).context("failed to encode version box")?;
        Ok(version_box_bytes)
    }

    async fn decrypt(&self, key: VersionBytesRef<'_>, enc_data: &[u8]) -> Result<Vec<u8>> {
        key.ensure_version(KEY_VERSION)
            .context("not matching key version")?;
        let key = secretbox::Key::from_slice(key.as_ref()).context("invalid key length")?;

        let version_box: VersionBytesRef =
            rmp_serde::from_read_ref(enc_data).context("failed to parse version box")?;
        version_box
            .ensure_version(DATA_VERSION)
            .context("not matching version of encryption box")?;

        let enc_box: EncBox = rmp_serde::from_read_ref(version_box.as_ref())
            .context("failed to parse encryption box")?;
        let clear_text = secretbox::open(&enc_box.enc_data, &enc_box.nonce, &key)
            .map_err(|_| Error::msg("failed decrypting data"))?;
        Ok(clear_text)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct EncBox<'a> {
    nonce: secretbox::Nonce,

    #[serde(borrow)]
    #[serde(with = "serde_bytes")]
    enc_data: Cow<'a, [u8]>,
}
