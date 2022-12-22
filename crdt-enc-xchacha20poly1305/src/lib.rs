use ::agnostik::spawn_blocking;
use ::anyhow::{Context, Error, Result};
use ::async_trait::async_trait;
use ::chacha20poly1305::{aead::Aead, Key, KeyInit, XChaCha20Poly1305, XNonce};
use ::crdt_enc::utils::{VersionBytes, VersionBytesRef};
use ::rand::{thread_rng, RngCore};
use ::serde::{Deserialize, Serialize};
use ::std::{borrow::Cow, fmt::Debug};
use ::uuid::Uuid;

const DATA_VERSION: Uuid = Uuid::from_u128(0xc7f269be_0ff5_4a77_99c3_7c23c96d5cb4);

const KEY_VERSION: Uuid = Uuid::from_u128(0x5df28591_439a_4cef_8ca6_8433276cc9ed);

const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 24;

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
        spawn_blocking(|| {
            let mut key = [0u8; KEY_LEN];
            thread_rng()
                .try_fill_bytes(&mut key)
                .context("Unable to get random data for secret key")?;
            Ok(VersionBytes::new(KEY_VERSION, key.into()))
        })
        .await
    }

    async fn encrypt(&self, key: VersionBytesRef<'_>, clear_text: Vec<u8>) -> Result<Vec<u8>> {
        key.ensure_version(KEY_VERSION)
            .context("not matching key version")?;
        if key.as_ref().len() != KEY_LEN {
            return Err(Error::msg("Invalid key length"));
        }
        let key = key.as_ref().to_vec();

        spawn_blocking(move || {
            let key = Key::from_slice(&key);
            let aead = XChaCha20Poly1305::new(key);
            let mut nonce = [0u8; NONCE_LEN];
            thread_rng()
                .try_fill_bytes(&mut nonce)
                .context("Unable to get random data for nonce")?;
            let xnonce = XNonce::from_slice(&nonce);
            let enc_data = aead
                .encrypt(xnonce, clear_text.as_ref())
                .context("Encryption failed")?;
            let enc_box = EncBox {
                nonce: Cow::Borrowed(nonce.as_ref()),
                enc_data: Cow::Owned(enc_data),
            };
            let enc_box_bytes =
                rmp_serde::to_vec_named(&enc_box).context("failed to encode encryption box")?;
            let version_box = VersionBytesRef::new(DATA_VERSION, enc_box_bytes.as_ref());
            let version_box_bytes =
                rmp_serde::to_vec_named(&version_box).context("failed to encode version box")?;
            Ok(version_box_bytes)
        })
        .await
    }

    async fn decrypt(&self, key: VersionBytesRef<'_>, enc_data: Vec<u8>) -> Result<Vec<u8>> {
        key.ensure_version(KEY_VERSION)
            .context("not matching key version")?;
        if key.as_ref().len() != KEY_LEN {
            return Err(Error::msg("Invalid key length"));
        }
        let key = key.as_ref().to_vec();

        spawn_blocking(move || {
            let version_box: VersionBytesRef =
                rmp_serde::from_slice(&enc_data).context("failed to parse version box")?;
            version_box
                .ensure_version(DATA_VERSION)
                .context("not matching version of encryption box")?;
            let enc_box: EncBox = rmp_serde::from_slice(version_box.as_ref())
                .context("failed to parse encryption box")?;
            if enc_box.nonce.as_ref().len() != NONCE_LEN {
                return Err(Error::msg("Invalid nonce length"));
            }
            let key = Key::from_slice(key.as_ref());
            let aead = XChaCha20Poly1305::new(key);
            let xnonce = XNonce::from_slice(&enc_box.nonce);
            let clear_text = aead
                .decrypt(&xnonce, enc_box.enc_data.as_ref())
                .context("Decryption failed")?;
            Ok(clear_text)
        })
        .await
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct EncBox<'a> {
    #[serde(borrow)]
    #[serde(with = "serde_bytes")]
    nonce: Cow<'a, [u8]>,

    #[serde(borrow)]
    #[serde(with = "serde_bytes")]
    enc_data: Cow<'a, [u8]>,
}
