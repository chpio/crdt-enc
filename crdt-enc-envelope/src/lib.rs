use crate::keys::{Key, Keys};
use ::agnostik::spawn_blocking;
use ::anyhow::{Context, Error, Result};
use ::async_trait::async_trait;
use ::chacha20poly1305::{Key as AeadKey, KeyInit, XChaCha20Poly1305, XNonce, aead::Aead};
use ::crdt_enc::{
    CoreSubHandle,
    protector::Protector,
    utils::{
        LockBox, VersionBytes, VersionBytesRef, decode_version_bytes_mvreg_custom_phf,
        encode_version_bytes_mvreg_custom,
    },
};
use ::crdts::{CvRDT, MVReg, ctx::ReadCtx};
use ::futures::lock::Mutex as AsyncMutex;
use ::rand::{TryRng, rng};
use ::serde::{Deserialize, Serialize};
use ::std::{borrow::Cow, fmt::Debug, mem};
use ::uuid::Uuid;

mod keys;

/// version of the outer sync envelope holding the (wrapped) `Keys` CRDT
const CURRENT_VERSION: Uuid = Uuid::from_u128(0x_59b8c30c_f4b0_405b_acf1_9e2202665dbf);

static SUPPORTED_VERSIONS: phf::Set<u128> = phf::phf_set! {
    // current
    0x_59b8c30c_f4b0_405b_acf1_9e2202665dbf_u128,
};

/// version tag for the raw content-encryption key bytes
const KEY_VERSION: Uuid = Uuid::from_u128(0x_3bb60b03_00df_4c79_a199_f96031511d4d);
const KEY_LEN: usize = 32;

/// version of the XChaCha20Poly1305 content envelope
const DATA_VERSION: Uuid = Uuid::from_u128(0x_ae6e17fd_8aa7_46c9_8797_89ecfbedbae9);
const NONCE_LEN: usize = 24;

/// Protects a single raw key blob (e.g. wraps it for one or more recipients, or encrypts it with a
/// password-derived key) — no CRDT/sync bookkeeping, that's [`EnvelopeProtector`]'s job.
#[async_trait]
pub trait KeySlotProtector
where
    Self: 'static + Debug + Send + Sync + Sized,
{
    /// Protects `key` so it can be stored/synced without exposing it in the clear (e.g.
    /// GPG-encrypts it for one or more recipients, or symmetrically encrypts it with a
    /// password-derived key).
    async fn wrap_key(&self, key: &[u8]) -> Result<Vec<u8>>;

    /// Reverses [`wrap_key`](KeySlotProtector::wrap_key), recovering the original key bytes.
    /// Should fail (`Err`) rather than return garbage if `wrapped` can't be
    /// authenticated/decrypted (e.g. wrong password, tampered data).
    async fn unwrap_key(&self, wrapped: &[u8]) -> Result<Vec<u8>>;
}

#[derive(Debug)]
struct MutData {
    core: Option<Box<dyn CoreSubHandle>>,
    remote_meta: MVReg<VersionBytes, Uuid>,
    keys: Keys,
}

/// A [`Protector`] implementing "envelope encryption": content is protected by a random, rotatable
/// content-encryption key, which is itself protected by a `KeySlotProtector` (e.g. a password or GPG
/// recipients). Manages the `Keys` CRDT (rotation, cross-device convergence) itself, so `KS` only ever
/// needs to wrap/unwrap a single small key blob.
#[derive(Debug)]
pub struct EnvelopeProtector<KS> {
    key_slot: KS,
    data: LockBox<MutData>,
    /// Guards every "read current keys, add a new one, publish it" sequence (initial bootstrap
    /// and, later, `rotate_key`), so two such sequences racing for the same actor can never
    /// derive colliding CRDT dots (which would otherwise silently drop one of the two keys, or
    /// worse, trigger the `panic!` in `Keys::latest_key`).
    key_write_lock: AsyncMutex<()>,
}

impl<KS> EnvelopeProtector<KS> {
    /// Creates a new `EnvelopeProtector` with no content key yet — one is bootstrapped
    /// automatically (via `key_slot`) the first time it's used with [`crdt_enc::Core::open`].
    pub fn new(key_slot: KS) -> EnvelopeProtector<KS> {
        EnvelopeProtector {
            key_slot,
            data: LockBox::new(MutData {
                core: None,
                remote_meta: MVReg::new(),
                keys: Keys::default(),
            }),
            key_write_lock: AsyncMutex::new(()),
        }
    }
}

#[async_trait]
impl<KS: KeySlotProtector> Protector for EnvelopeProtector<KS> {
    /// Stores a clone of `core`, used later by `set_remote_meta` to push a newly-bootstrapped
    /// content key back for syncing.
    async fn init(&self, core: &dyn CoreSubHandle) -> Result<()> {
        self.data.with(|data| {
            data.core = Some(dyn_clone::clone_box(core));
        });

        Ok(())
    }

    /// Merges in newly-synced key material, decoding each device's wrapped copy of the content
    /// key via `key_slot.unwrap_key`. If no content key exists anywhere yet, generates one,
    /// protects it via `key_slot.wrap_key`, and syncs it back to `core`. If multiple devices
    /// independently bootstrap a key before ever syncing with each other, they converge
    /// afterwards via `Keys::latest_key()`'s min-by-id tiebreak.
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

        let keys_ctx: ReadCtx<Keys, Uuid> = decode_version_bytes_mvreg_custom_phf(
            &remote_meta,
            &SUPPORTED_VERSIONS,
            |buf| async move { self.key_slot.unwrap_key(&buf).await },
        )
        .await?;

        self.data.with(|data| {
            data.keys.merge(keys_ctx.val.clone());
        });

        let need_new_key = self.data.with(|data| data.keys.latest_key().is_none());

        if need_new_key {
            self.publish_new_key(core, true).await?;
        }

        Ok(())
    }

    /// Encrypts `clear_text` with the current content key (XChaCha20Poly1305), tagging the
    /// ciphertext with that key's id so `decrypt` can look up the exact same key later even
    /// after a rotation. Fails if no content key has been established yet (see
    /// `set_remote_meta`).
    async fn encrypt(&self, clear_text: Vec<u8>) -> Result<Vec<u8>> {
        let key = self
            .data
            .with(|data| data.keys.latest_key())
            .context("no latest key")?;
        encrypt_content(key.id(), key.key(), clear_text).await
    }

    /// Reverses `encrypt`. Looks up the specific key the ciphertext was tagged with (not
    /// necessarily the current latest one, e.g. after a rotation) via `Keys::get_key`. Fails if
    /// that key is unknown, the ciphertext was tampered with, or authentication fails.
    async fn decrypt(&self, enc_data: Vec<u8>) -> Result<Vec<u8>> {
        let version_box: VersionBytesRef =
            rmp_serde::from_slice(&enc_data).context("failed to parse version box")?;
        version_box
            .ensure_version(DATA_VERSION)
            .context("not matching version of encryption box")?;
        let enc_box: EncBox = rmp_serde::from_slice(version_box.as_ref())
            .context("failed to parse encryption box")?;

        let key = self
            .data
            .with(|data| data.keys.get_key(enc_box.key_id))
            .with_context(|| format!("no key with id {}", enc_box.key_id))?;

        decrypt_content(key.key(), enc_box).await
    }
}

impl<KS: KeySlotProtector> EnvelopeProtector<KS> {
    /// Generates a fresh content key, protects it via `key_slot.wrap_key`, and publishes it as
    /// the new latest key. Old keys are never removed from the `Keys` CRDT, so content encrypted
    /// with them (tagged with their id, see `encrypt`) remains decryptable via `decrypt`'s
    /// `Keys::get_key` lookup.
    pub async fn rotate_key(&self) -> Result<()> {
        let core = self.data.try_with(|data| {
            Ok(dyn_clone::clone_box(
                &**data.core.as_ref().context("core is none")?,
            ))
        })?;

        self.publish_new_key(core, false).await
    }

    /// Shared implementation behind the initial key bootstrap (`set_remote_meta`, with
    /// `only_if_missing: true`) and `rotate_key` (`only_if_missing: false`). Everything here runs
    /// under `key_write_lock`, re-reading `data.remote_meta`/`data.keys` fresh after acquiring
    /// it, so two concurrent calls (e.g. two overlapping `rotate_key`s) can never derive
    /// colliding CRDT dots for the same actor.
    async fn publish_new_key(
        &self,
        core: Box<dyn CoreSubHandle>,
        only_if_missing: bool,
    ) -> Result<()> {
        let guard = self.key_write_lock.lock().await;

        let actor = core.info().actor();
        let remote_meta = self.data.with(|data| data.remote_meta.clone());
        let keys_ctx: ReadCtx<Keys, Uuid> = decode_version_bytes_mvreg_custom_phf(
            &remote_meta,
            &SUPPORTED_VERSIONS,
            |buf| async move { self.key_slot.unwrap_key(&buf).await },
        )
        .await?;

        self.data.with(|data| {
            data.keys.merge(keys_ctx.val.clone());
        });

        let remote_meta_to_push =
            if only_if_missing && self.data.with(|data| data.keys.latest_key().is_some()) {
                // someone else already published a key while we were waiting for the lock
                None
            } else {
                let mut key_bytes = vec![0u8; KEY_LEN];
                rng()
                    .try_fill_bytes(&mut key_bytes)
                    .context("Unable to get random data for content key")?;
                let new_key = Key::new(VersionBytes::new(KEY_VERSION, key_bytes));

                let mut new_keys = self.data.with(|data| data.keys.clone());
                new_keys.insert_latest_key(actor, new_key);

                let mut remote_meta = remote_meta;
                encode_version_bytes_mvreg_custom(
                    &mut remote_meta,
                    ReadCtx {
                        add_clock: keys_ctx.add_clock,
                        rm_clock: keys_ctx.rm_clock,
                        val: new_keys,
                    },
                    actor,
                    CURRENT_VERSION,
                    |buf| async move { self.key_slot.wrap_key(&buf).await },
                )
                .await?;

                Some(remote_meta)
            };

        // release lock by hand to prevent an early release by accident
        mem::drop(guard);

        if let Some(remote_meta) = remote_meta_to_push {
            // loop back through the standard decode/merge path so `data.keys` reflects the
            // round-tripped (wrapped+unwrapped) value, then push the new envelope to core
            self.set_remote_meta(Some(remote_meta.clone())).await?;
            core.set_remote_meta_protector(remote_meta).await?;
        }

        Ok(())
    }
}

async fn encrypt_content(
    key_id: Uuid,
    key: VersionBytesRef<'_>,
    clear_text: Vec<u8>,
) -> Result<Vec<u8>> {
    key.ensure_version(KEY_VERSION)
        .context("not matching key version")?;
    if key.as_ref().len() != KEY_LEN {
        return Err(Error::msg("Invalid key length"));
    }
    let key = key.as_ref().to_vec();

    spawn_blocking(move || {
        let aead_key = AeadKey::from_slice(&key);
        let aead = XChaCha20Poly1305::new(aead_key);
        let mut nonce = [0u8; NONCE_LEN];
        rng()
            .try_fill_bytes(&mut nonce)
            .context("Unable to get random data for nonce")?;
        let xnonce = XNonce::from_slice(&nonce);
        let enc_data = aead
            .encrypt(xnonce, clear_text.as_ref())
            .context("Encryption failed")?;
        let enc_box = EncBox {
            key_id,
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

/// Decrypts an already-parsed [`EncBox`] with `key` (which the caller has already looked up via
/// the box's `key_id`).
async fn decrypt_content(key: VersionBytesRef<'_>, enc_box: EncBox<'_>) -> Result<Vec<u8>> {
    key.ensure_version(KEY_VERSION)
        .context("not matching key version")?;
    if key.as_ref().len() != KEY_LEN {
        return Err(Error::msg("Invalid key length"));
    }
    if enc_box.nonce.as_ref().len() != NONCE_LEN {
        return Err(Error::msg("Invalid nonce length"));
    }
    let key = key.as_ref().to_vec();
    let nonce = enc_box.nonce.into_owned();
    let ciphertext = enc_box.enc_data.into_owned();

    spawn_blocking(move || {
        let aead_key = AeadKey::from_slice(&key);
        let aead = XChaCha20Poly1305::new(aead_key);
        let xnonce = XNonce::from_slice(&nonce);
        let clear_text = aead
            .decrypt(xnonce, ciphertext.as_ref())
            .context("Decryption failed")?;
        Ok(clear_text)
    })
    .await
}

#[derive(Serialize, Deserialize, Debug)]
struct EncBox<'a> {
    key_id: Uuid,

    #[serde(borrow)]
    #[serde(with = "serde_bytes")]
    nonce: Cow<'a, [u8]>,

    #[serde(borrow)]
    #[serde(with = "serde_bytes")]
    enc_data: Cow<'a, [u8]>,
}
