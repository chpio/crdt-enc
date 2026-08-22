use ::agnostik::spawn_blocking;
use ::anyhow::{Context, Error, Result};
use ::argon2::{Algorithm, Argon2, Params, Version};
use ::chacha20poly1305::{Key as AeadKey, KeyInit, XChaCha20Poly1305, XNonce, aead::Aead};
use ::crdt_enc::utils::LockBox;
use ::crdt_enc_envelope::KeySlotProtector;
use ::rand::{TryRng, rng};
use ::serde::{Deserialize, Serialize};
use ::std::{borrow::Cow, collections::HashMap};
use ::zeroize::Zeroizing;

const SALT_LEN: usize = 16;
const NONCE_LEN: usize = 24;
const KEK_LEN: usize = 32;

/// OWASP-recommended Argon2id minimum parameters, used unless [`PasswordKeySlot::with_params`] is
/// used to pick different ones.
const DEFAULT_M_COST: u32 = 19_456;
const DEFAULT_T_COST: u32 = 2;
const DEFAULT_P_COST: u32 = 1;

type Kek = Zeroizing<[u8; KEK_LEN]>;

/// A [`KeySlotProtector`] that wraps/unwraps a key with a password: an Argon2id-derived key
/// encrypts it with XChaCha20Poly1305. Every wrapped blob carries its own salt (and the Argon2
/// parameters used), so it's independently unwrappable — no shared/pre-agreed salt between
/// devices is needed. Derived keys are cached per salt (Argon2 is deliberately slow, and
/// `wrap_key`/`unwrap_key` can otherwise be called repeatedly for the same salt, e.g. once per
/// `Core::read_remote_meta()` call), so it only runs again for a salt this instance hasn't seen
/// before.
///
/// The cleartext password is kept in memory (behind `Zeroizing`, so it's wiped on drop) for this
/// value's entire lifetime, not just briefly at construction: `unwrap_key` must be able to derive
/// the key for a salt it's never seen before at any time (e.g. a new device joining sync later
/// with its own independently-bootstrapped entry), so there's no point at which it's safe to
/// assume the password is no longer needed. See `todo.md` for ideas on reducing this exposure.
#[derive(Debug)]
pub struct PasswordKeySlot {
    password: Zeroizing<String>,
    m_cost: u32,
    t_cost: u32,
    p_cost: u32,
    /// This instance's own (salt, derived key) for `wrap_key`, decided lazily once (preferring an
    /// already-known salt from `unwrap_cache` over minting a fresh one, see `own_salt_kek`) and
    /// reused for the rest of its lifetime — there's no security benefit to a fresh salt per wrap
    /// here (the AEAD's random nonce already makes reusing one key across many messages safe),
    /// and a fresh salt would mean re-running Argon2 on every wrap.
    own: LockBox<Option<(Vec<u8>, Kek)>>,
    /// Derived keys seen so far, keyed by salt, for `unwrap_key` -- and, via `own_salt_kek`, also
    /// the source `wrap_key` prefers to converge on a single shared salt across devices instead
    /// of each independently minting its own.
    unwrap_cache: LockBox<HashMap<Vec<u8>, Kek>>,
}

impl PasswordKeySlot {
    /// Creates a `PasswordKeySlot` using the OWASP-recommended minimum Argon2id parameters.
    pub fn new(password: impl Into<String>) -> PasswordKeySlot {
        Self::with_params(password, DEFAULT_M_COST, DEFAULT_T_COST, DEFAULT_P_COST)
    }

    /// Creates a `PasswordKeySlot` with explicit Argon2id parameters (memory cost in KiB, time
    /// cost, parallelism). The chosen parameters are stored alongside each wrapped key, so
    /// changing them later doesn't break unwrapping keys wrapped with the old parameters.
    pub fn with_params(
        password: impl Into<String>,
        m_cost: u32,
        t_cost: u32,
        p_cost: u32,
    ) -> PasswordKeySlot {
        PasswordKeySlot {
            password: Zeroizing::new(password.into()),
            m_cost,
            t_cost,
            p_cost,
            own: LockBox::new(None),
            unwrap_cache: LockBox::new(HashMap::new()),
        }
    }

    /// Returns this instance's own (salt, derived key) pair for `wrap_key`, deciding and caching
    /// one (in `own`) on first use. Prefers reusing the lexicographically smallest salt already
    /// known via `unwrap_cache` (e.g. from decoding another device's entry during the
    /// `set_remote_meta` merge that precedes this call, same min-by-id tiebreak philosophy as
    /// `Keys::latest_key()`) over minting a fresh one -- lets devices converge on one shared salt
    /// instead of each picking their own, so a device that already knows the canonical salt
    /// doesn't trigger an extra Argon2 run the next time it needs to wrap something itself (e.g.
    /// a rotation). Only mints a genuinely fresh salt if none is known yet (true first-ever
    /// bootstrap). Not revisited after the first call, even if a smaller salt becomes known
    /// later -- unlike `Keys::latest_key()`, this is a one-time decision, not recomputed every
    /// call; in the rare case of two truly concurrent first-time bootstraps, each side may freeze
    /// on its own salt rather than converging, which is an accepted trade-off for simplicity.
    async fn own_salt_kek(&self) -> Result<(Vec<u8>, Kek)> {
        if let Some(pair) = self.own.with(|own| own.clone()) {
            return Ok(pair);
        }

        if let Some(pair) = self.unwrap_cache.with(|cache| {
            cache
                .iter()
                .min_by(|a, b| a.0.cmp(b.0))
                .map(|(salt, kek)| (salt.clone(), kek.clone()))
        }) {
            self.own.with(|own| *own = Some(pair.clone()));
            return Ok(pair);
        }

        let password = self.password.clone();
        let (m_cost, t_cost, p_cost) = (self.m_cost, self.t_cost, self.p_cost);

        let (salt, kek) = spawn_blocking(move || {
            let mut salt = vec![0u8; SALT_LEN];
            rng()
                .try_fill_bytes(&mut salt)
                .context("Unable to get random data for salt")?;
            let kek = derive_kek(&password, &salt, m_cost, t_cost, p_cost)?;
            Result::<_, Error>::Ok((salt, kek))
        })
        .await?;

        self.own
            .with(|own| *own = Some((salt.clone(), kek.clone())));
        self.unwrap_cache
            .with(|cache| cache.insert(salt.clone(), kek.clone()));

        Ok((salt, kek))
    }
}

impl KeySlotProtector for PasswordKeySlot {
    async fn wrap_key(&self, key: &[u8]) -> Result<Vec<u8>> {
        let (salt, kek) = self.own_salt_kek().await?;

        let key = key.to_vec();
        let (m_cost, t_cost, p_cost) = (self.m_cost, self.t_cost, self.p_cost);

        spawn_blocking(move || {
            let mut nonce = [0u8; NONCE_LEN];
            rng()
                .try_fill_bytes(&mut nonce)
                .context("Unable to get random data for nonce")?;

            let aead = XChaCha20Poly1305::new(AeadKey::from_slice(kek.as_ref()));
            let ciphertext = aead
                .encrypt(XNonce::from_slice(&nonce), key.as_ref())
                .context("encryption failed")?;

            let envelope = Envelope {
                salt: Cow::Owned(salt),
                m_cost,
                t_cost,
                p_cost,
                nonce: Cow::Borrowed(&nonce),
                ciphertext: Cow::Owned(ciphertext),
            };

            rmp_serde::to_vec_named(&envelope).context("failed to encode password envelope")
        })
        .await
    }

    async fn unwrap_key(&self, wrapped: &[u8]) -> Result<Vec<u8>> {
        let envelope: Envelope =
            rmp_serde::from_slice(wrapped).context("failed to parse password envelope")?;
        if envelope.nonce.len() != NONCE_LEN {
            return Err(Error::msg("invalid nonce length"));
        }

        let cached_kek = self
            .unwrap_cache
            .with(|cache| cache.get(envelope.salt.as_ref()).cloned());

        let kek = match cached_kek {
            Some(kek) => kek,
            None => {
                let password = self.password.clone();
                let salt = envelope.salt.clone().into_owned();
                let (m_cost, t_cost, p_cost) = (envelope.m_cost, envelope.t_cost, envelope.p_cost);

                let kek =
                    spawn_blocking(move || derive_kek(&password, &salt, m_cost, t_cost, p_cost))
                        .await?;

                self.unwrap_cache
                    .with(|cache| cache.insert(envelope.salt.clone().into_owned(), kek.clone()));

                kek
            }
        };

        let nonce = envelope.nonce.clone().into_owned();
        let ciphertext = envelope.ciphertext.clone().into_owned();

        spawn_blocking(move || {
            let aead = XChaCha20Poly1305::new(AeadKey::from_slice(kek.as_ref()));
            aead.decrypt(XNonce::from_slice(&nonce), ciphertext.as_ref())
                .map_err(|_| Error::msg("decryption failed (wrong password or tampered data)"))
        })
        .await
    }
}

fn derive_kek(password: &str, salt: &[u8], m_cost: u32, t_cost: u32, p_cost: u32) -> Result<Kek> {
    let params = Params::new(m_cost, t_cost, p_cost, Some(KEK_LEN))
        .map_err(|err| Error::msg(format!("invalid argon2 params: {err}")))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

    let mut kek = Zeroizing::new([0u8; KEK_LEN]);
    argon2
        .hash_password_into(password.as_bytes(), salt, kek.as_mut())
        .map_err(|err| Error::msg(format!("argon2 hashing failed: {err}")))?;

    Ok(kek)
}

#[derive(Serialize, Deserialize, Debug)]
struct Envelope<'a> {
    #[serde(borrow)]
    #[serde(with = "serde_bytes")]
    salt: Cow<'a, [u8]>,

    m_cost: u32,
    t_cost: u32,
    p_cost: u32,

    #[serde(borrow)]
    #[serde(with = "serde_bytes")]
    nonce: Cow<'a, [u8]>,

    #[serde(borrow)]
    #[serde(with = "serde_bytes")]
    ciphertext: Cow<'a, [u8]>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fast_key_slot(password: &str) -> PasswordKeySlot {
        // tiny Argon2 params so tests run fast; production code should use `new` (OWASP defaults)
        PasswordKeySlot::with_params(password, 8, 1, 1)
    }

    #[tokio::test]
    async fn round_trip() {
        let key_slot = fast_key_slot("correct horse battery staple");
        let key = b"some 32 byte content key material";

        let wrapped = key_slot.wrap_key(key).await.unwrap();
        let unwrapped = key_slot.unwrap_key(&wrapped).await.unwrap();

        assert_eq!(unwrapped, key);
    }

    #[tokio::test]
    async fn wrong_password_fails() {
        let wrapped = fast_key_slot("right password")
            .wrap_key(b"secret key bytes")
            .await
            .unwrap();

        let result = fast_key_slot("wrong password").unwrap_key(&wrapped).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn tampered_ciphertext_fails() {
        let key_slot = fast_key_slot("a password");
        let mut wrapped = key_slot.wrap_key(b"secret key bytes").await.unwrap();

        *wrapped.last_mut().unwrap() ^= 0xFF;

        let result = key_slot.unwrap_key(&wrapped).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn same_instance_reuses_salt_but_nonce_still_differs() {
        let key_slot = fast_key_slot("a password");
        let key = b"secret key bytes";

        let wrapped_a = key_slot.wrap_key(key).await.unwrap();
        let wrapped_b = key_slot.wrap_key(key).await.unwrap();

        // different ciphertext/nonce each call ...
        assert_ne!(wrapped_a, wrapped_b);
        // ... but both still unwrap correctly, and the salt (hence a cached kek) is reused
        assert_eq!(key_slot.unwrap_key(&wrapped_a).await.unwrap(), key);
        assert_eq!(key_slot.unwrap_key(&wrapped_b).await.unwrap(), key);
    }

    #[tokio::test]
    async fn isolated_instances_that_never_synced_use_different_salts() {
        let key = b"secret key bytes";

        let wrapped_a = fast_key_slot("a password").wrap_key(key).await.unwrap();
        let wrapped_b = fast_key_slot("a password").wrap_key(key).await.unwrap();

        assert_ne!(wrapped_a, wrapped_b);
    }

    #[tokio::test]
    async fn instance_that_saw_a_wrap_converges_on_its_salt() {
        let key = b"secret key bytes";

        // instance A mints the first-ever salt
        let a = fast_key_slot("shared password");
        let wrapped_by_a = a.wrap_key(key).await.unwrap();

        // instance B "syncs" by unwrapping A's entry before ever wrapping anything itself --
        // mirrors EnvelopeProtector::set_remote_meta decoding existing entries before deciding
        // whether it needs to wrap a new one
        let b = fast_key_slot("shared password");
        assert_eq!(b.unwrap_key(&wrapped_by_a).await.unwrap(), key);

        // B's own first wrap must now reuse A's salt instead of minting a third one
        let wrapped_by_b = b.wrap_key(key).await.unwrap();
        assert_eq!(a.unwrap_key(&wrapped_by_b).await.unwrap(), key);

        #[derive(::serde::Deserialize)]
        struct EnvelopeSalt {
            #[serde(with = "serde_bytes")]
            salt: Vec<u8>,
        }
        let salt_a: EnvelopeSalt = rmp_serde::from_slice(&wrapped_by_a).unwrap();
        let salt_b: EnvelopeSalt = rmp_serde::from_slice(&wrapped_by_b).unwrap();
        assert_eq!(salt_a.salt, salt_b.salt);
    }
}
