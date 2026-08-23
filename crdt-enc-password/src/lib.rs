//! A [`crdt_enc_envelope::KeySlotProtector`] that protects the content-encryption key with a
//! password: an Argon2id-derived key encrypts it with XChaCha20Poly1305. See [`PasswordKeySlot`]
//! for details.
#![warn(missing_docs)]

use ::agnostik::spawn_blocking;
use ::anyhow::{Context, Error, Result};
use ::argon2::{Algorithm, Argon2, Params, Version};
use ::chacha20poly1305::{Key as AeadKey, KeyInit, XChaCha20Poly1305, XNonce, aead::Aead};
use ::crdt_enc::utils::LockBox;
use ::crdt_enc_envelope::{KeySlotProtector, at_rest::AtRest};
use ::rand::{TryRng, rng};
use ::serde::{Deserialize, Serialize};
use ::std::{borrow::Cow, collections::HashMap};
use ::zeroize::Zeroizing;

/// The byte length of an Argon2 salt.
const SALT_LEN: usize = 16;
/// The byte length of an XChaCha20Poly1305 nonce.
const NONCE_LEN: usize = 24;
/// The byte length of an Argon2-derived key-encryption key (XChaCha20Poly1305's key size).
const KEK_LEN: usize = 32;

/// An Argon2-derived key-encryption key, encrypted at rest -- it directly protects the actual
/// content-encryption key, so it's just as sensitive as the password it's derived from.
type Kek = AtRest;

/// A [`KeySlotProtector`] that wraps/unwraps a key with a password: an Argon2id-derived key
/// encrypts it with XChaCha20Poly1305. Every wrapped blob carries its own salt (and the Argon2
/// parameters used), so it's independently unwrappable — no shared/pre-agreed salt between
/// devices is needed. Derived keys are cached per salt (Argon2 is deliberately slow, and
/// `wrap_key`/`unwrap_key` can otherwise be called repeatedly for the same salt, e.g. once per
/// `Core::read_remote_meta()` call), so it only runs again for a salt this instance hasn't seen
/// before.
///
/// The password is kept resident (encrypted at rest via [`AtRest`], decrypted only for the
/// brief moment an Argon2 derivation actually needs it) for this value's entire lifetime, not just
/// briefly at construction: `unwrap_key` must be able to derive the key for a salt it's never seen
/// before at any time (e.g. a new device joining sync later with its own independently-bootstrapped
/// entry), so there's no point at which it's safe to assume the password is no longer needed. See
/// `todo.md` for ideas on reducing this exposure further.
#[derive(Debug)]
pub struct PasswordKeySlot {
    /// The password, encrypted at rest -- see the struct doc comment.
    password: AtRest,
    /// Argon2id memory cost (KiB) for keys this instance derives.
    m_cost: u32,
    /// Argon2id time cost (iterations) for keys this instance derives.
    t_cost: u32,
    /// Argon2id parallelism for keys this instance derives.
    p_cost: u32,
    /// Derived keys seen so far, keyed by salt, for `unwrap_key` -- and, via `own_salt_kek`, also
    /// the source `wrap_key` uses to converge on a single shared salt across devices instead of
    /// each independently minting its own.
    unwrap_cache: LockBox<HashMap<Vec<u8>, Kek>>,
}

impl PasswordKeySlot {
    /// Creates a `PasswordKeySlot` using the OWASP-recommended minimum Argon2id parameters (19456
    /// KiB memory cost, 2 iterations, parallelism 1). Takes the password as an already-encrypted
    /// [`AtRest`] rather than a plaintext string -- encrypt it at rest as early as possible
    /// at the call site (e.g. right after reading it from stdin/a prompt), so it's never a plain
    /// `String` inside this crate at all.
    pub fn new(password: AtRest) -> PasswordKeySlot {
        Self::with_params(password, 19_456, 2, 1)
    }

    /// Creates a `PasswordKeySlot` with explicit Argon2id parameters (memory cost in KiB, time
    /// cost, parallelism). The chosen parameters are stored alongside each wrapped key, so
    /// changing them later doesn't break unwrapping keys wrapped with the old parameters. See
    /// [`Self::new`] on why `password` is an already-encrypted [`AtRest`].
    pub fn with_params(password: AtRest, m_cost: u32, t_cost: u32, p_cost: u32) -> PasswordKeySlot {
        PasswordKeySlot {
            password,
            m_cost,
            t_cost,
            p_cost,
            unwrap_cache: LockBox::new(HashMap::new()),
        }
    }

    /// Returns the (salt, derived key) pair `wrap_key` should use: the lexicographically smallest
    /// salt already known via `unwrap_cache` (e.g. from decoding another device's entry during
    /// the `set_remote_meta` merge that precedes this call, same min-by-id tiebreak philosophy as
    /// `Keys::latest_key()`), so devices converge on one shared salt instead of each picking their
    /// own. Only mints a genuinely fresh salt if none is known yet (true first-ever bootstrap).
    ///
    /// Recomputed on every call rather than cached: since every derived key is immediately
    /// inserted into `unwrap_cache`, this only costs an Argon2 run the very first time (or if a
    /// still-smaller salt from another device shows up later, which is strictly an improvement,
    /// not a regression) -- there's no cheaper alternative to buy by freezing the decision, since
    /// the cleartext password already has to stay resident for `unwrap_key`'s sake regardless (see
    /// the struct doc comment).
    async fn own_salt_kek(&self) -> Result<(Vec<u8>, Kek)> {
        if let Some(pair) = self.unwrap_cache.with(|cache| {
            cache
                .iter()
                .min_by(|a, b| a.0.cmp(b.0))
                .map(|(salt, kek)| (salt.clone(), kek.clone()))
        }) {
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

        self.unwrap_cache
            .with(|cache| cache.insert(salt.clone(), kek.clone()));

        Ok((salt, kek))
    }
}

impl KeySlotProtector for PasswordKeySlot {
    /// Encrypts `key` with an Argon2id-derived key (see `own_salt_kek`) using XChaCha20Poly1305
    /// with a fresh random nonce, and encodes the result -- salt, Argon2 parameters, nonce,
    /// ciphertext -- as a self-describing `Envelope`.
    async fn wrap_key(&self, key: &[u8]) -> Result<Vec<u8>> {
        let (salt, kek) = self.own_salt_kek().await?;

        let key = key.to_vec();
        let (m_cost, t_cost, p_cost) = (self.m_cost, self.t_cost, self.p_cost);

        spawn_blocking(move || {
            let mut nonce = [0u8; NONCE_LEN];
            rng()
                .try_fill_bytes(&mut nonce)
                .context("Unable to get random data for nonce")?;

            let aead = XChaCha20Poly1305::new(
                &AeadKey::try_from(kek.decrypt().as_bytes())
                    .expect("kek is always KEK_LEN bytes by construction"),
            );
            let ciphertext = aead
                .encrypt(&XNonce::from(nonce), key.as_ref())
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

    /// Parses `wrapped` as an `Envelope`, derives (or looks up in `unwrap_cache`) the key for its
    /// salt/parameters, and decrypts it. Fails if the envelope can't be parsed, its nonce is the
    /// wrong length, or decryption fails (wrong password or tampered data).
    async fn unwrap_key(&self, wrapped: &[u8]) -> Result<Vec<u8>> {
        let envelope: Envelope =
            rmp_serde::from_slice(wrapped).context("failed to parse password envelope")?;
        let nonce: [u8; NONCE_LEN] = envelope
            .nonce
            .as_ref()
            .try_into()
            .map_err(|_| Error::msg("invalid nonce length"))?;

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

        let ciphertext = envelope.ciphertext.clone().into_owned();

        spawn_blocking(move || {
            let aead = XChaCha20Poly1305::new(
                &AeadKey::try_from(kek.decrypt().as_bytes())
                    .expect("kek is always KEK_LEN bytes by construction"),
            );
            let xnonce = XNonce::from(nonce);
            aead.decrypt(&xnonce, ciphertext.as_ref())
                .map_err(|_| Error::msg("decryption failed (wrong password or tampered data)"))
        })
        .await
    }
}

/// Derives a `KEK_LEN`-byte key-encryption key from `password` and `salt` via Argon2id. Decrypts
/// `password` itself, right before actually hashing it, rather than requiring callers to have
/// already exposed it.
fn derive_kek(
    password: &AtRest,
    salt: &[u8],
    m_cost: u32,
    t_cost: u32,
    p_cost: u32,
) -> Result<Kek> {
    let params = Params::new(m_cost, t_cost, p_cost, Some(KEK_LEN))
        .map_err(|err| Error::msg(format!("invalid argon2 params: {err}")))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

    let mut kek = Zeroizing::new([0u8; KEK_LEN]);
    argon2
        .hash_password_into(password.decrypt().as_bytes(), salt, kek.as_mut())
        .map_err(|err| Error::msg(format!("argon2 hashing failed: {err}")))?;

    Ok(AtRest::encrypt(kek.as_ref()))
}

/// A self-describing wrapped key: everything needed to re-derive the same key-encryption key and
/// decrypt the ciphertext travels alongside it, so no shared/pre-agreed salt between devices is
/// needed.
#[derive(Serialize, Deserialize, Debug)]
struct Envelope<'a> {
    /// The Argon2 salt used to derive the key-encryption key.
    #[serde(borrow)]
    #[serde(with = "serde_bytes")]
    salt: Cow<'a, [u8]>,

    /// The Argon2id memory cost used.
    m_cost: u32,
    /// The Argon2id time cost used.
    t_cost: u32,
    /// The Argon2id parallelism used.
    p_cost: u32,

    /// The XChaCha20Poly1305 nonce used.
    #[serde(borrow)]
    #[serde(with = "serde_bytes")]
    nonce: Cow<'a, [u8]>,

    /// The encrypted key bytes.
    #[serde(borrow)]
    #[serde(with = "serde_bytes")]
    ciphertext: Cow<'a, [u8]>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fast_key_slot(password: &str) -> PasswordKeySlot {
        // tiny Argon2 params so tests run fast; production code should use `new` (OWASP defaults)
        PasswordKeySlot::with_params(AtRest::encrypt(password), 8, 1, 1)
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
