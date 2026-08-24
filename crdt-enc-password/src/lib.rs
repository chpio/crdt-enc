//! A [`crdt_enc_envelope::KeySlotProtector`] that protects the content-encryption key with a
//! password: an Argon2id-derived key encrypts it with XChaCha20Poly1305. See [`PasswordKeySlot`]
//! for details.
#![warn(missing_docs)]

use ::anyhow::{Context, Error, Result};
use ::argon2::{Algorithm, Argon2, Params, Version};
use ::chacha20poly1305::{Key as AeadKey, KeyInit, XChaCha20Poly1305, XNonce, aead::Aead};
use ::crdt_enc::utils::{LockBox, VersionBytesRef};
use ::crdt_enc_envelope::{KeySlotProtector, at_rest::AtRest};
use ::rand::{TryRng, rng};
use ::serde::{Deserialize, Serialize};
use ::std::collections::HashMap;
use ::tokio::task::spawn_blocking;
use ::uuid::Uuid;
use ::zeroize::Zeroizing;

/// version of the wrapped-key envelope format
const ENVELOPE_VERSION: Uuid = Uuid::from_u128(0x_3dd69616_4892_4088_9143_c40025e6e11e);

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
    unwrap_cache: LockBox<HashMap<[u8; SALT_LEN], Kek>>,
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
    async fn own_salt_kek(&self) -> Result<([u8; SALT_LEN], Kek)> {
        if let Some(pair) = self.unwrap_cache.with(|cache| {
            cache
                .iter()
                .min_by(|a, b| a.0.cmp(b.0))
                .map(|(salt, kek)| (*salt, kek.clone()))
        }) {
            return Ok(pair);
        }

        let password = self.password.clone();
        let (m_cost, t_cost, p_cost) = (self.m_cost, self.t_cost, self.p_cost);

        let (salt, kek) = spawn_blocking(move || {
            let mut salt = [0u8; SALT_LEN];
            rng()
                .try_fill_bytes(&mut salt)
                .context("Unable to get random data for salt")?;
            let kek = derive_kek(&password, &salt, m_cost, t_cost, p_cost)?;
            Result::<_, Error>::Ok((salt, kek))
        })
        .await??;

        self.unwrap_cache
            .with(|cache| cache.insert(salt, kek.clone()));

        Ok((salt, kek))
    }
}

impl KeySlotProtector for PasswordKeySlot {
    /// Encrypts `key` with an Argon2id-derived key (see `own_salt_kek`) using XChaCha20Poly1305
    /// with a fresh random nonce, and encodes the result -- salt, Argon2 parameters, nonce,
    /// ciphertext -- as a self-describing `Envelope`, tagged with `ENVELOPE_VERSION` so the format
    /// can evolve safely.
    async fn wrap_key(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        let (salt, kek) = self.own_salt_kek().await?;

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
                salt,
                m_cost,
                t_cost,
                p_cost,
                nonce,
                ciphertext,
            };
            let envelope_bytes =
                rmp_serde::to_vec_named(&envelope).context("failed to encode password envelope")?;

            Ok(VersionBytesRef::new(ENVELOPE_VERSION, &envelope_bytes).serialize())
        })
        .await?
    }

    /// Parses `wrapped` as an `Envelope`, derives (or looks up in `unwrap_cache`) the key for its
    /// salt/parameters, and decrypts it. Fails if the envelope can't be parsed, its version tag
    /// doesn't match `ENVELOPE_VERSION`, its nonce is the wrong length, or decryption fails (wrong
    /// password or tampered data).
    async fn unwrap_key(&self, wrapped: Vec<u8>) -> Result<Zeroizing<Vec<u8>>> {
        let version_box =
            VersionBytesRef::deserialize(&wrapped).context("failed to parse password envelope")?;
        version_box
            .ensure_version(ENVELOPE_VERSION)
            .context("not matching version of password envelope")?;
        let envelope: Envelope = rmp_serde::from_slice(version_box.as_ref())
            .context("failed to parse password envelope")?;

        let cached_kek = self
            .unwrap_cache
            .with(|cache| cache.get(&envelope.salt).cloned());

        let kek = match cached_kek {
            Some(kek) => kek,
            None => {
                let password = self.password.clone();
                let salt = envelope.salt;
                let (m_cost, t_cost, p_cost) = (envelope.m_cost, envelope.t_cost, envelope.p_cost);

                let kek =
                    spawn_blocking(move || derive_kek(&password, &salt, m_cost, t_cost, p_cost))
                        .await??;

                self.unwrap_cache
                    .with(|cache| cache.insert(envelope.salt, kek.clone()));

                kek
            }
        };

        spawn_blocking(move || {
            let aead = XChaCha20Poly1305::new(
                &AeadKey::try_from(kek.decrypt().as_bytes())
                    .expect("kek is always KEK_LEN bytes by construction"),
            );
            let xnonce = XNonce::from(envelope.nonce);
            aead.decrypt(&xnonce, envelope.ciphertext.as_ref())
                .map(Zeroizing::new)
                .map_err(|_| Error::msg("decryption failed (wrong password or tampered data)"))
        })
        .await?
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
/// needed. Wrapped in an outer `VersionBytesRef`/`ENVELOPE_VERSION` tag by `wrap_key`/`unwrap_key`.
#[derive(Serialize, Deserialize, Debug)]
struct Envelope {
    /// The Argon2 salt used to derive the key-encryption key.
    #[serde(with = "serde_bytes")]
    salt: [u8; SALT_LEN],

    /// The Argon2id memory cost used.
    m_cost: u32,
    /// The Argon2id time cost used.
    t_cost: u32,
    /// The Argon2id parallelism used.
    p_cost: u32,

    /// The XChaCha20Poly1305 nonce used.
    #[serde(with = "serde_bytes")]
    nonce: [u8; NONCE_LEN],

    /// The encrypted key bytes.
    #[serde(with = "serde_bytes")]
    ciphertext: Vec<u8>,
}
