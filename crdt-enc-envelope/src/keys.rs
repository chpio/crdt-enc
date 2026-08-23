use crate::at_rest::AtRest;
use ::anyhow::Result;
use ::crdt_enc::utils::VersionBytes;
use ::crdts::{CmRDT, CvRDT, MVReg, Orswot};
use ::serde::{Deserialize, Deserializer, Serialize, Serializer};
use ::std::{
    borrow::Borrow,
    cmp::{Eq, Ord, Ordering, PartialEq},
    convert::Infallible,
    hash::{Hash, Hasher},
};
use ::uuid::Uuid;

/// The set of every content-encryption key this device knows about (an `Orswot`, so keys are never
/// truly removed, only added -- old keys stay around so content encrypted with them stays readable),
/// plus which one is current (`latest_key_id`, an `MVReg`). Converges via `Key: Ord`'s min-by-id
/// tiebreak when multiple actors concurrently create the first key before ever syncing.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Keys {
    /// The id of the key new content should be encrypted with, as seen by this device.
    latest_key_id: MVReg<Uuid, Uuid>,
    /// Every key this device has ever learned about, keyed by `Key::id` (via `Borrow<Uuid>`).
    keys: Orswot<Key, Uuid>,
}

impl CvRDT for Keys {
    /// Merging two `Keys` can never fail.
    type Validation = Infallible;

    /// Always succeeds; see `Validation`.
    fn validate_merge(&self, _other: &Self) -> Result<(), Infallible> {
        Ok(())
    }

    /// Merges both the key set and the latest-key register independently.
    fn merge(&mut self, other: Keys) {
        self.latest_key_id.merge(other.latest_key_id);
        self.keys.merge(other.keys);
    }
}

impl Keys {
    /// Looks up a specific key by id, e.g. to decrypt content that was tagged with it (not
    /// necessarily the current latest key).
    pub fn get_key(&self, key_id: Uuid) -> Option<Key> {
        self.keys.read().val.take(&key_id)
    }

    /// The key new content should be encrypted with, or `None` if no key has ever been established.
    /// If `latest_key_id` concurrently holds more than one value (two devices bootstrapped or
    /// rotated at the same time before syncing), deterministically picks the smallest by id so
    /// every device converges on the same answer. Panics if `latest_key_id` names a key that isn't
    /// in `keys` -- a bug in whatever inserted it, since `insert_latest_key` always adds both
    /// together.
    pub fn latest_key(&self) -> Option<Key> {
        let mut keys = self.keys.read().val;
        self.latest_key_id
            .read()
            .val
            .into_iter()
            .map(move |id| {
                keys.take(&id)
                    .unwrap_or_else(|| panic!("Could not find key for latest key id {}", id))
            })
            .min()
    }

    /// Adds `new_key` to the known key set and marks it as the latest, as one atomic pair of CRDT
    /// ops from `actor`.
    pub fn insert_latest_key(&mut self, actor: Uuid, new_key: Key) {
        let key_id = new_key.id();

        let write_ctx = self.keys.read_ctx().derive_add_ctx(actor);
        let op = self.keys.add(new_key, write_ctx);
        self.keys.apply(op);

        let write_ctx = self.latest_key_id.read_ctx().derive_add_ctx(actor);
        let op = self.latest_key_id.write(key_id, write_ctx);
        self.latest_key_id.apply(op);
    }
}

/// One content-encryption key: a random id (stable across rotations, used to tag encrypted content
/// so it stays decryptable after a later rotation) plus the raw key bytes -- kept encrypted at rest
/// under `REST_KEY` while held in memory, decrypted only for the brief moment `Key::key` is called.
#[derive(Clone, Debug)]
pub struct Key {
    /// This key's id.
    id: Uuid,
    /// The key bytes, encrypted under `REST_KEY`.
    key: AtRestKey,
}

impl Key {
    /// Creates a key with a fresh random id.
    pub fn new(key: VersionBytes) -> Key {
        Self::new_with_id(Uuid::new_v4(), key)
    }

    /// Creates a key with an explicit id, e.g. when reconstructing one that already has one.
    pub fn new_with_id(id: Uuid, key: VersionBytes) -> Key {
        Key {
            id,
            key: AtRestKey::encrypt(key),
        }
    }

    /// This key's id.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// The raw key bytes, decrypted from their at-rest encryption.
    pub fn key(&self) -> VersionBytes {
        self.key.decrypt()
    }
}

/// Wire-format mirror of `Key`, used only by `Key`'s hand-written `Serialize`/`Deserialize` --
/// `Key`'s own fields hold the at-rest-encrypted form, which is process-local and must never be
/// what's actually sent over the wire/stored in the synced `Keys` CRDT.
#[derive(Serialize, Deserialize)]
struct KeyWire {
    /// See `Key::id`.
    id: Uuid,
    /// See `Key::key`.
    key: VersionBytes,
}

impl Serialize for Key {
    /// Decrypts the at-rest-encrypted key material and serializes it via `KeyWire`.
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        KeyWire {
            id: self.id,
            key: self.key(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Key {
    /// Deserializes via `KeyWire`, then re-encrypts the key material at rest.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let wire = KeyWire::deserialize(deserializer)?;
        Ok(Key::new_with_id(wire.id, wire.key))
    }
}

/// A content-encryption key's raw bytes, encrypted at rest via the generic [`AtRest`] primitive.
/// The version tag itself isn't sensitive, so it's kept alongside in the clear rather than as part
/// of the protected payload.
#[derive(Clone, Debug)]
struct AtRestKey {
    /// The plaintext key's version tag.
    version: Uuid,
    /// The raw key bytes, encrypted at rest.
    content: AtRest,
}

impl AtRestKey {
    /// Encrypts `key`'s content at rest, keeping its version tag alongside in the clear.
    fn encrypt(key: VersionBytes) -> AtRestKey {
        let version = key.version();
        let content: Vec<u8> = key.into();

        AtRestKey {
            version,
            content: AtRest::encrypt(content),
        }
    }

    /// Reverses `encrypt`.
    fn decrypt(&self) -> VersionBytes {
        VersionBytes::new(self.version, self.content.decrypt().as_bytes().to_vec())
    }
}

impl Borrow<Uuid> for Key {
    /// Lets `Orswot<Key, Uuid>` look keys up by id without needing a whole `Key` to hash/compare.
    fn borrow(&self) -> &Uuid {
        &self.id
    }
}

impl Hash for Key {
    /// Hashes only the id, consistent with `Eq`/`Borrow<Uuid>`.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for Key {
    /// Two keys are equal iff their ids match (their key bytes are then implicitly identical too,
    /// since ids are randomly generated per key).
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for Key {}

impl PartialOrd for Key {
    /// See `Ord`.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl Ord for Key {
    /// Orders keys by id -- the deterministic tiebreak `Keys::latest_key` uses when multiple keys
    /// are concurrently marked latest.
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}
