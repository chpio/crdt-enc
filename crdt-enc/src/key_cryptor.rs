use crate::{
    utils::{VersionBytes, VersionBytesRef},
    CoreSubHandle,
};
use ::anyhow::Result;
use ::async_trait::async_trait;
use ::crdts::{CmRDT, CvRDT, MVReg, Orswot};
use ::serde::{Deserialize, Serialize};
use ::std::{
    borrow::Borrow,
    cmp::{Eq, Ord, Ordering, PartialEq},
    convert::Infallible,
    fmt::Debug,
    hash::{Hash, Hasher},
};
use ::uuid::Uuid;

#[async_trait]
pub trait KeyCryptor
where
    Self: 'static + Debug + Send + Sync + Sized,
{
    async fn init(&self, _core: &dyn CoreSubHandle) -> Result<()> {
        Ok(())
    }

    async fn set_remote_meta(&self, _data: Option<MVReg<VersionBytes, Uuid>>) -> Result<()> {
        Ok(())
    }

    async fn set_keys(&self, keys: Keys) -> Result<()>;
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Keys {
    latest_key_id: MVReg<Uuid, Uuid>,
    keys: Orswot<Key, Uuid>,
}

impl CvRDT for Keys {
    type Validation = Infallible;

    fn validate_merge(&self, _other: &Self) -> Result<(), Infallible> {
        Ok(())
    }

    fn merge(&mut self, other: Keys) {
        self.latest_key_id.merge(other.latest_key_id);
        self.keys.merge(other.keys);
    }
}

impl Keys {
    pub fn get_key(&self, key_id: Uuid) -> Option<Key> {
        self.keys.read().val.take(&key_id)
    }

    pub fn latest_key(&self) -> Option<Key> {
        let mut keys = self.keys.read().val;
        self.latest_key_id
            .read()
            .val
            .into_iter()
            .flat_map(move |id| keys.take(&id))
            .min()
    }

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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Key {
    id: Uuid,
    key: VersionBytes,
}

impl Key {
    pub fn new(key: VersionBytes) -> Key {
        Self::new_with_id(Uuid::new_v4(), key)
    }

    pub fn new_with_id(id: Uuid, key: VersionBytes) -> Key {
        Key { id, key }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn key(&self) -> VersionBytesRef<'_> {
        self.key.as_version_bytes_ref()
    }
}

impl Borrow<Uuid> for Key {
    fn borrow(&self) -> &Uuid {
        &self.id
    }
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for Key {}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}
