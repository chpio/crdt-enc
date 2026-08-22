use ::anyhow::Result;
use ::async_trait::async_trait;
use ::crdt_enc_envelope::KeySlotProtector;

pub fn init() {
    gpgme::init();
}

#[derive(Debug)]
pub struct KeyHandler;

impl KeyHandler {
    pub fn new() -> KeyHandler {
        KeyHandler
    }
}

impl Default for KeyHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl KeySlotProtector for KeyHandler {
    async fn wrap_key(&self, key: &[u8]) -> Result<Vec<u8>> {
        // TODO: encrypt for GPG recipients
        Ok(key.to_vec())
    }

    async fn unwrap_key(&self, wrapped: &[u8]) -> Result<Vec<u8>> {
        // TODO: decrypt via GPG
        Ok(wrapped.to_vec())
    }
}
