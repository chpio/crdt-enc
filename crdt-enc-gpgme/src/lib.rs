use ::anyhow::Result;
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
