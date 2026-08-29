//! Covers the two ways a content key can come into existence more than once at a time: two devices
//! that each bootstrapped one before ever meeting, and two overlapping bootstraps inside a single
//! `EnvelopeProtector`.

use ::anyhow::Result;
use ::crdt_enc::{OpenOptions, protector::Protector, utils::EmptyCrdt};
use ::crdt_enc_envelope::{EnvelopeProtector, KeySlotProtector, utils::SecretBytes};
use ::crdt_enc_tokio::Storage;
use ::crdts::MVReg;
use ::serde::Deserialize;
use ::std::{
    fs,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use ::tokio::sync::Notify;
use ::uuid::Uuid;

const CURRENT_DATA_VERSION: Uuid = Uuid::from_u128(0x_0d5a8e17_2c94_4b3f_a6e1_58fb7429c0d6);
const SUPPORTED_DATA_VERSIONS: &[Uuid] = &[CURRENT_DATA_VERSION];

/// A `KeySlotProtector` that doesn't protect anything -- see `two_devices.rs` for why the real
/// implementations aren't used here.
#[derive(Debug)]
struct NoopKeySlot;

impl KeySlotProtector for NoopKeySlot {
    async fn wrap_key(&self, key: SecretBytes) -> Result<Vec<u8>> {
        Ok(key.expose_secret().to_vec())
    }

    async fn unwrap_key(&self, wrapped: Vec<u8>) -> Result<SecretBytes> {
        Ok(SecretBytes::new(wrapped))
    }
}

/// Merges `from` into `to` the way a file-sync tool eventually would, so two trees that were
/// written in isolation end up as one.
fn merge_tree(from: &Path, to: &Path) {
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            fs::create_dir_all(&target).unwrap();
            merge_tree(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// Just enough of `EncBox` to read back which content key a stored blob was encrypted with. The
/// real struct is private, and only the key id matters here.
#[derive(Deserialize)]
struct EncBoxKeyId {
    key_id: Uuid,
}

/// Reads the key id off the single op file under `<remote>/ops/<actor>/0`.
fn key_id_of_first_op(remote: &Path, actor: Uuid) -> Uuid {
    let bytes = fs::read(remote.join("ops").join(actor.to_string()).join("0")).unwrap();
    // outer wrapper written by `Core`, then the protector's own `DATA_VERSION` wrapper
    let core_box = ::crdt_enc::utils::VersionBytesRef::deserialize(&bytes).unwrap();
    let enc_box = ::crdt_enc::utils::VersionBytesRef::deserialize(core_box.as_ref()).unwrap();
    let enc_box: EncBoxKeyId = rmp_serde::from_slice(enc_box.as_ref()).unwrap();
    enc_box.key_id
}

/// Two devices that never synced each bootstrap their own content key, so the merged `Keys` CRDT
/// ends up with *two* keys concurrently marked latest. `Keys::latest_key()` has to break that tie
/// the same way on every device (smallest key id wins) -- otherwise the two would keep encrypting
/// new content under different keys forever, and each new device would inherit whichever one it
/// happened to look at first.
#[tokio::test]
async fn devices_that_bootstrapped_in_isolation_converge_on_one_key() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let remote_a = tmp.path().join("remote-a");
    let remote_b = tmp.path().join("remote-b");

    let core_a = crdt_enc::Core::open(OpenOptions {
        storage: Storage::new(tmp.path().join("device-a"), remote_a.clone())?,
        protector: EnvelopeProtector::new(NoopKeySlot),
        create: true,
        supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
        current_data_version: CURRENT_DATA_VERSION,
    })
    .await?;
    let actor_a = core_a.info().actor();
    core_a
        .read_and_apply(|s: &MVReg<u64, Uuid>| {
            let read_ctx = s.read();
            Ok(vec![s.write(1, read_ctx.derive_add_ctx(actor_a))])
        })
        .await?;

    let core_b = crdt_enc::Core::open(OpenOptions {
        storage: Storage::new(tmp.path().join("device-b"), remote_b.clone())?,
        protector: EnvelopeProtector::new(NoopKeySlot),
        create: true,
        supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
        current_data_version: CURRENT_DATA_VERSION,
    })
    .await?;
    let actor_b = core_b.info().actor();
    core_b
        .read_and_apply(|s: &MVReg<u64, Uuid>| {
            let read_ctx = s.read();
            Ok(vec![s.write(2, read_ctx.derive_add_ctx(actor_b))])
        })
        .await?;

    // the two keys really were independent
    assert_ne!(
        key_id_of_first_op(&remote_a, actor_a),
        key_id_of_first_op(&remote_b, actor_b),
    );

    // the trees finally meet
    let remote_merged = tmp.path().join("remote-merged");
    fs::create_dir_all(&remote_merged)?;
    merge_tree(&remote_a, &remote_merged);
    merge_tree(&remote_b, &remote_merged);

    let core_c = crdt_enc::Core::open(OpenOptions {
        storage: Storage::new(tmp.path().join("device-c"), remote_merged.clone())?,
        protector: EnvelopeProtector::new(NoopKeySlot),
        create: true,
        supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
        current_data_version: CURRENT_DATA_VERSION,
    })
    .await?;

    // old keys are never dropped, so content from before the merge stays readable ...
    core_c.read_remote().await?;
    let mut values = core_c.with_state(|s: &MVReg<u64, Uuid>| Ok(s.read().val))?;
    values.sort_unstable();
    assert_eq!(values, vec![1, 2]);

    // ... and new content goes under exactly one of the two, chosen deterministically
    let actor_c = core_c.info().actor();
    core_c
        .read_and_apply(|s: &MVReg<u64, Uuid>| {
            let read_ctx = s.read();
            Ok(vec![s.write(3, read_ctx.derive_add_ctx(actor_c))])
        })
        .await?;

    let chosen = key_id_of_first_op(&remote_merged, actor_c);
    let candidates = [
        key_id_of_first_op(&remote_merged, actor_a),
        key_id_of_first_op(&remote_merged, actor_b),
    ];
    assert_eq!(
        chosen,
        candidates.into_iter().min().unwrap(),
        "the tie must be broken by the smallest key id"
    );

    // a fourth device seeing the very same tree must land on the very same key
    let core_d = crdt_enc::Core::open(OpenOptions {
        storage: Storage::new(tmp.path().join("device-d"), remote_merged.clone())?,
        protector: EnvelopeProtector::new(NoopKeySlot),
        create: true,
        supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
        current_data_version: CURRENT_DATA_VERSION,
    })
    .await?;
    let actor_d = core_d.info().actor();
    core_d
        .read_and_apply(|s: &MVReg<u64, Uuid>| {
            let read_ctx = s.read();
            Ok(vec![s.write(4, read_ctx.derive_add_ctx(actor_d))])
        })
        .await?;
    assert_eq!(key_id_of_first_op(&remote_merged, actor_d), chosen);

    Ok(())
}

/// A `Protector` that protects nothing and never publishes any metadata of its own. The raced
/// bootstrap below drives a *second*, standalone `EnvelopeProtector` against a host `Core`, and
/// that only works if the host leaves the protector slice of the remote meta alone -- two
/// `EnvelopeProtector`s sharing one actor would otherwise derive colliding CRDT dots, which is a
/// hazard of the test setup rather than anything `EnvelopeProtector` needs to survive.
#[derive(Debug)]
struct PassThrough;

impl Protector for PassThrough {
    async fn encrypt(&self, clear_text: SecretBytes) -> Result<Vec<u8>> {
        Ok(clear_text.expose_secret().to_vec())
    }

    async fn decrypt(&self, enc_data: Vec<u8>) -> Result<SecretBytes> {
        Ok(SecretBytes::new(enc_data))
    }
}

/// A `KeySlotProtector` whose first `wrap_key` blocks until it's told to continue, so a test can
/// park one bootstrap mid-flight and let a second one catch up with it.
#[derive(Debug)]
struct GatedKeySlot {
    /// Fired when the first `wrap_key` has started.
    entered: Arc<Notify>,
    /// Awaited by the first `wrap_key` before it returns.
    release: Arc<Notify>,
    /// How many keys were actually minted.
    wraps: Arc<AtomicUsize>,
}

impl KeySlotProtector for GatedKeySlot {
    async fn wrap_key(&self, key: SecretBytes) -> Result<Vec<u8>> {
        if self.wraps.fetch_add(1, Ordering::SeqCst) == 0 {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(key.expose_secret().to_vec())
    }

    async fn unwrap_key(&self, wrapped: Vec<u8>) -> Result<SecretBytes> {
        Ok(SecretBytes::new(wrapped))
    }
}

/// `set_remote_meta` decides whether a key is missing *before* taking `key_write_lock`, so two
/// calls can both decide "I need to bootstrap" and then queue up behind each other. The one that
/// gets the lock second has to notice that the first already published one and back out --
/// minting a second key there would leave two keys concurrently marked latest for no reason, on a
/// single device that never even had a sync partner.
#[tokio::test]
async fn a_second_bootstrap_backs_out_once_the_first_has_published() -> Result<()> {
    let tmp = tempfile::tempdir()?;

    // a host `Core`, only needed as the `CoreSubHandle` the protector under test publishes to --
    // it holds no app data of its own, hence `EmptyCrdt`
    let core: Arc<crdt_enc::Core<EmptyCrdt, Storage, PassThrough>> =
        crdt_enc::Core::open(OpenOptions {
            storage: Storage::new(tmp.path().join("device"), tmp.path().join("remote"))?,
            protector: PassThrough,
            create: true,
            supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
            current_data_version: CURRENT_DATA_VERSION,
        })
        .await?;

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let wraps = Arc::new(AtomicUsize::new(0));

    let protector = EnvelopeProtector::new(GatedKeySlot {
        entered: entered.clone(),
        release: release.clone(),
        wraps: wraps.clone(),
    });
    protector
        .init(&core as &dyn crdt_enc::CoreSubHandle)
        .await?;

    let (first, second, ()) = ::tokio::join!(
        protector.set_remote_meta(None),
        protector.set_remote_meta(None),
        async {
            // let the first bootstrap finish only once the second is already queued
            entered.notified().await;
            release.notify_one();
        },
    );
    first?;
    second?;

    assert_eq!(
        wraps.load(Ordering::SeqCst),
        1,
        "the second bootstrap should have backed out instead of minting another key"
    );

    // and the protector is left in a usable state, with exactly that one key
    let cipher = protector
        .encrypt(SecretBytes::new(b"hello".to_vec()))
        .await?;
    assert_eq!(protector.decrypt(cipher).await?.expose_secret(), b"hello");

    Ok(())
}
