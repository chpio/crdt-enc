//! End-to-end `Core` tests against an in-memory `Storage`/`Protector` pair.
//!
//! The point of the mocks is not to be a second `crdt-enc-tokio` -- it is that they can *misbehave
//! on demand*. A large part of `Core`'s job is noticing when storage or crypto goes wrong (a blob
//! it can't decrypt, a version it doesn't know, an op log with a hole in it), and none of those
//! paths can be reached through a backend that works. So both mocks take an injected fault, and
//! many tests here are "make exactly one thing go wrong, assert `Core` reports it instead of
//! silently carrying on with corrupted state".

use ::anyhow::{Error, Result};
use ::crdt_enc::{
    Core, CoreSubHandle, OpenOptions,
    protector::Protector,
    storage::Storage,
    utils::{LockBox, VersionBytes},
};
use ::crdts::{CmRDT, MVReg};
use ::serde::{Deserialize, Serialize};
use ::std::{collections::BTreeMap, sync::Arc};
use ::uuid::Uuid;

const CURRENT_DATA_VERSION: Uuid = Uuid::from_u128(0x_6f21b4c9_0d3e_4a7b_9c85_2e10d7f6ab34);
const OTHER_DATA_VERSION: Uuid = Uuid::from_u128(0x_c0ffee00_0000_4000_8000_000000000000);
const SUPPORTED_DATA_VERSIONS: &[Uuid] = &[CURRENT_DATA_VERSION];

/// The CRDT `Core` is parameterised over in these tests.
type State = MVReg<u64, Uuid>;
type TestCore = Core<State, MemStorage, MemProtector>;

// ---------------------------------------------------------------------------------------------
// fault injection
// ---------------------------------------------------------------------------------------------

/// One storage operation, so a test can name the single call it wants to see fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageFault {
    Init,
    SetRemoteMeta,
    LoadLocalMeta,
    StoreLocalMeta,
    ListRemoteMetaNames,
    LoadRemoteMetas,
    StoreRemoteMeta,
    RemoveRemoteMetas,
    ListStateNames,
    LoadStates,
    StoreState,
    RemoveStates,
    ListOpActors,
    LoadOps,
    StoreOps,
    RemoveOps,
}

/// One protector operation, ditto.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProtectorFault {
    Init,
    SetRemoteMeta,
    Encrypt,
    Decrypt,
}

/// A way for storage to hand back a blob that isn't quite what it was given -- what a corrupted,
/// half-synced, or foreign-format file looks like from `Core`'s side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tamper {
    /// Rewrite the outer version tag of every loaded blob, as an older/newer format would.
    OuterVersion,
    /// Replace the local meta's payload with something that isn't msgpack.
    LocalMetaBody,
    /// Same, for remote meta.
    RemoteMetaBody,
    /// Re-serve ops from version 0 no matter which version was asked for -- what a reader racing
    /// with another `read_remote_ops` call on the same `Core` sees.
    ReplayOpsFromZero,
    /// Drop each actor's first op, leaving a hole the documented `load_ops` contract forbids.
    OpGap,
}

/// How the protector should mangle the plaintext it hands back, to exercise `Core`'s checks on the
/// *inner* payload it just decrypted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Garble {
    /// Too short to even carry a version tag.
    TooShort,
    /// Well-formed, but tagged with a data version this `Core` wasn't opened with.
    WrongDataVersion,
    /// Correctly tagged, but the payload isn't msgpack.
    NotMsgpack,
}

// ---------------------------------------------------------------------------------------------
// the in-memory storage
// ---------------------------------------------------------------------------------------------

/// The synced tree, shared by every device in a test.
#[derive(Debug, Default)]
struct Remote {
    metas: BTreeMap<String, VersionBytes>,
    states: BTreeMap<String, VersionBytes>,
    ops: BTreeMap<(Uuid, u64), VersionBytes>,
    /// Hands out unique blob names. The real backend content-addresses instead; `Core` only
    /// requires that a name identify one immutable blob -- across *all* devices, hence the counter
    /// living here rather than per `MemStorage`.
    next_name: u64,
}

/// An in-memory [`Storage`]: `local` is per device, `remote` is shared between them, and
/// `fault`/`tamper` let a test break exactly one thing.
#[derive(Debug)]
struct MemStorage {
    local: Arc<LockBox<Option<VersionBytes>>>,
    remote: Arc<LockBox<Remote>>,
    fault: Arc<LockBox<Option<StorageFault>>>,
    tamper: Arc<LockBox<Option<Tamper>>>,
    /// The handle handed to `Storage::init`, kept so a test can drive `Core` through the
    /// object-safe `dyn CoreSubHandle` interface rather than its inherent methods.
    handle: Arc<LockBox<Option<Box<dyn CoreSubHandle>>>>,
    /// Every value `Storage::set_remote_meta` was notified with, in order.
    notified: Arc<LockBox<Vec<Option<MVReg<VersionBytes, Uuid>>>>>,
}

impl MemStorage {
    fn fault_if(&self, which: StorageFault) -> Result<()> {
        if self.fault.with(|fault| *fault == Some(which)) {
            return Err(Error::msg(format!("injected storage fault: {:?}", which)));
        }
        Ok(())
    }

    fn tamper(&self) -> Option<Tamper> {
        self.tamper.with(|tamper| *tamper)
    }

    /// Applies an `OuterVersion` tamper, if one is armed, to a blob on its way back to `Core`.
    fn maybe_retag(&self, blob: VersionBytes) -> VersionBytes {
        match self.tamper() {
            Some(Tamper::OuterVersion) => VersionBytes::new(Uuid::nil(), blob.into()),
            _ => blob,
        }
    }

    fn fresh_name(&self) -> String {
        self.remote.with(|remote| {
            remote.next_name += 1;
            format!("blob-{}", remote.next_name)
        })
    }
}

impl Storage for MemStorage {
    async fn init(&self, core: &dyn CoreSubHandle) -> Result<()> {
        self.fault_if(StorageFault::Init)?;
        self.handle
            .with(|handle| *handle = Some(dyn_clone::clone_box(core)));
        Ok(())
    }

    async fn set_remote_meta(&self, data: Option<MVReg<VersionBytes, Uuid>>) -> Result<()> {
        self.fault_if(StorageFault::SetRemoteMeta)?;
        self.notified.with(|seen| seen.push(data));
        Ok(())
    }

    async fn load_local_meta(&self) -> Result<Option<VersionBytes>> {
        self.fault_if(StorageFault::LoadLocalMeta)?;
        let meta = self.local.with(|local| local.clone());
        Ok(match (meta, self.tamper()) {
            (Some(meta), Some(Tamper::OuterVersion)) => {
                Some(VersionBytes::new(Uuid::nil(), meta.into()))
            }
            (Some(meta), Some(Tamper::LocalMetaBody)) => {
                Some(VersionBytes::new(meta.version(), vec![0xc1]))
            }
            (meta, _) => meta,
        })
    }

    async fn store_local_meta(&self, data: VersionBytes) -> Result<()> {
        self.fault_if(StorageFault::StoreLocalMeta)?;
        self.local.with(|local| *local = Some(data));
        Ok(())
    }

    async fn list_remote_meta_names(&self) -> Result<Vec<String>> {
        self.fault_if(StorageFault::ListRemoteMetaNames)?;
        Ok(self
            .remote
            .with(|remote| remote.metas.keys().cloned().collect()))
    }

    async fn load_remote_metas(&self, names: Vec<String>) -> Result<Vec<(String, VersionBytes)>> {
        self.fault_if(StorageFault::LoadRemoteMetas)?;
        let tamper = self.tamper();
        self.remote.try_with(|remote| {
            names
                .into_iter()
                .map(|name| {
                    let blob = remote
                        .metas
                        .get(&name)
                        .cloned()
                        .ok_or_else(|| Error::msg(format!("no remote meta {}", name)))?;
                    let blob = match tamper {
                        Some(Tamper::RemoteMetaBody) => {
                            VersionBytes::new(blob.version(), vec![0xc1])
                        }
                        _ => self.maybe_retag(blob),
                    };
                    Ok((name, blob))
                })
                .collect::<Result<Vec<_>>>()
        })
    }

    async fn store_remote_meta(&self, data: VersionBytes) -> Result<String> {
        self.fault_if(StorageFault::StoreRemoteMeta)?;
        let name = self.fresh_name();
        self.remote
            .with(|remote| remote.metas.insert(name.clone(), data));
        Ok(name)
    }

    async fn remove_remote_metas(&self, names: Vec<String>) -> Result<()> {
        self.fault_if(StorageFault::RemoveRemoteMetas)?;
        self.remote.with(|remote| {
            for name in names {
                remote.metas.remove(&name);
            }
        });
        Ok(())
    }

    async fn list_state_names(&self) -> Result<Vec<String>> {
        self.fault_if(StorageFault::ListStateNames)?;
        Ok(self
            .remote
            .with(|remote| remote.states.keys().cloned().collect()))
    }

    async fn load_states(&self, names: Vec<String>) -> Result<Vec<(String, VersionBytes)>> {
        self.fault_if(StorageFault::LoadStates)?;
        self.remote.try_with(|remote| {
            names
                .into_iter()
                .map(|name| {
                    let blob = remote
                        .states
                        .get(&name)
                        .cloned()
                        .ok_or_else(|| Error::msg(format!("no state {}", name)))?;
                    Ok((name, self.maybe_retag(blob)))
                })
                .collect::<Result<Vec<_>>>()
        })
    }

    async fn store_state(&self, data: VersionBytes) -> Result<String> {
        self.fault_if(StorageFault::StoreState)?;
        let name = self.fresh_name();
        self.remote
            .with(|remote| remote.states.insert(name.clone(), data));
        Ok(name)
    }

    async fn remove_states(&self, names: Vec<String>) -> Result<Vec<String>> {
        self.fault_if(StorageFault::RemoveStates)?;
        self.remote.with(|remote| {
            for name in &names {
                remote.states.remove(name);
            }
        });
        Ok(names)
    }

    async fn list_op_actors(&self) -> Result<Vec<Uuid>> {
        self.fault_if(StorageFault::ListOpActors)?;
        Ok(self.remote.with(|remote| {
            let mut actors: Vec<_> = remote.ops.keys().map(|(actor, _)| *actor).collect();
            actors.dedup();
            actors
        }))
    }

    async fn load_ops(
        &self,
        actor_first_versions: Vec<(Uuid, u64)>,
    ) -> Result<Vec<(Uuid, u64, VersionBytes)>> {
        self.fault_if(StorageFault::LoadOps)?;
        let tamper = self.tamper();
        Ok(self.remote.with(|remote| {
            let mut out = Vec::new();
            for (actor, first_version) in actor_first_versions {
                let first_version = match tamper {
                    Some(Tamper::ReplayOpsFromZero) => 0,
                    Some(Tamper::OpGap) => first_version + 1,
                    _ => first_version,
                };
                // contiguous from `first_version`, stopping at the first hole -- the contract
                // `Storage::load_ops` documents and `Core::read_remote_ops` relies on
                for version in first_version.. {
                    let Some(blob) = remote.ops.get(&(actor, version)) else {
                        break;
                    };
                    out.push((actor, version, self.maybe_retag(blob.clone())));
                }
            }
            out
        }))
    }

    async fn store_ops(&self, actor: Uuid, version: u64, data: VersionBytes) -> Result<()> {
        self.fault_if(StorageFault::StoreOps)?;
        self.remote
            .with(|remote| remote.ops.insert((actor, version), data));
        Ok(())
    }

    async fn remove_ops(&self, actor_last_versions: Vec<(Uuid, u64)>) -> Result<()> {
        self.fault_if(StorageFault::RemoveOps)?;
        self.remote.with(|remote| {
            for (actor, last_version) in actor_last_versions {
                remote
                    .ops
                    .retain(|(a, version), _| *a != actor || *version > last_version);
            }
        });
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------
// the in-memory protector
// ---------------------------------------------------------------------------------------------

/// An in-memory [`Protector`] that "encrypts" by inverting every byte -- enough that `Core`'s round
/// trips fail loudly if `encrypt`/`decrypt` were ever skipped, without dragging real crypto into
/// tests about `Core`'s own bookkeeping.
#[derive(Debug)]
struct MemProtector {
    fault: Arc<LockBox<Option<ProtectorFault>>>,
    garble: Arc<LockBox<Option<Garble>>>,
    handle: Arc<LockBox<Option<Box<dyn CoreSubHandle>>>>,
    notified: Arc<LockBox<Vec<Option<MVReg<VersionBytes, Uuid>>>>>,
}

impl MemProtector {
    fn fault_if(&self, which: ProtectorFault) -> Result<()> {
        if self.fault.with(|fault| *fault == Some(which)) {
            return Err(Error::msg(format!("injected protector fault: {:?}", which)));
        }
        Ok(())
    }
}

fn flip(buf: Vec<u8>) -> Vec<u8> {
    buf.into_iter().map(|byte| !byte).collect()
}

impl Protector for MemProtector {
    async fn init(&self, core: &dyn CoreSubHandle) -> Result<()> {
        self.fault_if(ProtectorFault::Init)?;
        self.handle
            .with(|handle| *handle = Some(dyn_clone::clone_box(core)));
        Ok(())
    }

    async fn set_remote_meta(&self, data: Option<MVReg<VersionBytes, Uuid>>) -> Result<()> {
        self.fault_if(ProtectorFault::SetRemoteMeta)?;
        self.notified.with(|seen| seen.push(data));
        Ok(())
    }

    async fn encrypt(&self, clear_text: Vec<u8>) -> Result<Vec<u8>> {
        self.fault_if(ProtectorFault::Encrypt)?;
        Ok(flip(clear_text))
    }

    async fn decrypt(&self, enc_data: Vec<u8>) -> Result<Vec<u8>> {
        self.fault_if(ProtectorFault::Decrypt)?;
        Ok(match self.garble.with(|garble| *garble) {
            None => flip(enc_data),
            Some(Garble::TooShort) => vec![0, 1, 2],
            Some(Garble::WrongDataVersion) => {
                VersionBytes::new(OTHER_DATA_VERSION, vec![0x90]).serialize()
            }
            Some(Garble::NotMsgpack) => {
                VersionBytes::new(CURRENT_DATA_VERSION, vec![0xc1]).serialize()
            }
        })
    }
}

// ---------------------------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------------------------

/// One opened `Core` plus the knobs that reach into the mocks behind it -- `Core` exposes its
/// protector but not its storage, so the shared handles are kept here instead.
#[derive(Debug)]
struct Device {
    core: Arc<TestCore>,
    local: Arc<LockBox<Option<VersionBytes>>>,
    storage_fault: Arc<LockBox<Option<StorageFault>>>,
    protector_fault: Arc<LockBox<Option<ProtectorFault>>>,
    tamper: Arc<LockBox<Option<Tamper>>>,
    garble: Arc<LockBox<Option<Garble>>>,
    storage_handle: Arc<LockBox<Option<Box<dyn CoreSubHandle>>>>,
    storage_notified: Arc<LockBox<Vec<Option<MVReg<VersionBytes, Uuid>>>>>,
    protector_notified: Arc<LockBox<Vec<Option<MVReg<VersionBytes, Uuid>>>>>,
}

impl Device {
    fn actor(&self) -> Uuid {
        self.core.info().actor()
    }

    /// The `dyn CoreSubHandle` a `Storage`/`Protector` implementation holds onto.
    fn sub_handle(&self) -> Box<dyn CoreSubHandle> {
        self.storage_handle
            .with(|handle| dyn_clone::clone_box(&**handle.as_ref().expect("init has run")))
    }

    /// Writes `val` into the register, the way an app would.
    async fn write(&self, val: u64) -> Result<()> {
        let actor = self.actor();
        self.core
            .read_and_apply(|state: &State| {
                let read_ctx = state.read();
                Ok(vec![state.write(val, read_ctx.derive_add_ctx(actor))])
            })
            .await
    }

    fn values(&self) -> Vec<u64> {
        self.core
            .with_state(|state: &State| Ok(state.read().val))
            .unwrap()
    }

    fn arm_storage(&self, fault: Option<StorageFault>) {
        self.storage_fault.with(|slot| *slot = fault);
    }

    fn arm_protector(&self, fault: Option<ProtectorFault>) {
        self.protector_fault.with(|slot| *slot = fault);
    }

    fn arm_tamper(&self, tamper: Option<Tamper>) {
        self.tamper.with(|slot| *slot = tamper);
    }

    fn arm_garble(&self, garble: Option<Garble>) {
        self.garble.with(|slot| *slot = garble);
    }
}

/// Everything needed to open a `Core`, so a test can reopen the same device (same `local`) or add
/// another one against the same `remote`.
#[derive(Clone)]
struct Builder {
    local: Arc<LockBox<Option<VersionBytes>>>,
    remote: Arc<LockBox<Remote>>,
    create: bool,
    current_data_version: Uuid,
    storage_fault: Arc<LockBox<Option<StorageFault>>>,
    protector_fault: Arc<LockBox<Option<ProtectorFault>>>,
    tamper: Arc<LockBox<Option<Tamper>>>,
    garble: Arc<LockBox<Option<Garble>>>,
}

impl Builder {
    /// A brand-new device against a brand-new shared remote.
    fn new() -> Builder {
        Builder::joining(Arc::new(LockBox::new(Remote::default())))
    }

    /// A brand-new device against an existing shared remote, i.e. a second device syncing in.
    fn joining(remote: Arc<LockBox<Remote>>) -> Builder {
        Builder {
            local: Arc::new(LockBox::new(None)),
            remote,
            create: true,
            current_data_version: CURRENT_DATA_VERSION,
            storage_fault: Arc::new(LockBox::new(None)),
            protector_fault: Arc::new(LockBox::new(None)),
            tamper: Arc::new(LockBox::new(None)),
            garble: Arc::new(LockBox::new(None)),
        }
    }

    fn create(mut self, create: bool) -> Builder {
        self.create = create;
        self
    }

    fn data_version(mut self, version: Uuid) -> Builder {
        self.current_data_version = version;
        self
    }

    fn storage_fault(self, fault: StorageFault) -> Builder {
        self.storage_fault.with(|slot| *slot = Some(fault));
        self
    }

    fn protector_fault(self, fault: ProtectorFault) -> Builder {
        self.protector_fault.with(|slot| *slot = Some(fault));
        self
    }

    fn tamper(self, tamper: Tamper) -> Builder {
        self.tamper.with(|slot| *slot = Some(tamper));
        self
    }

    async fn open(&self) -> Result<Device> {
        let storage_handle = Arc::new(LockBox::new(None));
        let storage_notified = Arc::new(LockBox::new(Vec::new()));
        let protector_notified = Arc::new(LockBox::new(Vec::new()));

        let storage = MemStorage {
            local: self.local.clone(),
            remote: self.remote.clone(),
            fault: self.storage_fault.clone(),
            tamper: self.tamper.clone(),
            handle: storage_handle.clone(),
            notified: storage_notified.clone(),
        };
        let protector = MemProtector {
            fault: self.protector_fault.clone(),
            garble: self.garble.clone(),
            handle: Arc::new(LockBox::new(None)),
            notified: protector_notified.clone(),
        };

        let core = Core::open(OpenOptions {
            storage,
            protector,
            create: self.create,
            supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
            current_data_version: self.current_data_version,
        })
        .await?;

        Ok(Device {
            core,
            local: self.local.clone(),
            storage_fault: self.storage_fault.clone(),
            protector_fault: self.protector_fault.clone(),
            tamper: self.tamper.clone(),
            garble: self.garble.clone(),
            storage_handle,
            storage_notified,
            protector_notified,
        })
    }
}

/// A single-value `MVReg`, for stuffing a made-up payload into a remote-meta register.
fn meta_reg(actor: Uuid, version: Uuid, payload: &[u8]) -> MVReg<VersionBytes, Uuid> {
    let mut reg: MVReg<VersionBytes, Uuid> = MVReg::new();
    let read_ctx = reg.read();
    let op = reg.write(
        VersionBytes::new(version, payload.to_vec()),
        read_ctx.derive_add_ctx(actor),
    );
    reg.apply(op);
    reg
}

// ---------------------------------------------------------------------------------------------
// opening
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn open_creates_local_meta_and_notifies_both_sides() {
    let device = Builder::new().open().await.unwrap();

    // both implementations are told about the (still empty) remote meta, so neither has to guess
    // whether the initial read happened
    assert_eq!(device.storage_notified.with(|seen| seen.len()), 1);
    assert!(device.storage_notified.with(|seen| seen[0].is_none()));
    assert_eq!(device.protector_notified.with(|seen| seen.len()), 1);
    assert!(device.protector_notified.with(|seen| seen[0].is_none()));

    assert!(device.local.with(|local| local.is_some()));
    assert!(device.values().is_empty());
    assert!(format!("{:?}", device.core).contains("Core"));
    assert!(format!("{:?}", device.core.info()).contains("Info"));
}

/// The device identity has to survive a restart: a new actor id on every open would fork the op
/// log and leave the old ops attributed to an actor nothing writes to any more.
#[tokio::test]
async fn reopening_reuses_the_stored_local_actor_id() {
    let builder = Builder::new();

    let first = builder.open().await.unwrap();
    let actor = first.actor();
    first.write(1).await.unwrap();
    drop(first);

    // `create: false` -- the local meta already exists, so nothing needs creating
    let second = builder.clone().create(false).open().await.unwrap();
    assert_eq!(second.actor(), actor);

    // and it picks its own op log back up rather than starting a second one
    second.core.read_remote().await.unwrap();
    assert_eq!(second.values(), vec![1]);
}

#[tokio::test]
async fn open_without_create_fails_when_nothing_is_stored() {
    let err = Builder::new()
        .create(false)
        .open()
        .await
        .expect_err("should refuse to invent a device identity");
    assert!(err.to_string().contains("create"), "got: {}", err);
}

#[tokio::test]
async fn open_reports_a_local_meta_it_cannot_read() {
    let builder = Builder::new();
    builder.open().await.unwrap();

    // written by a build using an outer wrapper format this one doesn't know
    builder
        .clone()
        .tamper(Tamper::OuterVersion)
        .open()
        .await
        .expect_err("an unknown outer version must not be deserialized");

    // right version, unreadable payload
    builder
        .clone()
        .tamper(Tamper::LocalMetaBody)
        .open()
        .await
        .expect_err("an unparsable local meta must not be ignored");
}

#[tokio::test]
async fn open_propagates_failures_from_either_side() {
    for fault in [
        StorageFault::LoadLocalMeta,
        StorageFault::StoreLocalMeta,
        StorageFault::Init,
        StorageFault::SetRemoteMeta,
        StorageFault::ListRemoteMetaNames,
    ] {
        let result = Builder::new().storage_fault(fault).open().await;
        assert!(result.is_err(), "expected {:?} to fail the open", fault);
    }

    for fault in [ProtectorFault::Init, ProtectorFault::SetRemoteMeta] {
        let result = Builder::new().protector_fault(fault).open().await;
        assert!(result.is_err(), "expected {:?} to fail the open", fault);
    }
}

// ---------------------------------------------------------------------------------------------
// ops
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ops_written_by_one_device_are_read_by_another() {
    let builder = Builder::new();
    let a = builder.open().await.unwrap();

    a.write(1).await.unwrap();
    a.write(2).await.unwrap();
    assert_eq!(a.values(), vec![2]);

    let b = Builder::joining(builder.remote.clone())
        .open()
        .await
        .unwrap();
    b.core.read_remote().await.unwrap();
    assert_eq!(b.values(), vec![2]);

    // a second pass has nothing new to do, and must not double-apply what it already has
    b.core.read_remote().await.unwrap();
    assert_eq!(b.values(), vec![2]);
}

#[tokio::test]
async fn read_and_apply_reports_an_error_from_its_closure() {
    let device = Builder::new().open().await.unwrap();

    device
        .core
        .read_and_apply(|_: &State| Err(Error::msg("the app decided not to")))
        .await
        .unwrap_err();

    assert!(device.values().is_empty());
}

/// An op that never reached storage must not be applied locally either, or this device silently
/// diverges from every other one.
#[tokio::test]
async fn read_and_apply_reports_encrypt_and_store_failures() {
    let device = Builder::new().open().await.unwrap();

    device.arm_protector(Some(ProtectorFault::Encrypt));
    device.write(1).await.unwrap_err();
    device.arm_protector(None);

    device.arm_storage(Some(StorageFault::StoreOps));
    device.write(1).await.unwrap_err();
    device.arm_storage(None);

    assert!(device.values().is_empty());

    // and the actor's op versions weren't burned by the failures either
    device.write(7).await.unwrap();
    assert_eq!(device.values(), vec![7]);
}

#[tokio::test]
async fn read_remote_ops_reports_storage_and_protector_failures() {
    let builder = Builder::new();
    let a = builder.open().await.unwrap();
    a.write(1).await.unwrap();

    let b = Builder::joining(builder.remote.clone())
        .open()
        .await
        .unwrap();

    for fault in [StorageFault::ListOpActors, StorageFault::LoadOps] {
        b.arm_storage(Some(fault));
        b.core.read_remote().await.unwrap_err();
    }
    b.arm_storage(None);

    b.arm_protector(Some(ProtectorFault::Decrypt));
    b.core.read_remote().await.unwrap_err();
    b.arm_protector(None);

    // an op blob wrapped in a version this build doesn't know
    b.arm_tamper(Some(Tamper::OuterVersion));
    b.core.read_remote().await.unwrap_err();
    b.arm_tamper(None);

    // ... and once nothing is broken any more, the same op reads fine
    b.core.read_remote().await.unwrap();
    assert_eq!(b.values(), vec![1]);
}

/// Whatever comes out of `decrypt` is still untrusted input: it has to clear the inner version
/// check and parse as the op list `Core` expects before any of it is applied.
#[tokio::test]
async fn read_remote_ops_rejects_a_decrypted_payload_it_cannot_use() {
    let builder = Builder::new();
    let a = builder.open().await.unwrap();
    a.write(1).await.unwrap();

    for garble in [
        Garble::TooShort,
        Garble::WrongDataVersion,
        Garble::NotMsgpack,
    ] {
        let b = Builder::joining(builder.remote.clone())
            .open()
            .await
            .unwrap();
        b.arm_garble(Some(garble));
        b.core
            .read_remote()
            .await
            .expect_err(&format!("expected {:?} to be caught", garble));
    }
}

/// Ops written by a device on a data version this one wasn't opened with must be refused rather
/// than fed to a deserializer that would read them as the wrong shape.
#[tokio::test]
async fn read_remote_ops_rejects_an_unsupported_data_version() {
    let builder = Builder::new();
    let a = builder
        .clone()
        .data_version(OTHER_DATA_VERSION)
        .open()
        .await
        .unwrap();
    a.write(1).await.unwrap();

    let b = Builder::joining(builder.remote.clone())
        .open()
        .await
        .unwrap();
    b.core.read_remote().await.unwrap_err();
}

/// `Core` replays each actor's ops in strict order, so a hole in the log is a storage bug it must
/// report -- applying the ops after the hole would corrupt the state silently.
#[tokio::test]
async fn read_remote_ops_rejects_a_hole_in_an_actor_op_log() {
    let builder = Builder::new();
    let a = builder.open().await.unwrap();
    a.write(1).await.unwrap();
    a.write(2).await.unwrap();

    let b = Builder::joining(builder.remote.clone())
        .open()
        .await
        .unwrap();
    b.arm_tamper(Some(Tamper::OpGap));

    let err = b.core.read_remote().await.unwrap_err();
    assert!(err.to_string().contains("op version"), "got: {}", err);
}

/// Being handed an op that was already applied is not corruption -- it's what a concurrent
/// `read_remote` on the same `Core` looks like -- so it's skipped rather than treated as an error.
#[tokio::test]
async fn read_remote_ops_skips_ops_it_already_applied() {
    let builder = Builder::new();
    let a = builder.open().await.unwrap();
    a.write(1).await.unwrap();

    let b = Builder::joining(builder.remote.clone())
        .open()
        .await
        .unwrap();
    b.core.read_remote().await.unwrap();
    assert_eq!(b.values(), vec![1]);

    // storage now re-serves version 0 forever, as a racing reader would
    b.arm_tamper(Some(Tamper::ReplayOpsFromZero));
    b.core.read_remote().await.unwrap();
    assert_eq!(b.values(), vec![1], "the op must not be applied twice");
}

// ---------------------------------------------------------------------------------------------
// compaction and states
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn compact_snapshots_the_state_and_drops_what_it_superseded() {
    let builder = Builder::new();
    let a = builder.open().await.unwrap();
    a.write(1).await.unwrap();
    a.write(2).await.unwrap();

    assert_eq!(builder.remote.with(|remote| remote.ops.len()), 2);

    a.core.compact().await.unwrap();

    assert_eq!(builder.remote.with(|remote| remote.states.len()), 1);
    assert!(
        builder.remote.with(|remote| remote.ops.is_empty()),
        "the ops folded into the snapshot are redundant"
    );

    // a second compaction supersedes the first snapshot rather than piling up next to it
    a.write(3).await.unwrap();
    a.core.compact().await.unwrap();
    assert_eq!(builder.remote.with(|remote| remote.states.len()), 1);

    // a device that only ever sees the snapshot still ends up at the same state
    let b = Builder::joining(builder.remote.clone())
        .open()
        .await
        .unwrap();
    b.core.read_remote().await.unwrap();
    assert_eq!(b.values(), vec![3]);
    assert_eq!(a.values(), b.values());
}

#[tokio::test]
async fn compact_reports_failures_at_every_step() {
    let builder = Builder::new();
    let device = builder.open().await.unwrap();
    device.write(1).await.unwrap();

    for fault in [
        StorageFault::ListStateNames,
        StorageFault::StoreState,
        StorageFault::RemoveStates,
        StorageFault::RemoveOps,
    ] {
        device.arm_storage(Some(fault));
        assert!(
            device.core.compact().await.is_err(),
            "expected {:?} to fail the compaction",
            fault
        );
    }
    device.arm_storage(None);

    device.arm_protector(Some(ProtectorFault::Encrypt));
    device.core.compact().await.unwrap_err();
    device.arm_protector(None);

    device.core.compact().await.unwrap();
}

#[tokio::test]
async fn read_remote_states_reports_storage_and_protector_failures() {
    let builder = Builder::new();
    let a = builder.open().await.unwrap();
    a.write(1).await.unwrap();
    a.core.compact().await.unwrap();

    let b = Builder::joining(builder.remote.clone())
        .open()
        .await
        .unwrap();

    b.arm_storage(Some(StorageFault::LoadStates));
    b.core.read_remote().await.unwrap_err();
    b.arm_storage(None);

    b.arm_protector(Some(ProtectorFault::Decrypt));
    b.core.read_remote().await.unwrap_err();
    b.arm_protector(None);

    b.arm_tamper(Some(Tamper::OuterVersion));
    b.core.read_remote().await.unwrap_err();
    b.arm_tamper(None);

    b.core.read_remote().await.unwrap();
    assert_eq!(b.values(), vec![1]);
}

#[tokio::test]
async fn read_remote_states_rejects_a_decrypted_snapshot_it_cannot_use() {
    let builder = Builder::new();
    let a = builder.open().await.unwrap();
    a.write(1).await.unwrap();
    a.core.compact().await.unwrap();

    for garble in [
        Garble::TooShort,
        Garble::WrongDataVersion,
        Garble::NotMsgpack,
    ] {
        let b = Builder::joining(builder.remote.clone())
            .open()
            .await
            .unwrap();
        b.arm_garble(Some(garble));
        b.core
            .read_remote()
            .await
            .expect_err(&format!("expected {:?} to be caught", garble));
    }
}

#[tokio::test]
async fn read_remote_states_rejects_an_unsupported_data_version() {
    let builder = Builder::new();
    let a = builder
        .clone()
        .data_version(OTHER_DATA_VERSION)
        .open()
        .await
        .unwrap();
    a.write(1).await.unwrap();
    a.core.compact().await.unwrap();

    let b = Builder::joining(builder.remote.clone())
        .open()
        .await
        .unwrap();
    b.core.read_remote().await.unwrap_err();
}

// ---------------------------------------------------------------------------------------------
// remote meta gossip
// ---------------------------------------------------------------------------------------------

/// The out-of-band channel a `Storage`/`Protector` uses to reach its counterpart on another device
/// -- how `EnvelopeProtector` ships its wrapped content key, for instance.
#[tokio::test]
async fn remote_meta_published_by_one_device_reaches_the_other() {
    const META_VERSION: Uuid = Uuid::from_u128(0x_5b7e1c02_4d9a_47f3_8c60_0a2b6e91d4f7);

    let builder = Builder::new();
    let a = builder.open().await.unwrap();
    let handle_a = a.sub_handle();

    handle_a
        .set_remote_meta_storage(meta_reg(
            handle_a.info().actor(),
            META_VERSION,
            b"from-storage",
        ))
        .await
        .unwrap();
    handle_a
        .set_remote_meta_protector(meta_reg(a.actor(), META_VERSION, b"from-protector"))
        .await
        .unwrap();

    // each publish supersedes this device's previous blob rather than accumulating
    assert_eq!(builder.remote.with(|remote| remote.metas.len()), 1);

    let b = Builder::joining(builder.remote.clone())
        .open()
        .await
        .unwrap();

    // `Core::open` already did the initial read, so B sees both slices right away
    let storage_seen = b
        .storage_notified
        .with(|seen| seen.last().cloned().flatten())
        .expect("storage should have been notified with a value");
    assert_eq!(
        storage_seen.read().val[0].as_ref(),
        b"from-storage".as_slice()
    );

    let protector_seen = b
        .protector_notified
        .with(|seen| seen.last().cloned().flatten())
        .expect("protector should have been notified with a value");
    assert_eq!(
        protector_seen.read().val[0].as_ref(),
        b"from-protector".as_slice()
    );
}

/// A re-read with nothing new must not re-notify: implementations treat a `set_remote_meta` call
/// as "something changed" and would redo work (an Argon2 derivation, say) on every poll.
#[tokio::test]
async fn read_remote_meta_only_notifies_when_something_changed() {
    const META_VERSION: Uuid = Uuid::from_u128(0x_5b7e1c02_4d9a_47f3_8c60_0a2b6e91d4f7);

    let builder = Builder::new();
    let a = builder.open().await.unwrap();

    let after_open = a.storage_notified.with(|seen| seen.len());

    a.core.read_remote_meta().await.unwrap();
    assert_eq!(
        a.storage_notified.with(|seen| seen.len()),
        after_open,
        "nothing new to merge, so nobody should be notified"
    );

    // now something *is* new: another device publishes
    let b = Builder::joining(builder.remote.clone())
        .open()
        .await
        .unwrap();
    b.sub_handle()
        .set_remote_meta_protector(meta_reg(b.actor(), META_VERSION, b"hello"))
        .await
        .unwrap();

    a.core.read_remote_meta().await.unwrap();
    assert_eq!(a.storage_notified.with(|seen| seen.len()), after_open + 1);
}

#[tokio::test]
async fn read_remote_meta_reports_a_blob_it_cannot_read() {
    const META_VERSION: Uuid = Uuid::from_u128(0x_5b7e1c02_4d9a_47f3_8c60_0a2b6e91d4f7);

    let builder = Builder::new();
    let a = builder.open().await.unwrap();
    a.sub_handle()
        .set_remote_meta_protector(meta_reg(a.actor(), META_VERSION, b"hello"))
        .await
        .unwrap();

    let b = Builder::joining(builder.remote.clone())
        .open()
        .await
        .unwrap();

    b.arm_storage(Some(StorageFault::LoadRemoteMetas));
    b.core.read_remote_meta().await.unwrap_err();
    b.arm_storage(None);

    // `b`'s own open already consumed the blob above, so `a` publishes a fresh one for the
    // remaining cases to actually have something to load
    a.sub_handle()
        .set_remote_meta_protector(meta_reg(a.actor(), META_VERSION, b"again"))
        .await
        .unwrap();

    // a failed read leaves the blob unread, so both tampering modes get a go at the same one
    b.arm_tamper(Some(Tamper::OuterVersion));
    b.core.read_remote_meta().await.unwrap_err();

    b.arm_tamper(Some(Tamper::RemoteMetaBody));
    b.core.read_remote_meta().await.unwrap_err();
    b.arm_tamper(None);

    b.core.read_remote_meta().await.unwrap();
}

#[tokio::test]
async fn publishing_remote_meta_reports_storage_failures() {
    const META_VERSION: Uuid = Uuid::from_u128(0x_5b7e1c02_4d9a_47f3_8c60_0a2b6e91d4f7);

    let device = Builder::new().open().await.unwrap();
    let handle = device.sub_handle();
    let actor = device.actor();

    device.arm_storage(Some(StorageFault::StoreRemoteMeta));
    handle
        .set_remote_meta_storage(meta_reg(actor, META_VERSION, b"x"))
        .await
        .unwrap_err();

    device.arm_storage(Some(StorageFault::RemoveRemoteMetas));
    handle
        .set_remote_meta_protector(meta_reg(actor, META_VERSION, b"x"))
        .await
        .unwrap_err();

    device.arm_storage(None);
    handle
        .set_remote_meta_protector(meta_reg(actor, META_VERSION, b"x"))
        .await
        .unwrap();
}

/// Everything a `Storage`/`Protector` can do to its owning `Core` goes through the type-erased
/// `dyn CoreSubHandle`, so each forwarded method needs to actually reach the inherent one.
#[tokio::test]
async fn the_sub_handle_forwards_every_method() {
    const META_VERSION: Uuid = Uuid::from_u128(0x_5b7e1c02_4d9a_47f3_8c60_0a2b6e91d4f7);

    let builder = Builder::new();
    let device = builder.open().await.unwrap();
    device.write(1).await.unwrap();

    let handle: Box<dyn CoreSubHandle> = device.sub_handle();

    assert_eq!(handle.info().actor(), device.actor());

    handle.read_remote().await.unwrap();
    handle.read_remote_meta().await.unwrap();

    handle
        .set_remote_meta_storage(meta_reg(device.actor(), META_VERSION, b"s"))
        .await
        .unwrap();
    handle
        .set_remote_meta_protector(meta_reg(device.actor(), META_VERSION, b"p"))
        .await
        .unwrap();

    handle.compact().await.unwrap();
    assert_eq!(builder.remote.with(|remote| remote.states.len()), 1);

    // the handle is `Debug` + `Clone` because implementations store and pass it around
    assert!(!format!("{:?}", handle).is_empty());
    let _cloned = dyn_clone::clone_box(&*handle);
}

#[tokio::test]
async fn the_protector_is_reachable_for_implementation_specific_calls() {
    let device = Builder::new().open().await.unwrap();

    // stands in for e.g. `EnvelopeProtector::rotate_key`, which isn't part of the generic trait
    device.core.protector().arm_check();
}

impl MemProtector {
    /// A method that only exists on the concrete type, to prove `Core::protector` hands one back.
    fn arm_check(&self) {
        assert!(self.handle.with(|handle| handle.is_some()), "init has run");
    }
}

/// A `Protector` that implements nothing but `encrypt`/`decrypt`, leaning on the trait's default
/// `init`/`set_remote_meta` hooks. That's the intended shape for a protector with no out-of-band
/// state -- a passphrase every device already knows, say -- so the defaults have to be enough to
/// open a `Core` and round-trip data through it.
#[derive(Debug)]
struct MinimalProtector;

impl Protector for MinimalProtector {
    async fn encrypt(&self, clear_text: Vec<u8>) -> Result<Vec<u8>> {
        Ok(flip(clear_text))
    }

    async fn decrypt(&self, enc_data: Vec<u8>) -> Result<Vec<u8>> {
        Ok(flip(enc_data))
    }
}

#[tokio::test]
async fn a_protector_can_rely_on_the_default_lifecycle_hooks() {
    let remote = Arc::new(LockBox::new(Remote::default()));

    let storage = MemStorage {
        local: Arc::new(LockBox::new(None)),
        remote: remote.clone(),
        fault: Arc::new(LockBox::new(None)),
        tamper: Arc::new(LockBox::new(None)),
        handle: Arc::new(LockBox::new(None)),
        notified: Arc::new(LockBox::new(Vec::new())),
    };

    let core: Arc<Core<State, MemStorage, MinimalProtector>> = Core::open(OpenOptions {
        storage,
        protector: MinimalProtector,
        create: true,
        supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
        current_data_version: CURRENT_DATA_VERSION,
    })
    .await
    .unwrap();

    let actor = core.info().actor();
    core.read_and_apply(|state: &State| {
        let read_ctx = state.read();
        Ok(vec![state.write(5, read_ctx.derive_add_ctx(actor))])
    })
    .await
    .unwrap();

    core.read_remote().await.unwrap();
    assert_eq!(
        core.with_state(|s: &State| Ok(s.read().val)).unwrap(),
        vec![5]
    );
}

/// A CRDT whose values refuse to serialize. `S` and `S::Op` are the *caller's* types, so `Core`
/// can't assume writing them to msgpack always succeeds -- and a serialization failure must abort
/// the write rather than persist a half-formed blob or apply the op locally anyway.
#[derive(Debug, Clone, Default)]
struct Unserializable;

impl Serialize for Unserializable {
    fn serialize<S: ::serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(::serde::ser::Error::custom(
            "this CRDT refuses to serialize",
        ))
    }
}

impl<'de> Deserialize<'de> for Unserializable {
    fn deserialize<D: ::serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Ok(Unserializable)
    }
}

impl CmRDT for Unserializable {
    type Op = Unserializable;
    type Validation = ::std::convert::Infallible;

    fn validate_op(&self, _op: &Self::Op) -> Result<(), Self::Validation> {
        Ok(())
    }

    fn apply(&mut self, _op: Self::Op) {}
}

impl ::crdts::CvRDT for Unserializable {
    type Validation = ::std::convert::Infallible;

    fn validate_merge(&self, _other: &Self) -> Result<(), Self::Validation> {
        Ok(())
    }

    fn merge(&mut self, _other: Self) {}
}

#[tokio::test]
async fn a_crdt_that_cannot_be_serialized_fails_the_write_and_the_compaction() {
    let remote = Arc::new(LockBox::new(Remote::default()));

    let storage = MemStorage {
        local: Arc::new(LockBox::new(None)),
        remote: remote.clone(),
        fault: Arc::new(LockBox::new(None)),
        tamper: Arc::new(LockBox::new(None)),
        handle: Arc::new(LockBox::new(None)),
        notified: Arc::new(LockBox::new(Vec::new())),
    };

    let core: Arc<Core<Unserializable, MemStorage, MinimalProtector>> = Core::open(OpenOptions {
        storage,
        protector: MinimalProtector,
        create: true,
        supported_data_versions: SUPPORTED_DATA_VERSIONS.to_vec(),
        current_data_version: CURRENT_DATA_VERSION,
    })
    .await
    .unwrap();

    // opening is fine -- nothing serializes app state until something is written
    core.read_and_apply(|_| Ok(vec![Unserializable]))
        .await
        .unwrap_err();
    core.compact().await.unwrap_err();

    assert!(remote.with(|remote| remote.ops.is_empty() && remote.states.is_empty()));
}
