use ::anyhow::Result;
use ::crdt_enc_envelope::{at_rest::AtRest, EnvelopeProtector};
use ::crdt_enc_password::PasswordKeySlot;
use ::crdt_enc_tokio::Storage;
use ::uuid::Uuid;

/// The inner-payload data version this example's `MVReg<u64, Uuid>` state is stamped with.
const CURRENT_DATA_VERSION: Uuid = Uuid::from_u128(0xaadfd5a6_6e19_4b24_a802_4fa27c72f20c);

/// The set of inner-payload data versions this example can read.
const SUPPORTED_DATA_VERSIONS: &[Uuid] = &[CURRENT_DATA_VERSION];

/// Wires the tokio filesystem storage and an `EnvelopeProtector<PasswordKeySlot>` together against a
/// `MVReg<u64, Uuid>` state, reading/writing under `./data/local` and `./data/remote`, then
/// increments the register by one and applies that as a new op -- a minimal reference for how the
/// pieces fit together end to end.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let data_dir = std::fs::canonicalize("./").unwrap().join("data");

    let storage = Storage::new(data_dir.join("local"), data_dir.join("remote"))?;
    let protector =
        EnvelopeProtector::new(PasswordKeySlot::new(AtRest::encrypt("example password")));
    let open_options = crdt_enc::OpenOptions {
        storage,
        protector,
        create: true,
        supported_data_versions: SUPPORTED_DATA_VERSIONS.iter().cloned().collect(),
        current_data_version: CURRENT_DATA_VERSION,
    };
    let repo = crdt_enc::Core::open(open_options).await?;
    let info = repo.info();

    // let actor_id = repo.actor_id();

    // repo.run(futures::stream::empty()).try_for_each(|state| {
    //     dbg!(state);
    // });

    // dbg!(&repo);

    repo.read_remote().await?;

    // dbg!(&repo);

    // repo.compact().await?;

    // dbg!(&repo);

    repo.read_and_apply(|s: &crdts::MVReg<u64, Uuid>| {
        let read_ctx = s.read();
        let new_val = read_ctx.val.iter().copied().max().unwrap_or(0) + 1;
        let op = s.write(new_val, read_ctx.derive_add_ctx(info.actor()));
        Ok(vec![op])
    })
    .await?;

    Ok(())
}
