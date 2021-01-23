use anyhow::Result;
use crdt_enc_gpgme::KeyHandler;
use crdt_enc_sodium::EncHandler;
use crdt_enc_tokio::Storage;
use futures::future::FutureExt;
use uuid::Uuid;

const CURRENT_DATA_VERSION: Uuid = Uuid::from_u128(1u128);

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    crdt_enc_sodium::init();

    let data_dir = std::fs::canonicalize("./").unwrap().join("data");

    let storage = Storage::new(data_dir.join("local"), data_dir.join("remote"))?;
    let cryptor = EncHandler::new();
    let key_cryptor = KeyHandler::new();
    let open_options = crdt_enc::OpenOptions {
        storage,
        cryptor,
        key_cryptor,
        create: true,
        supported_data_versions: [CURRENT_DATA_VERSION].iter().cloned().collect(),
        current_data_version: CURRENT_DATA_VERSION,
    };
    let (repo, info) = crdt_enc::Core::open(open_options).await?;

    // let actor_id = repo.actor_id();

    // repo.run(futures::stream::empty()).try_for_each(|state| {
    //     dbg!(state);
    // });

    // dbg!(&repo);

    repo.read_remote().await?;

    // dbg!(&repo);

    // repo.compact().await?;

    // dbg!(&repo);

    let op = repo.with_state(|s: &crdts::MVReg<u64, Uuid>| {
        let read_ctx = s.read();
        let new_val = read_ctx.val.iter().copied().max().unwrap_or(0) + 1;
        let op = s.write(new_val, read_ctx.derive_add_ctx(info.actor()));
        Ok(op)
    })?;

    dbg!(&op);

    repo.apply_ops(vec![op]).await?;

    Ok(())
}
