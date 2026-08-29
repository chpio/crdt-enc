//! Covers the local-filesystem `Storage` directly, rather than through a `Core`.
//!
//! Two things need that. First, `Core` only ever uses this backend correctly, so none of the "the
//! tree on disk isn't what we wrote" handling -- a truncated file, a directory where a blob should
//! be, a name that isn't valid UTF-8 -- is reachable from above; a sync tool merging two trees, or
//! a half-finished write, can produce all of them. Second, several of these operations (`remove_*`,
//! the lazily-created directories, the local lock) have observable behaviour of their own that is
//! worth pinning independently of whatever `Core` happens to call today.

use ::anyhow::Result;
use ::crdt_enc::{storage::Storage as _, utils::VersionBytes};
use ::crdt_enc_tokio::Storage;
use ::std::path::{Path, PathBuf};
use ::tokio::fs;
use ::uuid::Uuid;

const VERSION: Uuid = Uuid::from_u128(0x_1f0b4d63_7a25_4e88_9c31_a5e0d7462b19);

/// A `Storage` over fresh `local`/`remote` subdirectories of `tmp`, plus the remote path so tests
/// can inspect or corrupt the tree behind its back.
fn open(tmp: &Path) -> (Storage, PathBuf) {
    let remote = tmp.join("remote");
    let storage = Storage::new(tmp.join("local"), remote.clone()).unwrap();
    (storage, remote)
}

fn blob(content: &[u8]) -> VersionBytes {
    VersionBytes::new(VERSION, content.to_vec())
}

// ---------------------------------------------------------------------------------------------
// construction and the local lock
// ---------------------------------------------------------------------------------------------

/// Relative paths are rejected up front rather than being resolved against whatever the process's
/// working directory happens to be at the time -- which for a long-lived sync process is not a
/// stable thing to hang a data directory off.
#[test]
fn new_requires_both_paths_to_be_absolute() {
    let tmp = tempfile::tempdir().unwrap();

    let err = Storage::new(PathBuf::from("relative/local"), tmp.path().join("remote")).unwrap_err();
    assert!(err.to_string().contains("local path"), "got: {}", err);

    let err = Storage::new(tmp.path().join("local"), PathBuf::from("relative/remote")).unwrap_err();
    assert!(err.to_string().contains("remote path"), "got: {}", err);

    Storage::new(tmp.path().join("local"), tmp.path().join("remote")).unwrap();
}

/// The local tree holds this device's actor identity, and two processes sharing it would hand the
/// same actor id to both -- each then writing its own ops under that one actor. The lock is what
/// stops that, so it has to actually refuse the second opener.
#[tokio::test]
async fn local_storage_is_locked_against_a_second_opener() {
    let tmp = tempfile::tempdir().unwrap();
    let local = tmp.path().join("local");

    let first = Storage::new(local.clone(), tmp.path().join("remote")).unwrap();
    // the lock is taken lazily, on first local-meta access ...
    first.load_local_meta().await.unwrap();
    // ... and is a no-op once held
    first.load_local_meta().await.unwrap();

    let second = Storage::new(local.clone(), tmp.path().join("remote")).unwrap();
    let err = second.load_local_meta().await.unwrap_err();
    assert!(err.to_string().contains("already locked"), "got: {}", err);

    // releasing it makes the directory available again
    drop(first);
    second.load_local_meta().await.unwrap();
}

// ---------------------------------------------------------------------------------------------
// local meta
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn local_meta_round_trips_and_is_overwritten_in_place() {
    let tmp = tempfile::tempdir().unwrap();
    let (storage, _remote) = open(tmp.path());

    assert!(storage.load_local_meta().await.unwrap().is_none());

    storage.store_local_meta(blob(b"first")).await.unwrap();
    let loaded = storage.load_local_meta().await.unwrap().unwrap();
    assert_eq!(loaded.version(), VERSION);
    assert_eq!(loaded.as_ref(), b"first");

    // unlike everything under `remote`, local meta is mutable -- it is never synced
    storage.store_local_meta(blob(b"second")).await.unwrap();
    assert_eq!(
        storage.load_local_meta().await.unwrap().unwrap().as_ref(),
        b"second"
    );
}

#[tokio::test]
async fn local_meta_too_short_to_hold_a_version_tag_is_reported() {
    let tmp = tempfile::tempdir().unwrap();
    let local = tmp.path().join("local");
    fs::create_dir_all(&local).await.unwrap();
    fs::write(local.join("meta-data.msgpack"), b"short")
        .await
        .unwrap();

    let storage = Storage::new(local, tmp.path().join("remote")).unwrap();
    let err = storage.load_local_meta().await.unwrap_err();
    assert!(err.to_string().contains("failed parsing"), "got: {}", err);
}

/// A missing file means "nothing stored yet"; anything else is a real I/O failure and has to
/// surface rather than being flattened into the same `None`.
#[tokio::test]
async fn local_meta_read_errors_are_not_mistaken_for_a_missing_file() {
    let tmp = tempfile::tempdir().unwrap();
    let local = tmp.path().join("local");
    fs::create_dir_all(local.join("meta-data.msgpack"))
        .await
        .unwrap();

    let storage = Storage::new(local, tmp.path().join("remote")).unwrap();
    let err = storage.load_local_meta().await.unwrap_err();
    assert!(err.to_string().contains("failed reading"), "got: {}", err);
}

// ---------------------------------------------------------------------------------------------
// remote meta
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn remote_meta_round_trips_through_a_content_addressed_name() {
    let tmp = tempfile::tempdir().unwrap();
    let (storage, remote) = open(tmp.path());

    // nothing written yet, and the directory doesn't even exist -- that is an empty listing, not
    // an error, since a fresh device legitimately starts with no remote tree at all
    assert!(storage.list_remote_meta_names().await.unwrap().is_empty());
    assert!(!remote.join("meta").exists());

    let name = storage.store_remote_meta(blob(b"meta-one")).await.unwrap();
    let other = storage.store_remote_meta(blob(b"meta-two")).await.unwrap();

    let mut names = storage.list_remote_meta_names().await.unwrap();
    names.sort();
    let mut expected = vec![name.clone(), other.clone()];
    expected.sort();
    assert_eq!(names, expected);

    let loaded = storage.load_remote_metas(vec![name.clone()]).await.unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].0, name);
    assert_eq!(loaded[0].1.as_ref(), b"meta-one");

    // an already-missing blob is not an error: two devices can supersede the same one
    storage
        .remove_remote_metas(vec![name.clone(), "never-existed".to_string()])
        .await
        .unwrap();
    assert_eq!(storage.list_remote_meta_names().await.unwrap(), vec![other]);
}

#[tokio::test]
async fn remote_meta_load_reports_missing_and_unparsable_files() {
    let tmp = tempfile::tempdir().unwrap();
    let (storage, remote) = open(tmp.path());

    storage.store_remote_meta(blob(b"meta")).await.unwrap();

    let err = storage
        .load_remote_metas(vec!["not-there".to_string()])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("failed reading"), "got: {}", err);

    fs::write(remote.join("meta").join("truncated"), b"short")
        .await
        .unwrap();
    let err = storage
        .load_remote_metas(vec!["truncated".to_string()])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("failed parsing"), "got: {}", err);
}

/// `meta/` is supposed to contain only blob files. A directory in there (a sync tool's conflict
/// folder, say) is skipped rather than listed as a name nothing can load.
#[tokio::test]
async fn listing_remote_meta_skips_anything_that_is_not_a_file() {
    let tmp = tempfile::tempdir().unwrap();
    let (storage, remote) = open(tmp.path());

    let name = storage.store_remote_meta(blob(b"meta")).await.unwrap();
    fs::create_dir_all(remote.join("meta").join("a-directory"))
        .await
        .unwrap();

    assert_eq!(storage.list_remote_meta_names().await.unwrap(), vec![name]);
}

/// A `meta` that is a file rather than a directory isn't "no entries" -- something is badly wrong
/// with the tree, and quietly reporting it as empty would let `Core` conclude that no device has
/// ever published anything.
#[tokio::test]
async fn listing_reports_a_directory_that_is_not_one() {
    let tmp = tempfile::tempdir().unwrap();
    let (storage, remote) = open(tmp.path());

    fs::create_dir_all(&remote).await.unwrap();
    fs::write(remote.join("meta"), b"not a directory")
        .await
        .unwrap();

    let err = storage.list_remote_meta_names().await.unwrap_err();
    assert!(err.to_string().contains("failed listing"), "got: {}", err);
}

// ---------------------------------------------------------------------------------------------
// states
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn states_round_trip_and_can_be_removed() {
    let tmp = tempfile::tempdir().unwrap();
    let (storage, remote) = open(tmp.path());

    assert!(storage.list_state_names().await.unwrap().is_empty());

    let first = storage.store_state(blob(b"state-one")).await.unwrap();
    let second = storage.store_state(blob(b"state-two")).await.unwrap();

    let loaded = storage
        .load_states(vec![first.clone(), second.clone()])
        .await
        .unwrap();
    let mut contents: Vec<_> = loaded
        .iter()
        .map(|(_, blob)| blob.as_ref().to_vec())
        .collect();
    contents.sort();
    assert_eq!(contents, vec![b"state-one".to_vec(), b"state-two".to_vec()]);

    // the names come back unchanged, which is what `Core` folds into its read-state bookkeeping
    let removed = storage
        .remove_states(vec![first.clone(), "never-existed".to_string()])
        .await
        .unwrap();
    assert_eq!(removed, vec![first, "never-existed".to_string()]);
    assert_eq!(storage.list_state_names().await.unwrap(), vec![second]);

    fs::create_dir_all(remote.join("states").join("a-directory"))
        .await
        .unwrap();
    assert_eq!(storage.list_state_names().await.unwrap().len(), 1);
}

#[tokio::test]
async fn state_load_reports_missing_and_unparsable_files() {
    let tmp = tempfile::tempdir().unwrap();
    let (storage, remote) = open(tmp.path());

    storage.store_state(blob(b"state")).await.unwrap();

    let err = storage
        .load_states(vec!["not-there".to_string()])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("failed reading"), "got: {}", err);

    fs::write(remote.join("states").join("truncated"), b"short")
        .await
        .unwrap();
    let err = storage
        .load_states(vec!["truncated".to_string()])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("failed parsing"), "got: {}", err);
}

/// Content-addressed files are named by the hash of their content, so writing the same bytes twice
/// targets a file that already exists. That is a no-op, not a conflict -- the name already
/// determines the content, so there is nothing the second write could add.
#[tokio::test]
async fn storing_byte_identical_content_twice_is_a_no_op() {
    let tmp = tempfile::tempdir().unwrap();
    let (storage, _remote) = open(tmp.path());

    let first = storage.store_state(blob(b"identical")).await.unwrap();
    let second = storage.store_state(blob(b"identical")).await.unwrap();

    assert_eq!(first, second, "the same content must map to the same name");
    assert_eq!(
        storage.list_state_names().await.unwrap(),
        vec![first],
        "one name, one blob"
    );

    // and the same holds for remote meta, which is where `Core` actually re-writes unchanged bytes
    let first = storage.store_remote_meta(blob(b"same")).await.unwrap();
    let second = storage.store_remote_meta(blob(b"same")).await.unwrap();
    assert_eq!(first, second);
    assert_eq!(storage.list_remote_meta_names().await.unwrap(), vec![first]);
}

/// A blob that can't be written at all (here: a `states` path that is a file) has to be reported,
/// not silently dropped -- `Core` treats a successful `store_state` as "the snapshot is durable"
/// and goes straight on to delete the ops it superseded.
#[tokio::test]
async fn storing_reports_a_directory_it_cannot_create() {
    let tmp = tempfile::tempdir().unwrap();
    let (storage, remote) = open(tmp.path());

    fs::create_dir_all(&remote).await.unwrap();
    fs::write(remote.join("states"), b"not a directory")
        .await
        .unwrap();

    let err = storage.store_state(blob(b"state")).await.unwrap_err();
    assert!(err.to_string().contains("failed writing"), "got: {}", err);
}

/// Removing something that cannot be removed as a file (a directory, here) is a real failure --
/// unlike an already-missing file, which is the expected outcome of two devices compacting away
/// the same superseded blob.
#[tokio::test]
async fn removing_reports_a_failure_that_is_not_just_a_missing_file() {
    let tmp = tempfile::tempdir().unwrap();
    let (storage, remote) = open(tmp.path());

    fs::create_dir_all(remote.join("states").join("a-directory"))
        .await
        .unwrap();

    let err = storage
        .remove_states(vec!["a-directory".to_string()])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("failed removing"), "got: {}", err);
}

// ---------------------------------------------------------------------------------------------
// ops
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ops_round_trip_per_actor_and_version() {
    let tmp = tempfile::tempdir().unwrap();
    let (storage, _remote) = open(tmp.path());

    let actor_a = Uuid::new_v4();
    let actor_b = Uuid::new_v4();

    assert!(storage.list_op_actors().await.unwrap().is_empty());

    storage.store_ops(actor_a, 0, blob(b"a0")).await.unwrap();
    storage.store_ops(actor_a, 1, blob(b"a1")).await.unwrap();
    storage.store_ops(actor_b, 0, blob(b"b0")).await.unwrap();

    let mut actors = storage.list_op_actors().await.unwrap();
    actors.sort();
    let mut expected = vec![actor_a, actor_b];
    expected.sort();
    assert_eq!(actors, expected);

    // each actor's ops come back contiguously from the requested version
    let ops = storage
        .load_ops(vec![(actor_a, 0), (actor_b, 0)])
        .await
        .unwrap();
    let rendered: Vec<_> = ops
        .iter()
        .map(|(actor, version, blob)| (*actor, *version, blob.as_ref().to_vec()))
        .collect();
    assert_eq!(rendered.len(), 3);
    assert!(rendered.contains(&(actor_a, 0, b"a0".to_vec())));
    assert!(rendered.contains(&(actor_a, 1, b"a1".to_vec())));
    assert!(rendered.contains(&(actor_b, 0, b"b0".to_vec())));

    // starting part-way through skips what the caller already has
    let ops = storage.load_ops(vec![(actor_a, 1)]).await.unwrap();
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].1, 1);

    // and asking past the end is simply empty
    assert!(
        storage
            .load_ops(vec![(actor_a, 9)])
            .await
            .unwrap()
            .is_empty()
    );

    storage.remove_ops(vec![(actor_a, 0)]).await.unwrap();
    let ops = storage.load_ops(vec![(actor_a, 0)]).await.unwrap();
    assert!(
        ops.is_empty(),
        "version 0 is gone, so the walk stops at once"
    );
    assert_eq!(storage.load_ops(vec![(actor_a, 1)]).await.unwrap().len(), 1);

    // removing an op that is already gone is not an error
    storage.remove_ops(vec![(actor_a, 0)]).await.unwrap();
}

/// Op files are immutable once written -- silently overwriting one would rewrite history that
/// other devices may already have merged.
#[tokio::test]
async fn storing_the_same_op_version_twice_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let (storage, _remote) = open(tmp.path());
    let actor = Uuid::new_v4();

    storage.store_ops(actor, 0, blob(b"first")).await.unwrap();
    let err = storage
        .store_ops(actor, 0, blob(b"second"))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("failed writing ops"),
        "got: {}",
        err
    );

    let ops = storage.load_ops(vec![(actor, 0)]).await.unwrap();
    assert_eq!(ops[0].2.as_ref(), b"first");
}

#[tokio::test]
async fn op_load_reports_unreadable_and_unparsable_files() {
    let tmp = tempfile::tempdir().unwrap();
    let (storage, remote) = open(tmp.path());
    let actor = Uuid::new_v4();

    storage.store_ops(actor, 0, blob(b"op")).await.unwrap();

    fs::write(
        remote.join("ops").join(actor.to_string()).join("1"),
        b"short",
    )
    .await
    .unwrap();
    let err = storage.load_ops(vec![(actor, 0)]).await.unwrap_err();
    assert!(err.to_string().contains("failed parsing"), "got: {}", err);

    // a directory where an op file should be: an I/O error, not "the log ends here"
    let other = Uuid::new_v4();
    fs::create_dir_all(remote.join("ops").join(other.to_string()).join("0"))
        .await
        .unwrap();
    let err = storage.load_ops(vec![(other, 0)]).await.unwrap_err();
    assert!(err.to_string().contains("failed reading"), "got: {}", err);
}

#[tokio::test]
async fn removing_ops_reports_a_failure_that_is_not_just_a_missing_file() {
    let tmp = tempfile::tempdir().unwrap();
    let (storage, remote) = open(tmp.path());
    let actor = Uuid::new_v4();

    fs::create_dir_all(remote.join("ops").join(actor.to_string()).join("0"))
        .await
        .unwrap();

    let err = storage.remove_ops(vec![(actor, 0)]).await.unwrap_err();
    assert!(err.to_string().contains("failed removing"), "got: {}", err);
}

/// `ops/` holds one directory per actor, named by that actor's uuid. Anything else in there is
/// either not ours or corrupt: a plain file is skipped, but a directory whose name isn't a uuid is
/// reported, since silently ignoring it would drop that actor's entire op log.
#[tokio::test]
async fn listing_op_actors_skips_files_but_reports_a_non_uuid_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let (storage, remote) = open(tmp.path());
    let actor = Uuid::new_v4();

    storage.store_ops(actor, 0, blob(b"op")).await.unwrap();

    fs::write(remote.join("ops").join("a-file"), b"stray")
        .await
        .unwrap();
    assert_eq!(storage.list_op_actors().await.unwrap(), vec![actor]);

    fs::create_dir_all(remote.join("ops").join("not-a-uuid"))
        .await
        .unwrap();
    let err = storage.list_op_actors().await.unwrap_err();
    assert!(err.to_string().contains("into uuid"), "got: {}", err);
}

/// Filenames on Unix are bytes, not text. A tree that picked up a non-UTF-8 name (from a foreign
/// filesystem, a partial sync, or plain corruption) must be reported rather than lossily converted
/// into a name that maps back to no file at all.
#[cfg(unix)]
#[tokio::test]
async fn names_that_are_not_utf8_are_reported() -> Result<()> {
    use ::std::{ffi::OsStr, os::unix::ffi::OsStrExt};

    let invalid = OsStr::from_bytes(b"invalid-\xff-utf8");

    let tmp = tempfile::tempdir()?;
    let (storage, remote) = open(tmp.path());
    fs::create_dir_all(remote.join("meta")).await?;
    fs::write(remote.join("meta").join(invalid), b"x").await?;
    let err = storage.list_remote_meta_names().await.unwrap_err();
    assert!(err.to_string().contains("to string"), "got: {}", err);

    let tmp = tempfile::tempdir()?;
    let (storage, remote) = open(tmp.path());
    fs::create_dir_all(remote.join("states")).await?;
    fs::write(remote.join("states").join(invalid), b"x").await?;
    let err = storage.list_state_names().await.unwrap_err();
    assert!(err.to_string().contains("to string"), "got: {}", err);

    let tmp = tempfile::tempdir()?;
    let (storage, remote) = open(tmp.path());
    fs::create_dir_all(remote.join("ops").join(invalid)).await?;
    let err = storage.list_op_actors().await.unwrap_err();
    assert!(err.to_string().contains("to string"), "got: {}", err);

    Ok(())
}

// ---------------------------------------------------------------------------------------------
// failures around the local directory itself
// ---------------------------------------------------------------------------------------------

/// The local directory is created lazily, on first access. If it can't be created -- here because
/// something on the path is a file -- that has to fail loudly: `Core` would otherwise go on to
/// invent a fresh actor identity on every start.
#[tokio::test]
async fn local_dir_that_cannot_be_created_is_reported() {
    let tmp = tempfile::tempdir().unwrap();
    let blocked = tmp.path().join("in-the-way");
    fs::write(&blocked, b"a file, not a directory")
        .await
        .unwrap();

    let storage = Storage::new(blocked.join("local"), tmp.path().join("remote")).unwrap();
    let err = storage.load_local_meta().await.unwrap_err();
    assert!(
        err.to_string().contains("failed creating local dir"),
        "got: {}",
        err
    );
}

#[tokio::test]
async fn lock_file_that_cannot_be_opened_is_reported() {
    let tmp = tempfile::tempdir().unwrap();
    let local = tmp.path().join("local");
    // a directory where the lock file belongs
    fs::create_dir_all(local.join(".lock")).await.unwrap();

    let storage = Storage::new(local, tmp.path().join("remote")).unwrap();
    let err = storage.load_local_meta().await.unwrap_err();
    assert!(
        err.to_string().contains("failed opening lock file"),
        "got: {}",
        err
    );

    // storing has to take the same lock, and fail the same way
    let err = storage.store_local_meta(blob(b"x")).await.unwrap_err();
    assert!(
        err.to_string().contains("failed opening lock file"),
        "got: {}",
        err
    );
}

#[tokio::test]
async fn local_meta_that_cannot_be_written_is_reported() {
    let tmp = tempfile::tempdir().unwrap();
    let local = tmp.path().join("local");
    fs::create_dir_all(local.join("meta-data.msgpack"))
        .await
        .unwrap();

    let storage = Storage::new(local, tmp.path().join("remote")).unwrap();
    let err = storage.store_local_meta(blob(b"x")).await.unwrap_err();
    assert!(
        err.to_string().contains("failed writing local meta file"),
        "got: {}",
        err
    );
}

// ---------------------------------------------------------------------------------------------
// failures around the remote tree
// ---------------------------------------------------------------------------------------------

/// Each listing wraps its own failure with its own context, so an error message says which of the
/// three trees is broken rather than just "couldn't list a directory".
#[tokio::test]
async fn each_listing_names_the_tree_it_failed_on() {
    for (subdir, expected) in [
        ("states", "failed listing states"),
        ("ops", "failed listing actors"),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let (storage, remote) = open(tmp.path());
        fs::create_dir_all(&remote).await.unwrap();
        fs::write(remote.join(subdir), b"not a directory")
            .await
            .unwrap();

        let err = match subdir {
            "states" => storage.list_state_names().await.unwrap_err(),
            _ => storage.list_op_actors().await.unwrap_err(),
        };
        assert!(err.to_string().contains(expected), "got: {}", err);
    }
}

#[tokio::test]
async fn removing_remote_meta_reports_a_failure_that_is_not_just_a_missing_file() {
    let tmp = tempfile::tempdir().unwrap();
    let (storage, remote) = open(tmp.path());

    fs::create_dir_all(remote.join("meta").join("a-directory"))
        .await
        .unwrap();

    let err = storage
        .remove_remote_metas(vec!["a-directory".to_string()])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("failed removing"), "got: {}", err);
}

#[tokio::test]
async fn op_dir_that_cannot_be_created_is_reported() {
    let tmp = tempfile::tempdir().unwrap();
    let (storage, remote) = open(tmp.path());
    let actor = Uuid::new_v4();

    fs::create_dir_all(remote.join("ops")).await.unwrap();
    fs::write(remote.join("ops").join(actor.to_string()), b"a file")
        .await
        .unwrap();

    let err = storage.store_ops(actor, 0, blob(b"op")).await.unwrap_err();
    assert!(
        err.to_string().contains("failed creating op dir"),
        "got: {}",
        err
    );
}

/// Accepting a repeat write must not swallow *other* write failures. `Core` treats a successful
/// `store_state` as "the snapshot is durable" and goes straight on to delete the ops it superseded,
/// so a blob that never landed has to be reported rather than reported as already-there.
#[cfg(unix)]
#[tokio::test]
async fn storing_reports_a_write_failure_that_is_not_a_repeat() {
    use ::std::{fs::Permissions, os::unix::fs::PermissionsExt};

    let tmp = tempfile::tempdir().unwrap();
    let (storage, remote) = open(tmp.path());

    // the directory exists, so `create_dir_all` succeeds and the failure lands on the file write
    let states = remote.join("states");
    fs::create_dir_all(&states).await.unwrap();
    fs::set_permissions(&states, Permissions::from_mode(0o555))
        .await
        .unwrap();

    // root ignores the permission bits, so this scenario cannot be built there at all
    if fs::write(states.join(".probe"), b"").await.is_ok() {
        return;
    }

    let result = storage.store_state(blob(b"state")).await;

    // restore before asserting, so a failure here doesn't leave an undeletable temp directory
    fs::set_permissions(&states, Permissions::from_mode(0o755))
        .await
        .unwrap();

    let err = result.unwrap_err();
    assert!(
        format!("{:#}", err).contains("content addressible"),
        "got: {:#}",
        err
    );
}
