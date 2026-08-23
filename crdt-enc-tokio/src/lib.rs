//! A [`crdt_enc::storage::Storage`] backed by the local filesystem via Tokio. See [`Storage`] for
//! the on-disk layout.
#![warn(missing_docs)]

use ::anyhow::{Context, Error, Result, ensure};
use ::bytes::Buf;
use ::crdt_enc::utils::{LockBox, VersionBytes, VersionBytesRef};
use ::fs4::tokio::AsyncFileExt;
use ::futures::{
    future::{Either, TryFutureExt},
    stream::{self, Stream, StreamExt, TryStreamExt},
};
use ::std::{
    fmt::Debug,
    path::{Path, PathBuf},
    str::FromStr,
};
use ::tiny_keccak::{Hasher, Sha3};
use ::tokio::{
    fs,
    io::{self, AsyncWriteExt},
};
use ::tokio_stream::wrappers::ReadDirStream;
use ::uuid::Uuid;

/// A [`crdt_enc::storage::Storage`] backed by the local filesystem: `local_path` for device-local
/// meta, and `remote_path` (e.g. a Syncthing-shared tree) for everything that gets synced --
/// subdirectories `meta/`, `states/`, `ops/<actor-uuid>/<version>`. Every file is content-addressed
/// (named by hash of its content, for meta/states) or by actor+version (for ops) and, once written,
/// immutable -- files are only ever created or deleted, never modified in place.
#[derive(Debug)]
pub struct Storage {
    /// Device-local metadata directory, exclusively locked for this process (see
    /// `ensure_local_lock`) and never synced.
    local_path: PathBuf,
    /// The synced tree (e.g. shared via Syncthing) holding `meta/`, `states/`, `ops/`.
    remote_path: PathBuf,
    /// `None` until `ensure_local_lock` acquires the process-exclusive lock on `local_path` (on
    /// first local-meta access); released automatically when this file handle is dropped
    local_lock: LockBox<Option<fs::File>>,
}

impl Storage {
    /// Creates a `Storage` for the given (must be absolute) local/remote directories. Doesn't touch
    /// the filesystem yet -- directories are created lazily on first use.
    pub fn new(local_path: PathBuf, remote_path: PathBuf) -> Result<Storage> {
        ensure!(
            local_path.is_absolute(),
            "local path {} is not absolute",
            local_path.display()
        );
        ensure!(
            remote_path.is_absolute(),
            "remote path {} is not absolute",
            remote_path.display()
        );

        Ok(Storage {
            local_path,
            remote_path,
            local_lock: LockBox::new(None),
        })
    }

    /// Locks `local_path` for exclusive use by this process, failing if another process (e.g. this
    /// same actor's local storage opened twice) already holds it. No-op if already locked.
    async fn ensure_local_lock(&self) -> Result<()> {
        if self.local_lock.with(|lock| lock.is_some()) {
            return Ok(());
        }

        fs::create_dir_all(&self.local_path)
            .await
            .with_context(|| format!("failed creating local dir {}", self.local_path.display()))?;

        let lock_path = self.local_path.join(".lock");
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&lock_path)
            .await
            .with_context(|| format!("failed opening lock file {}", lock_path.display()))?;

        lock_file.try_lock().with_context(|| {
            format!(
                "local storage at {} is already locked by another process \
                 (is the same actor's local storage already open elsewhere?)",
                self.local_path.display()
            )
        })?;

        self.local_lock.with(|lock| *lock = Some(lock_file));

        Ok(())
    }
}

impl crdt_enc::storage::Storage for Storage {
    /// Reads `<local_path>/meta-data.msgpack`, if it exists.
    async fn load_local_meta(&self) -> Result<Option<VersionBytes>> {
        self.ensure_local_lock().await?;

        let path = self.local_path.join("meta-data.msgpack");
        let bytes = read_file_optional(&path)
            .await
            .with_context(|| format!("failed reading local meta file {}", path.display()))?;
        bytes
            .map(|bytes| {
                let lm = VersionBytes::deserialize(&bytes).with_context(|| {
                    format!("failed parsing local meta file {}", path.display())
                })?;
                Ok(lm)
            })
            .transpose()
    }

    /// Overwrites `<local_path>/meta-data.msgpack`.
    async fn store_local_meta(&self, meta: VersionBytes) -> Result<()> {
        self.ensure_local_lock().await?;

        let path = self.local_path.join("meta-data.msgpack");
        // TODO: catch concurrent writes, locking?
        write_file(&path, meta.buf())
            .await
            .with_context(|| format!("failed writing local meta file {:?}", path))?;
        Ok(())
    }

    /// Lists file names under `<remote_path>/meta/`.
    async fn list_remote_meta_names(&self) -> Result<Vec<String>> {
        let meta_dir = self.remote_path.join("meta");
        read_dir_optional_files(meta_dir)
            .map_err(|err| err.context("failed listing remote meta entries"))
            .and_then(|entry| async move {
                let name = entry.file_name().into_string().ok().with_context(|| {
                    format!(
                        "failed converting remote meta entry name to string for {}",
                        entry.path().display()
                    )
                })?;
                Ok(name)
            })
            .try_collect()
            .await
    }

    /// Reads the given files under `<remote_path>/meta/`, up to 32 concurrently.
    async fn load_remote_metas(&self, names: Vec<String>) -> Result<Vec<(String, VersionBytes)>> {
        let futs = names.into_iter().map(|name| {
            let mut path = self.remote_path.join("meta");
            path.push(&name);
            let path = path;

            async move {
                let bytes = fs::read(&path).await.with_context(|| {
                    format!("failed reading remote meta file {}", path.display())
                })?;
                let rm = VersionBytes::deserialize(&bytes).with_context(|| {
                    format!("failed parsing remote meta file {}", path.display())
                })?;
                Ok((name, rm))
            }
        });

        stream::iter(futs).buffer_unordered(32).try_collect().await
    }

    /// Writes a new content-addressed file under `<remote_path>/meta/`, named by the hash of its
    /// content, and returns that name.
    async fn store_remote_meta(&self, meta: VersionBytes) -> Result<String> {
        let meta_dir = self.remote_path.join("meta");
        write_content_addressible_file(&meta_dir, &meta.as_version_bytes_ref())
            .await
            .context("failed writing remote meta file")
    }

    /// Deletes the given files under `<remote_path>/meta/`, up to 32 concurrently; an
    /// already-missing file is not an error.
    async fn remove_remote_metas(&self, names: Vec<String>) -> Result<()> {
        let futs = names.into_iter().map(|name| {
            let mut path = self.remote_path.join("meta");
            path.push(&name);
            let path = path;

            async move {
                remove_file_optional(&path)
                    .await
                    .with_context(|| format!("failed removing remote meta file {}", name))
            }
        });

        stream::iter(futs).buffer_unordered(32).try_collect().await
    }

    /// Lists file names under `<remote_path>/states/`.
    async fn list_state_names(&self) -> Result<Vec<String>> {
        let states_dir = self.remote_path.join("states");
        read_dir_optional_files(states_dir)
            .map_err(|err| err.context("failed listing states"))
            .and_then(|entry| async move {
                let name = entry.file_name().into_string().ok().with_context(|| {
                    format!(
                        "failed converting state name to string for state file {}",
                        entry.path().display()
                    )
                })?;
                Ok(name)
            })
            .try_collect()
            .await
    }

    /// Reads the given files under `<remote_path>/states/`, up to 32 concurrently.
    async fn load_states(&self, names: Vec<String>) -> Result<Vec<(String, VersionBytes)>> {
        let futs = names.into_iter().map(|name| {
            let mut path = self.remote_path.join("states");
            path.push(&name);
            let path = path;

            async move {
                let block = fs::read(&path)
                    .await
                    .with_context(|| format!("failed reading state file {}", path.display()))?;
                let block = VersionBytes::deserialize(&block)
                    .with_context(|| format!("failed parsing state file {}", path.display()))?;
                Ok((name, block))
            }
        });

        stream::iter(futs).buffer_unordered(32).try_collect().await
    }

    /// Writes a new content-addressed file under `<remote_path>/states/`, named by the hash of its
    /// content, and returns that name.
    async fn store_state(&self, bytes: VersionBytes) -> Result<String> {
        let states_dir = self.remote_path.join("states");
        write_content_addressible_file(&states_dir, &bytes.as_version_bytes_ref())
            .await
            .context("failed writing state file")
    }

    /// Deletes the given files under `<remote_path>/states/`, up to 32 concurrently; an
    /// already-missing file is not an error. Returns `names` unchanged (echoed back for the caller's
    /// bookkeeping).
    async fn remove_states(&self, names: Vec<String>) -> Result<Vec<String>> {
        let futs = names
            .iter()
            .map(|name| {
                let mut path = self.remote_path.join("states");
                path.push(&name);
                let path = path;

                async move {
                    remove_file_optional(&path)
                        .await
                        .with_context(|| format!("failed removing state file {}", name))
                }
            })
            .map(Ok);

        stream::iter(futs)
            .try_for_each_concurrent(32, |f| f)
            .await?;

        Ok(names)
    }

    /// Lists the actor-uuid-named subdirectories under `<remote_path>/ops/`.
    async fn list_op_actors(&self) -> Result<Vec<Uuid>> {
        let ops_dir = self.remote_path.join("ops");
        read_dir_optional_dirs(ops_dir)
            .map_err(|err| err.context("failed listing actors"))
            .and_then(|entry| async move {
                let actor = entry.file_name();
                let actor = actor.to_str().with_context(|| {
                    format!("error converting actor dir name {:?} to string", actor)
                })?;
                let actor = Uuid::from_str(actor).with_context(|| {
                    format!("error converting actor dir string {} into uuid", actor)
                })?;
                Ok(actor)
            })
            .try_collect()
            .await
    }

    /// For each `(actor, first_version)`, reads `<remote_path>/ops/<actor>/<version>` for
    /// consecutively increasing versions starting at `first_version`, stopping at the first missing
    /// version -- relies on op files being written in strictly ascending, contiguous order per
    /// actor (`store_ops`'s contract), so a gap means "no more ops yet", not "a hole to skip over".
    async fn load_ops(
        &self,
        actor_first_versions: Vec<(Uuid, u64)>,
    ) -> Result<Vec<(Uuid, u64, VersionBytes)>> {
        /// Reads one op file, if it exists.
        async fn get_entry(
            path: &Path,
            actor: Uuid,
            version: u64,
        ) -> Result<Option<(Uuid, u64, VersionBytes)>> {
            let bytes = read_file_optional(path)
                .await
                .with_context(|| format!("failed reading op file {}", path.display()))?;

            let bytes = if let Some(bytes) = bytes {
                bytes
            } else {
                return Ok(None);
            };

            let data = VersionBytes::deserialize(&bytes)
                .with_context(|| format!("failed parsing op file {}", path.display()))?;

            Ok(Some((actor, version, data)))
        }

        let path = self.remote_path.join("ops");

        stream::iter(actor_first_versions)
            .map(move |(actor, first_version)| {
                let path = path.join(actor.to_string());

                async move {
                    let ops = stream::iter(first_version..)
                        .then(move |version| {
                            let path = path.join(version.to_string());
                            async move { get_entry(&path, actor, version).await }
                        })
                        .take_while(|res| {
                            let res = match res {
                                Ok(None) => false,
                                Ok(Some(_)) => true,
                                Err(_) => true,
                            };
                            async move { res }
                        })
                        .try_filter_map(|opt| async move { Ok(opt) })
                        .try_collect::<Vec<_>>()
                        .await?;

                    Result::<_, Error>::Ok(stream::iter(ops).map(Ok))
                }
            })
            .buffer_unordered(32)
            .try_flatten()
            .try_collect()
            .await
    }

    /// Writes `<remote_path>/ops/<actor>/<version>` as a new file, failing if it already exists
    /// (`write_new_file`) -- callers must supply strictly ascending, contiguous versions per actor.
    async fn store_ops(&self, actor: Uuid, version: u64, bytes: VersionBytes) -> Result<()> {
        let mut path = self.remote_path.join("ops");
        path.push(actor.to_string());

        fs::create_dir_all(&path)
            .await
            .with_context(|| format!("failed creating op dir {:?} for actor {}", path, actor))?;

        path.push(version.to_string());
        write_new_file(&path, bytes.buf())
            .await
            .with_context(|| format!("failed writing ops file {:?}", path))?;
        Ok(())
    }

    /// Deletes the given `<remote_path>/ops/<actor>/<version>` files, up to 32 concurrently; an
    /// already-missing file is not an error.
    async fn remove_ops(&self, names: Vec<(Uuid, u64)>) -> Result<()> {
        let futs = names.into_iter().map(|(actor, version)| {
            let mut path = self.remote_path.join("ops");
            path.push(actor.to_string());
            path.push(version.to_string());
            let path = path;

            async move {
                remove_file_optional(&path).await.with_context(|| {
                    format!(
                        "failed removing ops file {} for actor {} version {}",
                        path.display(),
                        actor,
                        version
                    )
                })
            }
        });

        stream::iter(futs).buffer_unordered(32).try_collect().await
    }
}

/// Writes `buf` to `path`, creating or truncating it if it already exists.
async fn write_file(path: &Path, buf: impl Buf) -> io::Result<()> {
    write_file_inner(path, buf, false).await
}

/// Writes `buf` to `path`, failing if `path` already exists (for content that must never be
/// silently overwritten, e.g. ops).
async fn write_new_file(path: &Path, buf: impl Buf) -> io::Result<()> {
    write_file_inner(path, buf, true).await
}

/// Shared implementation behind `write_file`/`write_new_file`: opens `path` (in create-new or
/// create-or-truncate mode per `create_new`), writes `buf`, then flushes and fsyncs before
/// returning.
async fn write_file_inner(path: &Path, mut buf: impl Buf, create_new: bool) -> io::Result<()> {
    let mut open_options = fs::OpenOptions::new();
    if create_new {
        open_options.create_new(true);
    } else {
        open_options.create(true).truncate(true);
    }
    let mut file = open_options.write(true).open(path).await?;

    while buf.has_remaining() {
        file.write_buf(&mut buf).await?;
    }

    // flush internal buffers
    file.flush().await?;
    // fsync
    file.sync_all().await?;
    // TODO: close explicitly to catch closing errors
    // TODO: 1. write to tmp file 2. rename tmp file to real file
    Ok(())
}

/// Like `read_dir_optional`, but yields only subdirectory entries.
fn read_dir_optional_dirs(path: PathBuf) -> impl Stream<Item = Result<fs::DirEntry>> + 'static {
    read_dir_optional_filter_types(path, false)
}

/// Like `read_dir_optional`, but yields only regular-file entries.
fn read_dir_optional_files(path: PathBuf) -> impl Stream<Item = Result<fs::DirEntry>> + 'static {
    read_dir_optional_filter_types(path, true)
}

/// Shared implementation behind `read_dir_optional_dirs`/`read_dir_optional_files`: lists `path`
/// (treating a missing directory as empty, via `read_dir_optional`) and filters to just files or
/// just directories depending on `is_file`.
fn read_dir_optional_filter_types(
    path: PathBuf,
    is_file: bool,
) -> impl Stream<Item = Result<fs::DirEntry>> + 'static {
    read_dir_optional(path)
        .map(move |entry| async move {
            let entry = entry?;
            let ty = entry.file_type().await.with_context(|| {
                format!("failed getting file type for {}", entry.path().display())
            })?;
            match is_file {
                true if ty.is_file() => Ok(Some(entry)),
                false if ty.is_dir() => Ok(Some(entry)),
                _ => Ok(None),
            }
        })
        .buffer_unordered(32)
        .try_filter_map(|res| async move { Ok(res) })
}

/// Lists `path`'s entries as a stream, treating a missing directory as an empty listing rather than
/// an error (every other I/O error still propagates).
fn read_dir_optional(path: PathBuf) -> impl Stream<Item = Result<fs::DirEntry>> + 'static {
    async move {
        match fs::read_dir(&path).await {
            Ok(dir) => {
                let entry_stream = ReadDirStream::new(dir).map(move |entry| {
                    entry.with_context(|| format!("failed getting entry from {}", path.display()))
                });
                Ok(Either::Left(entry_stream))
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                let empty = stream::empty();
                Ok(Either::Right(empty))
            }
            Err(err) => Err(err).context(format!("failed listing entries in {}", path.display())),
        }
    }
    .try_flatten_stream()
}

/// Reads `path`'s full content, treating a missing file as `None` rather than an error (every other
/// I/O error still propagates).
async fn read_file_optional(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::read(&path).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).context(format!("failed reading file {}", path.display())),
    }
}

/// Writes `bytes` as a new immutable file under `dir_path`, named by the base32 encoding of its
/// SHA3-256 hash (so writing the same content twice is naturally idempotent), and returns that
/// name.
async fn write_content_addressible_file(
    dir_path: &Path,
    bytes: &VersionBytesRef<'_>,
) -> Result<String> {
    let mut digest = Sha3::v256();
    let mut buf = bytes.buf();
    while buf.has_remaining() {
        let chunk = buf.chunk();
        digest.update(chunk);
        let chunk_len = chunk.len();
        buf.advance(chunk_len);
    }
    let mut digest_output = [0; 32];
    digest.finalize(&mut digest_output);
    let block_id = data_encoding::BASE32_NOPAD.encode(&digest_output);

    fs::create_dir_all(dir_path)
        .await
        .with_context(|| format!("failed creating dir {}", dir_path.display()))?;
    let file_path = dir_path.join(&block_id);
    write_new_file(&file_path, bytes.buf())
        .await
        .with_context(|| {
            format!(
                "failed writing content addressible file {}",
                file_path.display()
            )
        })?;
    Ok(block_id)
}

/// Deletes `path`, treating it already being missing as success rather than an error (every other
/// I/O error still propagates).
async fn remove_file_optional(path: &Path) -> Result<()> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).context(format!("failed removing file {}", path.display())),
    }
}
