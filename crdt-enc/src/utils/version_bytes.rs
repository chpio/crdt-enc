use ::bytes::Buf;
use ::serde::{Deserialize, Serialize};
use ::std::{borrow::Cow, fmt, io::IoSlice};
use ::uuid::Uuid;

/// The error returned by the `ensure_version*` family when a blob's version tag isn't one this
/// build supports.
#[derive(Debug)]
pub struct VersionError {
    /// The version(s) that would have been accepted.
    expected: Vec<Uuid>,
    /// The version actually found on the blob.
    got: Uuid,
}

impl fmt::Display for VersionError {
    /// Renders as `version check failed, got: <got>, expected one of: <expected...>`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "version check failed, got: {}, expected one of: ",
            self.got
        )?;
        for (i, e) in self.expected.iter().enumerate() {
            if i != 0 {
                f.write_str(", ")?;
            }
            fmt::Display::fmt(e, f)?;
        }
        Ok(())
    }
}

impl std::error::Error for VersionError {}

/// A UUID version tag prepended to an owned byte blob, used everywhere data is serialized so
/// on-disk/on-wire formats can evolve safely: readers check the tag via `ensure_version`/
/// `ensure_versions`/`ensure_versions_phf` before attempting to deserialize the content. See
/// `VersionBytesRef` for the borrowed counterpart and `VersionBytesBuf` for a zero-copy `bytes::Buf`
/// view over either.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VersionBytes(Uuid, #[serde(with = "serde_bytes")] Vec<u8>);

impl VersionBytes {
    /// Tags `content` with `version`.
    pub fn new(version: Uuid, content: Vec<u8>) -> VersionBytes {
        VersionBytes(version, content)
    }

    /// The version tag.
    pub fn version(&self) -> Uuid {
        self.0
    }

    /// Fails unless `self.version() == version`.
    pub fn ensure_version(&self, version: Uuid) -> Result<(), VersionError> {
        self.as_version_bytes_ref().ensure_version(version)
    }

    /// Fails unless `self.version()` is one of `versions`. `versions` needs to be sorted!
    pub fn ensure_versions(&self, versions: &[Uuid]) -> Result<(), VersionError> {
        self.as_version_bytes_ref().ensure_versions(versions)
    }

    /// Fails unless `self.version()` is a member of the `phf::Set` `versions` (e.g. a crate's
    /// `SUPPORTED_VERSIONS` constant).
    ///
    /// ```
    /// use ::crdt_enc::utils::VersionBytes;
    /// use ::uuid::Uuid;
    ///
    /// static SUPPORTED_VERSIONS: phf::Set<u128> = phf::phf_set! {
    ///     0x_a57761b0_c4b4_48fc_aa81_485cb2e37862_u128,
    /// };
    ///
    /// let vb = VersionBytes::new(
    ///     Uuid::from_u128(0x_a57761b0_c4b4_48fc_aa81_485cb2e37862),
    ///     Vec::new(),
    /// );
    /// vb.ensure_versions_phf(&SUPPORTED_VERSIONS).unwrap();
    ///
    /// let vb_wrong_version = VersionBytes::new(
    ///     Uuid::from_u128(0x_0),
    ///     Vec::new(),
    /// );
    /// vb_wrong_version.ensure_versions_phf(&SUPPORTED_VERSIONS).unwrap_err();
    /// ```
    pub fn ensure_versions_phf(&self, versions: &phf::Set<u128>) -> Result<(), VersionError> {
        self.as_version_bytes_ref().ensure_versions_phf(versions)
    }

    /// Borrows this value as a `VersionBytesRef`.
    pub fn as_version_bytes_ref(&self) -> VersionBytesRef<'_> {
        VersionBytesRef::new(self.version(), self.as_ref())
    }

    /// A zero-copy `bytes::Buf` view over the version tag followed by the content.
    pub fn buf(&self) -> VersionBytesBuf<'_> {
        VersionBytesBuf::new(self.version(), self.as_ref())
    }

    /// Parses a version tag plus content back out of `serialize`'s output.
    pub fn deserialize(slice: &[u8]) -> Result<VersionBytes, DeserializeError> {
        Ok(VersionBytesRef::deserialize(slice)?.into())
    }

    /// Serializes the version tag followed by the content into one contiguous buffer.
    pub fn serialize(&self) -> Vec<u8> {
        self.as_version_bytes_ref().serialize()
    }
}

impl From<VersionBytes> for Vec<u8> {
    /// Discards the version tag, keeping only the content.
    fn from(v: VersionBytes) -> Vec<u8> {
        v.1
    }
}

impl From<VersionBytesRef<'_>> for VersionBytes {
    /// Copies the borrowed content into an owned `VersionBytes`.
    fn from(v: VersionBytesRef<'_>) -> VersionBytes {
        VersionBytes::new(v.version(), v.into())
    }
}

impl AsRef<[u8]> for VersionBytes {
    /// The content, without the version tag.
    fn as_ref(&self) -> &[u8] {
        self.1.as_ref()
    }
}

/// The borrowed (or copy-on-write) counterpart of `VersionBytes` -- avoids an owned copy when the
/// content is already available as a borrowed slice, e.g. right after loading a blob from storage.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VersionBytesRef<'a>(
    Uuid,
    #[serde(borrow)]
    #[serde(with = "serde_bytes")]
    Cow<'a, [u8]>,
);

impl<'a> VersionBytesRef<'a> {
    /// Tags `content` with `version`, borrowing it.
    pub fn new(version: Uuid, content: &'a [u8]) -> VersionBytesRef<'a> {
        VersionBytesRef(version, Cow::Borrowed(content))
    }

    /// The version tag.
    pub fn version(&self) -> Uuid {
        self.0
    }

    /// Fails unless `self.version() == version`.
    pub fn ensure_version(&self, version: Uuid) -> Result<(), VersionError> {
        if self.version() != version {
            Err(VersionError {
                expected: vec![version],
                got: self.version(),
            })
        } else {
            Ok(())
        }
    }

    /// Fails unless `self.version()` is one of `versions`. `versions` needs to be sorted!
    pub fn ensure_versions(&self, versions: &[Uuid]) -> Result<(), VersionError> {
        if versions.binary_search(&self.version()).is_err() {
            Err(VersionError {
                expected: versions.to_owned(),
                got: self.version(),
            })
        } else {
            Ok(())
        }
    }

    /// Fails unless `self.version()` is a member of the `phf::Set` `versions` (e.g. a crate's
    /// `SUPPORTED_VERSIONS` constant).
    ///
    /// ```
    /// use ::crdt_enc::utils::VersionBytesRef;
    /// use ::uuid::Uuid;
    ///
    /// static SUPPORTED_VERSIONS: phf::Set<u128> = phf::phf_set! {
    ///     0x_a57761b0_c4b4_48fc_aa81_485cb2e37862_u128,
    /// };
    ///
    /// let vb = VersionBytesRef::new(
    ///     Uuid::from_u128(0x_a57761b0_c4b4_48fc_aa81_485cb2e37862),
    ///     &[],
    /// );
    /// vb.ensure_versions_phf(&SUPPORTED_VERSIONS).unwrap();
    ///
    /// let vb_wrong_version = VersionBytesRef::new(
    ///     Uuid::from_u128(0x_0),
    ///     &[],
    /// );
    /// vb_wrong_version.ensure_versions_phf(&SUPPORTED_VERSIONS).unwrap_err();
    /// ```
    pub fn ensure_versions_phf(&self, versions: &phf::Set<u128>) -> Result<(), VersionError> {
        if versions.contains(&self.version().as_u128()) {
            Ok(())
        } else {
            Err(VersionError {
                expected: versions.iter().copied().map(Uuid::from_u128).collect(),
                got: self.version(),
            })
        }
    }

    /// A zero-copy `bytes::Buf` view over the version tag followed by the content.
    pub fn buf(&self) -> VersionBytesBuf<'_> {
        VersionBytesBuf::new(self.version(), self.as_ref())
    }

    /// Parses a version tag plus content back out of `serialize`'s output, borrowing from `slice`.
    pub fn deserialize(slice: &'a [u8]) -> Result<VersionBytesRef<'a>, DeserializeError> {
        if slice.len() < VERSION_LEN {
            return Err(DeserializeError::InvalidLength);
        }

        let mut version = [0; VERSION_LEN];
        version.copy_from_slice(&slice[0..VERSION_LEN]);
        let version = Uuid::from_bytes(version);

        Ok(VersionBytesRef::new(version, &slice[VERSION_LEN..]))
    }

    /// Serializes the version tag followed by the content into one contiguous buffer.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = self.buf();
        let mut vec = Vec::with_capacity(buf.remaining());
        while buf.has_remaining() {
            let chunk = buf.chunk();
            vec.extend_from_slice(chunk);
            let chunk_len = chunk.len();
            buf.advance(chunk_len);
        }
        vec
    }
}

impl<'a> From<VersionBytesRef<'a>> for Vec<u8> {
    /// Discards the version tag, keeping only the content (cloning it if it was borrowed).
    fn from(v: VersionBytesRef<'a>) -> Vec<u8> {
        v.1.into()
    }
}

impl<'a> From<&'a VersionBytes> for VersionBytesRef<'a> {
    /// Borrows an owned `VersionBytes`'s content instead of copying it.
    fn from(v: &'a VersionBytes) -> VersionBytesRef<'a> {
        VersionBytesRef::new(v.version(), v.as_ref())
    }
}

impl<'a> AsRef<[u8]> for VersionBytesRef<'a> {
    /// The content, without the version tag.
    fn as_ref(&self) -> &[u8] {
        self.1.as_ref()
    }
}

/// The error returned by `VersionBytes::deserialize`/`VersionBytesRef::deserialize` when the input
/// is too short to even contain a version tag.
#[derive(Debug)]
#[non_exhaustive]
pub enum DeserializeError {
    /// The input was shorter than `VERSION_LEN` bytes.
    InvalidLength,
}

impl fmt::Display for DeserializeError {
    /// Renders as `invalid length`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        "invalid length".fmt(f)
    }
}

impl std::error::Error for DeserializeError {}

/// The byte length of a serialized `Uuid` version tag.
const VERSION_LEN: usize = 16;

/// A zero-copy `bytes::Buf` view over a version tag followed by borrowed content, without having to
/// first concatenate them into one contiguous buffer.
#[derive(Debug, Clone)]
pub struct VersionBytesBuf<'a> {
    /// The current read position, counted from the start of the version tag (i.e. spans both the
    /// tag and the content).
    pos: usize,
    /// The version tag's raw bytes.
    version: uuid::Bytes,
    /// The content that follows the version tag.
    content: &'a [u8],
}

impl<'a> VersionBytesBuf<'a> {
    /// Creates a buffer that yields `version`'s bytes followed by `content`.
    pub fn new(version: Uuid, content: &'a [u8]) -> VersionBytesBuf<'a> {
        VersionBytesBuf {
            pos: 0,
            version: version.into_bytes(),
            content,
        }
    }
}

impl<'a> Buf for VersionBytesBuf<'a> {
    /// Bytes left to read, across both the version tag and the content.
    fn remaining(&self) -> usize {
        VERSION_LEN + self.content.len() - self.pos
    }

    /// The next contiguous chunk: the rest of the version tag if `pos` is still inside it,
    /// otherwise the rest of the content.
    fn chunk(&self) -> &[u8] {
        if self.pos < VERSION_LEN {
            &self.version[self.pos..]
        } else {
            let pos = self.pos - VERSION_LEN;
            if self.content.len() <= pos {
                &[]
            } else {
                &self.content[pos..]
            }
        }
    }

    /// Advances the read position by `cnt` bytes.
    fn advance(&mut self, cnt: usize) {
        assert!(cnt <= self.remaining());
        self.pos += cnt;
    }

    /// Fills `dst` with up to two vectored slices: the remaining version tag and/or the remaining
    /// content, whichever haven't been fully read yet.
    fn chunks_vectored<'b>(&'b self, dst: &mut [IoSlice<'b>]) -> usize {
        if dst.len() == 0 {
            return 0;
        }

        if self.pos < VERSION_LEN {
            dst[0] = IoSlice::new(&self.version[self.pos..]);

            if dst.len() == 1 {
                1
            } else {
                dst[1] = IoSlice::new(self.content);
                2
            }
        } else {
            let pos = self.pos - VERSION_LEN;
            if self.content.len() == pos {
                0
            } else {
                dst[0] = IoSlice::new(&self.content[pos..]);
                1
            }
        }
    }
}
