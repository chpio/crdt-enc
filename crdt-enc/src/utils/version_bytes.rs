use ::bytes::Buf;
use ::serde::{Deserialize, Serialize};
use ::std::{borrow::Cow, fmt, io::IoSlice};
use ::uuid::Uuid;

#[derive(Debug)]
pub struct VersionError {
    expected: Vec<Uuid>,
    got: Uuid,
}

impl fmt::Display for VersionError {
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VersionBytes(Uuid, #[serde(with = "serde_bytes")] Vec<u8>);

impl VersionBytes {
    pub fn new(version: Uuid, content: Vec<u8>) -> VersionBytes {
        VersionBytes(version, content)
    }

    pub fn version(&self) -> Uuid {
        self.0
    }

    pub fn ensure_version(&self, version: Uuid) -> Result<(), VersionError> {
        self.as_version_bytes_ref().ensure_version(version)
    }

    /// `versions` needs to be sorted!
    pub fn ensure_versions(&self, versions: &[Uuid]) -> Result<(), VersionError> {
        self.as_version_bytes_ref().ensure_versions(versions)
    }

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

    pub fn as_version_bytes_ref(&self) -> VersionBytesRef<'_> {
        VersionBytesRef::new(self.version(), self.as_ref())
    }

    pub fn buf(&self) -> VersionBytesBuf<'_> {
        VersionBytesBuf::new(self.version(), self.as_ref())
    }

    pub fn deserialize(slice: &[u8]) -> Result<VersionBytes, DeserializeError> {
        Ok(VersionBytesRef::deserialize(slice)?.into())
    }

    pub fn serialize(&self) -> Vec<u8> {
        self.as_version_bytes_ref().serialize()
    }
}

impl From<VersionBytes> for Vec<u8> {
    fn from(v: VersionBytes) -> Vec<u8> {
        v.1
    }
}

impl From<VersionBytesRef<'_>> for VersionBytes {
    fn from(v: VersionBytesRef<'_>) -> VersionBytes {
        VersionBytes::new(v.version(), v.into())
    }
}

impl AsRef<[u8]> for VersionBytes {
    fn as_ref(&self) -> &[u8] {
        self.1.as_ref()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VersionBytesRef<'a>(
    Uuid,
    #[serde(borrow)]
    #[serde(with = "serde_bytes")]
    Cow<'a, [u8]>,
);

impl<'a> VersionBytesRef<'a> {
    pub fn new(version: Uuid, content: &'a [u8]) -> VersionBytesRef<'a> {
        VersionBytesRef(version, Cow::Borrowed(content))
    }

    pub fn version(&self) -> Uuid {
        self.0
    }

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

    /// `versions` needs to be sorted!
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

    pub fn buf(&self) -> VersionBytesBuf<'_> {
        VersionBytesBuf::new(self.version(), self.as_ref())
    }

    pub fn deserialize(slice: &'a [u8]) -> Result<VersionBytesRef<'a>, DeserializeError> {
        if slice.len() < VERSION_LEN {
            return Err(DeserializeError::InvalidLength);
        }

        let mut version = [0; VERSION_LEN];
        version.copy_from_slice(&slice[0..VERSION_LEN]);
        let version = Uuid::from_bytes(version);

        Ok(VersionBytesRef::new(version, &slice[VERSION_LEN..]))
    }

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
    fn from(v: VersionBytesRef<'a>) -> Vec<u8> {
        v.1.into()
    }
}

impl<'a> From<&'a VersionBytes> for VersionBytesRef<'a> {
    fn from(v: &'a VersionBytes) -> VersionBytesRef<'a> {
        VersionBytesRef::new(v.version(), v.as_ref())
    }
}

impl<'a> AsRef<[u8]> for VersionBytesRef<'a> {
    fn as_ref(&self) -> &[u8] {
        self.1.as_ref()
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum DeserializeError {
    InvalidLength,
}

impl fmt::Display for DeserializeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        "invalid length".fmt(f)
    }
}

impl std::error::Error for DeserializeError {}

const VERSION_LEN: usize = 16;

#[derive(Debug, Clone)]
pub struct VersionBytesBuf<'a> {
    pos: usize,
    version: uuid::Bytes,
    content: &'a [u8],
}

impl<'a> VersionBytesBuf<'a> {
    pub fn new(version: Uuid, content: &'a [u8]) -> VersionBytesBuf<'a> {
        VersionBytesBuf {
            pos: 0,
            version: *version.as_bytes(),
            content,
        }
    }
}

impl<'a> Buf for VersionBytesBuf<'a> {
    fn remaining(&self) -> usize {
        VERSION_LEN + self.content.len() - self.pos
    }

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

    fn advance(&mut self, cnt: usize) {
        assert!(cnt <= self.remaining());
        self.pos += cnt;
    }

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
