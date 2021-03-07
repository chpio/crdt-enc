use ::bytes::Buf;
use ::serde::{Deserialize, Serialize};
use ::std::{borrow::Cow, convert::TryFrom, fmt, io::IoSlice};
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
        if self.0 != version {
            Err(VersionError {
                expected: vec![version],
                got: self.0,
            })
        } else {
            Ok(())
        }
    }

    /// `versions` needs to be sorted!
    pub fn ensure_versions(&self, versions: &[Uuid]) -> Result<(), VersionError> {
        if versions.binary_search(&self.0).is_err() {
            Err(VersionError {
                expected: versions.to_owned(),
                got: self.0,
            })
        } else {
            Ok(())
        }
    }

    pub fn as_version_bytes_ref(&self) -> VersionBytesRef<'_> {
        VersionBytesRef::new(self.0, &self.1)
    }

    pub fn buf(&self) -> VersionBytesBuf<'_> {
        VersionBytesBuf::new(self.0, &self.1)
    }

    pub fn from_slice(slice: &[u8]) -> Result<VersionBytes, ParseError> {
        TryFrom::try_from(slice)
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.as_version_bytes_ref().to_vec()
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.1
    }
}

impl From<VersionBytes> for Vec<u8> {
    fn from(v: VersionBytes) -> Vec<u8> {
        v.1
    }
}

impl From<VersionBytesRef<'_>> for VersionBytes {
    fn from(v: VersionBytesRef<'_>) -> VersionBytes {
        VersionBytes::new(v.0, v.1.into())
    }
}

impl AsRef<[u8]> for VersionBytes {
    fn as_ref(&self) -> &[u8] {
        self.1.as_ref()
    }
}

impl TryFrom<&[u8]> for VersionBytes {
    type Error = ParseError;

    fn try_from(buf: &[u8]) -> Result<VersionBytes, ParseError> {
        Ok(VersionBytesRef::try_from(buf)?.into())
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
        if self.0 != version {
            Err(VersionError {
                expected: vec![version],
                got: self.0,
            })
        } else {
            Ok(())
        }
    }

    /// `versions` needs to be sorted!
    pub fn ensure_versions(&self, versions: &[Uuid]) -> Result<(), VersionError> {
        if versions.binary_search(&self.0).is_err() {
            Err(VersionError {
                expected: versions.to_owned(),
                got: self.0,
            })
        } else {
            Ok(())
        }
    }

    pub fn buf(&self) -> VersionBytesBuf<'_> {
        VersionBytesBuf::new(self.0, &self.1)
    }

    pub fn from_slice(slice: &'a [u8]) -> Result<VersionBytesRef<'a>, ParseError> {
        TryFrom::try_from(slice)
    }

    pub fn to_vec(&self) -> Vec<u8> {
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

impl<'a> AsRef<[u8]> for VersionBytesRef<'a> {
    fn as_ref(&self) -> &[u8] {
        self.1.as_ref()
    }
}

impl<'a> From<&'a VersionBytes> for VersionBytesRef<'a> {
    fn from(v: &'a VersionBytes) -> VersionBytesRef<'a> {
        VersionBytesRef::new(v.0, &v.1)
    }
}

impl<'a> TryFrom<&'a [u8]> for VersionBytesRef<'a> {
    type Error = ParseError;

    fn try_from(buf: &'a [u8]) -> Result<VersionBytesRef<'a>, ParseError> {
        if buf.len() < VERSION_LEN {
            return Err(ParseError::InvalidLength);
        }

        let mut version = [0; 16];
        version.copy_from_slice(&buf[0..16]);
        let version = Uuid::from_bytes(version);

        Ok(VersionBytesRef::new(version, &buf[VERSION_LEN..]))
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ParseError {
    InvalidLength,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        "invalid length".fmt(f)
    }
}

impl std::error::Error for ParseError {}

const VERSION_LEN: usize = 16;

#[derive(Debug, Clone)]
pub struct VersionBytesBuf<'a> {
    pos: usize,
    version: [u8; VERSION_LEN],
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
        // TODO: TESTING!

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
            if self.content.len() <= pos {
                0
            } else {
                dst[0] = IoSlice::new(&self.content[pos..]);
                1
            }
        }
    }
}
