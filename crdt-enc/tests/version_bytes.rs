//! Covers the `VersionBytes`/`VersionBytesRef` surface: the version checks and the errors they
//! produce, the owned/borrowed conversions between them, and the `serialize`/`deserialize` pair.
//! `VersionBytesBuf`'s `Buf` impl has its own file, `version_box_buf.rs`.

use ::bytes::Buf;
use ::crdt_enc::utils::{VersionBytes, VersionBytesBuf, VersionBytesRef};
use ::uuid::Uuid;

static SUPPORTED_VERSIONS: phf::Set<u128> = phf::phf_set! {
    0x_a57761b0_c4b4_48fc_aa81_485cb2e37862_u128,
};

#[test]
fn version_and_content_are_kept_apart() {
    const VERSION: Uuid = Uuid::from_u128(0x_a57761b0_c4b4_48fc_aa81_485cb2e37862);

    let vb = VersionBytes::new(VERSION, vec![1, 2, 3]);

    assert_eq!(vb.version(), VERSION);
    assert_eq!(vb.as_ref(), [1, 2, 3]);

    // `Into<Vec<u8>>` drops the tag, keeping only the content
    let content: Vec<u8> = vb.into();
    assert_eq!(content, [1, 2, 3]);
}

#[test]
fn ensure_version_accepts_only_an_exact_match() {
    const VERSION: Uuid = Uuid::from_u128(0x_a57761b0_c4b4_48fc_aa81_485cb2e37862);
    const OTHER_VERSION: Uuid = Uuid::from_u128(0x_2b0e6ac4_1d51_4a1e_9b1e_6d2f4c8a7e30);

    let vb = VersionBytes::new(VERSION, Vec::new());
    vb.ensure_version(VERSION).unwrap();
    vb.ensure_version(OTHER_VERSION).unwrap_err();

    let vbr = VersionBytesRef::new(VERSION, &[]);
    vbr.ensure_version(VERSION).unwrap();
    vbr.ensure_version(OTHER_VERSION).unwrap_err();
}

#[test]
fn ensure_versions_accepts_any_member_of_the_sorted_slice() {
    const VERSION_A: Uuid = Uuid::from_u128(0x_1111_1111_1111_1111_1111_111111111111);
    const VERSION_B: Uuid = Uuid::from_u128(0x_2222_2222_2222_2222_2222_222222222222);
    const UNSUPPORTED: Uuid = Uuid::from_u128(0x_3333_3333_3333_3333_3333_333333333333);

    let supported = [VERSION_A, VERSION_B];

    VersionBytes::new(VERSION_A, Vec::new())
        .ensure_versions(&supported)
        .unwrap();
    VersionBytes::new(VERSION_B, Vec::new())
        .ensure_versions(&supported)
        .unwrap();
    VersionBytes::new(UNSUPPORTED, Vec::new())
        .ensure_versions(&supported)
        .unwrap_err();

    VersionBytesRef::new(VERSION_A, &[])
        .ensure_versions(&supported)
        .unwrap();
    VersionBytesRef::new(UNSUPPORTED, &[])
        .ensure_versions(&supported)
        .unwrap_err();
}

#[test]
fn ensure_versions_phf_accepts_any_member_of_the_set() {
    const SUPPORTED: Uuid = Uuid::from_u128(0x_a57761b0_c4b4_48fc_aa81_485cb2e37862);
    const UNSUPPORTED: Uuid = Uuid::from_u128(0x_0);

    VersionBytes::new(SUPPORTED, Vec::new())
        .ensure_versions_phf(&SUPPORTED_VERSIONS)
        .unwrap();
    VersionBytes::new(UNSUPPORTED, Vec::new())
        .ensure_versions_phf(&SUPPORTED_VERSIONS)
        .unwrap_err();

    VersionBytesRef::new(SUPPORTED, &[])
        .ensure_versions_phf(&SUPPORTED_VERSIONS)
        .unwrap();
    VersionBytesRef::new(UNSUPPORTED, &[])
        .ensure_versions_phf(&SUPPORTED_VERSIONS)
        .unwrap_err();
}

/// The rendered `VersionError` is what a user actually sees when an old build meets a newer
/// on-disk format, so it has to name both what was found and what would have been accepted.
#[test]
fn version_error_lists_what_it_expected() {
    const VERSION_A: Uuid = Uuid::from_u128(0x_1111_1111_1111_1111_1111_111111111111);
    const VERSION_B: Uuid = Uuid::from_u128(0x_2222_2222_2222_2222_2222_222222222222);
    const GOT: Uuid = Uuid::from_u128(0x_3333_3333_3333_3333_3333_333333333333);

    // one expected version
    let err = VersionBytes::new(GOT, Vec::new())
        .ensure_version(VERSION_A)
        .unwrap_err();
    let rendered = err.to_string();
    assert_eq!(
        rendered,
        format!(
            "version check failed, got: {}, expected one of: {}",
            GOT, VERSION_A
        )
    );

    // several expected versions, comma-separated
    let err = VersionBytes::new(GOT, Vec::new())
        .ensure_versions(&[VERSION_A, VERSION_B])
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        format!(
            "version check failed, got: {}, expected one of: {}, {}",
            GOT, VERSION_A, VERSION_B
        )
    );

    // `Debug` is derived, but is what `anyhow` prints in a backtrace -- make sure it stays useful
    assert!(format!("{:?}", err).contains("VersionError"));
}

#[test]
fn serialize_round_trips_through_deserialize() {
    const VERSION: Uuid = Uuid::from_u128(0x_a57761b0_c4b4_48fc_aa81_485cb2e37862);

    let vb = VersionBytes::new(VERSION, vec![9, 8, 7]);
    let bytes = vb.serialize();

    // the tag is the first 16 bytes, content follows
    assert_eq!(&bytes[..16], VERSION.as_bytes());
    assert_eq!(&bytes[16..], [9, 8, 7]);

    let owned = VersionBytes::deserialize(&bytes).unwrap();
    assert_eq!(owned.version(), VERSION);
    assert_eq!(owned.as_ref(), [9, 8, 7]);

    let borrowed = VersionBytesRef::deserialize(&bytes).unwrap();
    assert_eq!(borrowed.version(), VERSION);
    assert_eq!(borrowed.as_ref(), [9, 8, 7]);
    assert_eq!(borrowed.serialize(), bytes);
}

/// Anything shorter than a bare version tag can't even be split into tag and content, so it's
/// rejected before any content-level parsing is attempted.
#[test]
fn deserialize_rejects_input_too_short_for_a_tag() {
    let err = VersionBytes::deserialize(&[0; 15]).unwrap_err();
    assert_eq!(err.to_string(), "invalid length");
    assert!(format!("{:?}", err).contains("InvalidLength"));

    VersionBytesRef::deserialize(&[0; 15]).unwrap_err();

    // exactly a tag and nothing else is still valid: an empty payload
    let empty = VersionBytes::deserialize(&[0; 16]).unwrap();
    assert_eq!(empty.version(), Uuid::nil());
    assert_eq!(empty.as_ref(), []);
}

#[test]
fn owned_and_borrowed_convert_both_ways() {
    const VERSION: Uuid = Uuid::from_u128(0x_a57761b0_c4b4_48fc_aa81_485cb2e37862);

    let owned = VersionBytes::new(VERSION, vec![4, 5, 6]);

    // &VersionBytes -> VersionBytesRef, borrowing rather than copying
    let borrowed: VersionBytesRef<'_> = (&owned).into();
    assert_eq!(borrowed.version(), VERSION);
    assert_eq!(borrowed.as_ref(), [4, 5, 6]);

    // ... and the same thing spelled as the inherent method
    let borrowed = owned.as_version_bytes_ref();
    assert_eq!(borrowed.as_ref(), [4, 5, 6]);

    // VersionBytesRef -> VersionBytes, copying the content into an owned buffer
    let back: VersionBytes = borrowed.clone().into();
    assert_eq!(back.version(), VERSION);
    assert_eq!(back.as_ref(), [4, 5, 6]);

    // VersionBytesRef -> Vec<u8> drops the tag
    let content: Vec<u8> = borrowed.into();
    assert_eq!(content, [4, 5, 6]);
}

/// Both types hand out the same `Buf` view over tag-then-content, whether the content is owned or
/// borrowed.
#[test]
fn buf_spans_tag_then_content() {
    const VERSION: Uuid = Uuid::from_u128(0x_a57761b0_c4b4_48fc_aa81_485cb2e37862);

    let owned = VersionBytes::new(VERSION, vec![1, 2]);
    assert_eq!(owned.buf().remaining(), 16 + 2);
    assert_eq!(owned.as_version_bytes_ref().buf().remaining(), 16 + 2);
}

/// Once the content is fully consumed `chunk()` has nothing left to hand out -- `Buf`'s contract
/// says it must return an empty slice there rather than panicking on the out-of-range index.
#[test]
fn chunk_is_empty_once_fully_consumed() {
    const VERSION: Uuid = Uuid::from_u128(0x_a57761b0_c4b4_48fc_aa81_485cb2e37862);

    let mut buf = VersionBytesBuf::new(VERSION, &[1, 2, 3]);
    buf.advance(16 + 3);

    assert_eq!(buf.remaining(), 0);
    assert_eq!(buf.chunk(), []);
}

/// `VersionError`'s `Display` writes in several steps, and every one of them has to propagate a
/// writer failure instead of swallowing it -- `write!` into a `String` never fails, so a formatter
/// that always does is the only way to exercise that.
#[test]
fn version_error_display_propagates_a_writer_failure() {
    use ::std::fmt::Write;

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write_str(&mut self, _: &str) -> ::std::fmt::Result {
            Err(::std::fmt::Error)
        }
    }

    const VERSION: Uuid = Uuid::from_u128(0x_a57761b0_c4b4_48fc_aa81_485cb2e37862);
    const GOT: Uuid = Uuid::from_u128(0x_3333_3333_3333_3333_3333_333333333333);

    let err = VersionBytes::new(GOT, Vec::new())
        .ensure_version(VERSION)
        .unwrap_err();

    write!(FailingWriter, "{}", err).unwrap_err();

    /// Fails only once `remaining` writes have gone through, so the failure lands at a different
    /// point in the output each time.
    struct FailAfter {
        remaining: usize,
    }

    impl Write for FailAfter {
        fn write_str(&mut self, _: &str) -> ::std::fmt::Result {
            if self.remaining == 0 {
                return Err(::std::fmt::Error);
            }
            self.remaining -= 1;
            Ok(())
        }
    }

    // several expected versions, so the separator and per-version writes inside the loop get a
    // turn at being the one that fails
    const OTHER: Uuid = Uuid::from_u128(0x_b0000000_0000_4000_8000_000000000000);
    let err = VersionBytes::new(GOT, Vec::new())
        .ensure_versions(&[VERSION, OTHER])
        .unwrap_err();

    for remaining in 0..40 {
        // every one of these must return, not panic; only the last few can succeed
        let _ = write!(FailAfter { remaining }, "{}", err);
    }
    write!(FailAfter { remaining: 1000 }, "{}", err).unwrap();
}

/// `crdt-enc-envelope`'s `Key` writes its key material out through `VersionBytesRef` and reads it
/// back as `VersionBytes` -- a cross-type round trip through the real on-disk format, done that way
/// so serializing can borrow out of a `SecretBytes` instead of copying the raw key into an owned
/// buffer. That only holds if the two encode identically.
///
/// The frozen bytes matter as much as the equality: comparing the two against each other would
/// still pass if a serde or rmp-serde change moved both at once, while silently making every
/// `Keys` blob already on disk unreadable.
#[test]
fn owned_and_borrowed_encode_identically() {
    const VERSION: Uuid = Uuid::from_u128(0x_a57761b0_c4b4_48fc_aa81_485cb2e37862);
    const CONTENT: &[u8] = &[1, 2, 3];

    let from_owned =
        rmp_serde::to_vec_named(&VersionBytes::new(VERSION, CONTENT.to_vec())).unwrap();
    let from_borrowed = rmp_serde::to_vec_named(&VersionBytesRef::new(VERSION, CONTENT)).unwrap();

    assert_eq!(from_owned, from_borrowed);

    // captured once from the encoding above, then frozen: a two-element array holding the version
    // tag and the content, each as a msgpack `bin`
    const ENCODED: &[u8] = &[
        0x92, 0xc4, 0x10, 0xa5, 0x77, 0x61, 0xb0, 0xc4, 0xb4, 0x48, 0xfc, 0xaa, 0x81, 0x48, 0x5c,
        0xb2, 0xe3, 0x78, 0x62, 0xc4, 0x03, 0x01, 0x02, 0x03,
    ];
    assert_eq!(from_owned, ENCODED);

    // each side reads what the other wrote
    let owned: VersionBytes = rmp_serde::from_slice(&from_borrowed).unwrap();
    assert_eq!(owned.version(), VERSION);
    assert_eq!(owned.as_ref(), CONTENT);

    let borrowed: VersionBytesRef<'_> = rmp_serde::from_slice(&from_owned).unwrap();
    assert_eq!(borrowed.version(), VERSION);
    assert_eq!(borrowed.as_ref(), CONTENT);
}
