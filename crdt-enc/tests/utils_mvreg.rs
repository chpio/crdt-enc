//! Covers the `MVReg<VersionBytes, Uuid>` encode/decode helpers -- the extension point a
//! `Protector` plugs its crypto into -- plus the small pieces around them (`EmptyCrdt`, `LockBox`).

use ::anyhow::{Error, Result};
use ::crdt_enc::utils::{
    EmptyCrdt, LockBox, VersionBytes, decode_version_bytes_mvreg,
    decode_version_bytes_mvreg_custom, decode_version_bytes_mvreg_custom_phf,
    encode_version_bytes_mvreg, encode_version_bytes_mvreg_custom,
};
use ::crdts::{CmRDT, CvRDT, MVReg, ctx::ReadCtx};
use ::serde::{Deserialize, Serialize};
use ::std::convert::Infallible;
use ::uuid::Uuid;

const VERSION: Uuid = Uuid::from_u128(0x_9c1d4f2a_63b8_4e07_bb95_4a1f0c8d2e51);
const UNSUPPORTED_VERSION: Uuid = Uuid::from_u128(0x_11111111_1111_1111_1111_111111111111);
const SUPPORTED: &[Uuid] = &[VERSION];

static SUPPORTED_PHF: phf::Set<u128> = phf::phf_set! {
    0x_9c1d4f2a_63b8_4e07_bb95_4a1f0c8d2e51_u128,
};

/// A minimal `CvRDT` whose merge is observable (unlike `EmptyCrdt`'s), so a test can tell whether
/// every concurrent value in a register actually made it into the decoded result.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
struct MaxU64(u64);

impl CvRDT for MaxU64 {
    type Validation = Infallible;

    fn validate_merge(&self, _other: &Self) -> Result<(), Infallible> {
        Ok(())
    }

    fn merge(&mut self, other: Self) {
        self.0 = self.0.max(other.0);
    }
}

/// Writes `val` into `reg` as one op from `actor`, deriving the causal context from `reg` itself.
fn write(reg: &mut MVReg<VersionBytes, Uuid>, actor: Uuid, val: MaxU64, version: Uuid) {
    let read_ctx = reg.read();
    let ctx = ReadCtx {
        add_clock: read_ctx.add_clock,
        rm_clock: read_ctx.rm_clock,
        val,
    };
    encode_version_bytes_mvreg(reg, ctx, actor, version).unwrap();
}

#[test]
fn plain_encode_decode_round_trips() {
    let actor = Uuid::new_v4();
    let mut reg = MVReg::new();

    write(&mut reg, actor, MaxU64(7), VERSION);

    let decoded: ReadCtx<MaxU64, Uuid> = decode_version_bytes_mvreg(&reg, SUPPORTED).unwrap();
    assert_eq!(decoded.val, MaxU64(7));
}

/// An empty register decodes to `T::default()` rather than failing -- that's what lets a protector
/// distinguish "nothing published yet" from "published something" without a separate flag.
#[test]
fn decoding_an_empty_register_yields_the_default() {
    let reg = MVReg::new();

    let decoded: ReadCtx<MaxU64, Uuid> = decode_version_bytes_mvreg(&reg, SUPPORTED).unwrap();
    assert_eq!(decoded.val, MaxU64::default());
}

/// Two devices writing before ever syncing leave the register holding both values concurrently;
/// decoding has to fold *all* of them together, not pick one.
#[test]
fn concurrent_values_are_all_merged() {
    let actor_a = Uuid::new_v4();
    let actor_b = Uuid::new_v4();

    let mut reg_a = MVReg::new();
    write(&mut reg_a, actor_a, MaxU64(3), VERSION);

    let mut reg_b = MVReg::new();
    write(&mut reg_b, actor_b, MaxU64(11), VERSION);

    reg_a.merge(reg_b);
    assert_eq!(reg_a.read().val.len(), 2, "expected a concurrent register");

    let decoded: ReadCtx<MaxU64, Uuid> = decode_version_bytes_mvreg(&reg_a, SUPPORTED).unwrap();
    assert_eq!(decoded.val, MaxU64(11));
}

#[test]
fn plain_decode_rejects_an_unsupported_version() {
    let actor = Uuid::new_v4();
    let mut reg = MVReg::new();

    write(&mut reg, actor, MaxU64(7), UNSUPPORTED_VERSION);

    decode_version_bytes_mvreg::<MaxU64>(&reg, SUPPORTED).unwrap_err();
}

#[test]
fn plain_decode_rejects_content_that_is_not_msgpack() {
    let actor = Uuid::new_v4();
    let mut reg: MVReg<VersionBytes, Uuid> = MVReg::new();

    let read_ctx = reg.read();
    let op = reg.write(
        // 0xc1 is msgpack's "never used" byte -- valid for no type at all
        VersionBytes::new(VERSION, vec![0xc1]),
        read_ctx.derive_add_ctx(actor),
    );
    reg.apply(op);

    decode_version_bytes_mvreg::<MaxU64>(&reg, SUPPORTED).unwrap_err();
}

/// Inverts every byte -- stands in for a real `Protector::encrypt`/`decrypt` pair, so the test can
/// tell that `buf_encode`/`buf_decode` were actually applied rather than bypassed.
fn flip(buf: Vec<u8>) -> Vec<u8> {
    buf.into_iter().map(|b| !b).collect()
}

#[tokio::test]
async fn custom_encode_decode_round_trips() {
    let actor = Uuid::new_v4();
    let mut reg: MVReg<VersionBytes, Uuid> = MVReg::new();

    let read_ctx = reg.read();
    encode_version_bytes_mvreg_custom(
        &mut reg,
        ReadCtx {
            add_clock: read_ctx.add_clock,
            rm_clock: read_ctx.rm_clock,
            val: MaxU64(42),
        },
        actor,
        VERSION,
        |buf| async move { Ok(flip(buf)) },
    )
    .await
    .unwrap();

    // without the matching decode the payload is unreadable ...
    decode_version_bytes_mvreg::<MaxU64>(&reg, SUPPORTED).unwrap_err();

    // ... with it, the value comes back
    let decoded: ReadCtx<MaxU64, Uuid> =
        decode_version_bytes_mvreg_custom(&reg, SUPPORTED, |buf| async move { Ok(flip(buf)) })
            .await
            .unwrap();
    assert_eq!(decoded.val, MaxU64(42));

    // the `phf` variant differs only in how it checks the version
    let decoded: ReadCtx<MaxU64, Uuid> = decode_version_bytes_mvreg_custom_phf(
        &reg,
        &SUPPORTED_PHF,
        |buf| async move { Ok(flip(buf)) },
    )
    .await
    .unwrap();
    assert_eq!(decoded.val, MaxU64(42));
}

#[tokio::test]
async fn custom_decode_rejects_an_unsupported_version() {
    let actor = Uuid::new_v4();
    let mut reg: MVReg<VersionBytes, Uuid> = MVReg::new();

    let read_ctx = reg.read();
    encode_version_bytes_mvreg_custom(
        &mut reg,
        ReadCtx {
            add_clock: read_ctx.add_clock,
            rm_clock: read_ctx.rm_clock,
            val: MaxU64(42),
        },
        actor,
        UNSUPPORTED_VERSION,
        |buf| async move { Ok(flip(buf)) },
    )
    .await
    .unwrap();

    decode_version_bytes_mvreg_custom::<MaxU64, _, _, Vec<u8>>(&reg, SUPPORTED, |buf| async move {
        Ok(flip(buf))
    })
    .await
    .unwrap_err();

    decode_version_bytes_mvreg_custom_phf::<MaxU64, _, _, Vec<u8>>(
        &reg,
        &SUPPORTED_PHF,
        |buf| async move { Ok(flip(buf)) },
    )
    .await
    .unwrap_err();
}

/// A failing `buf_decode` is the "wrong password / unknown key" case: it has to surface as an
/// error rather than being swallowed into a default-valued result.
#[tokio::test]
async fn custom_decode_propagates_a_failing_buf_decode() {
    let actor = Uuid::new_v4();
    let mut reg = MVReg::new();

    write(&mut reg, actor, MaxU64(1), VERSION);

    decode_version_bytes_mvreg_custom::<MaxU64, _, _, Vec<u8>>(&reg, SUPPORTED, |_| async move {
        Err(Error::msg("cannot decrypt"))
    })
    .await
    .unwrap_err();

    decode_version_bytes_mvreg_custom_phf::<MaxU64, _, _, Vec<u8>>(
        &reg,
        &SUPPORTED_PHF,
        |_| async move { Err(Error::msg("cannot decrypt")) },
    )
    .await
    .unwrap_err();
}

#[tokio::test]
async fn custom_encode_propagates_a_failing_buf_encode() {
    let actor = Uuid::new_v4();
    let mut reg: MVReg<VersionBytes, Uuid> = MVReg::new();

    let read_ctx = reg.read();
    encode_version_bytes_mvreg_custom(
        &mut reg,
        ReadCtx {
            add_clock: read_ctx.add_clock,
            rm_clock: read_ctx.rm_clock,
            val: MaxU64(42),
        },
        actor,
        VERSION,
        |_| async move { Err(Error::msg("cannot encrypt")) },
    )
    .await
    .unwrap_err();

    assert!(
        reg.read().val.is_empty(),
        "nothing should have been written"
    );
}

#[tokio::test]
async fn custom_decode_rejects_content_that_is_not_msgpack() {
    let actor = Uuid::new_v4();
    let mut reg: MVReg<VersionBytes, Uuid> = MVReg::new();

    let read_ctx = reg.read();
    let op = reg.write(
        // 0xc1 is msgpack's "never used" byte -- valid for no type at all
        VersionBytes::new(VERSION, vec![0xc1]),
        read_ctx.derive_add_ctx(actor),
    );
    reg.apply(op);

    decode_version_bytes_mvreg_custom::<MaxU64, _, _, Vec<u8>>(&reg, SUPPORTED, |buf| async move {
        Ok(buf)
    })
    .await
    .unwrap_err();

    decode_version_bytes_mvreg_custom_phf::<MaxU64, _, _, Vec<u8>>(
        &reg,
        &SUPPORTED_PHF,
        |buf| async move { Ok(buf) },
    )
    .await
    .unwrap_err();
}

/// `EmptyCrdt` exists to be a stand-in `S` for `Core` in tests/tools that only care about
/// storage/protector behaviour, so every one of its no-op impls has to actually be a no-op.
#[test]
fn empty_crdt_is_a_no_op_in_both_directions() {
    let mut crdt = EmptyCrdt;

    crdt.validate_op(&()).unwrap();
    crdt.apply(());

    let other = EmptyCrdt;
    crdt.validate_merge(&other).unwrap();
    crdt.merge(other);

    // round-trips through the wire format `Core` would persist it with
    let bytes = rmp_serde::to_vec_named(&crdt).unwrap();
    let _: EmptyCrdt = rmp_serde::from_slice(&bytes).unwrap();

    assert!(format!("{:?}", crdt).contains("EmptyCrdt"));
}

#[test]
fn lock_box_hands_out_the_guarded_value_mutably() {
    let boxed = LockBox::new(1u32);

    let doubled = boxed.with(|v| {
        *v *= 2;
        *v
    });
    assert_eq!(doubled, 2);
    assert_eq!(boxed.with(|v| *v), 2);

    assert!(format!("{:?}", boxed).contains("LockBox"));
}

#[test]
fn lock_box_try_with_forwards_both_outcomes() {
    let boxed = LockBox::new(1u32);

    assert_eq!(boxed.try_with(|v| Ok(*v + 1)).unwrap(), 2);
    boxed
        .try_with(|_| Result::<u32>::Err(Error::msg("nope")))
        .unwrap_err();
}

/// A value that cannot be serialized has to surface as an error from the encode helpers, not a
/// half-written register entry -- `T: Serialize` is the caller's type, so this crate cannot assume
/// it always succeeds.
#[tokio::test]
async fn encode_reports_a_value_it_cannot_serialize() {
    struct Unserializable;

    impl Serialize for Unserializable {
        fn serialize<S: ::serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(::serde::ser::Error::custom(
                "this value refuses to serialize",
            ))
        }
    }

    let actor = Uuid::new_v4();
    let mut reg: MVReg<VersionBytes, Uuid> = MVReg::new();

    let read_ctx = reg.read();
    encode_version_bytes_mvreg(
        &mut reg,
        ReadCtx {
            add_clock: read_ctx.add_clock,
            rm_clock: read_ctx.rm_clock,
            val: Unserializable,
        },
        actor,
        VERSION,
    )
    .unwrap_err();

    let read_ctx = reg.read();
    encode_version_bytes_mvreg_custom(
        &mut reg,
        ReadCtx {
            add_clock: read_ctx.add_clock,
            rm_clock: read_ctx.rm_clock,
            val: Unserializable,
        },
        actor,
        VERSION,
        |buf| async move { Ok(buf) },
    )
    .await
    .unwrap_err();

    assert!(
        reg.read().val.is_empty(),
        "nothing should have been written"
    );
}
