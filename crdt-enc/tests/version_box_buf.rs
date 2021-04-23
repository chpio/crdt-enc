use bytes::Buf;
use crdt_enc::utils::VersionBytesBuf;
use std::io::IoSlice;
use uuid::Uuid;

const UUID: Uuid = Uuid::from_u128(0xd8d2cf50_a5c6_433b_98e6_8c268fd84fa0);

#[test]
fn simple() {
    let mut buf = VersionBytesBuf::new(UUID, &[1, 2, 3]);
    assert_eq!(buf.remaining(), 16 + 3);

    let chunk = buf.chunk();
    assert_eq!(chunk.len(), 16);
    assert_eq!(chunk, UUID.as_bytes());

    buf.advance(16);

    assert_eq!(buf.remaining(), 3);

    let chunk = buf.chunk();
    assert_eq!(chunk.len(), 3);
    assert_eq!(chunk, [1, 2, 3]);

    buf.advance(3);

    assert_eq!(buf.remaining(), 0);

    // advance by 0 should never panic
    buf.advance(0);

    assert_eq!(buf.remaining(), 0);
}

#[test]
fn unaligned_advance() {
    let mut buf = VersionBytesBuf::new(UUID, &[1, 2, 3]);

    buf.advance(4);

    assert_eq!(buf.remaining(), 12 + 3);

    let chunk = buf.chunk();
    assert_eq!(chunk.len(), 12);
    assert_eq!(chunk, &UUID.as_bytes()[4..]);

    buf.advance(12 + 1);

    assert_eq!(buf.remaining(), 2);

    let chunk = buf.chunk();
    assert_eq!(chunk.len(), 2);
    assert_eq!(chunk, [2, 3]);

    buf.advance(2);

    assert_eq!(buf.remaining(), 0);

    // advance by 0 should never panic
    buf.advance(0);

    assert_eq!(buf.remaining(), 0);
}

#[test]
#[should_panic]
fn out_of_bounds_advance() {
    let mut buf = VersionBytesBuf::new(UUID, &[1, 2, 3]);
    buf.advance(16 + 3 + 1);
}

#[test]
fn vectored() {
    let mut buf = VersionBytesBuf::new(UUID, &[1, 2, 3]);

    let mut io_slice = [];
    assert_eq!(buf.chunks_vectored(&mut io_slice), 0);

    let mut io_slice = [IoSlice::new(&[99, 98])];
    assert_eq!(buf.chunks_vectored(&mut io_slice), 1);
    assert_eq!(io_slice[0].as_ref(), UUID.as_bytes());

    let mut io_slice = [IoSlice::new(&[99, 98]), IoSlice::new(&[97, 96])];
    assert_eq!(buf.chunks_vectored(&mut io_slice), 2);
    assert_eq!(io_slice[0].as_ref(), UUID.as_bytes());
    assert_eq!(io_slice[1].as_ref(), [1, 2, 3]);

    let mut io_slice = [
        IoSlice::new(&[99, 98]),
        IoSlice::new(&[97, 96]),
        IoSlice::new(&[95, 94]),
    ];
    assert_eq!(buf.chunks_vectored(&mut io_slice), 2);
    assert_eq!(io_slice[0].as_ref(), UUID.as_bytes());
    assert_eq!(io_slice[1].as_ref(), [1, 2, 3]);
    assert_eq!(io_slice[2].as_ref(), [95, 94]);

    buf.advance(5);

    let mut io_slice = [IoSlice::new(&[99, 98])];
    assert_eq!(buf.chunks_vectored(&mut io_slice), 1);
    assert_eq!(io_slice[0].as_ref(), &UUID.as_bytes()[5..]);

    let mut io_slice = [IoSlice::new(&[99, 98]), IoSlice::new(&[97, 96])];
    assert_eq!(buf.chunks_vectored(&mut io_slice), 2);
    assert_eq!(io_slice[0].as_ref(), &UUID.as_bytes()[5..]);
    assert_eq!(io_slice[1].as_ref(), [1, 2, 3]);

    let mut io_slice = [
        IoSlice::new(&[99, 98]),
        IoSlice::new(&[97, 96]),
        IoSlice::new(&[95, 94]),
    ];
    assert_eq!(buf.chunks_vectored(&mut io_slice), 2);
    assert_eq!(io_slice[0].as_ref(), &UUID.as_bytes()[5..]);
    assert_eq!(io_slice[1].as_ref(), [1, 2, 3]);
    assert_eq!(io_slice[2].as_ref(), [95, 94]);

    buf.advance(11 + 1);

    let mut io_slice = [IoSlice::new(&[99, 98])];
    assert_eq!(buf.chunks_vectored(&mut io_slice), 1);
    assert_eq!(io_slice[0].as_ref(), [2, 3]);

    let mut io_slice = [IoSlice::new(&[99, 98]), IoSlice::new(&[97, 96])];
    assert_eq!(buf.chunks_vectored(&mut io_slice), 1);
    assert_eq!(io_slice[0].as_ref(), [2, 3]);
    assert_eq!(io_slice[1].as_ref(), [97, 96]);

    buf.advance(2);

    let mut io_slice = [IoSlice::new(&[99, 98])];
    assert_eq!(buf.chunks_vectored(&mut io_slice), 0);
    assert_eq!(io_slice[0].as_ref(), [99, 98]);

    let mut io_slice = [IoSlice::new(&[99, 98]), IoSlice::new(&[97, 96])];
    assert_eq!(buf.chunks_vectored(&mut io_slice), 0);
    assert_eq!(io_slice[0].as_ref(), [99, 98]);
    assert_eq!(io_slice[1].as_ref(), [97, 96]);
}
