use std::io;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;
use broker_app::protocol::*;
use broker_core::delivery::{AckOutcome, AckRejection};
use super::support::*;

#[test]
fn append_header_has_independent_golden_bytes() {
    let mut h = request(Op::Append); h.body = 3; h.partition = 2;
    h.id = 0x0102_0304_0506_0708;
    let mut golden = [0; 64];
    golden[..24].copy_from_slice(&[0x50,0x42,0x4e,0x57, 1,1,0,0, 0,2,0,0,
        0,0,0,3, 1,2,3,4,5,6,7,8]);
    assert_eq!(h.encode(), golden);
    assert_eq!(Header::decode(&golden).unwrap(), h);
}
#[test]
fn each_request_shape_round_trips() {
    for op in [Op::Append, Op::Fetch, Op::Ack, Op::Checkpoint, Op::Progress, Op::Retain] {
        let mut h = request(op);
        if op == Op::Fetch { h.args = [64, MAX_ENCODED as u64, 0, 0, 0]; }
        if op == Op::Ack { h.args = [1, 0, 1, 10_000, 0]; }
        validate_request(h).unwrap();
        assert_eq!(Header::decode(&h.encode()).unwrap(), h);
    }
}
#[test]
fn common_header_rejects_unknown_and_reserved_fields() {
    for (index, value) in [(0, 0), (4, 2), (5, 99), (6, 99), (7, 1), (11, 1)] {
        let mut raw = request(Op::Progress).encode(); raw[index] = value;
        assert!(Header::decode(&raw).is_err());
    }
    let mut raw = request(Op::Progress).encode(); raw[16..24].fill(0);
    assert!(Header::decode(&raw).is_err());
}
#[test]
fn request_limits_are_checked_before_body_work() {
    let mut h = request(Op::Append); h.body = 1_048_577;
    assert!(validate_request(h).is_err());
    h.body = 0; h.partition = 4; assert!(validate_request(h).is_err());
    h.partition = 0; h.group = 2; assert!(validate_request(h).is_err());
    h.group = 0; h.args[0] = 1; assert!(validate_request(h).is_err());
    for (count, bytes) in [(0, 64), (65, 64), (1, 0), (1, MAX_ENCODED as u64 + 1)] {
        let mut h = request(Op::Fetch); h.args = [count, bytes, 0, 0, 0];
        assert!(validate_request(h).is_err());
    }
    for op in [Op::Fetch, Op::Ack, Op::Checkpoint, Op::Progress, Op::Retain] {
        let mut h = request(op); h.body = 1; assert!(validate_request(h).is_err());
    }
}
#[test]
fn response_must_match_scope_identifier_and_status() {
    let h = request(Op::Progress);
    let valid = h.response(Status::Ok, [2, 1, 0, 0, 1], 0);
    validate_response(h, valid).unwrap();
    for reply in [Header { id: 2, ..valid }, Header { partition: 1, ..valid },
        Header { group: 1, ..valid }, Header { op: Op::Ack, ..valid },
        Header { status: Status::Request, ..valid }, Header { body: 1, ..valid }]
    { assert!(validate_response(h, reply).is_err()); }
    assert!(validate_response(h, h.response(Status::Busy, [1, 0, 0, 0, 0], 0)).is_err());
}
#[test]
fn fetch_response_dimensions_and_stop_reason_are_exact() {
    let mut h = request(Op::Fetch); h.args = [2, 70, 0, 0, 0];
    validate_response(h, h.response(Status::Ok, [2, 70, 1, 0, 1], 134)).unwrap();
    validate_response(h, h.response(Status::Ok, [1, 35, 0, 0, 1], 67)).unwrap();
    validate_response(h, h.response(Status::Ok, [0, 0, 2, 71, 1], 0)).unwrap();
    for args in [[3, 105, 1, 0, 1], [2, 70, 0, 0, 1], [0, 0, 2, 70, 1],
        [1, 35, 0, 0, 0], [0, 1, 0, 0, 1]]
    { assert!(validate_response(h, h.response(Status::Ok, args, 0)).is_err()); }
}
#[test]
fn wire_body_formula_includes_tokens_but_excludes_envelope() {
    assert_eq!(body_size(0, 0).unwrap(), 0);
    assert_eq!(body_size(2, 70).unwrap(), 134);
    assert_eq!(body_size(64, MAX_ENCODED).unwrap(), MAX_WIRE_BODY);
    assert!(body_size(0, 1).is_err());
    assert!(body_size(2, 63).is_err());
    assert!(body_size(65, 2080).is_err());
}
#[test]
fn acknowledgement_codes_preserve_every_core_outcome() {
    let mut values = vec![AckOutcome::Applied, AckOutcome::Duplicate, AckOutcome::AlreadyRetired];
    values.extend([AckRejection::WrongEpoch, AckRejection::WrongPartition, AckRejection::WrongGroup,
        AckRejection::NotIssued, AckRejection::StaleGeneration, AckRejection::WrongDeadline,
        AckRejection::Expired].into_iter().map(AckOutcome::Rejected));
    for value in values { assert_eq!(decode_ack(encode_ack(value)).unwrap(), value); }
    assert!(decode_ack(9).is_err());
}
#[tokio::test]
async fn fixed_header_handles_every_split_and_clean_or_partial_eof() {
    let h = request(Op::Progress);
    for cut in 1..64 {
        let (mut stream, _) = ScriptIo::new(h.encode().to_vec(), cut);
        assert_eq!(read_header(&mut stream, Instant::now() + Duration::from_secs(2)).await.unwrap(), Some(h));
    }
    for len in 0..64 {
        let (mut stream, _) = ScriptIo::new(h.encode()[..len].to_vec(), len);
        let result = read_header(&mut stream, Instant::now() + Duration::from_secs(2)).await;
        if len == 0 { assert!(result.unwrap().is_none()); }
        else { assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof); }
    }
}
#[tokio::test(start_paused = true)]
async fn header_deadline_is_not_reset_after_each_byte() {
    let (mut writer, mut reader) = tokio::io::duplex(1);
    let feeder = tokio::spawn(async move {
        for byte in request(Op::Progress).encode() {
            if writer.write_all(&[byte]).await.is_err() { break; }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    });
    let error = read_header(&mut reader, Instant::now() + Duration::from_millis(100)).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    feeder.abort(); let _ = feeder.await;
}
#[tokio::test(start_paused = true)]
async fn ready_io_does_not_bypass_an_expired_deadline() {
    let (mut stream, _) = ScriptIo::new(request(Op::Progress).encode().to_vec(), 64);
    assert_eq!(read_header(&mut stream, Instant::now()).await.unwrap_err().kind(), io::ErrorKind::TimedOut);
}
#[tokio::test(start_paused = true)]
async fn blocked_output_expires_under_one_absolute_budget() {
    let (stream, capture) = ScriptIo::new(Vec::new(), 0);
    let mut stream = stream.block_writes_after(3);
    let error = write_bytes(&mut stream, b"abcdef", Instant::now() + Duration::from_millis(100))
        .await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(capture.output.lock().unwrap().as_slice(), b"abc");
}
