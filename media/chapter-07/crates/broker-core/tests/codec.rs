use broker_core::codec::{
    HEADER_LEN, MAX_PAYLOAD, Probe, RecordView, SegmentHeader, decode_record,
    decode_record_header, decode_segment, encode_record, encode_segment, record_len,
};
use broker_core::error::{FormatError, IdError};
use broker_core::types::{Epoch, GroupId, Offset, PartitionId};

const SEGMENT: [u8; 32] = [
    0x50, 0x42, 0x4c, 0x53, 0x00, 0x01, 0x00, 0x20,
    0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0xf8, 0x35, 0x0e, 0x5f,
];
const RECORD: [u8; 35] = [
    0x50, 0x42, 0x4c, 0x52, 0x00, 0x01, 0x00, 0x20,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
    0x00, 0x00, 0x00, 0x03, 0x9e, 0x5e, 0x43, 0xa8,
    0x00, 0x00, 0x00, 0x00, 0x89, 0xee, 0x57, 0x48,
    0x63, 0x61, 0x74,
];

fn reseal(bytes: &mut [u8]) {
    let crc = crc32fast::hash(&bytes[..28]);
    bytes[28..32].copy_from_slice(&crc.to_be_bytes());
}

#[test]
fn identifiers_check_ranges_and_successors() {
    assert_eq!(PartitionId::new(3).unwrap().get(), 3);
    assert!(PartitionId::new(4).is_err());
    assert_eq!(GroupId::new(1).unwrap().get(), 1);
    assert!(GroupId::new(2).is_err());
    assert_eq!(Epoch::new(0), Err(IdError::ZeroEpoch));
    assert_eq!(Epoch::new(1).unwrap().next().unwrap().get(), 2);
    assert_eq!(Epoch::new(u64::MAX).unwrap().next(), Err(IdError::Exhausted("epoch")));
    assert_eq!(Offset::new(u64::MAX).next(), Err(IdError::Exhausted("offset")));
}

#[test]
fn crc_variant_is_ieee_not_crc32c() {
    assert_eq!(crc32fast::hash(b"123456789"), 0xcbf4_3926);
    assert_eq!(crc32fast::hash(b""), 0);
}

#[test]
fn segment_matches_independent_golden_bytes() {
    let header = SegmentHeader { partition: PartitionId::new(2).unwrap(), base: Offset::new(7) };
    assert_eq!(encode_segment(header), SEGMENT);
    assert_eq!(decode_segment(&SEGMENT).unwrap(), Probe::Complete { value: header, consumed: 32 });
}

#[test]
fn record_matches_independent_golden_bytes() {
    assert_eq!(encode_record(Offset::new(7), b"cat").unwrap(), RECORD.to_vec());
    assert_eq!(decode_record(&RECORD).unwrap(), Probe::Complete {
        value: RecordView { offset: Offset::new(7), payload: b"cat" }, consumed: 35,
    });
}

#[test]
fn every_short_header_and_body_is_incomplete() {
    for end in 0..HEADER_LEN {
        assert_eq!(decode_segment(&SEGMENT[..end]).unwrap(), Probe::Incomplete { required: 32 });
        assert_eq!(decode_record(&RECORD[..end]).unwrap(), Probe::Incomplete { required: 32 });
    }
    for end in HEADER_LEN..RECORD.len() {
        assert_eq!(decode_record(&RECORD[..end]).unwrap(), Probe::Incomplete { required: 35 });
    }
}

#[test]
fn empty_payload_is_a_complete_record() {
    let bytes = encode_record(Offset::new(0), b"").unwrap();
    assert_eq!(bytes.len(), HEADER_LEN);
    assert_eq!(decode_record(&bytes).unwrap(), Probe::Complete {
        value: RecordView { offset: Offset::new(0), payload: b"" }, consumed: HEADER_LEN,
    });
}

#[test]
fn limit_is_inclusive_and_checked_before_allocation() {
    let payload = vec![0xa5; MAX_PAYLOAD];
    let bytes = encode_record(Offset::new(0), &payload).unwrap();
    assert_eq!(bytes.len(), HEADER_LEN + MAX_PAYLOAD);
    let Probe::Complete { value, .. } = decode_record(&bytes).unwrap() else { panic!("incomplete") };
    assert_eq!(value.payload, payload.as_slice());
    assert_eq!(record_len(MAX_PAYLOAD + 1), Err(FormatError::TooLarge {
        length: MAX_PAYLOAD + 1, limit: MAX_PAYLOAD,
    }));
    let mut h: [u8; HEADER_LEN] = RECORD[..HEADER_LEN].try_into().unwrap();
    h[16..20].copy_from_slice(&1_048_577_u32.to_be_bytes());
    reseal(&mut h);
    assert!(matches!(decode_record(&h), Err(FormatError::TooLarge { .. })));
}

#[test]
fn length_arithmetic_cannot_wrap() {
    assert_eq!(record_len(usize::MAX), Err(FormatError::LengthOverflow));
}

#[test]
fn untrusted_length_fails_header_crc_before_requesting_a_body() {
    let mut h: [u8; HEADER_LEN] = RECORD[..HEADER_LEN].try_into().unwrap();
    h[16..20].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(decode_record(&h), Err(FormatError::HeaderChecksum { .. })));
    reseal(&mut h);
    assert!(matches!(decode_record(&h), Err(FormatError::TooLarge { .. })));
}

#[test]
fn valid_crc_does_not_make_unsupported_fields_valid() {
    for (index, value, expected) in [
        (0, b'X', FormatError::Magic),
        (5, 2, FormatError::Version(2)),
        (7, 33, FormatError::HeaderLength(33)),
        (24, 1, FormatError::Reserved),
    ] {
        let mut bytes = RECORD;
        bytes[index] = value;
        reseal(&mut bytes);
        assert_eq!(decode_record(&bytes), Err(expected));
    }
}

#[test]
fn segment_identity_and_reserved_bytes_are_checked() {
    let mut bytes = SEGMENT;
    bytes[8..10].copy_from_slice(&4_u16.to_be_bytes());
    reseal(&mut bytes);
    assert!(matches!(decode_segment(&bytes), Err(FormatError::Identity(_))));
    for index in [10, 11, 20, 27] {
        let mut bytes = SEGMENT;
        bytes[index] = 1;
        reseal(&mut bytes);
        assert_eq!(decode_segment(&bytes), Err(FormatError::Reserved));
    }
}

#[test]
fn a_record_requires_a_representable_exclusive_end() {
    assert!(encode_record(Offset::new(u64::MAX - 1), b"").is_ok());
    assert!(matches!(encode_record(Offset::new(u64::MAX), b""), Err(FormatError::Identity(_))));
    let mut bytes = RECORD;
    bytes[8..16].copy_from_slice(&u64::MAX.to_be_bytes());
    reseal(&mut bytes);
    assert!(matches!(decode_record(&bytes), Err(FormatError::Identity(_))));
}

#[test]
fn every_single_bit_change_in_a_complete_record_is_detected() {
    for byte in 0..RECORD.len() {
        for bit in 0..8 {
            let mut damaged = RECORD;
            damaged[byte] ^= 1_u8 << bit;
            assert!(decode_record(&damaged).is_err(), "byte {byte}, bit {bit}");
        }
    }
}

#[test]
fn payload_corruption_is_not_an_incomplete_tail() {
    let mut damaged = RECORD;
    damaged[34] ^= 1;
    assert!(matches!(decode_record(&damaged), Err(FormatError::PayloadChecksum { .. })));
}

#[test]
fn coalesced_input_consumes_one_record_and_borrows_its_bytes() {
    let mut bytes = RECORD.to_vec();
    bytes.extend_from_slice(&encode_record(Offset::new(8), b"dog").unwrap());
    let Probe::Complete { value, consumed } = decode_record(&bytes).unwrap() else { panic!("incomplete") };
    assert_eq!(consumed, 35);
    assert_eq!(value.payload.as_ptr(), bytes[HEADER_LEN..].as_ptr());
    let Probe::Complete { value: second, consumed: n } = decode_record(&bytes[consumed..]).unwrap()
        else { panic!("second record incomplete") };
    assert_eq!(second.offset, Offset::new(8));
    assert_eq!(second.payload, b"dog");
    assert_eq!(n + consumed, bytes.len());
}

#[test]
fn header_only_validation_exposes_a_bounded_read_size() {
    let h: &[u8; HEADER_LEN] = RECORD[..HEADER_LEN].try_into().unwrap();
    let header = decode_record_header(h).unwrap();
    assert_eq!(header.offset(), Offset::new(7));
    assert_eq!(header.payload_len(), 3);
    assert_eq!(header.total_len(), 35);
}

#[test]
fn rejected_input_is_not_modified() {
    let mut bytes = RECORD;
    bytes[0] ^= 1;
    let before = bytes;
    assert!(decode_record(&bytes).is_err());
    assert_eq!(bytes, before);
}
