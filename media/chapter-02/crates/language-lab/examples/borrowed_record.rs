fn demonstrate_moves() {
    let tag = 7u8;
    let copied_tag = tag;
    assert_eq!(tag, copied_tag);
    println!("copy: tag={tag} copied={copied_tag}");

    let original = vec![b'c', 0, 255];
    let mut moved = original;
    let cloned = moved.clone();
    moved[0] = b'd';

    assert_eq!(moved.as_slice(), &[100, 0, 255]);
    assert_eq!(cloned.as_slice(), &[99, 0, 255]);
    println!("move/clone: moved={moved:?} cloned={cloned:?}");
}

fn demonstrate_text() {
    let label = String::from("é");
    let bytes = label.as_bytes();
    assert_eq!(bytes, &[0xc3, 0xa9]);
    assert_eq!(label.len(), 2);
    println!("text: label={label} bytes={bytes:?} length={}", label.len());
}

const HEADER_LEN: usize = 1;
const PAYLOAD_LEN: usize = 3;
const RECORD_LEN: usize = HEADER_LEN + PAYLOAD_LEN;

struct OwnedRecord {
    bytes: Vec<u8>,
}

struct RecordView<'a> {
    tag: u8,
    payload: &'a [u8],
}

impl OwnedRecord {
    fn new(tag: u8, payload: [u8; PAYLOAD_LEN]) -> Self {
        let mut bytes = Vec::with_capacity(RECORD_LEN);
        bytes.push(tag);
        bytes.extend_from_slice(&payload);
        Self { bytes }
    }

    fn as_bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[HEADER_LEN..RECORD_LEN]
    }

    fn replace(&mut self, tag: u8, payload: [u8; PAYLOAD_LEN]) {
        self.bytes[0] = tag;
        self.bytes[HEADER_LEN..RECORD_LEN].copy_from_slice(&payload);
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

fn view_record<'a>(bytes: &'a [u8]) -> RecordView<'a> {
    assert_eq!(
        bytes.len(),
        RECORD_LEN,
        "fixed record requires exactly four bytes"
    );
    RecordView {
        tag: bytes[0],
        payload: &bytes[HEADER_LEN..RECORD_LEN],
    }
}

fn main() {
    demonstrate_moves();
    demonstrate_text();

    let mut record = OwnedRecord::new(7, [b'c', 0, 255]);
    let saved = {
        let first = view_record(record.as_bytes());
        let same_payload = first.payload;
        assert_eq!(first.tag, 7);
        assert_eq!(same_payload, &[99, 0, 255]);
        println!("view: tag={} payload={:?}", first.tag, first.payload);
        first.payload.to_vec()
    };
    println!("saved: {saved:?}");

    record.replace(8, [b'd', b'o', b'g']);
    {
        let reused = view_record(record.as_bytes());
        assert_eq!(reused.tag, 8);
        assert_eq!(reused.payload, &[100, 111, 103]);
        println!("reused: tag={} payload={:?}", reused.tag, reused.payload);
    }
    assert_eq!(saved.as_slice(), &[99, 0, 255]);

    {
        let payload = record.payload_mut();
        payload[0] = b'D';
        println!("mutable view: {payload:?}");
    }

    let mut raw = record.into_bytes();
    assert_eq!(raw.as_slice(), &[8, 68, 111, 103]);
    println!("transferred bytes: {raw:?}");
    let capacity_before_clear = raw.capacity();
    raw.clear();
    assert!(raw.is_empty());
    assert_eq!(raw.capacity(), capacity_before_clear);
    drop(raw);

    assert_eq!(saved.as_slice(), &[99, 0, 255]);
    println!("saved after owner drop: {saved:?}");
}

#[cfg(test)]
mod tests {
    use super::{OwnedRecord, view_record};

    #[test]
    fn reads_tag_and_binary_payload() {
        let record = OwnedRecord::new(7, [b'c', 0, 255]);
        let first = view_record(record.as_bytes());
        let second = view_record(record.as_bytes());
        assert_eq!(first.tag, 7);
        assert_eq!(first.payload, &[99, 0, 255]);
        assert_eq!(second.payload, first.payload);
    }

    #[test]
    fn mutable_payload_preserves_the_tag() {
        let mut record = OwnedRecord::new(7, [b'c', 0, 255]);
        let payload = record.payload_mut();
        payload[0] = b'C';
        payload[2] = 128;
        assert_eq!(record.as_bytes(), &[7, 67, 0, 128]);
    }

    #[test]
    fn replacement_does_not_change_saved_payload() {
        let mut record = OwnedRecord::new(7, [b'c', 0, 255]);
        let saved = view_record(record.as_bytes()).payload.to_vec();
        record.replace(8, [b'd', b'o', b'g']);
        assert_eq!(record.as_bytes(), &[8, 100, 111, 103]);
        assert_eq!(saved.as_slice(), &[99, 0, 255]);
    }

    #[test]
    fn copied_payload_outlives_its_source() {
        let saved = {
            let record = OwnedRecord::new(7, [b'c', 0, 255]);
            view_record(record.as_bytes()).payload.to_vec()
        };
        assert_eq!(saved.as_slice(), &[99, 0, 255]);
    }

    #[test]
    fn consuming_conversion_returns_the_whole_buffer() {
        let record = OwnedRecord::new(9, [0, 128, 255]);
        let mut raw = record.into_bytes();
        assert_eq!(raw.as_slice(), &[9, 0, 128, 255]);
        let capacity = raw.capacity();
        raw.clear();
        assert!(raw.is_empty());
        assert_eq!(raw.capacity(), capacity);
    }

    #[test]
    fn cloned_byte_vectors_can_change_independently() {
        let mut first = vec![99u8, 0, 255];
        let mut second = first.clone();
        first[0] = 100;
        second[2] = 128;
        assert_eq!(first.as_slice(), &[100, 0, 255]);
        assert_eq!(second.as_slice(), &[99, 0, 128]);
    }
}

#[cfg(test)]
mod shape_tests {
    use super::view_record;

    #[test]
    #[should_panic(expected = "fixed record requires exactly four bytes")]
    fn rejects_a_short_fixed_record() {
        view_record(&[7, 99, 0]);
    }

    #[test]
    #[should_panic(expected = "fixed record requires exactly four bytes")]
    fn rejects_extra_bytes() {
        view_record(&[7, 99, 0, 255, 9]);
    }
}
