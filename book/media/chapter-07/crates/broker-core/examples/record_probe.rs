use std::error::Error;

use broker_core::codec::{Probe, SegmentHeader, decode_record, encode_record, encode_segment};
use broker_core::types::{Offset, PartitionId};

fn main() -> Result<(), Box<dyn Error>> {
    let segment = encode_segment(SegmentHeader {
        partition: PartitionId::new(2)?, base: Offset::new(7),
    });
    let record = encode_record(Offset::new(7), b"cat")?;
    let short = decode_record(&record[..34])?;
    assert!(matches!(short, Probe::Incomplete { required: 35 }));
    if let Probe::Complete { value, consumed } = decode_record(&record)? {
        assert_eq!(value.payload, b"cat");
        println!("segment={} record={} offset={} payload={} consumed={}",
            segment.len(), record.len(), value.offset.get(), value.payload.len(), consumed);
    } else {
        panic!("complete fixture was not decoded");
    }
    println!("34 bytes: incomplete; need 35 total");
    Ok(())
}
