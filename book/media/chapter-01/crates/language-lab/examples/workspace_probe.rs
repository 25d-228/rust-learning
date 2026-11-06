use language_lab::byte_summary::{sum_pair, summarize};

fn main() {
    let fixture: [u8; 6] = [0, b'A', 127, 128, 255, 0];
    let (length, zeros, high_bytes) = summarize(&fixture);

    assert_eq!(length, 6);
    assert_eq!(zeros, 2);
    assert_eq!(high_bytes, 2);
    assert!(zeros + high_bytes <= length);
    assert_eq!(fixture[1], b'A');

    let carry = sum_pair(255, 1);
    let maximum = sum_pair(255, 255);
    assert_eq!(carry, 256);
    assert_eq!(maximum, 510);

    println!("bytes={length} zero={zeros} high={high_bytes}");
    println!("sum_pair(255, 1)={carry}");
    println!("sum_pair(255, 255)={maximum}");
    println!("workspace probe: ok");
}
