/// Returns (input length, zero-byte count, count of bytes at least 128).
pub fn summarize(bytes: &[u8]) -> (usize, usize, usize) {
    let mut index: usize = 0;
    let mut zeros: usize = 0;
    let mut high_bytes: usize = 0;

    while index < bytes.len() {
        let byte = bytes[index];
        if byte == 0 {
            zeros += 1;
        }
        if byte >= 128 {
            high_bytes += 1;
        }
        index += 1;
    }

    (bytes.len(), zeros, high_bytes)
}

/// Returns the exact sum of two byte values, including a possible carry.
pub fn sum_pair(left: u8, right: u8) -> u16 {
    u16::from(left) + u16::from(right)
}

#[cfg(test)]
mod tests {
    use super::{sum_pair, summarize};

    #[test]
    fn empty_input_is_zero() {
        assert_eq!(summarize(&[]), (0, 0, 0));
    }

    #[test]
    fn counts_mixed_fixture() {
        let fixture: [u8; 6] = [0, b'A', 127, 128, 255, 0];
        assert_eq!(summarize(&fixture), (6, 2, 2));
    }

    #[test]
    fn classifies_threshold_and_extremes() {
        assert_eq!(summarize(&[0]), (1, 1, 0));
        assert_eq!(summarize(&[127]), (1, 0, 0));
        assert_eq!(summarize(&[128]), (1, 0, 1));
        assert_eq!(summarize(&[255]), (1, 0, 1));
    }

    #[test]
    fn counts_repeated_values() {
        assert_eq!(summarize(&[0; 4]), (4, 4, 0));
        assert_eq!(summarize(&[255; 4]), (4, 0, 4));
    }

    #[test]
    fn visits_final_byte() {
        assert_eq!(summarize(&[65, 0]), (2, 1, 0));
        assert_eq!(summarize(&[65, 255]), (2, 0, 1));
    }

    #[test]
    fn widens_before_addition() {
        assert_eq!(sum_pair(0, 0), 0);
        assert_eq!(sum_pair(255, 0), 255);
        assert_eq!(sum_pair(0, 255), 255);
        assert_eq!(sum_pair(254, 1), 255);
        assert_eq!(sum_pair(255, 1), 256);
        assert_eq!(sum_pair(1, 255), 256);
        assert_eq!(sum_pair(255, 255), 510);
    }
}
