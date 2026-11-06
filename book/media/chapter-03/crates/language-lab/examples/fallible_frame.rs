use std::error::Error;
use std::fmt;

const HEADER_LEN: usize = 4;

#[derive(Debug, PartialEq, Eq)]
enum FrameError {
    LengthOverflow,
    TooLarge { length: usize, limit: usize },
    Truncated { available: usize, total_required: usize },
}

#[derive(Debug, PartialEq, Eq)]
enum Probe<'a> {
    NeedMore { total_required: usize },
    Ready { payload: &'a [u8], consumed: usize },
}

enum InputState {
    Open,
    Ended,
}

fn checked_frame_len(length: usize) -> Result<usize, FrameError> {
    match HEADER_LEN.checked_add(length) {
        Some(total) => Ok(total),
        None => Err(FrameError::LengthOverflow),
    }
}

fn probe_frame(input: &[u8], max_payload: usize) -> Result<Probe<'_>, FrameError> {
    if input.len() < HEADER_LEN {
        return Ok(Probe::NeedMore {
            total_required: HEADER_LEN,
        });
    }

    let encoded = u32::from_be_bytes([input[0], input[1], input[2], input[3]]);
    let length = usize::try_from(encoded).map_err(|_| FrameError::LengthOverflow)?;
    if length > max_payload {
        return Err(FrameError::TooLarge {
            length,
            limit: max_payload,
        });
    }

    let total = checked_frame_len(length)?;
    if input.len() < total {
        return Ok(Probe::NeedMore {
            total_required: total,
        });
    }

    Ok(Probe::Ready {
        payload: &input[HEADER_LEN..total],
        consumed: total,
    })
}

fn take_frame(
    buffer: &mut Vec<u8>,
    max_payload: usize,
    state: InputState,
) -> Result<Option<Vec<u8>>, FrameError> {
    let (owned, consumed) = match probe_frame(buffer.as_slice(), max_payload)? {
        Probe::NeedMore { total_required } => {
            return match state {
                InputState::Open => Ok(None),
                InputState::Ended => {
                    if buffer.is_empty() {
                        Ok(None)
                    } else {
                        Err(FrameError::Truncated {
                            available: buffer.len(),
                            total_required,
                        })
                    }
                }
            };
        }
        Probe::Ready { payload, consumed } => (payload.to_vec(), consumed),
    };

    drop(buffer.drain(..consumed));
    Ok(Some(owned))
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LengthOverflow => write!(f, "frame length does not fit usize"),
            Self::TooLarge { length, limit } => {
                write!(f, "payload length {length} exceeds limit {limit}")
            }
            Self::Truncated {
                available,
                total_required,
            } => write!(
                f,
                "truncated frame: have {available} bytes, need {total_required} total"
            ),
        }
    }
}

impl Error for FrameError {}

#[derive(Debug, PartialEq, Eq)]
struct DecodeError {
    offset: usize,
    source: FrameError,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cannot decode frame starting at byte {}", self.offset)
    }
}

impl Error for DecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

fn count_finished_frames(input: &[u8], max_payload: usize) -> Result<usize, DecodeError> {
    let mut buffer = input.to_vec();
    let mut count = 0;

    loop {
        let offset = input.len() - buffer.len();
        let next = take_frame(&mut buffer, max_payload, InputState::Ended)
            .map_err(|source| DecodeError { offset, source })?;
        match next {
            Some(_) => count += 1,
            None => return Ok(count),
        }
    }
}

fn report_error(error: &dyn Error) {
    println!("error: {error}");
    let mut source = error.source();
    while let Some(cause) = source {
        println!("caused by: {cause}");
        source = cause.source();
    }
}

fn main() {
    let mut buffer = vec![0, 0];
    assert_eq!(take_frame(&mut buffer, 8, InputState::Open), Ok(None));
    assert_eq!(buffer, vec![0, 0]);
    println!("partial header: kept {} bytes", buffer.len());

    buffer.extend_from_slice(&[0, 3, b'c', b'a']);
    assert_eq!(take_frame(&mut buffer, 8, InputState::Open), Ok(None));
    assert_eq!(buffer, vec![0, 0, 0, 3, b'c', b'a']);
    println!("partial body: kept {} bytes", buffer.len());

    buffer.extend_from_slice(&[b't', 0, 0, 0, 3, b'd', b'o', b'g']);
    let first = take_frame(&mut buffer, 8, InputState::Open)
        .expect("valid first frame")
        .expect("complete first frame");
    assert_eq!(first.as_slice(), b"cat");
    assert_eq!(buffer, vec![0, 0, 0, 3, b'd', b'o', b'g']);
    println!("first: {first:?}; remaining={}", buffer.len());

    let second = take_frame(&mut buffer, 8, InputState::Ended)
        .expect("valid second frame")
        .expect("complete second frame");
    assert_eq!(second.as_slice(), b"dog");
    assert_eq!(take_frame(&mut buffer, 8, InputState::Ended), Ok(None));
    println!("second: {second:?}; clean end={}", buffer.is_empty());
    drop(buffer);
    assert_eq!(first.as_slice(), b"cat");

    let mut empty_frame = vec![0, 0, 0, 0];
    let empty = take_frame(&mut empty_frame, 0, InputState::Ended)
        .expect("zero is within the limit")
        .expect("an empty payload is still a frame");
    assert!(empty.is_empty());
    assert!(empty_frame.is_empty());
    println!("empty payload: {} bytes; consumed {HEADER_LEN}", empty.len());

    let mut invalid = vec![0, 0, 1, 0];
    let before = invalid.clone();
    let error = take_frame(&mut invalid, 8, InputState::Open)
        .expect_err("length 256 exceeds the limit");
    assert_eq!(error, FrameError::TooLarge { length: 256, limit: 8 });
    assert_eq!(invalid, before);
    println!("rejected: {error}; kept {} bytes", invalid.len());

    let complete = [0, 0, 0, 3, b'c', b'a', b't', 0, 0, 0, 3, b'd', b'o', b'g'];
    assert_eq!(count_finished_frames(&complete, 8), Ok(2));
    let truncated = &complete[..complete.len() - 1];
    let error = count_finished_frames(truncated, 8)
        .expect_err("the second frame is missing its last byte");
    assert_eq!(error.offset, 7);
    report_error(&error);
}

#[cfg(test)]
mod probe_tests {
    use super::{FrameError, HEADER_LEN, Probe, checked_frame_len, probe_frame};

    #[test]
    fn short_headers_request_four_total_bytes() {
        for available in 0..HEADER_LEN {
            let input = vec![0; available];
            assert_eq!(
                probe_frame(&input, 8),
                Ok(Probe::NeedMore { total_required: 4 })
            );
        }
    }

    #[test]
    fn partial_bodies_request_the_whole_frame_length() {
        let frame = [0, 0, 0, 3, b'c', b'a', b't'];
        for available in HEADER_LEN..frame.len() {
            assert_eq!(
                probe_frame(&frame[..available], 8),
                Ok(Probe::NeedMore { total_required: 7 })
            );
        }
    }

    #[test]
    fn ready_exposes_only_the_first_payload() {
        let input = [0, 0, 0, 3, b'c', b'a', b't', 0, 0, 0, 0];
        assert_eq!(
            probe_frame(&input, 3),
            Ok(Probe::Ready { payload: b"cat", consumed: 7 })
        );
    }

    #[test]
    fn checked_length_rejects_overflow_without_allocating() {
        assert_eq!(checked_frame_len(0), Ok(4));
        assert_eq!(checked_frame_len(3), Ok(7));
        assert_eq!(checked_frame_len(usize::MAX), Err(FrameError::LengthOverflow));
        assert_eq!(checked_frame_len(usize::MAX - HEADER_LEN), Ok(usize::MAX));
    }
}

#[cfg(test)]
mod take_tests {
    use super::{FrameError, InputState, take_frame};

    #[test]
    fn incomplete_input_is_not_consumed() {
        let frame = [0, 0, 0, 3, b'c', b'a', b't'];
        for available in 0..frame.len() {
            let mut buffer = frame[..available].to_vec();
            let before = buffer.clone();
            assert_eq!(take_frame(&mut buffer, 8, InputState::Open), Ok(None));
            assert_eq!(buffer, before);
        }
    }

    #[test]
    fn oversized_input_is_rejected_before_waiting_for_its_body() {
        let mut buffer = vec![0, 0, 1, 0];
        let before = buffer.clone();
        assert_eq!(
            take_frame(&mut buffer, 8, InputState::Open),
            Err(FrameError::TooLarge { length: 256, limit: 8 })
        );
        assert_eq!(buffer, before);
        assert_eq!(
            take_frame(&mut buffer, 8, InputState::Ended),
            Err(FrameError::TooLarge { length: 256, limit: 8 })
        );
        assert_eq!(buffer, before);
    }

    #[test]
    fn empty_payload_is_a_frame_even_at_end_of_input() {
        let mut buffer = vec![0, 0, 0, 0];
        assert_eq!(
            take_frame(&mut buffer, 0, InputState::Ended),
            Ok(Some(Vec::new()))
        );
        assert!(buffer.is_empty());
        assert_eq!(take_frame(&mut buffer, 0, InputState::Ended), Ok(None));
    }

    #[test]
    fn exact_limit_and_arbitrary_bytes_are_accepted() {
        let mut buffer = vec![0, 0, 0, 3, 0, 128, 255];
        assert_eq!(
            take_frame(&mut buffer, 3, InputState::Ended),
            Ok(Some(vec![0, 128, 255]))
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn owned_payload_survives_reuse_and_destruction() {
        let mut buffer = vec![0, 0, 0, 3, b'c', b'a', b't'];
        let saved = take_frame(&mut buffer, 8, InputState::Open).unwrap().unwrap();
        buffer.extend_from_slice(&[255, 0, 1]);
        drop(buffer);
        assert_eq!(saved.as_slice(), b"cat");
    }
}

#[cfg(test)]
mod finish_tests {
    use super::{DecodeError, FrameError, InputState, count_finished_frames, take_frame};
    use std::error::Error;

    #[test]
    fn ended_partial_input_is_truncated_and_unchanged() {
        let frame = [0, 0, 0, 3, b'c', b'a', b't'];
        for available in 1..frame.len() {
            let mut buffer = frame[..available].to_vec();
            let before = buffer.clone();
            let total_required = if available < 4 { 4 } else { 7 };
            assert_eq!(
                take_frame(&mut buffer, 8, InputState::Ended),
                Err(FrameError::Truncated { available, total_required })
            );
            assert_eq!(buffer, before);
        }
    }

    #[test]
    fn finished_count_handles_clean_boundaries() {
        assert_eq!(count_finished_frames(&[], 8), Ok(0));
        assert_eq!(count_finished_frames(&[0, 0, 0, 0], 0), Ok(1));
        assert_eq!(count_finished_frames(&[0, 0, 0, 0, 0, 0, 0, 0], 0), Ok(2));
    }

    #[test]
    fn context_keeps_the_second_frame_offset_and_cause() {
        let input = [0, 0, 0, 3, b'c', b'a', b't', 0, 0, 0, 3, b'd', b'o'];
        let error = count_finished_frames(&input, 8).unwrap_err();
        assert_eq!(
            error,
            DecodeError {
                offset: 7,
                source: FrameError::Truncated { available: 6, total_required: 7 },
            }
        );
        assert_eq!(error.to_string(), "cannot decode frame starting at byte 7");
        let cause = error.source().expect("the wrapper keeps its cause");
        assert_eq!(cause.to_string(), "truncated frame: have 6 bytes, need 7 total");
        assert!(cause.source().is_none());
    }

    #[test]
    fn every_two_chunk_split_preserves_both_frames() {
        let input = [0, 0, 0, 3, b'c', b'a', b't', 0, 0, 0, 3, b'd', b'o', b'g'];
        for split in 0..=input.len() {
            let mut buffer = input[..split].to_vec();
            let mut decoded = Vec::new();
            loop {
                match take_frame(&mut buffer, 8, InputState::Open).unwrap() {
                    Some(payload) => decoded.push(payload),
                    None => break,
                }
            }
            buffer.extend_from_slice(&input[split..]);
            loop {
                match take_frame(&mut buffer, 8, InputState::Ended).unwrap() {
                    Some(payload) => decoded.push(payload),
                    None => break,
                }
            }
            assert_eq!(decoded, vec![b"cat".to_vec(), b"dog".to_vec()]);
            assert!(buffer.is_empty());
        }
    }
}
