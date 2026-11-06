#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Entry {
    offset: u64,
    position: u64,
}

trait EntrySource {
    fn next_entry(&mut self) -> Option<Entry>;
}

struct SliceSource<'a> {
    entries: &'a [Entry],
    next: usize,
}

impl<'a> SliceSource<'a> {
    fn new(entries: &'a [Entry]) -> Self {
        Self { entries, next: 0 }
    }
}

impl EntrySource for SliceSource<'_> {
    fn next_entry(&mut self) -> Option<Entry> {
        let entry = self.entries.get(self.next).copied()?;
        self.next += 1;
        Some(entry)
    }
}

struct GeneratedSource {
    next: u32,
    end: u32,
}

impl GeneratedSource {
    fn new(count: u32) -> Self {
        Self { next: 0, end: count }
    }
}

impl EntrySource for GeneratedSource {
    fn next_entry(&mut self) -> Option<Entry> {
        if self.next == self.end {
            return None;
        }
        let index = u64::from(self.next);
        self.next += 1;
        Some(Entry {
            offset: 10 + 2 * index,
            position: 32 * index,
        })
    }
}

fn iterator_ownership_checks() {
    let mut words = vec![String::from("red"), String::from("blue")];
    let lengths: Vec<usize> = words.iter().map(|word| word.len()).collect();
    assert_eq!(lengths, vec![3, 4]);
    assert_eq!(words.len(), 2);

    for word in words.iter_mut() {
        word.push('!');
    }
    let first: Vec<String> = words.into_iter().take(1).collect();
    assert_eq!(first, vec![String::from("red!")]);

    let mut mapped = 0;
    let prefix: Vec<u32> = (0..1000)
        .map(|value| {
            mapped += 1;
            value * 2
        })
        .take(3)
        .collect();
    assert_eq!(prefix, vec![0, 2, 4]);
    assert_eq!(mapped, 3);
}

#[derive(Debug, PartialEq, Eq)]
struct ZeroCapacity;

#[derive(Debug)]
struct Ring<T> {
    slots: Box<[Option<T>]>,
    head: usize,
    len: usize,
}

impl<T> Ring<T> {
    fn new(capacity: usize) -> Result<Self, ZeroCapacity> {
        if capacity == 0 {
            return Err(ZeroCapacity);
        }
        let slots: Vec<Option<T>> = (0..capacity).map(|_| None).collect();
        Ok(Self {
            slots: slots.into_boxed_slice(),
            head: 0,
            len: 0,
        })
    }

    fn capacity(&self) -> usize {
        self.slots.len()
    }

    fn len(&self) -> usize {
        self.len
    }

    fn physical(&self, logical: usize) -> usize {
        assert!(logical < self.capacity());
        let before_wrap = self.capacity() - self.head;
        if logical < before_wrap {
            self.head + logical
        } else {
            logical - before_wrap
        }
    }

    fn get(&self, logical: usize) -> Option<&T> {
        if logical >= self.len {
            return None;
        }
        self.slots[self.physical(logical)].as_ref()
    }
}

impl<T> Ring<T> {
    fn try_push(&mut self, value: T) -> Result<(), T> {
        if self.len == self.capacity() {
            return Err(value);
        }
        let tail = self.physical(self.len);
        assert!(self.slots[tail].is_none());
        self.slots[tail] = Some(value);
        self.len += 1;
        Ok(())
    }

    fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        let value = self.slots[self.head].take().expect("occupied head");
        self.head += 1;
        if self.head == self.capacity() {
            self.head = 0;
        }
        self.len -= 1;
        Some(value)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ScanError {
    ZeroCapacity,
    OutOfOrder { previous: u64, next: u64 },
}

#[derive(Debug)]
struct ScanReport {
    visited: usize,
    selected: usize,
    evicted: usize,
    retained: Ring<Entry>,
}

fn scan<S, F>(
    source: &mut S,
    max_items: usize,
    capacity: usize,
    mut keep: F,
) -> Result<ScanReport, ScanError>
where
    S: EntrySource + ?Sized,
    F: FnMut(&Entry) -> bool,
{
    let mut retained = Ring::new(capacity).map_err(|_| ScanError::ZeroCapacity)?;
    let mut previous = None;
    let mut visited = 0;
    let mut selected = 0;
    let mut evicted = 0;

    while visited < max_items {
        let entry = match source.next_entry() {
            Some(entry) => entry,
            None => break,
        };
        if let Some(previous) = previous {
            if entry.offset <= previous {
                return Err(ScanError::OutOfOrder {
                    previous,
                    next: entry.offset,
                });
            }
        }
        previous = Some(entry.offset);
        visited += 1;
        if keep(&entry) {
            selected += 1;
            if let Err(entry) = retained.try_push(entry) {
                retained.pop().expect("a full ring has an oldest entry");
                retained.try_push(entry).expect("one slot was released");
                evicted += 1;
            }
        }
    }
    Ok(ScanReport {
        visited,
        selected,
        evicted,
        retained,
    })
}

fn predecessor(entries: &Ring<Entry>, key: u64) -> Option<Entry> {
    let mut low = 0;
    let mut high = entries.len();
    while low < high {
        let middle = low + (high - low) / 2;
        let entry = entries.get(middle).expect("logical index is occupied");
        if entry.offset <= key {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    match low.checked_sub(1) {
        Some(index) => entries.get(index).copied(),
        None => None,
    }
}

fn linear_predecessor(entries: &Ring<Entry>, key: u64) -> Option<Entry> {
    let mut found = None;
    for logical in 0..entries.len() {
        let entry = *entries.get(logical).expect("logical index is occupied");
        if entry.offset <= key {
            found = Some(entry);
        }
    }
    found
}

fn snapshot(entries: &Ring<Entry>) -> Vec<Entry> {
    (0..entries.len())
        .map(|logical| *entries.get(logical).expect("logical index is occupied"))
        .collect()
}

fn fixture() -> [Entry; 6] {
    [
        Entry {
            offset: 10,
            position: 0,
        },
        Entry {
            offset: 12,
            position: 32,
        },
        Entry {
            offset: 14,
            position: 64,
        },
        Entry {
            offset: 16,
            position: 96,
        },
        Entry {
            offset: 18,
            position: 128,
        },
        Entry {
            offset: 20,
            position: 160,
        },
    ]
}

fn wrapped_lookup_checks() {
    let mut entries = Ring::new(3).unwrap();
    assert_eq!(predecessor(&entries, 0), None);
    for index in 0..40u64 {
        let entry = Entry {
            offset: 2 * index,
            position: 32 * index,
        };
        if let Err(entry) = entries.try_push(entry) {
            entries.pop().unwrap();
            entries.try_push(entry).unwrap();
        }
        for key in 0..=80 {
            assert_eq!(
                predecessor(&entries, key),
                linear_predecessor(&entries, key)
            );
        }
        assert_eq!(
            predecessor(&entries, u64::MAX),
            entries.get(entries.len() - 1).copied()
        );
    }
}

fn boxed_fixture_checks() {
    let rows = fixture();
    let mut sources: [Box<dyn EntrySource + '_>; 2] = [
        Box::new(SliceSource::new(&rows)),
        Box::new(GeneratedSource::new(6)),
    ];
    for source in &mut sources {
        let report = scan(source.as_mut(), 6, 2, |entry| entry.offset % 4 == 2).unwrap();
        assert_eq!((report.visited, report.selected, report.evicted), (6, 3, 1));
        assert_eq!(snapshot(&report.retained), vec![rows[2], rows[4]]);
    }
}

fn main() {
    let rows = fixture();
    let mut slice = SliceSource::new(&rows);
    let mut generated = GeneratedSource::new(6);
    let first = scan(&mut slice, 6, 2, |entry| entry.offset % 4 == 2).unwrap();
    let second = scan(&mut generated, 6, 2, |entry| entry.offset % 4 == 2).unwrap();

    for (label, report) in [("slice", &first), ("generated", &second)] {
        assert_eq!((report.visited, report.selected, report.evicted), (6, 3, 1));
        assert_eq!(snapshot(&report.retained), vec![rows[2], rows[4]]);
        let offsets: Vec<u64> = snapshot(&report.retained)
            .into_iter()
            .map(|entry| entry.offset)
            .collect();
        println!(
            "{label}: visited={} selected={} evicted={} retained={offsets:?}",
            report.visited, report.selected, report.evicted
        );
    }
    for key in [13, 14, 17, 18, 99] {
        let found = predecessor(&first.retained, key);
        assert_eq!(found, linear_predecessor(&first.retained, key));
        println!("lookup {key} -> {found:?}");
    }
    boxed_fixture_checks();
    println!("boxed sources agree with the static scans");
    iterator_ownership_checks();
    println!("iterator ownership checks passed");
    wrapped_lookup_checks();
    println!("ring wraparound and predecessor boundary checks passed");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, VecDeque};

    #[test]
    fn zero_capacity_is_rejected() {
        assert_eq!(Ring::<u64>::new(0).unwrap_err(), ZeroCapacity);
    }

    #[test]
    fn full_ring_returns_the_owned_value_without_changing_slots() {
        let mut ring = Ring::new(1).unwrap();
        ring.try_push(String::from("first")).unwrap();
        let rejected = ring.try_push(String::from("second")).unwrap_err();
        assert_eq!(rejected, "second");
        assert_eq!(ring.len(), 1);
        assert_eq!(ring.get(0).map(|word| word.as_str()), Some("first"));
        assert_eq!(ring.get(1), None);
        assert_eq!(ring.pop(), Some(String::from("first")));
        ring.try_push(rejected).unwrap();
        assert_eq!(ring.pop(), Some(String::from("second")));
        assert_eq!(ring.pop(), None);
    }

    #[test]
    fn one_slot_can_wrap_repeatedly() {
        let mut ring = Ring::new(1).unwrap();
        for value in 0..20 {
            ring.try_push(value).unwrap();
            assert_eq!(ring.pop(), Some(value));
            assert_eq!(ring.len(), 0);
            assert_eq!(ring.capacity(), 1);
            assert_eq!(ring.get(0), None);
        }
    }

    #[test]
    fn mixed_ring_operations_match_a_bounded_deque() {
        for capacity in 1..=5 {
            let mut ring = Ring::new(capacity).unwrap();
            let mut reference = VecDeque::new();
            for step in 0..200 {
                if step % 3 == 2 {
                    assert_eq!(ring.pop(), reference.pop_front());
                } else if reference.len() == capacity {
                    assert_eq!(ring.try_push(step), Err(step));
                } else {
                    ring.try_push(step).unwrap();
                    reference.push_back(step);
                }
                assert_eq!(ring.len(), reference.len());
                assert_eq!(ring.capacity(), capacity);
                for logical in 0..reference.len() {
                    assert_eq!(ring.get(logical), reference.get(logical));
                }
                assert_eq!(ring.get(ring.len()), None);
                assert_eq!(
                    ring.slots.iter().filter(|slot| slot.is_some()).count(),
                    ring.len()
                );
            }
            while let Some(expected) = reference.pop_front() {
                assert_eq!(ring.pop(), Some(expected));
            }
            assert_eq!(ring.pop(), None);
        }
    }

    #[test]
    fn predecessor_handles_empty_exact_gap_and_extreme_keys() {
        let mut ring = Ring::new(3).unwrap();
        assert_eq!(predecessor(&ring, u64::MAX), None);
        let rows = fixture();
        for index in [0, 2, 4] {
            ring.try_push(rows[index]).unwrap();
        }
        for (key, expected) in [
            (0, None),
            (9, None),
            (10, Some(rows[0])),
            (13, Some(rows[0])),
            (14, Some(rows[2])),
            (17, Some(rows[2])),
            (18, Some(rows[4])),
            (u64::MAX, Some(rows[4])),
        ] {
            assert_eq!(predecessor(&ring, key), expected);
        }
    }

    #[test]
    fn predecessor_matches_linear_search_after_repeated_wraps() {
        wrapped_lookup_checks();
    }

    #[test]
    fn ordered_map_agrees_on_the_same_retained_entries() {
        let mut source = GeneratedSource::new(20);
        let report = scan(&mut source, 20, 3, |_| true).unwrap();
        let map: BTreeMap<u64, u64> = snapshot(&report.retained)
            .into_iter()
            .map(|entry| (entry.offset, entry.position))
            .collect();
        for key in 0..=60 {
            let expected = map
                .range(..=key)
                .next_back()
                .map(|(&offset, &position)| Entry { offset, position });
            assert_eq!(predecessor(&report.retained, key), expected);
        }
    }

    struct CountingSource {
        inner: GeneratedSource,
        calls: usize,
    }

    impl EntrySource for CountingSource {
        fn next_entry(&mut self) -> Option<Entry> {
            self.calls += 1;
            self.inner.next_entry()
        }
    }

    #[test]
    fn both_sources_match_the_fixture_and_remain_exhausted() {
        let rows = fixture();
        let mut slice = SliceSource::new(&rows);
        let mut generated = GeneratedSource::new(6);
        for expected in rows {
            assert_eq!(slice.next_entry(), Some(expected));
            assert_eq!(generated.next_entry(), Some(expected));
        }
        for _ in 0..2 {
            assert_eq!(slice.next_entry(), None);
            assert_eq!(generated.next_entry(), None);
        }
    }

    #[test]
    fn empty_source_produces_an_empty_report() {
        let mut source = SliceSource::new(&[]);
        let report = scan(&mut source, 10, 2, |_| panic!("no entry to select")).unwrap();
        assert_eq!((report.visited, report.selected, report.evicted), (0, 0, 0));
        assert_eq!(report.retained.len(), 0);
    }

    #[test]
    fn zero_limit_and_invalid_capacity_do_not_pull() {
        let mut source = CountingSource {
            inner: GeneratedSource::new(10),
            calls: 0,
        };
        let report = scan(&mut source, 0, 2, |_| panic!("zero work budget")).unwrap();
        assert_eq!(report.visited, 0);
        assert_eq!(source.calls, 0);
        assert_eq!(
            scan(&mut source, 3, 0, |_| true).unwrap_err(),
            ScanError::ZeroCapacity
        );
        assert_eq!(source.calls, 0);
    }

    #[test]
    fn rejected_entries_still_consume_the_work_budget() {
        let mut source = CountingSource {
            inner: GeneratedSource::new(100),
            calls: 0,
        };
        let mut predicate_calls = 0;
        let report = scan(&mut source, 4, 2, |_| {
            predicate_calls += 1;
            false
        })
        .unwrap();
        assert_eq!((report.visited, report.selected, report.evicted), (4, 0, 0));
        assert_eq!(source.calls, 4);
        assert_eq!(predicate_calls, 4);
        assert_eq!(source.inner.next, 4);
        assert_eq!(report.retained.len(), 0);
    }

    #[test]
    fn selected_entries_are_retained_or_explicitly_evicted() {
        let mut source = GeneratedSource::new(6);
        let report = scan(&mut source, 6, 2, |entry| entry.offset % 4 == 2).unwrap();
        let rows = fixture();
        assert_eq!((report.visited, report.selected, report.evicted), (6, 3, 1));
        assert_eq!(snapshot(&report.retained), vec![rows[2], rows[4]]);
        assert_eq!(predecessor(&report.retained, 13), None);
    }

    #[test]
    fn duplicate_and_decreasing_offsets_fail_before_filtering() {
        for bad_offset in [12, 11] {
            let mut rows = fixture();
            rows[2].offset = bad_offset;
            let mut source = SliceSource::new(&rows);
            let mut predicate_calls = 0;
            let error = scan(&mut source, 6, 2, |_| {
                predicate_calls += 1;
                false
            })
            .unwrap_err();
            assert_eq!(
                error,
                ScanError::OutOfOrder {
                    previous: 12,
                    next: bad_offset,
                }
            );
            assert_eq!(predicate_calls, 2);
            assert_eq!(source.next, 3);
        }
    }

    #[test]
    fn boxed_sources_use_the_same_scan() {
        boxed_fixture_checks();
    }

    #[test]
    fn iterator_ownership_and_lazy_prefix_are_checked() {
        iterator_ownership_checks();
    }

    #[test]
    fn collecting_before_take_visits_the_whole_input() {
        let mut mapped = 0;
        let whole: Vec<u32> = (0..8)
            .map(|value| {
                mapped += 1;
                value * 2
            })
            .collect();
        assert_eq!(mapped, 8);
        assert_eq!(whole.len(), 8);
        let prefix: Vec<u32> = whole.into_iter().take(3).collect();
        assert_eq!(prefix, vec![0, 2, 4]);
    }
}
