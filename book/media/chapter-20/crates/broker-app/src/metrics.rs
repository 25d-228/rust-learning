use std::io::{self, Write};
use std::time::Duration;

pub const BINS: usize = 257;

// All integer bounds are microseconds, after rounding each duration upward.
pub fn bounds(index: usize) -> (u64, Option<u64>) {
    assert!(index < BINS);
    if index < 16 { return (index as u64, Some(index as u64)); }
    if index == BINS - 1 { return (1u64 << 34, None); }
    let exponent = 4 + (index - 16) / 8;
    let width = 1u64 << (exponent - 3);
    let low = (1u64 << exponent) + ((index - 16) % 8) as u64 * width;
    (low, Some(low + width - 1))
}
fn bucket(value: u64) -> usize {
    if value < 16 { return value as usize; }
    if value >= 1u64 << 34 { return BINS - 1; }
    let exponent = 63 - value.leading_zeros() as usize;
    let width = 1u64 << (exponent - 3);
    16 + (exponent - 4) * 8 + ((value - (1u64 << exponent)) / width) as usize
}

#[derive(Debug, Clone)]
pub struct Histogram {
    bins: [u64; BINS],
    pub count: u64,
    pub total_us: u128,
    pub max_us: u64,
    pub valid: bool,
}
impl Default for Histogram {
    fn default() -> Self {
        Self { bins: [0; BINS], count: 0, total_us: 0, max_us: 0, valid: true }
    }
}
impl Histogram {
    pub fn record(&mut self, elapsed: Option<Duration>) {
        let Some(elapsed) = elapsed else { self.valid = false; return; };
        let nanos = elapsed.as_nanos();
        let micros = nanos / 1000 + u128::from(nanos % 1000 != 0);
        match u64::try_from(micros) {
            Ok(value) => self.record_us(value),
            Err(_) => self.valid = false,
        }
    }
    pub fn record_us(&mut self, value: u64) {
        let index = bucket(value);
        let next = (self.count.checked_add(1), self.bins[index].checked_add(1),
            self.total_us.checked_add(u128::from(value)));
        let (Some(count), Some(bin), Some(total)) = next else { self.valid = false; return; };
        self.count = count; self.bins[index] = bin; self.total_us = total;
        self.max_us = self.max_us.max(value);
    }
    pub fn merge(&mut self, other: &Self) {
        self.valid &= other.valid;
        let (Some(count), Some(total)) = (self.count.checked_add(other.count),
            self.total_us.checked_add(other.total_us)) else { self.valid = false; return; };
        for (left, right) in self.bins.iter_mut().zip(other.bins.iter()) {
            let Some(value) = left.checked_add(*right) else { self.valid = false; return; };
            *left = value;
        }
        self.count = count; self.total_us = total; self.max_us = self.max_us.max(other.max_us);
    }
    pub fn quantile(&self, numerator: u64, denominator: u64) -> Option<(u64, Option<u64>)> {
        if !self.valid || self.count == 0 || denominator == 0
            || numerator == 0 || numerator > denominator { return None; }
        let rank = (u128::from(self.count) * u128::from(numerator))
            .div_ceil(u128::from(denominator));
        let mut cumulative = 0u128;
        for (index, count) in self.bins.iter().enumerate() {
            cumulative += u128::from(*count);
            if cumulative >= rank { return Some(bounds(index)); }
        }
        None
    }
    pub fn write(&self, name: &str, summary: &mut impl Write, bins: &mut impl Write) -> io::Result<()> {
        write!(summary, "{name}\t{}\t{}\t{}\t{}", self.valid, self.count, self.total_us, self.max_us)?;
        for numerator in [50, 95, 99] {
            match self.quantile(numerator, 100) {
                Some((low, high)) => write!(summary, "\t{low}\t{}",
                    high.map(|value| value.to_string()).unwrap_or_else(|| "inf".to_owned()))?,
                None => write!(summary, "\tNA\tNA")?,
            }
        }
        writeln!(summary)?;
        for (index, count) in self.bins.iter().enumerate().filter(|(_, count)| **count != 0) {
            let (low, high) = bounds(index);
            writeln!(bins, "{name}\t{low}\t{}\t{count}",
                high.map(|value| value.to_string()).unwrap_or_else(|| "inf".to_owned()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn all_finite_buckets_are_adjacent_and_route_both_boundaries() {
        for index in 0..BINS - 1 {
            let (low, high) = bounds(index);
            assert_eq!(bucket(low), index); assert_eq!(bucket(high.unwrap()), index);
            assert_eq!(high.unwrap() + 1, bounds(index + 1).0);
        }
    }
    #[test]
    fn submicrosecond_observations_round_up_but_zero_is_zero() {
        let mut h = Histogram::default();
        for n in [0, 1, 999, 1000, 1001] { h.record(Some(Duration::from_nanos(n))); }
        assert_eq!((h.bins[0], h.bins[1], h.bins[2]), (1, 3, 1));
        assert_eq!((h.count, h.total_us, h.max_us), (5, 5, 2));
    }
    #[test]
    fn nearest_rank_returns_an_interval_not_a_false_exact_sample() {
        let mut h = Histogram::default(); for value in [1, 2, 3, 16, 100] { h.record_us(value); }
        assert_eq!(h.quantile(50, 100), Some((3, Some(3))));
        assert_eq!(h.quantile(95, 100), Some((96, Some(103))));
        assert_eq!(h.quantile(99, 100), Some((96, Some(103))));
    }
    #[test]
    fn merge_pools_counts_instead_of_averaging_percentiles() {
        let mut a = Histogram::default(); a.record_us(100);
        let mut b = Histogram::default(); for _ in 0..99 { b.record_us(1); }
        a.merge(&b); assert_eq!(a.quantile(99, 100), Some((1, Some(1))));
        assert_eq!((a.count, a.total_us), (100, 199));
    }
    #[test]
    fn empty_and_invalid_quantile_arguments_are_not_zero_latency() {
        let mut h = Histogram::default(); assert_eq!(h.quantile(99, 100), None);
        h.record_us(7);
        for (n, d) in [(0, 1), (1, 0), (2, 1)] { assert_eq!(h.quantile(n, d), None); }
    }
    #[test]
    fn overflow_bucket_keeps_exact_observed_maximum() {
        let mut h = Histogram::default(); h.record_us(u64::MAX);
        assert_eq!(h.quantile(99, 100), Some((1u64 << 34, None)));
        assert_eq!(h.max_us, u64::MAX);
    }
    #[test]
    fn counter_overflow_invalidates_without_wrapping() {
        let mut h = Histogram::default(); h.count = u64::MAX; h.record_us(1);
        assert!(!h.valid); assert_eq!(h.count, u64::MAX); assert_eq!(h.bins[1], 0);
    }
    #[test]
    fn a_reversed_clock_sample_invalidates_the_distribution() {
        let mut h = Histogram::default(); h.record(None);
        assert!(!h.valid); assert_eq!(h.count, 0); assert_eq!(h.quantile(99, 100), None);
    }
}
