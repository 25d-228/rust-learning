use super::*;
use std::collections::BTreeMap;

fn off(value: u64) -> Offset { Offset::new(value) }
fn fixture(width: usize, start: u64) -> LeaseWindow {
    LeaseWindow::new(PartitionId::new(0).unwrap(), GroupId::new(0).unwrap(), off(start),
        LeaseConfig::new(Epoch::new(7).unwrap(), width, 10).unwrap())
}
fn issue(window: &mut LeaseWindow, visible: u64, count: usize, now: u64) -> Vec<LeaseToken> {
    let plan = window.plan(off(visible), count, now).unwrap();
    window.install(&plan, off(visible), now).unwrap()
}
fn unchanged(window: &LeaseWindow) -> (GroupState, Vec<Option<Attempt>>, Offset, bool) {
    (window.state(), window.attempts.to_vec(), window.retry_cursor, window.prefer_retry)
}

#[test]
fn configuration_and_plan_bounds_are_checked() {
    let epoch = Epoch::new(7).unwrap();
    assert!(LeaseConfig::new(epoch, 0, 10).is_err());
    assert!(LeaseConfig::new(epoch, 4097, 10).is_err());
    assert!(matches!(LeaseConfig::new(epoch, 1, 0), Err(LeaseError::LeaseMillis(0))));
    assert!(LeaseConfig::new(epoch, 1, 60_001).is_err());
    assert!(LeaseConfig::new(epoch, 4096, 60_000).is_ok());
    let window = fixture(2, 0);
    for count in [0, 1025] {
        assert!(matches!(window.plan(off(2), count, 0), Err(LeaseError::RecordLimit(_))));
    }
}

#[test]
fn issue_then_retry_changes_attempt_not_occupancy() {
    let mut window = fixture(2, 0);
    let first = issue(&mut window, 2, 2, 100);
    assert_eq!(first[0], LeaseToken { epoch: Epoch::new(7).unwrap(),
        partition: PartitionId::new(0).unwrap(), group: GroupId::new(0).unwrap(),
        offset: off(0), generation: 1, deadline_ms: 110 });
    let before = window.state();
    assert!(issue(&mut window, 2, 2, 109).is_empty());
    let retried = issue(&mut window, 2, 2, 110);
    assert_eq!(window.state(), before);
    assert_eq!(retried.iter().map(|token| token.generation).collect::<Vec<_>>(), vec![2, 2]);
    assert_eq!(retried[0].deadline_ms, 120);
    assert_eq!(window.acknowledge(first[0], 110).outcome,
        AckOutcome::Rejected(AckRejection::StaleGeneration));
}

#[test]
fn just_before_deadline_can_complete_and_prevent_retry() {
    let mut window = fixture(1, 0);
    let token = issue(&mut window, 1, 1, 0)[0];
    assert_eq!(window.acknowledge(token, 9).outcome, AckOutcome::Applied);
    assert!(issue(&mut window, 1, 1, 10).is_empty());
    assert_eq!(window.state().window.completed_end, off(1));
}

#[test]
fn exact_deadline_is_fenced_in_both_command_orders() {
    for retry_first in [false, true] {
        let mut window = fixture(1, 0);
        let old = issue(&mut window, 1, 1, 0)[0];
        if retry_first {
            assert_eq!(issue(&mut window, 1, 1, 10)[0].generation, 2);
        }
        let before = unchanged(&window);
        let expected = if retry_first { AckRejection::StaleGeneration } else { AckRejection::Expired };
        assert_eq!(window.acknowledge(old, 10).outcome, AckOutcome::Rejected(expected));
        assert_eq!(unchanged(&window), before);
        if !retry_first { assert_eq!(issue(&mut window, 1, 1, 10)[0].generation, 2); }
        assert_eq!(window.state().window.completed_end, off(0));
    }
}

#[test]
fn each_identity_field_is_checked_without_mutating_delivery_state() {
    let mut window = fixture(2, 0);
    let token = issue(&mut window, 1, 1, 0)[0];
    let variants = [
        (LeaseToken { epoch: Epoch::new(8).unwrap(), ..token }, AckRejection::WrongEpoch),
        (LeaseToken { partition: PartitionId::new(1).unwrap(), ..token }, AckRejection::WrongPartition),
        (LeaseToken { group: GroupId::new(1).unwrap(), ..token }, AckRejection::WrongGroup),
        (LeaseToken { offset: off(1), ..token }, AckRejection::NotIssued),
        (LeaseToken { generation: 0, ..token }, AckRejection::StaleGeneration),
        (LeaseToken { deadline_ms: 11, ..token }, AckRejection::WrongDeadline),
    ];
    for (wrong, expected) in variants {
        let before = unchanged(&window);
        assert_eq!(window.acknowledge(wrong, 1).outcome, AckOutcome::Rejected(expected));
        assert_eq!(unchanged(&window), before);
    }
}

#[test]
fn duplicate_retired_and_wrong_epoch_are_distinct() {
    let mut window = fixture(2, 0);
    let tokens = issue(&mut window, 2, 2, 0);
    assert_eq!(window.acknowledge(tokens[1], 1).retired, 0);
    let before = unchanged(&window);
    assert_eq!(window.acknowledge(tokens[1], 10).outcome, AckOutcome::Duplicate);
    assert_eq!(unchanged(&window), before);
    let retry = issue(&mut window, 2, 2, 10);
    assert_eq!(retry.len(), 1); // Completed followers are not retried, even after their deadline.
    assert_eq!(window.acknowledge(retry[0], 11).retired, 2);
    let retired = window.acknowledge(tokens[1], 11);
    assert_eq!((retired.outcome, retired.retired), (AckOutcome::AlreadyRetired, 0));
    let wrong_epoch = LeaseToken { epoch: Epoch::new(8).unwrap(), ..tokens[1] };
    assert_eq!(window.acknowledge(wrong_epoch, 11).outcome,
        AckOutcome::Rejected(AckRejection::WrongEpoch));
}

#[test]
fn retry_cursor_rotates_even_when_the_head_expires_again() {
    let mut window = fixture(3, 0);
    issue(&mut window, 3, 3, 0);
    let mut seen = Vec::new();
    for now in [10, 20, 30, 40] { seen.push(issue(&mut window, 3, 1, now)[0].offset.get()); }
    assert_eq!(seen, vec![0, 1, 2, 0]);
    assert_eq!(window.state().window.occupied, 3);
}

#[test]
fn retry_and_new_service_alternate_across_single_record_polls() {
    let mut window = fixture(4, 0);
    issue(&mut window, 4, 2, 0);
    let mut seen = Vec::new();
    for now in [10, 20, 30, 40] {
        let token = issue(&mut window, 4, 1, now)[0];
        seen.push((token.offset.get(), token.generation));
    }
    assert_eq!(seen, vec![(0, 2), (2, 1), (1, 2), (3, 1)]);
    assert_eq!(window.state().window.occupied, 4);
}

#[test]
fn planning_and_an_unreturned_suffix_do_not_advance_state() {
    let mut window = fixture(4, 0);
    issue(&mut window, 4, 2, 0);
    let before = unchanged(&window);
    let plan = window.plan(off(4), 4, 10).unwrap();
    assert_eq!(plan.iter().map(|selected| selected.offset.get()).collect::<Vec<_>>(), vec![0, 2, 1, 3]);
    assert_eq!(unchanged(&window), before);
    window.install(&plan[..1], off(4), 10).unwrap();
    assert_eq!(window.state().window.issued_end, off(2));
    assert_eq!(window.attempt(off(1)).generation, 1);
    let next = issue(&mut window, 4, 1, 10);
    assert_eq!((next[0].offset.get(), next[0].generation), (2, 1));
}

#[test]
fn deadline_overflow_does_not_partially_install() {
    let mut window = fixture(2, 0);
    let plan = window.plan(off(2), 2, u64::MAX - 5).unwrap();
    let before = unchanged(&window);
    assert_eq!(window.install(&plan, off(2), u64::MAX - 5), Err(LeaseError::DeadlineExhausted));
    assert_eq!(unchanged(&window), before);
}

#[test]
fn generation_exhaustion_does_not_wrap_or_install_other_candidates() {
    let mut window = fixture(2, 0);
    issue(&mut window, 2, 2, 0);
    let position = window.position(off(1));
    window.attempts[position].as_mut().unwrap().generation = u64::MAX;
    let before = unchanged(&window);
    assert!(matches!(window.plan(off(2), 2, 10), Err(LeaseError::GenerationExhausted(value)) if value == off(1)));
    assert_eq!(unchanged(&window), before);
}

#[test]
fn maximum_offsets_keep_distinct_slots_and_retire_without_wrapping() {
    let mut window = fixture(2, u64::MAX - 2);
    let first = issue(&mut window, u64::MAX, 2, 0);
    assert_ne!(window.position(first[0].offset), window.position(first[1].offset));
    let retry = issue(&mut window, u64::MAX, 2, 10);
    window.acknowledge(retry[1], 11);
    assert_eq!(window.acknowledge(retry[0], 11).retired, 2);
    assert_eq!(window.state().window.completed_end, off(u64::MAX));
    assert!(issue(&mut window, u64::MAX, 2, 11).is_empty());
}

#[test]
fn repeated_slot_reuse_does_not_make_a_retired_token_current() {
    let mut window = fixture(2, 0);
    let old = issue(&mut window, 40, 1, 0)[0];
    window.acknowledge(old, 0);
    for value in 1..40 {
        let token = issue(&mut window, 40, 1, value)[0];
        assert_eq!(token.offset.get(), value);
        assert_eq!(token.generation, 1);
        let before = unchanged(&window);
        assert_eq!(window.acknowledge(old, value).outcome, AckOutcome::AlreadyRetired);
        assert_eq!(unchanged(&window), before);
        assert_eq!(window.acknowledge(token, value).outcome, AckOutcome::Applied);
    }
}

#[test]
fn mixed_tokens_match_an_ordered_map_through_retries_and_wraps() {
    for width in [1, 2, 3, 7] {
        let mut window = fixture(width, 0);
        let mut map: BTreeMap<u64, (u64, u64, bool)> = BTreeMap::new();
        let mut history = Vec::new();
        let mut completed = 0;
        let mut issued = 0;
        for step in 0..200u64 {
            let now = step * 3;
            let tokens = issue(&mut window, 80, 3, now);
            for token in tokens {
                let key = token.offset.get();
                if let Some(&(generation, deadline, done)) = map.get(&key) {
                    assert!(!done && deadline <= now);
                    assert_eq!(token.generation, generation + 1);
                } else {
                    assert_eq!(key, issued);
                    assert_eq!(token.generation, 1);
                    issued += 1;
                }
                map.insert(key, (token.generation, token.deadline_ms, false));
                history.push(token);
            }
            if !history.is_empty() {
                let index = if step % 3 == 0 { 0 } else { history.len() - 1 };
                let token = history[index];
                let key = token.offset.get();
                let expected = if key < completed { AckOutcome::AlreadyRetired } else {
                    let entry = map.get_mut(&key).unwrap();
                    if entry.0 != token.generation { AckOutcome::Rejected(AckRejection::StaleGeneration) }
                    else if entry.2 { AckOutcome::Duplicate }
                    else if now >= entry.1 { AckOutcome::Rejected(AckRejection::Expired) }
                    else { entry.2 = true; AckOutcome::Applied }
                };
                while map.get(&completed).is_some_and(|entry| entry.2) {
                    map.remove(&completed);
                    completed += 1;
                }
                assert_eq!(window.acknowledge(token, now).outcome, expected);
            }
            let state = window.state().window;
            assert_eq!((state.completed_end.get(), state.issued_end.get()), (completed, issued));
            assert_eq!(state.occupied, map.len());
            for (&key, &(generation, deadline_ms, done)) in &map {
                assert_eq!(window.attempt(off(key)), Attempt { generation, deadline_ms });
                assert_eq!(window.window.status(off(key)),
                    Some(if done { Slot::Completed } else { Slot::Outstanding }));
            }
        }
    }
}
