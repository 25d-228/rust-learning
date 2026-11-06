use broker_core::delivery::{AckOutcome, AckRejection};
use super::driver::Rig;
use super::support::*;

#[test]
fn generator_has_fixed_wrapping_vectors() {
    let mut generator = Generator(0);
    assert_eq!(generator.next(), 1442695040888963407);
    assert_eq!(generator.next(), 1876011003808476466);
    assert_eq!(generator.next(), 11166244414315200793);
    let mut a = Generator(u64::MAX);
    let mut b = Generator(u64::MAX);
    for bound in 1..100 { assert_eq!(a.pick(bound), b.pick(bound)); }
}

#[test]
fn seeded_sequences_match_the_reference() {
    for seed in selected_seeds() {
        let mut trace = Trace::new(seed, "seeded_sequences_match_the_reference");
        let mut random = Generator(seed);
        let mut rig = Rig::new();
        for step in 0..96 {
            let operation = random.pick(9);
            let group = random.pick(2);
            trace.step(format!("step={step} operation={operation} group={group} now={} end={} floor={}",
                rig.model.now, rig.model.end(), rig.model.floor()));
            match operation {
                0 | 1 => { rig.append(); }
                2 => {
                    let count = 1 + random.pick(4);
                    let bytes = [1, 39, 40, 79, 80, 160][random.pick(6)];
                    trace.step(format!("fetch count={count} bytes={bytes}"));
                    rig.fetch(group, count, bytes);
                }
                3 if !rig.tokens[group].is_empty() => {
                    let token = rig.tokens[group][random.pick(rig.tokens[group].len())];
                    trace.step(format!("ack {token:?}"));
                    rig.ack(group, token);
                }
                3 => {}
                4 => {
                    let by = [0, 1, 9, 10, 11][random.pick(5)];
                    trace.step(format!("advance={by}"));
                    rig.advance(by);
                }
                5 => rig.checkpoint(group, random.pick(2) == 0),
                6 => rig.retain(),
                7 => rig.restart(),
                8 if rig.model.bytes() + 144 <= QUOTA => rig.abandoned_append_then_fence(),
                8 => { rig.append(); }
                _ => unreachable!(),
            }
            rig.check();
        }
        rig.stop();
    }
}

#[test]
fn lost_append_reply_is_not_a_missing_record() {
    let mut trace = Trace::new(0x1801, "lost_append_reply_is_not_a_missing_record");
    let mut rig = Rig::new();
    trace.step("submit id0, drop its receiver, await same-lane id1");
    rig.abandoned_append_then_fence();
    rig.check();
    trace.step("plain join and restart with neither group checkpointed");
    rig.restart();
    rig.check();
    assert_eq!(rig.model.end(), 2);
    assert_eq!(rig.fetch(0, 4, 160).len(), 2);
    rig.stop();
}

#[test]
fn gap_checkpoint_lost_reply_retry_and_retention_compose() {
    let mut trace = Trace::new(0x1802, "gap_checkpoint_lost_reply_retry_and_retention_compose");
    let mut rig = Rig::new();
    for _ in 0..4 { assert!(rig.append()); }
    trace.step("fetch 0..4; complete followers 2,1,3 above the missing head");
    let old = rig.fetch(0, 4, 160);
    for index in [2, 1, 3] { assert_eq!(rig.ack(0, old[index]), AckOutcome::Applied); }
    rig.checkpoint(0, true);
    rig.retain();
    assert_eq!((rig.model.groups[0].persisted, rig.model.floor()), (0, 0));
    trace.step("deadline equality rejects head; retry changes generation; old head is stale");
    rig.advance(10);
    assert_eq!(rig.ack(0, old[0]), AckOutcome::Rejected(AckRejection::Expired));
    let retry = rig.fetch(0, 1, 40)[0];
    assert_eq!(rig.ack(0, old[0]), AckOutcome::Rejected(AckRejection::StaleGeneration));
    assert_eq!(rig.ack(0, retry), AckOutcome::Applied);
    assert_eq!(rig.model.groups[0].next, 4);
    trace.step("checkpoint group0 with lost reply; group1 still protects all records");
    rig.checkpoint(0, true);
    rig.retain();
    assert_eq!(rig.model.floor(), 0);
    trace.step("complete/checkpoint group1; replace consumed active segment and retain");
    for token in rig.fetch(1, 4, 160) { rig.ack(1, token); }
    rig.checkpoint(1, false);
    rig.retain();
    rig.check();
    assert_eq!((rig.model.floor(), rig.model.bytes()), (4, 32));
    trace.step("restart retains floor4; old token is fenced before retired-offset classification");
    rig.restart();
    assert_eq!(rig.ack(0, retry), AckOutcome::Rejected(AckRejection::WrongEpoch));
    rig.check();
    rig.stop();
}

#[test]
fn all_four_record_completion_orders_checkpoint_only_the_prefix() {
    for a in 0..4 {
        for b in 0..4 {
            for c in 0..4 {
                for d in 0..4 {
                    let order = [a, b, c, d];
                    if (0..4).any(|i| order[..i].contains(&order[i])) { continue; }
                    let seed = (a * 64 + b * 16 + c * 4 + d) as u64;
                    let mut trace = Trace::new(seed, "completion_order");
                    let mut rig = Rig::new();
                    for _ in 0..4 { assert!(rig.append()); }
                    let tokens = rig.fetch(0, 4, 160);
                    for index in order {
                        trace.step(format!("order={order:?}; complete index={index}; checkpoint; retain"));
                        rig.ack(0, tokens[index]);
                        rig.checkpoint(0, false);
                        rig.retain();
                        rig.check();
                        assert_eq!(rig.model.floor(), 0); // Other group's persisted frontier is zero.
                    }
                    trace.step("restart after final checkpoint");
                    rig.restart();
                    rig.check();
                    assert_eq!((rig.model.groups[0].next, rig.model.groups[1].next), (4, 0));
                    rig.stop();
                }
            }
        }
    }
}

#[test]
fn an_old_epoch_is_rejected_even_when_attempt_numbers_collide() {
    let mut trace = Trace::new(0x1803, "epoch_collision");
    let mut rig = Rig::new();
    assert!(rig.append());
    let old = rig.fetch(0, 1, 40)[0];
    trace.step("lose volatile window and reset process-local clock");
    rig.restart();
    let new = rig.fetch(0, 1, 40)[0];
    assert_eq!((old.offset, old.generation, old.deadline_ms), (new.offset, new.generation, new.deadline_ms));
    assert_ne!(old.epoch, new.epoch);
    assert_eq!(rig.ack(0, old), AckOutcome::Rejected(AckRejection::WrongEpoch));
    assert_eq!(rig.ack(0, new), AckOutcome::Applied);
    rig.check();
    rig.stop();
}

#[test]
fn a_byte_limited_retry_does_not_install_the_unreturned_attempt() {
    let mut trace = Trace::new(0x1804, "nonfitting_retry");
    let mut rig = Rig::new();
    for _ in 0..2 { assert!(rig.append()); }
    let old = rig.fetch(0, 2, 80);
    trace.step("expire both; one byte below two encoded records returns only one retry");
    rig.advance(10);
    let one = rig.fetch(0, 2, 79);
    assert_eq!(one.len(), 1);
    let skipped = old.iter().find(|token| token.offset != one[0].offset).unwrap();
    assert_eq!(rig.ack(0, *skipped), AckOutcome::Rejected(AckRejection::Expired));
    assert_eq!(rig.ack(0, old[one[0].offset.get() as usize]), AckOutcome::Rejected(AckRejection::StaleGeneration));
    rig.check();
    rig.stop();
}

#[test]
fn a_stalled_group_keeps_disk_full_until_both_checkpoints_advance() {
    let mut trace = Trace::new(0x1805, "disk_pressure_sequence");
    let mut rig = Rig::new();
    trace.step("fill data quota; a refused input does not acquire an offset");
    while rig.append() {}
    assert_eq!(rig.model.end(), 8);
    for group in 0..2 {
        while rig.model.groups[group].next < rig.model.end() {
            for token in rig.fetch(group, 4, 160) { rig.ack(group, token); }
        }
        trace.step(format!("group{group} checkpoint; attempt reclamation"));
        rig.checkpoint(group, false);
        rig.retain();
        if group == 0 { assert!(!rig.append()); }
    }
    assert_eq!((rig.model.floor(), rig.model.bytes()), (8, 32));
    trace.step("admit at offset8 after reclamation; restart and inspect retained range");
    assert!(rig.append());
    rig.restart();
    rig.check();
    assert_eq!((rig.model.floor(), rig.model.end()), (8, 9));
    rig.stop();
}
