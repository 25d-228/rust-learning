use super::*;
use crate::delivery::GroupState;
use crate::delivery::lease::AckRejection;
use crate::delivery::window::WindowState;
use crate::io::StorageFile;

fn group(value: u8) -> GroupId { GroupId::new(value).unwrap() }
fn offset(value: u64) -> Offset { Offset::new(value) }
fn progress(starts: [u64; 2], visible: u64) -> Progress {
    Progress::new(PathBuf::new(), Epoch::new(1).unwrap(), PartitionId::new(0).unwrap(),
        starts.map(offset), offset(visible), CheckpointPolicy::new(Duration::from_secs(1)).unwrap()).unwrap()
}
// Synthetic internal observations exercise guards; production obtains Ack only from LeaseWindow.
fn ack(g: u8, next: u64, outcome: AckOutcome) -> Ack {
    Ack { outcome, retired: 0, state: GroupState {
        epoch: Epoch::new(1).unwrap(), partition: PartitionId::new(0).unwrap(), group: group(g),
        window: WindowState { completed_end: offset(next), issued_end: offset(next), occupied: 0, capacity: 4 },
    } }
}

#[test]
fn policy_rejects_zero_and_more_than_sixty_seconds() {
    assert!(CheckpointPolicy::new(Duration::ZERO).is_err());
    assert!(CheckpointPolicy::new(Duration::from_secs(61)).is_err());
    assert!(CheckpointPolicy::new(Duration::from_secs(60)).is_ok());
}

#[test]
fn recovered_frontiers_cannot_exceed_visibility() {
    assert!(Progress::new(PathBuf::new(), Epoch::new(1).unwrap(), PartitionId::new(0).unwrap(),
        [offset(3), offset(4)], offset(3), CheckpointPolicy::new(Duration::from_secs(1)).unwrap()).is_err());
    let p = progress([2, 3], 3);
    assert_eq!(p.state(group(1)).persisted_next, offset(3));
    assert!(p.deadline().is_none());
}

#[test]
fn first_dirty_deadline_is_not_postponed_by_later_completions() {
    let mut p = progress([0, 0], 10);
    let now = Instant::now();
    p.observe(ack(0, 1, AckOutcome::Applied), offset(10), now).unwrap();
    let first = p.deadline().unwrap();
    p.observe(ack(0, 9, AckOutcome::Applied), offset(10), now + Duration::from_millis(900)).unwrap();
    assert_eq!(p.deadline(), Some(first));
    assert!(!p.due(group(0), first - Duration::from_nanos(1)));
    assert!(p.due(group(0), first));
    assert_eq!(p.state(group(0)).persisted_next, offset(0));
}

#[test]
fn group_deadlines_are_independent_and_the_earliest_wins() {
    let mut p = progress([0, 0], 4);
    let now = Instant::now();
    p.observe(ack(1, 1, AckOutcome::Applied), offset(4), now).unwrap();
    p.observe(ack(0, 2, AckOutcome::Applied), offset(4), now + Duration::from_millis(100)).unwrap();
    let deadline = p.deadline().unwrap();
    assert!(p.due(group(1), deadline));
    assert!(!p.due(group(0), deadline));
    assert_eq!(p.state(group(0)).volatile_next, offset(2));
    assert_eq!(p.state(group(1)).volatile_next, offset(1));
}

#[test]
fn regressions_overshoots_and_wrong_scope_preserve_progress() {
    let mut p = progress([2, 0], 5);
    let before = p.state(group(0));
    let mut cases = vec![ack(0, 1, AckOutcome::Applied), ack(0, 6, AckOutcome::Applied)];
    let mut wrong = ack(0, 3, AckOutcome::Applied); wrong.state.epoch = Epoch::new(2).unwrap(); cases.push(wrong);
    let mut wrong = ack(0, 3, AckOutcome::Applied); wrong.state.partition = PartitionId::new(1).unwrap(); cases.push(wrong);
    for a in cases {
        assert!(p.observe(a, offset(5), Instant::now()).is_err());
        assert_eq!(p.state(group(0)), before);
        assert!(p.deadline().is_none());
    }
}

#[test]
fn rejected_and_retired_reports_are_not_new_acceptances() {
    let mut p = progress([0, 0], 1);
    for outcome in [AckOutcome::Rejected(AckRejection::Expired), AckOutcome::AlreadyRetired] {
        assert!(matches!(p.observe(ack(0, 0, outcome), offset(1), Instant::now()).unwrap(),
            ProgressAck::NotApplied { .. }));
        assert!(p.deadline().is_none());
    }
    assert!(matches!(p.observe(ack(0, 0, AckOutcome::Duplicate), offset(1), Instant::now()).unwrap(),
        ProgressAck::Accepted(_)));
}

struct NoIo;
impl SyncIo for NoIo {
    fn sync(&mut self, _: Offset, _: &mut dyn StorageFile) -> std::io::Result<()> { panic!("unexpected data sync") }
    fn sync_metadata(&mut self, _: &mut dyn StorageFile) -> std::io::Result<()> { panic!("unexpected publication") }
}

#[test]
fn clean_checkpoint_needs_no_file_or_synchronization() {
    let mut p = progress([7, 3], 7);
    let result = p.checkpoint(group(0), offset(7), &mut NoIo).unwrap();
    assert!(!result.published);
    assert_eq!((result.progress.volatile_next, result.progress.persisted_next), (offset(7), offset(7)));
    assert!(p.deadline().is_none());
}

#[test]
fn max_is_a_valid_exclusive_frontier_without_addition() {
    let mut p = progress([u64::MAX - 1, 0], u64::MAX);
    p.observe(ack(0, u64::MAX, AckOutcome::Applied), offset(u64::MAX), Instant::now()).unwrap();
    assert_eq!(p.state(group(0)).volatile_next, offset(u64::MAX));
    assert_eq!(p.state(group(0)).persisted_next, offset(u64::MAX - 1));
}

#[test]
fn explicit_policy_can_be_dirty_without_installing_a_timer() {
    let mut p = progress([0, 0], 1);
    p.policy = CheckpointPolicy::explicit_only();
    p.observe(ack(0, 1, AckOutcome::Applied), offset(1), Instant::now()).unwrap();
    assert_eq!(p.state(group(0)).volatile_next, offset(1));
    assert_eq!(p.state(group(0)).persisted_next, offset(0));
    assert!(p.deadline().is_none());
}
