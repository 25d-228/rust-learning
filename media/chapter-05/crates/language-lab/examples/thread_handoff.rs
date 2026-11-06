use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;

fn local_aliases() {
    let notes = Rc::new(RefCell::new(vec![String::from("created")]));
    let alias = Rc::clone(&notes);
    {
        let view = notes.borrow();
        assert_eq!(view.len(), 1);
        assert!(alias.try_borrow_mut().is_err());
    }
    alias.borrow_mut().push(String::from("ready"));
    assert_eq!(notes.borrow()[1], "ready");
    assert_eq!(Rc::strong_count(&notes), 2);
    drop(alias);
    assert_eq!(Rc::strong_count(&notes), 1);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lane {
    Data,
    Control,
}

impl Lane {
    fn index(self) -> usize {
        match self {
            Self::Data => 0,
            Self::Control => 1,
        }
    }

    fn other(self) -> Self {
        match self {
            Self::Data => Self::Control,
            Self::Control => Self::Data,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReserveError {
    Full,
    Closed,
}

struct LaneState {
    queue: VecDeque<String>,
    reserved: usize,
    limit: usize,
}

impl LaneState {
    fn new(limit: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(limit),
            reserved: 0,
            limit,
        }
    }

    fn has_space(&self) -> bool {
        self.queue.len() < self.limit - self.reserved
    }
}

struct State {
    lanes: [LaneState; 2],
    closed: bool,
    next: Lane,
    popped: usize,
    #[cfg(test)]
    waits_started: usize,
}

struct Handoff {
    state: Mutex<State>,
    changed: Condvar,
    #[cfg(test)]
    wait_observed: Condvar,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Snapshot {
    queued: [usize; 2],
    reserved: [usize; 2],
    closed: bool,
    popped: usize,
}

struct Reservation {
    handoff: Arc<Handoff>,
    lane: Lane,
    active: bool,
}

impl Handoff {
    fn new(data_limit: usize, control_limit: usize) -> Option<Arc<Self>> {
        if data_limit == 0 || control_limit == 0 {
            return None;
        }
        Some(Arc::new(Self {
            state: Mutex::new(State {
                lanes: [LaneState::new(data_limit), LaneState::new(control_limit)],
                closed: false,
                next: Lane::Control,
                popped: 0,
                #[cfg(test)]
                waits_started: 0,
            }),
            changed: Condvar::new(),
            #[cfg(test)]
            wait_observed: Condvar::new(),
        }))
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        match self.state.lock() {
            Ok(state) => state,
            Err(_) => std::process::abort(),
        }
    }
}

impl Handoff {
    fn wait<'a>(&self, state: MutexGuard<'a, State>) -> MutexGuard<'a, State> {
        #[cfg(test)]
        let state = {
            let mut state = state;
            state.waits_started += 1;
            self.wait_observed.notify_all();
            state
        };
        match self.changed.wait(state) {
            Ok(state) => state,
            Err(_) => std::process::abort(),
        }
    }

    fn snapshot(&self) -> Snapshot {
        let state = self.lock();
        Snapshot {
            queued: [state.lanes[0].queue.len(), state.lanes[1].queue.len()],
            reserved: [state.lanes[0].reserved, state.lanes[1].reserved],
            closed: state.closed,
            popped: state.popped,
        }
    }
}

impl Handoff {
    fn try_reserve(self: &Arc<Self>, lane: Lane) -> Result<Reservation, ReserveError> {
        let mut state = self.lock();
        if state.closed {
            return Err(ReserveError::Closed);
        }
        let target = &mut state.lanes[lane.index()];
        if !target.has_space() {
            return Err(ReserveError::Full);
        }
        target.reserved += 1;
        Ok(Reservation {
            handoff: Arc::clone(self),
            lane,
            active: true,
        })
    }

    fn reserve(self: &Arc<Self>, lane: Lane) -> Result<Reservation, ReserveError> {
        let mut state = self.lock();
        loop {
            if state.closed {
                return Err(ReserveError::Closed);
            }
            let target = &mut state.lanes[lane.index()];
            if target.has_space() {
                target.reserved += 1;
                return Ok(Reservation {
                    handoff: Arc::clone(self),
                    lane,
                    active: true,
                });
            }
            state = self.wait(state);
        }
    }
}

impl Reservation {
    fn commit(mut self, message: String) -> Result<(), String> {
        let mut state = self.handoff.lock();
        if state.closed {
            drop(state);
            return Err(message);
        }
        let target = &mut state.lanes[self.lane.index()];
        target.queue.push_back(message);
        target.reserved -= 1;
        self.active = false;
        drop(state);
        self.handoff.changed.notify_all();
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.active {
            let mut state = self.handoff.lock();
            state.lanes[self.lane.index()].reserved -= 1;
            drop(state);
            self.handoff.changed.notify_all();
        }
    }
}

impl Handoff {
    fn recv(&self) -> Option<(Lane, String)> {
        let mut state = self.lock();
        loop {
            for lane in [state.next, state.next.other()] {
                if let Some(message) = state.lanes[lane.index()].queue.pop_front() {
                    state.next = lane.other();
                    state.popped = state.popped.wrapping_add(1);
                    drop(state);
                    self.changed.notify_all();
                    return Some((lane, message));
                }
            }
            if state.closed {
                return None;
            }
            state = self.wait(state);
        }
    }

    fn close(&self) {
        {
            let mut state = self.lock();
            state.closed = true;
        }
        self.changed.notify_all();
    }
}

#[derive(Debug, PartialEq, Eq)]
enum PublishError {
    Closed(String),
    Empty(String),
}

fn publish_checked(
    handoff: &Arc<Handoff>,
    lane: Lane,
    message: String,
) -> Result<(), PublishError> {
    let reservation = match handoff.reserve(lane) {
        Ok(reservation) => reservation,
        Err(_) => return Err(PublishError::Closed(message)),
    };
    if message.is_empty() {
        return Err(PublishError::Empty(message));
    }
    reservation.commit(message).map_err(PublishError::Closed)
}

fn thread_traits() {
    fn needs_send<T: Send>() {}
    fn needs_sync<T: Sync>() {}
    needs_send::<String>();
    needs_send::<RefCell<String>>();
    needs_send::<Reservation>();
    needs_send::<Arc<Handoff>>();
    needs_sync::<Handoff>();
    needs_sync::<Arc<Handoff>>();
}

struct CloseOnExit(Arc<Handoff>);

impl Drop for CloseOnExit {
    fn drop(&mut self) {
        self.0.close();
    }
}

fn consume(
    handoff: Arc<Handoff>,
    completed: Arc<AtomicUsize>,
) -> Vec<(Lane, String)> {
    let _close_on_exit = CloseOnExit(Arc::clone(&handoff));
    let mut report = Vec::new();
    while let Some(item) = handoff.recv() {
        report.push(item);
        completed.fetch_add(1, Ordering::Relaxed);
    }
    report
}

fn main() -> std::io::Result<()> {
    local_aliases();
    thread_traits();
    let handoff = Handoff::new(3, 2).expect("positive lane limits");
    assert_eq!(
        publish_checked(&handoff, Lane::Data, String::new()),
        Err(PublishError::Empty(String::new()))
    );
    assert_eq!(handoff.snapshot().reserved, [0, 0]);
    publish_checked(&handoff, Lane::Data, String::from("data-0")).unwrap();
    publish_checked(&handoff, Lane::Data, String::from("data-1")).unwrap();
    let pending = handoff.try_reserve(Lane::Data).unwrap();
    assert!(matches!(
        handoff.try_reserve(Lane::Data),
        Err(ReserveError::Full)
    ));
    publish_checked(&handoff, Lane::Control, String::from("control-0")).unwrap();
    publish_checked(&handoff, Lane::Control, String::from("control-1")).unwrap();
    let before = handoff.snapshot();
    assert_eq!(before.queued, [2, 2]);
    assert_eq!(before.reserved, [1, 0]);
    handoff.close();
    assert_eq!(pending.commit(String::from("late")), Err(String::from("late")));

    let completed = Arc::new(AtomicUsize::new(0));
    let worker_handoff = Arc::clone(&handoff);
    let worker_completed = Arc::clone(&completed);
    let worker = thread::Builder::new()
        .name(String::from("handoff-owner"))
        .spawn(move || consume(worker_handoff, worker_completed))?;
    let joined = worker.join();
    let report = match joined {
        Ok(report) => report,
        Err(_) => return Err(std::io::Error::other("handoff worker panicked")),
    };
    let expected = vec![
        (Lane::Control, String::from("control-0")),
        (Lane::Data, String::from("data-0")),
        (Lane::Control, String::from("control-1")),
        (Lane::Data, String::from("data-1")),
    ];
    assert_eq!(report, expected);
    let after = handoff.snapshot();
    assert_eq!(after.queued, [0, 0]);
    assert_eq!(after.reserved, [0, 0]);
    assert!(after.closed);
    assert_eq!(after.popped, 4);
    assert_eq!(completed.load(Ordering::Relaxed), 4);
    println!("full data lane; control admission remained available");
    println!("validation and closed-commit reservations returned");
    for (lane, message) in report {
        println!("{lane:?}: {message}");
    }
    println!("joined: popped=4 completed=4 queued=[0, 0] reserved=[0, 0]");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn saw_wait(handoff: &Handoff, target: usize) -> bool {
        let (state, _) = handoff
            .wait_observed
            .wait_timeout_while(handoff.lock(), Duration::from_secs(5), |state| {
                state.waits_started < target
            })
            .unwrap();
        state.waits_started >= target
    }

    fn put(handoff: &Arc<Handoff>, lane: Lane, message: &str) {
        handoff
            .try_reserve(lane)
            .unwrap()
            .commit(message.to_owned())
            .unwrap();
    }

    #[test]
    fn local_borrows_and_thread_bounds_are_checked() {
        local_aliases();
        thread_traits();
    }

    #[test]
    fn both_lane_limits_must_be_positive() {
        assert!(Handoff::new(0, 1).is_none());
        assert!(Handoff::new(1, 0).is_none());
        assert!(Handoff::new(1, 1).is_some());
    }

    #[test]
    fn reservation_consumes_capacity_but_not_the_other_lane() {
        let handoff = Handoff::new(1, 1).unwrap();
        let data = handoff.try_reserve(Lane::Data).unwrap();
        assert!(matches!(
            handoff.try_reserve(Lane::Data),
            Err(ReserveError::Full)
        ));
        let control = handoff.try_reserve(Lane::Control).unwrap();
        assert_eq!(handoff.snapshot().reserved, [1, 1]);
        drop(data);
        drop(control);
        assert_eq!(handoff.snapshot().reserved, [0, 0]);
    }

    #[test]
    fn early_validation_error_returns_capacity() {
        let handoff = Handoff::new(1, 1).unwrap();
        let result = publish_checked(&handoff, Lane::Data, String::new());
        assert_eq!(result, Err(PublishError::Empty(String::new())));
        assert_eq!(handoff.snapshot().reserved, [0, 0]);
        put(&handoff, Lane::Data, "valid");
        assert_eq!(handoff.snapshot().queued, [1, 0]);
    }

    #[test]
    fn ready_lanes_alternate_and_each_lane_is_fifo() {
        let handoff = Handoff::new(3, 2).unwrap();
        for text in ["d0", "d1", "d2"] {
            put(&handoff, Lane::Data, text);
        }
        for text in ["c0", "c1"] {
            put(&handoff, Lane::Control, text);
        }
        handoff.close();
        let mut actual = Vec::new();
        while let Some(item) = handoff.recv() {
            actual.push(item);
        }
        assert_eq!(
            actual,
            vec![
                (Lane::Control, "c0".to_owned()),
                (Lane::Data, "d0".to_owned()),
                (Lane::Control, "c1".to_owned()),
                (Lane::Data, "d1".to_owned()),
                (Lane::Data, "d2".to_owned()),
            ]
        );
        assert_eq!(handoff.snapshot().popped, 5);
    }

    #[test]
    fn commit_order_is_not_reservation_order() {
        let handoff = Handoff::new(2, 1).unwrap();
        let first = handoff.try_reserve(Lane::Data).unwrap();
        let second = handoff.try_reserve(Lane::Data).unwrap();
        second.commit("second".to_owned()).unwrap();
        first.commit("first".to_owned()).unwrap();
        handoff.close();
        assert_eq!(handoff.recv(), Some((Lane::Data, "second".to_owned())));
        assert_eq!(handoff.recv(), Some((Lane::Data, "first".to_owned())));
        assert_eq!(handoff.recv(), None);
    }

    #[test]
    fn close_drains_commits_and_rejects_pending_ownership() {
        let handoff = Handoff::new(2, 1).unwrap();
        put(&handoff, Lane::Data, "accepted");
        let pending = handoff.try_reserve(Lane::Data).unwrap();
        handoff.close();
        handoff.close();
        assert!(matches!(
            handoff.try_reserve(Lane::Control),
            Err(ReserveError::Closed)
        ));
        assert_eq!(pending.commit("late".to_owned()), Err("late".to_owned()));
        assert_eq!(handoff.snapshot().reserved, [0, 0]);
        assert_eq!(handoff.recv(), Some((Lane::Data, "accepted".to_owned())));
        assert_eq!(handoff.recv(), None);
        assert_eq!(
            publish_checked(&handoff, Lane::Data, "owned".to_owned()),
            Err(PublishError::Closed("owned".to_owned()))
        );
    }

    #[test]
    fn closed_empty_receiver_does_not_wait_for_unused_reservations() {
        let handoff = Handoff::new(1, 1).unwrap();
        let pending = handoff.try_reserve(Lane::Data).unwrap();
        handoff.close();
        assert_eq!(handoff.recv(), None);
        assert_eq!(handoff.snapshot().reserved, [1, 0]);
        drop(pending);
        assert_eq!(handoff.snapshot().reserved, [0, 0]);
    }

    #[test]
    fn dropping_a_reservation_wakes_a_capacity_waiter() {
        let handoff = Handoff::new(1, 1).unwrap();
        let occupied = handoff.try_reserve(Lane::Data).unwrap();
        let peer = Arc::clone(&handoff);
        let worker = thread::spawn(move || peer.reserve(Lane::Data));
        let observed = saw_wait(&handoff, 1);
        drop(occupied);
        let acquired = worker.join().unwrap();
        handoff.close();
        assert!(observed, "producer never reached its wait path");
        drop(acquired.unwrap());
        assert_eq!(handoff.snapshot().reserved, [0, 0]);
    }

    #[test]
    fn removing_a_message_wakes_a_capacity_waiter() {
        let handoff = Handoff::new(1, 1).unwrap();
        put(&handoff, Lane::Data, "first");
        let peer = Arc::clone(&handoff);
        let worker = thread::spawn(move || peer.reserve(Lane::Data));
        let observed = saw_wait(&handoff, 1);
        let first = handoff.recv();
        let acquired = worker.join().unwrap();
        handoff.close();
        assert!(observed, "producer never reached its wait path");
        assert_eq!(first, Some((Lane::Data, "first".to_owned())));
        drop(acquired.unwrap());
        assert_eq!(handoff.snapshot().reserved, [0, 0]);
    }

    #[test]
    fn commit_wakes_an_empty_receiver() {
        let handoff = Handoff::new(1, 1).unwrap();
        let peer = Arc::clone(&handoff);
        let worker = thread::spawn(move || peer.recv());
        let observed = saw_wait(&handoff, 1);
        let sent = publish_checked(&handoff, Lane::Control, "wake".to_owned());
        let received = worker.join().unwrap();
        handoff.close();
        assert!(observed, "receiver never reached its wait path");
        assert_eq!(sent, Ok(()));
        assert_eq!(received, Some((Lane::Control, "wake".to_owned())));
    }

    #[test]
    fn close_wakes_a_capacity_waiter() {
        let handoff = Handoff::new(1, 1).unwrap();
        let occupied = handoff.try_reserve(Lane::Data).unwrap();
        let peer = Arc::clone(&handoff);
        let worker = thread::spawn(move || peer.reserve(Lane::Data));
        let observed = saw_wait(&handoff, 1);
        handoff.close();
        let result = worker.join().unwrap();
        drop(occupied);
        assert!(observed, "producer never reached its wait path");
        assert!(matches!(result, Err(ReserveError::Closed)));
        assert_eq!(handoff.snapshot().reserved, [0, 0]);
    }

    #[test]
    fn unrelated_notification_rechecks_and_close_wakes_receiver() {
        let handoff = Handoff::new(1, 1).unwrap();
        let peer = Arc::clone(&handoff);
        let worker = thread::spawn(move || peer.recv());
        let first_wait = saw_wait(&handoff, 1);
        handoff.changed.notify_all();
        let second_wait = saw_wait(&handoff, 2);
        handoff.close();
        let result = worker.join().unwrap();
        assert!(first_wait && second_wait, "receiver did not recheck and wait");
        assert_eq!(result, None);
    }

    #[test]
    fn unwinding_outside_the_lock_returns_a_reservation() {
        let handoff = Handoff::new(1, 1).unwrap();
        let peer = Arc::clone(&handoff);
        let worker = thread::spawn(move || {
            let _reservation = peer.try_reserve(Lane::Data).unwrap();
            panic!("injected after reserve released the mutex");
        });
        let result = worker.join();
        assert!(result.is_err());
        assert_eq!(handoff.snapshot().reserved, [0, 0]);
        assert!(!handoff.state.is_poisoned());
        put(&handoff, Lane::Data, "still usable");
    }

    #[test]
    fn consumer_exit_guard_closes_and_releases_blocked_producer() {
        let handoff = Handoff::new(1, 1).unwrap();
        put(&handoff, Lane::Data, "accepted");
        let peer = Arc::clone(&handoff);
        let producer = thread::spawn(move || peer.reserve(Lane::Data));
        let observed = saw_wait(&handoff, 1);
        let owner = Arc::clone(&handoff);
        let consumer = thread::spawn(move || {
            let _close_on_exit = CloseOnExit(owner);
            panic!("injected consumer failure outside the mutex");
        });
        let consumer_result = consumer.join();
        let producer_result = producer.join().unwrap();
        assert!(observed, "producer never reached its wait path");
        assert!(consumer_result.is_err());
        assert!(matches!(producer_result, Err(ReserveError::Closed)));
        assert!(handoff.snapshot().closed);
        assert_eq!(handoff.snapshot().queued, [1, 0]);
    }

    #[test]
    fn concurrent_producers_finish_before_normal_closure() {
        fn producer(
            handoff: Arc<Handoff>,
            lane: Lane,
        ) -> thread::JoinHandle<Result<(), PublishError>> {
            thread::spawn(move || {
                for index in 0..32 {
                    publish_checked(&handoff, lane, format!("{lane:?}-{index}"))?;
                }
                Ok(())
            })
        }

        let handoff = Handoff::new(2, 1).unwrap();
        let completed = Arc::new(AtomicUsize::new(0));
        let owner = Arc::clone(&handoff);
        let metric = Arc::clone(&completed);
        let consumer = thread::spawn(move || consume(owner, metric));
        let data = producer(Arc::clone(&handoff), Lane::Data);
        let control = producer(Arc::clone(&handoff), Lane::Control);
        let data_result = data.join();
        let control_result = control.join();
        handoff.close();
        let consumer_result = consumer.join();

        assert_eq!(data_result.unwrap(), Ok(()));
        assert_eq!(control_result.unwrap(), Ok(()));
        let report = consumer_result.unwrap();
        assert_eq!(report.len(), 64);
        for lane in [Lane::Data, Lane::Control] {
            let actual: Vec<String> = report
                .iter()
                .filter(|(source, _)| *source == lane)
                .map(|(_, message)| message.clone())
                .collect();
            let expected: Vec<String> = (0..32)
                .map(|index| format!("{lane:?}-{index}"))
                .collect();
            assert_eq!(actual, expected);
        }
        let snapshot = handoff.snapshot();
        assert_eq!(snapshot.queued, [0, 0]);
        assert_eq!(snapshot.reserved, [0, 0]);
        assert_eq!(snapshot.popped, 64);
        assert_eq!(completed.load(Ordering::Relaxed), 64);
        assert!(snapshot.closed);
    }
}
