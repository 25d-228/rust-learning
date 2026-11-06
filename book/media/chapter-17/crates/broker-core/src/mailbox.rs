use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionError {
    Full, Closed, DiskFull, InvalidRecord, MemoryFull, TooLarge, WaitersFull, Deadline,
}

#[derive(Debug)]
pub struct Rejected<T> { pub reason: AdmissionError, pub value: T }
#[derive(Debug)]
pub enum Event<D, C> { Data(D), Control(C), Deadline, Closed }

#[derive(Clone, Copy)]
enum Lane { Data, Query }
struct Queues<D, C> {
    data: VecDeque<D>,
    control: VecDeque<C>,
    urgent: VecDeque<C>,
    reserved_data: usize,
    reserved_query: usize,
    closed: bool,
    checkpoint_on_close: bool,
    next_lane: usize,
    #[cfg(test)]
    waiting: bool,
}
pub struct Mailbox<D, C> {
    queues: Mutex<Queues<D, C>>,
    changed: Condvar,
    space: Arc<Notify>,
    data_capacity: usize,
    control_capacity: usize,
    urgent_capacity: usize,
    #[cfg(test)]
    observed: Condvar,
}
impl<D, C> Mailbox<D, C> {
    pub fn new(data: usize, control: usize) -> Result<Self, &'static str> {
        Self::with_urgent(data, control, control)
    }
    pub fn with_urgent(data: usize, control: usize, urgent: usize) -> Result<Self, &'static str> {
        if [data, control, urgent].iter().any(|n| !(1..=4096).contains(n)) {
            return Err("each mailbox lane needs 1..=4096 slots");
        }
        fn lane<T>(count: usize) -> Result<VecDeque<T>, &'static str> {
            let mut queue = VecDeque::new();
            queue.try_reserve_exact(count).map_err(|_| "mailbox allocation failed")?;
            Ok(queue)
        }
        Ok(Self {
            queues: Mutex::new(Queues {
                data: lane(data)?, control: lane(control + urgent)?, urgent: lane(urgent)?,
                reserved_data: 0, reserved_query: 0, closed: false, checkpoint_on_close: false, next_lane: 1,
                #[cfg(test)] waiting: false,
            }),
            changed: Condvar::new(), space: Arc::new(Notify::new()),
            data_capacity: data, control_capacity: control, urgent_capacity: urgent,
            #[cfg(test)] observed: Condvar::new(),
        })
    }
    pub(crate) fn notification(&self) -> Arc<Notify> { Arc::clone(&self.space) }
    pub(crate) fn is_closed(&self) -> bool {
        self.queues.lock().unwrap_or_else(|e| e.into_inner()).closed
    }
    pub fn try_data(&self, value: D) -> Result<(), Rejected<D>> {
        let mut q = self.queues.lock().expect("mailbox mutex poisoned");
        if q.closed { return Err(Rejected { reason: AdmissionError::Closed, value }); }
        if q.data.len() + q.reserved_data == self.data_capacity {
            return Err(Rejected { reason: AdmissionError::Full, value });
        }
        q.data.push_back(value); self.changed.notify_one(); Ok(())
    }
    pub fn try_control(&self, value: C) -> Result<(), Rejected<C>> {
        let mut q = self.queues.lock().expect("mailbox mutex poisoned");
        if q.closed { return Err(Rejected { reason: AdmissionError::Closed, value }); }
        if q.control.len() + q.reserved_query == self.control_capacity {
            return Err(Rejected { reason: AdmissionError::Full, value });
        }
        q.control.push_back(value); self.changed.notify_one(); Ok(())
    }
    pub fn try_urgent(&self, value: C) -> Result<(), Rejected<C>> {
        let mut q = self.queues.lock().expect("mailbox mutex poisoned");
        if q.closed { return Err(Rejected { reason: AdmissionError::Closed, value }); }
        if q.urgent.len() == self.urgent_capacity {
            return Err(Rejected { reason: AdmissionError::Full, value });
        }
        q.urgent.push_back(value); self.changed.notify_one(); Ok(())
    }
    fn reserve(self: &Arc<Self>, lane: Lane) -> Result<Slot<D, C>, AdmissionError> {
        let mut q = self.queues.lock().expect("mailbox mutex poisoned");
        if q.closed { return Err(AdmissionError::Closed); }
        match lane {
            Lane::Data => {
                if q.data.len() + q.reserved_data == self.data_capacity { return Err(AdmissionError::Full); }
                q.reserved_data += 1;
            }
            Lane::Query => {
                if q.control.len() + q.reserved_query == self.control_capacity { return Err(AdmissionError::Full); }
                q.reserved_query += 1;
            }
        }
        Ok(Slot { mailbox: Arc::clone(self), lane, active: true, signal: true })
    }
    pub(crate) fn reserve_data(self: &Arc<Self>) -> Result<Slot<D, C>, AdmissionError> {
        self.reserve(Lane::Data)
    }
    pub(crate) fn reserve_query(self: &Arc<Self>) -> Result<Slot<D, C>, AdmissionError> {
        self.reserve(Lane::Query)
    }
    // Exactly one owner; each continuously ready lane gets one turn in three.
    pub fn recv(&self, deadline: Option<Instant>) -> Event<D, C> {
        let mut q = self.queues.lock().expect("mailbox mutex poisoned");
        loop {
            if deadline.is_some_and(|end| Instant::now() >= end) { return Event::Deadline; }
            for distance in 0..3 {
                let lane = (q.next_lane + distance) % 3;
                let event = match lane {
                    0 => q.data.pop_front().map(Event::Data),
                    1 => q.control.pop_front().map(Event::Control),
                    _ => q.urgent.pop_front().map(Event::Control),
                };
                if let Some(event) = event {
                    q.next_lane = (lane + 1) % 3;
                    self.space.notify_waiters();
                    return event;
                }
            }
            if q.closed { return Event::Closed; }
            #[cfg(test)] { q.waiting = true; self.observed.notify_all(); }
            q = match deadline {
                Some(end) => self.changed.wait_timeout(q, end.saturating_duration_since(Instant::now()))
                    .expect("mailbox mutex poisoned").0,
                None => self.changed.wait(q).expect("mailbox mutex poisoned"),
            };
            #[cfg(test)] { q.waiting = false; }
        }
    }
    pub fn close(&self) {
        let mut q = self.queues.lock().unwrap_or_else(|e| e.into_inner());
        q.closed = true; self.changed.notify_all(); self.space.notify_waiters();
    }
    // The flag and admission closure have one linearization point. No queue slot is needed.
    pub(crate) fn finish(&self) -> bool {
        let mut q = self.queues.lock().unwrap_or_else(|e| e.into_inner());
        if q.closed { return q.checkpoint_on_close; }
        q.checkpoint_on_close = true;
        q.closed = true;
        self.changed.notify_all();
        self.space.notify_waiters();
        true
    }
    pub(crate) fn checkpoint_requested(&self) -> bool {
        self.queues.lock().unwrap_or_else(|e| e.into_inner()).checkpoint_on_close
    }
    pub fn abort(&self) -> (VecDeque<D>, VecDeque<C>) {
        let mut q = self.queues.lock().unwrap_or_else(|e| e.into_inner());
        q.closed = true;
        let data = std::mem::take(&mut q.data);
        let mut control = std::mem::take(&mut q.control);
        control.append(&mut q.urgent); // Capacity was reserved for both lanes at construction.
        self.changed.notify_all(); self.space.notify_waiters();
        (data, control)
    }
}

// A reservation counts before its body exists; it does not own an enqueued command yet.
pub(crate) struct Slot<D, C> {
    mailbox: Arc<Mailbox<D, C>>,
    lane: Lane,
    active: bool,
    signal: bool,
}
impl<D, C> Slot<D, C> {
    pub(crate) fn cancel_quietly(mut self) { self.signal = false; }
    pub(crate) fn send_data(mut self, value: D) -> Result<(), Rejected<D>> {
        assert!(matches!(self.lane, Lane::Data));
        let mut q = self.mailbox.queues.lock().expect("mailbox mutex poisoned");
        q.reserved_data -= 1; self.active = false;
        if q.closed { return Err(Rejected { reason: AdmissionError::Closed, value }); }
        q.data.push_back(value); self.mailbox.changed.notify_one(); Ok(())
    }
    pub(crate) fn send_query(mut self, value: C) -> Result<(), Rejected<C>> {
        assert!(matches!(self.lane, Lane::Query));
        let mut q = self.mailbox.queues.lock().expect("mailbox mutex poisoned");
        q.reserved_query -= 1; self.active = false;
        if q.closed { return Err(Rejected { reason: AdmissionError::Closed, value }); }
        q.control.push_back(value); self.mailbox.changed.notify_one(); Ok(())
    }
}
impl<D, C> Drop for Slot<D, C> {
    fn drop(&mut self) {
        if self.active {
            let mut q = self.mailbox.queues.lock().unwrap_or_else(|e| e.into_inner());
            match self.lane {
                Lane::Data => q.reserved_data -= 1,
                Lane::Query => q.reserved_query -= 1,
            }
            drop(q);
            if self.signal { self.mailbox.space.notify_waiters(); }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn observe_wait<D, C>(mailbox: &Mailbox<D, C>) {
        let end = Instant::now() + Duration::from_secs(5);
        let mut q = mailbox.queues.lock().unwrap();
        while !q.waiting {
            assert!(Instant::now() < end, "owner did not reach the wait");
            q = mailbox.observed.wait_timeout(q, end.saturating_duration_since(Instant::now()))
                .unwrap().0;
        }
    }

    #[test]
    fn lanes_have_independent_caps_and_return_rejected_ownership() {
        let q = Mailbox::<Vec<u8>, u8>::new(1, 1).unwrap();
        q.try_data(vec![1]).unwrap();
        let refusal = q.try_data(vec![2]).unwrap_err();
        assert_eq!(refusal.reason, AdmissionError::Full);
        assert_eq!(refusal.value, vec![2]);
        q.try_control(9).unwrap();
        assert_eq!(q.try_control(8).unwrap_err().reason, AdmissionError::Full);
        assert!(matches!(q.recv(None), Event::Control(9)));
        assert!(matches!(q.recv(None), Event::Data(v) if v == vec![1]));
    }

    #[test]
    fn both_busy_lanes_alternate_and_preserve_each_fifo() {
        let q = Mailbox::new(3, 3).unwrap();
        for n in 0..3 { q.try_data(n).unwrap(); q.try_control(n + 10).unwrap(); }
        for n in 0..3 {
            assert!(matches!(q.recv(None), Event::Control(v) if v == n + 10));
            assert!(matches!(q.recv(None), Event::Data(v) if v == n));
        }
    }

    #[test]
    fn expired_deadline_precedes_ready_work() {
        let q = Mailbox::<u8, u8>::new(1, 1).unwrap();
        q.try_data(1).unwrap(); q.try_control(2).unwrap();
        assert!(matches!(q.recv(Some(Instant::now())), Event::Deadline));
        assert!(matches!(q.recv(None), Event::Control(2)));
        assert!(matches!(q.recv(None), Event::Data(1)));
    }

    #[test]
    fn timed_empty_wait_reaches_its_deadline() {
        let q = Mailbox::<u8, u8>::new(1, 1).unwrap();
        let end = Instant::now() + Duration::from_millis(5);
        assert!(matches!(q.recv(Some(end)), Event::Deadline));
        assert!(Instant::now() >= end);
    }

    #[test]
    fn a_control_command_wakes_an_observed_waiter() {
        let q = Arc::new(Mailbox::<u8, u8>::new(1, 1).unwrap());
        let receiver = Arc::clone(&q);
        let thread = thread::spawn(move || receiver.recv(Some(Instant::now() + Duration::from_secs(5))));
        observe_wait(&q);
        q.try_control(7).unwrap();
        assert!(matches!(thread.join().unwrap(), Event::Control(7)));
    }

    #[test]
    fn closure_wakes_an_observed_waiter_and_rejects_new_work() {
        let q = Arc::new(Mailbox::<u8, u8>::new(1, 1).unwrap());
        let receiver = Arc::clone(&q);
        let thread = thread::spawn(move || receiver.recv(Some(Instant::now() + Duration::from_secs(5))));
        observe_wait(&q); q.close();
        assert!(matches!(thread.join().unwrap(), Event::Closed));
        assert_eq!(q.try_data(3).unwrap_err().reason, AdmissionError::Closed);
        assert_eq!(q.try_control(4).unwrap_err().reason, AdmissionError::Closed);
    }

    #[test]
    fn close_drains_but_abort_returns_all_remaining_values() {
        let q = Mailbox::<u8, u8>::new(2, 2).unwrap();
        q.try_data(1).unwrap(); q.try_control(2).unwrap(); q.close();
        assert!(matches!(q.recv(None), Event::Control(2)));
        assert!(matches!(q.recv(None), Event::Data(1)));
        assert!(matches!(q.recv(None), Event::Closed));
        let q = Mailbox::<u8, u8>::new(1, 1).unwrap();
        q.try_data(3).unwrap(); q.try_control(4).unwrap();
        let (data, control) = q.abort();
        assert_eq!(data.into_iter().collect::<Vec<_>>(), vec![3]);
        assert_eq!(control.into_iter().collect::<Vec<_>>(), vec![4]);
        assert!(matches!(q.recv(None), Event::Closed));
    }

    #[test]
    fn invalid_lane_sizes_are_rejected() {
        assert!(Mailbox::<(), ()>::new(0, 1).is_err());
        assert!(Mailbox::<(), ()>::new(1, 4097).is_err());
    }
}

#[cfg(test)]
mod reservation_tests {
    use super::*;

    #[test]
    fn reserved_slots_count_before_enqueue_and_cancellation_returns_them() {
        let q = Arc::new(Mailbox::<u8, u8>::with_urgent(1, 1, 1).unwrap());
        let slot = q.reserve_data().unwrap();
        assert_eq!(q.try_data(8).unwrap_err().reason, AdmissionError::Full);
        drop(slot); q.try_data(9).unwrap();
        assert!(matches!(q.recv(None), Event::Data(9)));
    }
    #[test]
    fn reserved_queries_cannot_consume_the_urgent_lane() {
        let q = Arc::new(Mailbox::<u8, u8>::with_urgent(1, 1, 1).unwrap());
        let query = q.reserve_query().unwrap();
        assert_eq!(q.try_control(2).unwrap_err().reason, AdmissionError::Full);
        q.try_urgent(3).unwrap();
        assert_eq!(q.try_urgent(4).unwrap_err().reason, AdmissionError::Full);
        assert!(matches!(q.recv(None), Event::Control(3)));
        query.send_query(5).unwrap();
        assert!(matches!(q.recv(None), Event::Control(5)));
    }
    #[test]
    fn three_busy_lanes_get_round_robin_service() {
        let q = Mailbox::with_urgent(2, 2, 2).unwrap();
        for n in 0..2 { q.try_data(n).unwrap(); q.try_control(n + 10).unwrap(); q.try_urgent(n + 20).unwrap(); }
        for n in 0..2 {
            assert!(matches!(q.recv(None), Event::Control(v) if v == n + 10));
            assert!(matches!(q.recv(None), Event::Control(v) if v == n + 20));
            assert!(matches!(q.recv(None), Event::Data(v) if v == n));
        }
    }
    #[test]
    fn close_rejects_a_reserved_commit_and_abort_preserves_every_lane() {
        let q = Arc::new(Mailbox::<u8, u8>::with_urgent(2, 2, 2).unwrap());
        let slot = q.reserve_data().unwrap();
        q.try_data(1).unwrap(); q.try_control(2).unwrap(); q.try_urgent(3).unwrap(); q.close();
        assert_eq!(slot.send_data(4).unwrap_err().reason, AdmissionError::Closed);
        let (data, controls) = q.abort();
        assert_eq!(data.into_iter().collect::<Vec<_>>(), vec![1]);
        assert_eq!(controls.into_iter().collect::<Vec<_>>(), vec![2, 3]);
    }
}
