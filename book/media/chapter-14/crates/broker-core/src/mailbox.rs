use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionError { Full, Closed, DiskFull, InvalidRecord }

#[derive(Debug)]
pub struct Rejected<T> {
    pub reason: AdmissionError,
    pub value: T,
}

#[derive(Debug)]
pub enum Event<D, C> { Data(D), Control(C), Deadline, Closed }

struct Queues<D, C> {
    data: VecDeque<D>,
    control: VecDeque<C>,
    closed: bool,
    prefer_control: bool,
    #[cfg(test)]
    waiting: bool,
}

pub struct Mailbox<D, C> {
    queues: Mutex<Queues<D, C>>,
    changed: Condvar,
    data_capacity: usize,
    control_capacity: usize,
    #[cfg(test)]
    observed: Condvar,
}

impl<D, C> Mailbox<D, C> {
    pub fn new(data_capacity: usize, control_capacity: usize) -> Result<Self, &'static str> {
        if !(1..=4096).contains(&data_capacity) || !(1..=4096).contains(&control_capacity) {
            return Err("each mailbox lane needs 1..=4096 slots");
        }
        Ok(Self {
            queues: Mutex::new(Queues {
                data: VecDeque::with_capacity(data_capacity),
                control: VecDeque::with_capacity(control_capacity),
                closed: false, prefer_control: true,
                #[cfg(test)]
                waiting: false,
            }),
            changed: Condvar::new(), data_capacity, control_capacity,
            #[cfg(test)]
            observed: Condvar::new(),
        })
    }

    pub fn try_data(&self, value: D) -> Result<(), Rejected<D>> {
        let mut q = self.queues.lock().expect("mailbox mutex poisoned");
        if q.closed { return Err(Rejected { reason: AdmissionError::Closed, value }); }
        if q.data.len() == self.data_capacity {
            return Err(Rejected { reason: AdmissionError::Full, value });
        }
        q.data.push_back(value);
        self.changed.notify_one();
        Ok(())
    }

    pub fn try_control(&self, value: C) -> Result<(), Rejected<C>> {
        let mut q = self.queues.lock().expect("mailbox mutex poisoned");
        if q.closed { return Err(Rejected { reason: AdmissionError::Closed, value }); }
        if q.control.len() == self.control_capacity {
            return Err(Rejected { reason: AdmissionError::Full, value });
        }
        q.control.push_back(value);
        self.changed.notify_one();
        Ok(())
    }

    // Exactly one owner calls recv. A deadline is absolute, not a fresh delay per wakeup.
    pub fn recv(&self, deadline: Option<Instant>) -> Event<D, C> {
        let mut q = self.queues.lock().expect("mailbox mutex poisoned");
        loop {
            if deadline.is_some_and(|end| Instant::now() >= end) { return Event::Deadline; }
            if !q.control.is_empty() && (q.prefer_control || q.data.is_empty()) {
                q.prefer_control = false;
                return Event::Control(q.control.pop_front().expect("checked control queue"));
            }
            if let Some(value) = q.data.pop_front() {
                q.prefer_control = true;
                return Event::Data(value);
            }
            if q.closed { return Event::Closed; }
            #[cfg(test)]
            { q.waiting = true; self.observed.notify_all(); }
            q = match deadline {
                Some(end) => self.changed.wait_timeout(q, end.saturating_duration_since(Instant::now()))
                    .expect("mailbox mutex poisoned").0,
                None => self.changed.wait(q).expect("mailbox mutex poisoned"),
            };
            #[cfg(test)]
            { q.waiting = false; }
        }
    }

    // Close admission, but keep accepted commands available to the owner.
    pub fn close(&self) {
        let mut q = self.queues.lock().unwrap_or_else(|e| e.into_inner());
        q.closed = true;
        self.changed.notify_all();
    }

    // Terminal cleanup only: move values out, then drop/respond outside this mutex.
    pub fn abort(&self) -> (VecDeque<D>, VecDeque<C>) {
        let mut q = self.queues.lock().unwrap_or_else(|e| e.into_inner());
        q.closed = true;
        let values = (std::mem::take(&mut q.data), std::mem::take(&mut q.control));
        self.changed.notify_all();
        values
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
