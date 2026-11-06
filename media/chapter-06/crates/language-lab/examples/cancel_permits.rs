use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::{JoinError, JoinHandle};

#[derive(Default)]
struct YieldOnce {
    yielded: bool,
}

impl Future for YieldOnce {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.yielded {
            Poll::Ready(())
        } else {
            this.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

async fn borrowed_len(bytes: &[u8]) -> usize {
    YieldOnce::default().await;
    bytes.len()
}

#[derive(Debug, PartialEq, Eq)]
enum RequestError {
    BudgetClosed,
    Empty,
    GateClosed,
    InboxClosed,
    ReplyLost,
    Cancelled,
}

struct Job {
    payload: Vec<u8>,
    permit: OwnedSemaphorePermit,
    reply: oneshot::Sender<usize>,
}

#[derive(Debug, PartialEq, Eq)]
struct WorkReport {
    bytes: usize,
    reply_sent: bool,
}

struct Gate {
    reached: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

struct Controller {
    reached: oneshot::Receiver<()>,
    resume: oneshot::Sender<()>,
}

fn gate() -> (Gate, Controller) {
    let (reached_tx, reached_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    (
        Gate {
            reached: reached_tx,
            resume: resume_rx,
        },
        Controller {
            reached: reached_rx,
            resume: resume_tx,
        },
    )
}

impl Gate {
    async fn pause(self) -> Result<(), RequestError> {
        let Self { reached, resume } = self;
        reached.send(()).map_err(|_| RequestError::GateClosed)?;
        resume.await.map_err(|_| RequestError::GateClosed)
    }
}

async fn request(
    budget: Arc<Semaphore>,
    inbox: mpsc::Sender<Job>,
    payload: Vec<u8>,
    prepared: Option<Gate>,
) -> Result<usize, RequestError> {
    let permit = budget
        .acquire_owned()
        .await
        .map_err(|_| RequestError::BudgetClosed)?;
    if payload.is_empty() {
        return Err(RequestError::Empty);
    }
    if let Some(point) = prepared {
        point.pause().await?;
    }

    let slot = inbox
        .reserve()
        .await
        .map_err(|_| RequestError::InboxClosed)?;
    let (reply, response) = oneshot::channel();
    slot.send(Job {
        payload,
        permit,
        reply,
    });
    response.await.map_err(|_| RequestError::ReplyLost)
}

async fn complete(job: Job, executing: Option<Gate>) -> Result<WorkReport, RequestError> {
    if let Some(point) = executing {
        point.pause().await?;
    }
    let Job {
        payload,
        permit,
        reply,
    } = job;
    let bytes = payload.len();
    drop(payload);
    drop(permit);
    let reply_sent = reply.send(bytes).is_ok();
    Ok(WorkReport { bytes, reply_sent })
}

async fn cancel_on<F>(operation: F, cancelled: oneshot::Receiver<()>) -> Result<usize, RequestError>
where
    F: Future<Output = Result<usize, RequestError>>,
{
    tokio::select! {
        biased;
        _ = cancelled => Err(RequestError::Cancelled),
        result = operation => result,
    }
}

async fn observed<T>(receiver: oneshot::Receiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), receiver)
        .await
        .expect("fixture notification timed out")
        .expect("fixture sender disappeared")
}

async fn joined<T>(mut task: JoinHandle<T>) -> Result<T, JoinError> {
    match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
        Ok(result) => result,
        Err(_) => {
            task.abort();
            let _ = task.await;
            panic!("fixture task timed out; cancellation was requested and joined");
        }
    }
}

async fn before_handoff_demo() {
    let budget = Arc::new(Semaphore::new(1));
    let (tx, mut rx) = mpsc::channel(1);
    let (point, Controller { reached, resume }) = gate();
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let operation = request(Arc::clone(&budget), tx.clone(), b"cat".to_vec(), Some(point));
    let caller = tokio::spawn(cancel_on(operation, cancel_rx));

    observed(reached).await;
    assert_eq!(budget.available_permits(), 0);
    cancel_tx.send(()).unwrap();
    assert_eq!(joined(caller).await.unwrap(), Err(RequestError::Cancelled));
    assert_eq!(budget.available_permits(), 1);
    assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    assert!(resume.send(()).is_err());
}

async fn after_handoff_demo() {
    let budget = Arc::new(Semaphore::new(1));
    let (tx, mut rx) = mpsc::channel(1);
    let (point, Controller { reached, resume }) = gate();
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let operation = request(Arc::clone(&budget), tx, b"cat".to_vec(), None);
    let caller = tokio::spawn(cancel_on(operation, cancel_rx));
    let worker = tokio::spawn(async move {
        let job = rx.recv().await.expect("one accepted job");
        complete(job, Some(point)).await
    });

    observed(reached).await;
    assert_eq!(budget.available_permits(), 0);
    cancel_tx.send(()).unwrap();
    assert_eq!(joined(caller).await.unwrap(), Err(RequestError::Cancelled));
    assert_eq!(budget.available_permits(), 0);
    resume.send(()).unwrap();
    assert_eq!(
        joined(worker).await.unwrap(),
        Ok(WorkReport {
            bytes: 3,
            reply_sent: false,
        })
    );
    assert_eq!(budget.available_permits(), 1);
}

async fn detached_handle_demo() {
    let budget = Arc::new(Semaphore::new(1));
    let task_budget = Arc::clone(&budget);
    let (point, Controller { reached, resume }) = gate();
    let (done_tx, done_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let permit = task_budget.acquire_owned().await.unwrap();
        point.pause().await.unwrap();
        drop(permit);
        done_tx.send(()).unwrap();
    });

    observed(reached).await;
    drop(task);
    assert_eq!(budget.available_permits(), 0);
    resume.send(()).unwrap();
    observed(done_rx).await;
    assert_eq!(budget.available_permits(), 1);
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let bytes = vec![10, 20, 30];
    assert_eq!(borrowed_len(&bytes).await, 3);
    println!("polling: borrowed length=3");
    before_handoff_demo().await;
    println!("before handoff: cancelled, free=1, queue=empty");
    after_handoff_demo().await;
    println!("after handoff: cancelled, worker bytes=3, reply_sent=false, free=1");
    detached_handle_demo().await;
    println!("dropped handle: task continued, free=0 then 1");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::{pending, poll_fn, ready};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
        poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
    }

    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn yield_once_requests_a_repoll() {
        let count = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&count));
        let mut cx = Context::from_waker(&waker);
        let mut future = YieldOnce::default();
        assert_eq!(Pin::new(&mut future).poll(&mut cx), Poll::Pending);
        assert_eq!(count.0.load(Ordering::Relaxed), 1);
        assert_eq!(Pin::new(&mut future).poll(&mut cx), Poll::Ready(()));
        assert_eq!(count.0.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unpolled_body_is_lazy_but_captures_are_owned() {
        let budget = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&budget).acquire_owned().await.unwrap();
        let (started_tx, mut started_rx) = oneshot::channel();
        let future = async move {
            started_tx.send(()).unwrap();
            pending::<()>().await;
            drop(permit);
        };
        assert_eq!(budget.available_permits(), 0);
        assert!(matches!(
            started_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        drop(future);
        assert_eq!(budget.available_permits(), 1);
        assert!(matches!(
            started_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_pending_acquire_does_not_invent_capacity() {
        let budget = Arc::new(Semaphore::new(1));
        let held = Arc::clone(&budget).acquire_owned().await.unwrap();
        let mut waiting = Box::pin(Arc::clone(&budget).acquire_owned());
        assert!(poll_once(waiting.as_mut()).await.is_pending());
        drop(waiting);
        assert_eq!(budget.available_permits(), 0);
        drop(held);
        assert_eq!(budget.available_permits(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn early_validation_error_returns_the_permit() {
        let budget = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(1);
        assert_eq!(
            request(Arc::clone(&budget), tx.clone(), vec![], None).await,
            Err(RequestError::Empty)
        );
        assert_eq!(budget.available_permits(), 1);
        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn closed_budget_precedes_payload_validation() {
        let budget = Arc::new(Semaphore::new(1));
        budget.close();
        let (tx, _rx) = mpsc::channel(1);
        assert_eq!(
            request(Arc::clone(&budget), tx, vec![], None).await,
            Err(RequestError::BudgetClosed)
        );
        assert!(budget.is_closed());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn closed_inbox_returns_the_permit() {
        let budget = Arc::new(Semaphore::new(1));
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        assert_eq!(
            request(Arc::clone(&budget), tx, b"cat".to_vec(), None).await,
            Err(RequestError::InboxClosed)
        );
        assert_eq!(budget.available_permits(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn abandoned_controller_returns_the_permit() {
        let budget = Arc::new(Semaphore::new(1));
        let (tx, _rx) = mpsc::channel(1);
        let (point, Controller { reached, resume }) = gate();
        drop(resume);
        assert_eq!(
            request(Arc::clone(&budget), tx, b"cat".to_vec(), Some(point)).await,
            Err(RequestError::GateClosed)
        );
        observed(reached).await;
        assert_eq!(budget.available_permits(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancellation_before_handoff_restores_capacity() {
        before_handoff_demo().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancellation_after_handoff_keeps_the_worker_permit() {
        after_handoff_demo().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn normal_completion_releases_capacity_before_reply() {
        let budget = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(1);
        let caller = tokio::spawn(request(Arc::clone(&budget), tx, b"cat".to_vec(), None));
        let job = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(budget.available_permits(), 0);
        assert_eq!(
            complete(job, None).await,
            Ok(WorkReport {
                bytes: 3,
                reply_sent: true,
            })
        );
        assert_eq!(budget.available_permits(), 1);
        assert_eq!(joined(caller).await.unwrap(), Ok(3));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_accepted_job_reports_a_lost_reply() {
        let budget = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(1);
        let caller = tokio::spawn(request(Arc::clone(&budget), tx, b"cat".to_vec(), None));
        let job = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        drop(job);
        assert_eq!(joined(caller).await.unwrap(), Err(RequestError::ReplyLost));
        assert_eq!(budget.available_permits(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_a_pin_borrow_does_not_drop_the_owned_future() {
        let budget = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(1);
        let (point, Controller { reached, resume }) = gate();
        let mut operation = Box::pin(request(
            Arc::clone(&budget),
            tx.clone(),
            b"cat".to_vec(),
            Some(point),
        ));
        assert!(poll_once(operation.as_mut()).await.is_pending());
        observed(reached).await;
        let borrowed = operation.as_mut();
        drop(borrowed);
        assert_eq!(budget.available_permits(), 0);
        drop(operation);
        assert_eq!(budget.available_permits(), 1);
        assert!(resume.send(()).is_err());
        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelling_a_slot_wait_preserves_the_queued_job() {
        let budget = Arc::new(Semaphore::new(2));
        let (tx, mut rx) = mpsc::channel(1);
        let (reply, _response) = oneshot::channel();
        let held = Arc::clone(&budget).acquire_owned().await.unwrap();
        assert!(tx
            .try_send(Job {
                payload: b"first".to_vec(),
                permit: held,
                reply,
            })
            .is_ok());
        let (point, Controller { reached, resume }) = gate();
        let mut operation = Box::pin(request(
            Arc::clone(&budget),
            tx.clone(),
            b"second".to_vec(),
            Some(point),
        ));
        assert!(poll_once(operation.as_mut()).await.is_pending());
        observed(reached).await;
        resume.send(()).unwrap();
        assert!(poll_once(operation.as_mut()).await.is_pending());
        assert_eq!(budget.available_permits(), 0);
        drop(operation);
        assert_eq!(budget.available_permits(), 1);
        let job = rx.try_recv().unwrap();
        assert_eq!(job.payload.as_slice(), b"first");
        drop(job);
        assert_eq!(budget.available_permits(), 2);
        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ready_cancel_or_closed_sender_wins_a_ready_result() {
        for send_signal in [false, true] {
            let (tx, rx) = oneshot::channel();
            if send_signal {
                tx.send(()).unwrap();
            } else {
                drop(tx);
            }
            assert_eq!(
                cancel_on(ready(Ok(7)), rx).await,
                Err(RequestError::Cancelled)
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_a_join_handle_does_not_abort_the_task() {
        detached_handle_demo().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn abort_then_join_observes_permit_cleanup() {
        let budget = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(1);
        let (point, Controller { reached, resume }) = gate();
        let task = tokio::spawn(request(
            Arc::clone(&budget),
            tx.clone(),
            b"cat".to_vec(),
            Some(point),
        ));
        observed(reached).await;
        assert_eq!(budget.available_permits(), 0);
        task.abort();
        assert!(joined(task).await.unwrap_err().is_cancelled());
        assert_eq!(budget.available_permits(), 1);
        assert!(resume.send(()).is_err());
        assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }
}
