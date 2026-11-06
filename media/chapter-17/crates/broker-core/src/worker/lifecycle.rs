use std::sync::Arc;
use tokio::sync::watch;
use super::WorkerState;
use crate::storage::error::StorageError;

// A completion notice does not consume the unique permission to join the thread.
#[derive(Debug, Clone)]
pub enum OwnerExit {
    Stopped { state: WorkerState, checkpointed: bool },
    Failed { state: WorkerState, cause: Arc<StorageError> },
    Panicked,
}
pub type ExitWatch = watch::Receiver<Option<OwnerExit>>;
