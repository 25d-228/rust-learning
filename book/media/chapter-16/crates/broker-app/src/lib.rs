pub mod client;
pub mod local;
pub mod protocol;
pub mod server;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
