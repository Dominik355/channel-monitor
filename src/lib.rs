#[cfg(any(feature = "flume", feature = "kanal", feature = "broadcast"))]
mod common;
#[cfg(feature = "flume")]
pub mod flume;
#[cfg(feature = "kanal")]
pub mod kanal;

#[cfg(any(feature = "flume", feature = "kanal", feature = "broadcast"))]
pub use common::set_update_interval;
