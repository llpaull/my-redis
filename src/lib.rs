pub mod cmd;

pub mod connection;
pub use connection::Connection;

pub mod client;
pub use client::Client;

pub mod resp;
pub use resp::RESPType;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
