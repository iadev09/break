//! Optional, isolated responders for measuring the client. No server is started
//! by importing this module. Control messages never cross the request path.
//!
//! A child calls [`serve`] with its own startup function. Its parent starts it
//! with [`Process::spawn`], samples process CPU and stops it after client drain.
//! [`native::start`] supplies the reference Hyper responder; other applications
//! can implement [`Responder`] without depending on Hyper's handler contract.

mod control;
mod cpu;
pub mod native;
mod server;

use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroUsize};

use serde::{Deserialize, Serialize};

pub use control::{Process, serve};
pub use cpu::Cpu;
pub use server::Server;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

/// Wire protocol and transport. H2c is cleartext prior knowledge, not upgrade.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    H1,
    H2c,
    H2,
}

impl Protocol {
    pub fn name(self) -> &'static str {
        match self {
            Self::H1 => "h1",
            Self::H2c => "h2c",
            Self::H2 => "h2",
        }
    }
}

/// Responder resources, declared before startup; no load or duration defaults.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub protocol: Protocol,
    /// Serving runtime worker threads. The blocking control thread is separate.
    pub threads: NonZeroUsize,
    /// Advertised per-connection HTTP/2 stream limit; not a connection limit.
    pub max_streams: NonZeroU32,
}

/// Sent only after the listener and executor are ready. No private key is sent.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ready {
    pub address: SocketAddr,
    pub config: Config,
    pub implementation: String,
    /// Ephemeral local fixture trust, not a certificate verification bypass.
    pub trust_pem: Option<String>,
}

impl Ready {
    pub fn url(&self) -> String {
        let scheme =
            if self.config.protocol == Protocol::H2 { "https" } else { "http" };
        format!("{scheme}://{}/plain", self.address)
    }
}

/// An already started server. Stopping must close admission and join owned work.
/// Implementations keep framework and transport preparation outside measurement.
pub trait Responder {
    fn ready(&self) -> &Ready;
    fn stop(self) -> Result<()>;
}
