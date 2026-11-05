#![doc = include_str!("../README.md")]

mod acceptance;
mod http1;
mod latency;
mod load;
mod report;
mod run;
mod worker;

#[cfg(feature = "cli")]
pub mod cli;

pub use acceptance::Acceptance;
pub use http1::Target;
pub use latency::LatencyStats;
pub use load::LoadStats;
pub use report::{
    BreakReason, Broken, Counts, Done, Observer, Phase, PhaseStats, Setups,
    Stats,
};
pub use run::{ConnectionPlan, Plan, run, run_connections};
pub use tokio_util::sync::CancellationToken;
