#![doc = include_str!("../README.md")]

mod acceptance;
mod cpu;
mod http1;
#[cfg(feature = "http2")]
mod http2;
mod latency;
mod load;
mod report;
mod run;
mod worker;

#[cfg(feature = "cli")]
pub mod cli;

#[cfg(all(feature = "fixture", unix))]
pub mod fixture;

pub use acceptance::Acceptance;
pub use cpu::{MachineCpu, MachineSample};
pub use http1::Target;
pub use latency::LatencyStats;
pub use load::LoadStats;
pub use report::{
    BreakReason, Broken, Counts, Done, Negotiated, Observer, Phase, PhaseStats,
    Setups, Stats,
};
pub use run::{ConnectionPlan, Plan, run, run_connections};
pub use tokio_util::sync::CancellationToken;
