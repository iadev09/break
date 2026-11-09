use std::time::Duration;

use clap::{Parser, ValueEnum};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Protocol {
    H1,
    H2,
}

impl Protocol {
    pub fn name(self) -> &'static str {
        match self {
            Self::H1 => "h1",
            Self::H2 => "h2c",
        }
    }
}

#[derive(Parser)]
#[command(about = "Measure brk's client CPU and observed throughput")]
pub struct Args {
    /// Protocols to compare. Their order reverses on alternate repetitions.
    #[arg(long, value_enum, value_delimiter = ',', default_value = "h1,h2")]
    pub protocols: Vec<Protocol>,
    /// Fixed connection counts, tested separately.
    #[arg(long, value_delimiter = ',', required = true)]
    pub connections: Vec<usize>,
    /// Concurrent requests per connection, tested separately; not a request rate.
    #[arg(long, value_delimiter = ',', required = true)]
    pub depths: Vec<usize>,
    /// Client thread counts, tested separately. Actual count is also reported.
    #[arg(long, value_delimiter = ',', required = true)]
    pub threads: Vec<usize>,
    /// Admission duration of each measured phase, for example 10s.
    #[arg(long, value_parser = humantime::parse_duration)]
    pub phase: Duration,
    /// Complete-request deadline, including setup for a new connection.
    #[arg(long, value_parser = humantime::parse_duration)]
    pub timeout: Duration,
    #[arg(long, default_value_t = 99.99)]
    pub accept: f64,
    /// Number of repetitions of every configuration.
    #[arg(long, default_value_t = 1)]
    pub repeat: usize,
    /// Responder threads in a separate child process. Required without --url.
    #[arg(long, required_unless_present = "url", conflicts_with = "url")]
    pub server_threads: Option<usize>,
    /// Use an external responder instead of the child fixture. Must return ok.
    /// Select the protocol it serves with --protocols.
    #[arg(long)]
    pub url: Option<String>,
    /// Internal child role; not part of the public benchmark command.
    #[arg(long, hide = true)]
    pub fixture: bool,
}

impl Args {
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.repeat == 0
            || self.connections.is_empty()
            || self.depths.is_empty()
            || self.threads.is_empty()
            || self.protocols.is_empty()
            || self.connections.contains(&0)
            || self.depths.contains(&0)
            || self.threads.contains(&0)
            || self.server_threads == Some(0)
        {
            return Err("counts must be positive".into());
        }
        for &connections in &self.connections {
            for &depth in &self.depths {
                brk::Plan {
                    conn: connections,
                    start: depth,
                    pace: 1,
                    phase_duration: self.phase,
                    timeout: self.timeout,
                    accept: self.accept,
                    max_concurrency: Some(depth),
                    threads: Some(self.threads[0]),
                }
                .validate()?;
            }
        }
        u32::try_from(*self.depths.iter().max().unwrap())?;
        if let Some(url) = &self.url {
            brk::Target::new(url)?;
            if !url.starts_with("http://") {
                return Err(
                    "this bench compares cleartext HTTP/1.1 and H2c".into()
                );
            }
        }
        Ok(())
    }
}
