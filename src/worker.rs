use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use tokio::sync::{mpsc as async_mpsc, watch};

use crate::http1::Prepared;
use crate::{
    BreakReason, CancellationToken, Counts, LatencyStats, LoadStats, Phase,
    Setups,
};

pub(crate) struct Command {
    pub phase: Phase,
    /// The connection test: how many owners this worker runs in the phase
    /// and how many requests each of their connections carries. `None` is
    /// the request test, which drives the worker's retained connections.
    pub churn: Option<Churn>,
    pub timeout: std::time::Duration,
    pub start: watch::Receiver<Option<Instant>>,
    pub phase_stop: CancellationToken,
    /// Where the phase's connections report reaching their depth, and this
    /// worker its end.
    pub loaded: std::sync::Arc<crate::load::Loaded>,
    pub ready: Sender<()>,
    pub reply: Sender<Summary>,
}

#[derive(Clone, Copy)]
pub(crate) struct Churn {
    pub owners: usize,
    pub requests: usize,
}

#[derive(Default)]
pub(crate) struct Summary {
    pub counts: Counts,
    pub at_target: usize,
    pub failure: Option<BreakReason>,
    pub load: LoadStats,
    pub latency: LatencyStats,
    pub setups: Setups,
    pub tls: Option<crate::Negotiated>,
}

struct Worker {
    commands: async_mpsc::UnboundedSender<Command>,
    thread: Option<JoinHandle<()>>,
}

pub(crate) struct Workers {
    workers: Vec<Worker>,
    stop: CancellationToken,
}

impl Workers {
    /// `owners` bounds the worker threads; `connections` of them are kept
    /// open across phases. The connection test keeps none. Without declared
    /// `threads` there is one for every CPU the process may run on.
    pub fn start(
        prepared: &Prepared,
        owners: usize,
        connections: usize,
        threads: Option<usize>,
        stop: CancellationToken,
    ) -> Result<Self, BreakReason> {
        let threads = match threads {
            Some(threads) => threads,
            None => thread::available_parallelism()?.get(),
        };
        let count = threads.min(owners);
        let mut owner = Self { workers: Vec::new(), stop };
        owner
            .workers
            .try_reserve(count)
            .map_err(|_| BreakReason::Resources("CPU worker handles"))?;
        let mut startup = Vec::new();
        startup
            .try_reserve(count)
            .map_err(|_| BreakReason::Resources("CPU readiness handles"))?;
        for index in 0..count {
            let client = prepared.client();
            let kept =
                connections / count + usize::from(index < connections % count);
            let stop = owner.stop.clone();
            let (commands, receiver) = async_mpsc::unbounded_channel();
            let (ready, waiting) = mpsc::channel();
            let thread =
                thread::Builder::new().name(format!("brk-{index}")).spawn(
                    move || {
                        // Both protocol types use the same readiness engine;
                        // each instantiation keeps its concrete connection state.
                        #[cfg(feature = "http2")]
                        if client.http2.is_some() {
                            return crate::http1::engine::serve::<
                                crate::http2::Connection,
                            >(
                                client, kept, receiver, stop, ready
                            );
                        }
                        crate::http1::engine::serve::<
                            crate::http1::connection::Conn,
                        >(
                            client, kept, receiver, stop, ready
                        );
                    },
                )?;
            owner.workers.push(Worker { commands, thread: Some(thread) });
            startup.push(waiting);
        }
        for ready in startup {
            receive(ready)??;
        }
        Ok(owner)
    }

    pub fn len(&self) -> usize {
        self.workers.len()
    }

    pub fn send(
        &self,
        index: usize,
        command: Command,
    ) -> Result<(), BreakReason> {
        self.workers[index]
            .commands
            .send(command)
            .map_err(|_| BreakReason::Worker("command channel closed"))
    }

    pub fn finish(mut self) -> Result<(), BreakReason> {
        self.stop.cancel();
        let mut failed = false;
        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                failed |= thread.join().is_err();
            }
        }
        if failed {
            Err(BreakReason::Worker("worker panicked"))
        } else {
            Ok(())
        }
    }
}

impl Drop for Workers {
    fn drop(&mut self) {
        self.stop.cancel();
        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                let _ = thread.join();
            }
        }
    }
}

pub(crate) fn receive<T>(reply: Receiver<T>) -> Result<T, BreakReason> {
    reply.recv().map_err(|_| BreakReason::Worker("reply channel closed"))
}
