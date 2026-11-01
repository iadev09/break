use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::sync::{mpsc as async_mpsc, watch};

use crate::http1::{Client, Connection, Prepared, Window, run_slot};
use crate::{BreakReason, CancellationToken, Counts, Phase};

pub(crate) struct Command {
    pub phase: Phase,
    pub timeout: std::time::Duration,
    pub start: watch::Receiver<Option<Instant>>,
    pub phase_stop: CancellationToken,
    pub ready: Sender<()>,
    pub reply: Sender<Summary>,
}

#[derive(Default)]
pub(crate) struct Summary {
    pub counts: Counts,
    pub at_target: usize,
    pub failure: Option<BreakReason>,
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
    pub fn start(
        prepared: &Prepared,
        connections: usize,
        stop: CancellationToken,
    ) -> Result<Self, BreakReason> {
        let count = thread::available_parallelism()?.get().min(connections);
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
            let mut slots = Vec::new();
            let slots_count =
                connections / count + usize::from(index < connections % count);
            slots
                .try_reserve(slots_count)
                .map_err(|_| BreakReason::Resources("connection owners"))?;
            slots.resize_with(slots_count, || None);
            let stop = owner.stop.clone();
            let (commands, receiver) = async_mpsc::unbounded_channel();
            let (ready, waiting) = mpsc::channel();
            let thread = thread::Builder::new()
                .name(format!("brk-{index}"))
                .spawn(move || {
                    let runtime =
                        match tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                        {
                            Ok(runtime) => runtime,
                            Err(error) => {
                                let _ = ready.send(Err(error));
                                return;
                            }
                        };
                    let _ = ready.send(Ok(()));
                    runtime.block_on(serve(client, slots, receiver, stop));
                })?;
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

async fn serve(
    client: Client,
    mut slots: Vec<Option<Connection>>,
    mut commands: async_mpsc::UnboundedReceiver<Command>,
    stop: CancellationToken,
) {
    loop {
        let mut command = tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            command = commands.recv() => match command { Some(command) => command, None => break },
        };
        let _ = command.ready.send(());
        let start = tokio::select! {
            biased;
            _ = stop.cancelled() => None,
            changed = command.start.wait_for(|start| start.is_some()) => changed.ok().and_then(|value| *value),
        };
        let mut summary = Summary::default();
        if let Some(start) = start {
            let window = Window {
                start,
                duration: command.phase.duration,
                timeout: command.timeout,
                per_connection: command.phase.requests_per_connection,
            };
            let mut pending = FuturesUnordered::new();
            for (index, slot) in slots.iter_mut().enumerate() {
                let connection = slot.take();
                let client = &client;
                let stop = &stop;
                let phase_stop = &command.phase_stop;
                pending.push(async move {
                    (
                        index,
                        run_slot(client, connection, window, stop, phase_stop)
                            .await,
                    )
                });
            }
            while let Some((index, (connection, result))) = pending.next().await
            {
                slots[index] = connection;
                summary.counts.add(result.counts);
                summary.at_target += usize::from(result.at_target);
                if let Some(failure) = result.failure {
                    summary.failure.get_or_insert(failure);
                }
            }
        }
        let _ = command.reply.send(summary);
    }
}
