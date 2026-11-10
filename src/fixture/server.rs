use super::{Config, Ready, Responder, Result};
use crate::CancellationToken;
use std::future::Future;
use std::net::{SocketAddr, TcpListener};
use tokio::{
    runtime::Runtime,
    task::{JoinHandle, JoinSet},
};

/// Optional accept/executor ownership for a fixture that supplies its own
/// connection driver. No request wrapper, shared counter or per-request IPC.
/// Construct and stop it from the child's blocking control thread, outside
/// another async runtime. The callback owns each connection through drain.
pub struct Server {
    ready: Ready,
    runtime: Option<Runtime>,
    stop: CancellationToken,
    task: Option<JoinHandle<Result<()>>>,
}

impl Server {
    pub fn start<F, A>(
        config: Config,
        implementation: impl Into<String>,
        trust_pem: Option<String>,
        serve: F,
    ) -> Result<Self>
    where
        F: Fn(tokio::net::TcpStream, SocketAddr, CancellationToken) -> A
            + Send
            + 'static,
        A: Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(config.threads.get())
            .enable_all()
            .build()?;
        let listener = {
            let _entered = runtime.enter();
            tokio::net::TcpListener::from_std(listener)?
        };
        let stop = CancellationToken::new();
        let accepting = stop.clone();
        let task = runtime.spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    () = accepting.cancelled() => break,
                    accepted = listener.accept() => {
                        let (stream, peer) = accepted?;
                        stream.set_nodelay(true)?;
                        connections.spawn(serve(stream, peer, accepting.clone()));
                    }
                    Some(ended) = connections.join_next(), if !connections.is_empty() => { ended?; }
                }
            }
            drop(listener);
            while let Some(ended) = connections.join_next().await { ended?; }
            Ok(())
        });
        Ok(Self {
            ready: Ready {
                address,
                config,
                implementation: implementation.into(),
                trust_pem,
            },
            runtime: Some(runtime),
            stop,
            task: Some(task),
        })
    }
}

impl Responder for Server {
    fn ready(&self) -> &Ready {
        &self.ready
    }

    fn stop(mut self) -> Result<()> {
        self.stop.cancel();
        let task = self.task.take().expect("server task is owned");
        self.runtime
            .as_ref()
            .expect("server runtime is owned")
            .block_on(task)??;
        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        // Explicit stop drains; unwinding/startup failure drops only owned work.
        self.stop.cancel();
        drop(self.runtime.take());
    }
}
