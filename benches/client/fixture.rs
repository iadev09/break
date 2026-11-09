use std::io::{self, BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc;
use std::thread;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinSet;

use super::Result;
use super::config::{Args, Protocol};
use super::cpu::Cpu;

pub struct Fixture {
    child: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
    pub address: SocketAddr,
}

impl Fixture {
    pub fn start(
        args: &Args,
        protocol: Protocol,
    ) -> Result<Self> {
        let mut child = Command::new(std::env::current_exe()?)
            .args([
                "--fixture",
                "--protocols",
                match protocol {
                    Protocol::H1 => "h1",
                    Protocol::H2 => "h2",
                },
                "--connections",
                "1",
                "--threads",
                "1",
                "--depths",
                &args.depths.iter().max().unwrap().to_string(),
                "--server-threads",
                &args.server_threads.unwrap().to_string(),
                "--phase",
                &humantime::format_duration(args.phase).to_string(),
                "--timeout",
                &humantime::format_duration(args.timeout).to_string(),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let input = child.stdin.take();
        let output = BufReader::new(child.stdout.take().unwrap());
        let mut fixture = Self {
            child,
            input,
            output,
            address: "127.0.0.1:0".parse().unwrap(),
        };
        let ready = fixture.line()?;
        let address = ready
            .strip_prefix("ready ")
            .ok_or("invalid responder readiness message")?;
        fixture.address = address.trim().parse()?;
        Ok(fixture)
    }

    fn line(&mut self) -> Result<String> {
        let mut line = String::new();
        if self.output.read_line(&mut line)? == 0 {
            return Err("responder closed its control channel".into());
        }
        Ok(line)
    }

    pub fn sample(&mut self) -> Result<Cpu> {
        let input = self.input.as_mut().ok_or("responder already stopped")?;
        writeln!(input, "sample")?;
        input.flush()?;
        let line = self.line()?;
        let mut fields = line.split_whitespace();
        if fields.next() != Some("cpu") {
            return Err("invalid responder CPU message".into());
        }
        let user_ns = fields.next().ok_or("missing user CPU")?.parse()?;
        let system_ns = fields.next().ok_or("missing system CPU")?.parse()?;
        Ok(Cpu { user_ns, system_ns })
    }

    pub fn stop(&mut self) -> Result<()> {
        drop(self.input.take());
        if !self.child.wait()?.success() {
            return Err("responder process failed".into());
        }
        Ok(())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.input.take());
        let _ = self.child.wait();
    }
}

/// The child owns every responder thread and every accepted connection.
pub fn serve(args: &Args) -> Result<()> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let address = listener.local_addr()?;
    let protocol = args.protocols[0];
    let max_streams = u32::try_from(*args.depths.iter().max().unwrap())?;
    let stop = brk::CancellationToken::new();
    let (ready, started) = mpsc::channel();
    let mut workers = Vec::new();
    let mut routes = Vec::new();
    for _ in 0..args.server_threads.unwrap() {
        let (route, mut incoming) =
            tokio::sync::mpsc::unbounded_channel::<std::net::TcpStream>();
        routes.push(route);
        let stop = stop.clone();
        let ready = ready.clone();
        workers.push(thread::spawn(move || -> io::Result<()> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all().build()?;
            runtime.block_on(async move {
                let _ = ready.send(());
                drop(ready);
                let mut connections = JoinSet::new();
                loop {
                    tokio::select! {
                        () = stop.cancelled() => break,
                        stream = incoming.recv() => {
                            let Some(stream) = stream else { break; };
                            let stream = TcpStream::from_std(stream)?;
                            connections.spawn(async move {
                                match protocol {
                                    Protocol::H1 => h1(stream).await,
                                    Protocol::H2 => h2(stream, max_streams).await,
                                }
                            });
                        }
                        Some(ended) = connections.join_next(), if !connections.is_empty() => {
                            ended.map_err(io::Error::other)?;
                        }
                    }
                }
                connections.shutdown().await;
                Ok(())
            })
        }));
    }
    // Assign connections once, rather than measuring an accidental accept
    // distribution. Requests stay on that worker; no per-request channel hop.
    let accepting = stop.clone();
    let accepting_ready = ready.clone();
    workers.push(thread::spawn(move || -> io::Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all().build()?;
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener)?;
            let _ = accepting_ready.send(());
            drop(accepting_ready);
            let mut next = 0;
            loop {
                tokio::select! {
                    () = accepting.cancelled() => break,
                    accepted = listener.accept() => {
                        let (stream, _) = accepted?;
                        stream.set_nodelay(true)?;
                        routes[next].send(stream.into_std()?)
                            .map_err(|_| io::Error::other("responder worker stopped"))?;
                        next = (next + 1) % routes.len();
                    }
                }
            }
            Ok(())
        })
    }));
    drop(ready);
    for _ in 0..=args.server_threads.unwrap() {
        started.recv()?;
    }
    let mut output = io::stdout().lock();
    writeln!(output, "ready {address}")?;
    output.flush()?;
    let commands = (|| -> Result<()> {
        for line in io::stdin().lock().lines() {
            if line? != "sample" {
                return Err("unknown responder control command".into());
            }
            let cpu = Cpu::read()?;
            writeln!(output, "cpu {} {}", cpu.user_ns, cpu.system_ns)?;
            output.flush()?;
        }
        Ok(())
    })();
    stop.cancel();
    for worker in workers {
        worker.join().map_err(|_| "responder thread panicked")??;
    }
    commands
}

/// The benchmark only sends the fixed GET target. Batch pipelined replies so
/// the fixture does not deliberately spend a write syscall per request.
async fn h1(mut stream: TcpStream) {
    const REPLY: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    let mut input = Vec::new();
    let mut output = Vec::new();
    let mut read = [0; 16 * 1024];
    loop {
        let size = match stream.read(&mut read).await {
            Ok(0) | Err(_) => return,
            Ok(size) => size,
        };
        input.extend_from_slice(&read[..size]);
        let mut used = 0;
        while let Some(end) =
            input[used..].windows(4).position(|window| window == b"\r\n\r\n")
        {
            let head = &input[used..used + end];
            if !head.starts_with(b"GET /plain HTTP/1.1\r\n") {
                return;
            }
            used += end + 4;
            output.extend_from_slice(REPLY);
        }
        input.drain(..used);
        if !output.is_empty() {
            if stream.write_all(&output).await.is_err() {
                return;
            }
            output.clear();
        }
    }
}

async fn h2(
    stream: TcpStream,
    max_streams: u32,
) {
    let mut builder = h2::server::Builder::new();
    builder.max_concurrent_streams(max_streams);
    let Ok(mut connection) = builder.handshake::<_, Bytes>(stream).await else {
        return;
    };
    while let Some(Ok((request, mut respond))) = connection.accept().await {
        if request.method() != http::Method::GET
            || request.uri().path() != "/plain"
        {
            return;
        }
        let response = http::Response::builder()
            .status(200)
            .header(http::header::CONTENT_LENGTH, 2)
            .body(())
            .unwrap();
        if let Ok(mut body) = respond.send_response(response, false) {
            let _ = body.send_data(Bytes::from_static(b"ok"), true);
        }
    }
}
