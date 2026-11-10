use std::io::{self, BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::{Config, Cpu, Ready, Responder, Result};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Request {
    Sample,
    Stop,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum Reply {
    Ready(Ready),
    Cpu(Cpu),
    Stopped,
}

fn write(
    output: &mut impl Write,
    value: &impl Serialize,
) -> Result<()> {
    serde_json::to_writer(&mut *output, value)?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}

fn read<T: DeserializeOwned>(input: &mut impl BufRead) -> Result<Option<T>> {
    let mut line = String::new();
    if input.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    Ok(Some(serde_json::from_str(&line)?))
}

/// Run a child on stdin/stdout. Reserve stdout for this control protocol;
/// diagnostics belong on stderr. EOF also stops the responder.
pub fn serve<S: Responder>(
    start: impl FnOnce(Config) -> Result<S>
) -> Result<()> {
    let mut input = io::stdin().lock();
    let config = read(&mut input)?.ok_or("missing fixture configuration")?;
    let server = start(config)?;
    let mut output = io::stdout().lock();
    let commands = (|| -> Result<()> {
        write(&mut output, &Reply::Ready(server.ready().clone()))?;
        while let Some(command) = read(&mut input)? {
            match command {
                Request::Sample => {
                    write(&mut output, &Reply::Cpu(Cpu::read()?))?
                }
                Request::Stop => break,
            }
        }
        Ok(())
    })();
    let stopped = server.stop();
    commands?;
    stopped?;
    write(&mut output, &Reply::Stopped)
}

/// Parent ownership of one responder process, independent of its framework.
pub struct Process {
    child: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
    ready: Ready,
    stopped: bool,
}

impl Process {
    /// The supplied executable must call [`serve`]. Startup is acknowledged by
    /// readiness, not by a guessed delay. Child stderr remains visible.
    pub fn spawn(
        command: &mut Command,
        config: &Config,
    ) -> Result<Self> {
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let input = child.stdin.take();
        let output = BufReader::new(child.stdout.take().expect("piped stdout"));
        let mut process = Self {
            child,
            input,
            output,
            ready: Ready {
                address: "127.0.0.1:0".parse()?,
                config: config.clone(),
                implementation: String::new(),
                trust_pem: None,
            },
            stopped: false,
        };
        write(process.input.as_mut().expect("piped stdin"), config)?;
        let Some(Reply::Ready(ready)) = read(&mut process.output)? else {
            return Err("responder did not acknowledge readiness".into());
        };
        if ready.config != *config
            || ready.address.port() == 0
            || (config.protocol == super::Protocol::H2)
                != ready.trust_pem.is_some()
        {
            return Err(
                "responder readiness differs from requested profile".into()
            );
        }
        process.ready = ready;
        Ok(process)
    }

    pub fn ready(&self) -> &Ready {
        &self.ready
    }

    pub fn sample(&mut self) -> Result<Cpu> {
        write(
            self.input.as_mut().ok_or("responder stopped")?,
            &Request::Sample,
        )?;
        match read(&mut self.output)? {
            Some(Reply::Cpu(cpu)) => Ok(cpu),
            _ => Err("responder did not return a CPU sample".into()),
        }
    }

    pub fn stop(&mut self) -> Result<()> {
        if self.stopped {
            return Ok(());
        }
        write(self.input.as_mut().ok_or("responder stopped")?, &Request::Stop)?;
        let acknowledged =
            matches!(read(&mut self.output)?, Some(Reply::Stopped));
        drop(self.input.take());
        let status = self.child.wait()?;
        self.stopped = true;
        if !acknowledged || !status.success() {
            return Err(
                format!("responder did not stop cleanly: {status}").into()
            );
        }
        Ok(())
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        // Closing the control pipe requests shutdown, including on startup or
        // measurement failure. Reap only the child this handle owns.
        drop(self.input.take());
        if !self.stopped {
            let _ = self.child.wait();
        }
    }
}
