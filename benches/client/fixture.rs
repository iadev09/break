use super::{
    Result,
    config::{Args, Protocol},
};
use brk::fixture::{Config, Process, Protocol as WireProtocol};
use std::process::Command;

pub fn start(
    args: &Args,
    protocol: Protocol,
) -> Result<Process> {
    let mut command = if let Some(binary) = &args.fixture_bin {
        Command::new(binary)
    } else {
        let mut command = Command::new(std::env::current_exe()?);
        command.arg("--fixture");
        command
    };
    Process::spawn(
        &mut command,
        &Config {
            protocol: match protocol {
                Protocol::H1 => WireProtocol::H1,
                Protocol::H2 => WireProtocol::H2c,
                Protocol::H2Tls => WireProtocol::H2,
            },
            threads: args.server_threads.unwrap().try_into()?,
            max_streams: u32::try_from(*args.depths.iter().max().unwrap())?
                .try_into()?,
        },
    )
}
