mod connection;
mod decode;

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use bytes::Bytes;
use http::{StatusCode, Uri};

use crate::{BreakReason, Counts};
pub(crate) use connection::{Connection, Window, run_slot};

/// One GET target and optional exact response validation.
#[derive(Clone, Debug)]
pub struct Target {
    uri: Uri,
    status: Option<StatusCode>,
    body: Option<Bytes>,
}

impl Target {
    pub fn new(url: &str) -> Result<Self, BreakReason> {
        let uri: Uri = url
            .parse()
            .map_err(|_| BreakReason::Invalid("invalid target URL"))?;
        if uri.scheme_str() != Some("http") || uri.host().is_none() {
            return Err(BreakReason::Invalid(
                "this adapter requires an absolute http:// URL",
            ));
        }
        let authority = uri
            .authority()
            .ok_or(BreakReason::Invalid("missing host"))?
            .as_str();
        if authority.contains('@') {
            return Err(BreakReason::Invalid("userinfo is not supported"));
        }
        let port = if authority.starts_with('[') {
            authority.split_once("]:").map(|(_, port)| port)
        } else {
            authority.rsplit_once(':').map(|(_, port)| port)
        };
        if uri.host() == Some("")
            || port.is_some_and(|port| port.parse::<u16>().is_err())
        {
            return Err(BreakReason::Invalid("invalid target host or port"));
        }
        Ok(Self { uri, status: None, body: None })
    }

    pub fn expect_status(
        mut self,
        status: StatusCode,
    ) -> Self {
        self.status = Some(status);
        self
    }

    pub fn expect_body(
        mut self,
        body: impl Into<Bytes>,
    ) -> Self {
        self.body = Some(body.into());
        self
    }

    pub(crate) fn resolve(&self) -> Result<Prepared, BreakReason> {
        let host =
            self.uri.host().ok_or(BreakReason::Invalid("missing host"))?;
        let host = host
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .unwrap_or(host);
        let addresses: Arc<[SocketAddr]> =
            (host, self.uri.port_u16().unwrap_or(80))
                .to_socket_addrs()?
                .collect::<Vec<_>>()
                .into();
        if addresses.is_empty() {
            return Err(BreakReason::Invalid("host resolved to no addresses"));
        }
        let path = self.uri.path_and_query().map_or("/", |path| path.as_str());
        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: {}\r\n\r\n",
            self.uri.authority().unwrap()
        );
        Ok(Prepared {
            addresses,
            request: request.into_bytes(),
            status: self.status,
            body: self.body.clone(),
        })
    }
}

pub(crate) struct Prepared {
    addresses: Arc<[SocketAddr]>,
    request: Vec<u8>,
    status: Option<StatusCode>,
    body: Option<Bytes>,
}

impl Prepared {
    pub(crate) fn client(&self) -> Client {
        Client {
            addresses: self.addresses.clone(),
            request: self.request.clone().into_boxed_slice(),
            status: self.status,
            body: self.body.clone(),
        }
    }
}

pub(crate) struct Client {
    addresses: Arc<[SocketAddr]>,
    request: Box<[u8]>,
    status: Option<StatusCode>,
    body: Option<Bytes>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Outcome {
    Accepted,
    Timeout,
    Transport,
    Status,
    Body,
}

impl Outcome {
    pub(crate) fn record(
        self,
        counts: &mut Counts,
    ) {
        counts.attempted += 1;
        match self {
            Self::Accepted => counts.successful += 1,
            Self::Timeout => counts.timeout += 1,
            Self::Transport => counts.transport += 1,
            Self::Status => counts.status += 1,
            Self::Body => counts.body += 1,
        }
    }
}

pub(crate) fn is_local_resource(error: &std::io::Error) -> bool {
    let local = matches!(
        error.kind(),
        std::io::ErrorKind::OutOfMemory | std::io::ErrorKind::AddrNotAvailable
    );
    #[cfg(unix)]
    let local = local
        || matches!(
            error.raw_os_error(),
            Some(libc::EMFILE | libc::ENFILE | libc::ENOBUFS)
        );
    local
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_rejects_unsupported_or_incomplete_urls() {
        for invalid in [
            "",
            "/plain",
            "https://example.test",
            "http:///",
            "ftp://example.test",
            "http://user:pass@example.test",
            "http://example.test:70000/plain",
            "http://example.test:not-a-port/plain",
            "http://[::1]:70000/plain",
        ] {
            assert!(Target::new(invalid).is_err(), "{invalid}");
        }
        assert!(Target::new("http://[::1]:8080/plain?q=1").is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn descriptor_exhaustion_is_a_client_resource_failure() {
        assert!(is_local_resource(&std::io::Error::from_raw_os_error(
            libc::EMFILE
        )));
        assert!(!is_local_resource(&std::io::Error::from_raw_os_error(
            libc::ECONNREFUSED
        )));
    }
}
