mod connection;
mod decode;
mod transport;

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use bytes::Bytes;
use http::{StatusCode, Uri};

use crate::{BreakReason, Counts};
pub(crate) use connection::{Connection, Window, run_slot};

/// One GET target and optional exact response validation.
///
/// An `http://` URL is requested over TCP, an `https://` URL over TLS on the
/// same connection model. Nothing else about a run depends on the scheme.
#[derive(Clone, Debug)]
pub struct Target {
    uri: Uri,
    status: Option<StatusCode>,
    body: Option<Bytes>,
    connect: Option<SocketAddr>,
    #[cfg(feature = "tls")]
    trust: Option<Vec<rustls::pki_types::CertificateDer<'static>>>,
    #[cfg(feature = "tls")]
    resume: bool,
}

impl Target {
    pub fn new(url: &str) -> Result<Self, BreakReason> {
        let uri: Uri = url
            .parse()
            .map_err(|_| BreakReason::Invalid("invalid target URL"))?;
        let scheme = uri.scheme_str();
        let supported = scheme == Some("http")
            || (scheme == Some("https") && cfg!(feature = "tls"));
        if !supported || uri.host().is_none() {
            return Err(BreakReason::Invalid(if cfg!(feature = "tls") {
                "this adapter requires an absolute http:// or https:// URL"
            } else {
                "this build requires an absolute http:// URL"
            }));
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
            || port.is_some_and(|port| {
                port.parse::<u16>().map_or(true, |port| port == 0)
            })
        {
            return Err(BreakReason::Invalid("invalid target host or port"));
        }
        Ok(Self {
            uri,
            status: None,
            body: None,
            connect: None,
            #[cfg(feature = "tls")]
            trust: None,
            #[cfg(feature = "tls")]
            resume: true,
        })
    }

    /// Connect to this address instead of resolving the URL's host. The URL
    /// still names the host: in the `Host` header and, over TLS, as the server
    /// name the certificate must match.
    pub fn connect_to(
        mut self,
        address: SocketAddr,
    ) -> Self {
        self.connect = Some(address);
        self
    }

    /// For an `https://` target, trust exactly the authorities in this PEM
    /// instead of the platform's.
    #[cfg(feature = "tls")]
    pub fn trust_pem(
        mut self,
        pem: &[u8],
    ) -> Result<Self, BreakReason> {
        use rustls::pki_types::CertificateDer;
        use rustls::pki_types::pem::PemObject;

        let authorities = CertificateDer::pem_slice_iter(pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| BreakReason::Invalid("invalid trust PEM"))?;
        if authorities.is_empty() {
            return Err(BreakReason::Invalid(
                "the trust PEM holds no certificate",
            ));
        }
        self.trust = Some(authorities);
        Ok(self)
    }

    /// Make every TLS handshake a full one. Sessions are resumed otherwise,
    /// as clients that return to a server do.
    #[cfg(feature = "tls")]
    pub fn without_resumption(mut self) -> Self {
        self.resume = false;
        self
    }

    /// What every connection to an `https://` target presents and verifies.
    #[cfg(feature = "tls")]
    fn tls(
        &self,
        host: &str,
    ) -> Result<transport::secure::Tls, BreakReason> {
        let mut roots = rustls::RootCertStore::empty();
        match &self.trust {
            Some(authorities) => {
                for authority in authorities {
                    roots.add(authority.clone()).map_err(|_| {
                        BreakReason::Invalid(
                            "a trusted authority is not a usable certificate",
                        )
                    })?;
                }
            }
            None => {
                let platform = rustls_native_certs::load_native_certs();
                roots.add_parsable_certificates(platform.certs);
                if roots.is_empty() {
                    return Err(BreakReason::Invalid(
                        "no platform trust roots; name the authority to trust",
                    ));
                }
            }
        }
        let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| BreakReason::Invalid("no usable TLS protocol version"))?
        .with_root_certificates(roots)
        .with_no_client_auth();
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        if !self.resume {
            config.resumption = rustls::client::Resumption::disabled();
        }
        let name = rustls::pki_types::ServerName::try_from(host.to_owned())
            .map_err(|_| {
                BreakReason::Invalid("target host is not a TLS server name")
            })?;
        Ok(transport::secure::Tls {
            config: Arc::new(config),
            name,
            resume: self.resume,
        })
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
        let secure = self.uri.scheme_str() == Some("https");
        let port = self.uri.port_u16().unwrap_or(if secure { 443 } else { 80 });
        let addresses: Arc<[SocketAddr]> = match self.connect {
            Some(address) => [address].into(),
            None => (host, port).to_socket_addrs()?.collect::<Vec<_>>().into(),
        };
        if addresses.is_empty() {
            return Err(BreakReason::Invalid("host resolved to no addresses"));
        }
        #[cfg(feature = "tls")]
        let tls = secure.then(|| self.tls(host)).transpose()?;
        let path = self.uri.path_and_query().map_or("/", |path| path.as_str());
        let host_line = self.uri.authority().unwrap();
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: {host_line}\r\n\r\n");
        // The last request of a connection that the client means to end.
        let closing = format!(
            "GET {path} HTTP/1.1\r\nHost: {host_line}\r\nConnection: close\r\n\r\n"
        );
        Ok(Prepared {
            addresses,
            request: request.into_bytes(),
            closing: closing.into_bytes(),
            status: self.status,
            body: self.body.clone(),
            #[cfg(feature = "tls")]
            tls,
        })
    }
}

pub(crate) struct Prepared {
    addresses: Arc<[SocketAddr]>,
    request: Vec<u8>,
    closing: Vec<u8>,
    status: Option<StatusCode>,
    body: Option<Bytes>,
    #[cfg(feature = "tls")]
    tls: Option<transport::secure::Tls>,
}

impl Prepared {
    pub(crate) fn client(&self) -> Client {
        Client {
            addresses: self.addresses.clone(),
            request: self.request.clone().into_boxed_slice(),
            closing: self.closing.clone().into_boxed_slice(),
            status: self.status,
            body: self.body.clone(),
            #[cfg(feature = "tls")]
            tls: self.tls.clone(),
        }
    }
}

pub(crate) struct Client {
    addresses: Arc<[SocketAddr]>,
    request: Box<[u8]>,
    closing: Box<[u8]>,
    status: Option<StatusCode>,
    body: Option<Bytes>,
    #[cfg(feature = "tls")]
    tls: Option<transport::secure::Tls>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Outcome {
    Accepted,
    Timeout,
    Transport,
    Status,
    Body,
    Abandoned,
    Connect,
    ConnectTimeout,
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
            Self::Abandoned => counts.abandoned += 1,
            Self::Connect => counts.connect += 1,
            Self::ConnectTimeout => counts.connect_timeout += 1,
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
            "http:///",
            "ftp://example.test",
            "http://user:pass@example.test",
            "http://example.test:70000/plain",
            "http://example.test:0/plain",
            "http://example.test:not-a-port/plain",
            "http://[::1]:70000/plain",
        ] {
            assert!(Target::new(invalid).is_err(), "{invalid}");
        }
        assert!(Target::new("http://[::1]:8080/plain?q=1").is_ok());
        assert_eq!(
            Target::new("https://example.test").is_ok(),
            cfg!(feature = "tls")
        );
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
