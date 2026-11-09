//! What carries one connection's bytes: a TCP stream as it is, or the same
//! stream under TLS. Both are driven by socket readiness and non-blocking
//! reads and writes, so the connection loop is the same for either. The TLS
//! session is driven by the shared Mio engine for either HTTP version.

use super::Client;

/// One owner's TLS sessions: what a single visitor remembers of the server.
#[cfg(feature = "tls")]
pub(crate) type Sessions = secure::Tls;
#[cfg(not(feature = "tls"))]
pub(crate) type Sessions = std::convert::Infallible;

/// The TLS sessions of one owner, when the target is `https://`.
pub(crate) fn own_sessions(client: &Client) -> Option<Sessions> {
    #[cfg(feature = "tls")]
    return client.tls.as_ref().map(secure::Tls::visitor);
    #[cfg(not(feature = "tls"))]
    {
        let _ = client;
        None
    }
}

#[cfg(feature = "tls")]
pub(crate) mod secure {
    use std::io::{self, ErrorKind, Read, Write};
    use std::sync::Arc;

    use rustls::pki_types::ServerName;

    /// The prepared client side of every connection to one target.
    #[derive(Clone)]
    pub(crate) struct Tls {
        pub(crate) config: Arc<rustls::ClientConfig>,
        pub(crate) name: ServerName<'static>,
        pub(crate) resume: bool,
    }

    impl Tls {
        /// The same client with a session memory of its own, holding this
        /// one server. A store shared by every owner would let each resume
        /// the newest session and hide how many the server has to keep.
        pub(crate) fn visitor(&self) -> Self {
            let mut config = (*self.config).clone();
            if self.resume {
                config.resumption =
                    rustls::client::Resumption::in_memory_sessions(1);
            }
            Self {
                config: Arc::new(config),
                name: self.name.clone(),
                resume: self.resume,
            }
        }
    }

    /// A non-blocking socket as the TLS session reads and writes it. The
    /// session is the same over whichever readiness loop owns the socket.
    pub(crate) trait Wire {
        fn receive(
            &self,
            buffer: &mut [u8],
        ) -> io::Result<usize>;

        fn send(
            &self,
            bytes: &[u8],
        ) -> io::Result<usize>;
    }

    impl Wire for mio::net::TcpStream {
        fn receive(
            &self,
            buffer: &mut [u8],
        ) -> io::Result<usize> {
            let mut socket = self;
            socket.read(buffer)
        }

        fn send(
            &self,
            bytes: &[u8],
        ) -> io::Result<usize> {
            let mut socket = self;
            socket.write(bytes)
        }
    }

    struct Socket<'a, S>(&'a S);

    impl<S: Wire> Read for Socket<'_, S> {
        fn read(
            &mut self,
            buffer: &mut [u8],
        ) -> io::Result<usize> {
            self.0.receive(buffer)
        }
    }

    impl<S: Wire> Write for Socket<'_, S> {
        fn write(
            &mut self,
            bytes: &[u8],
        ) -> io::Result<usize> {
            self.0.send(bytes)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    pub(crate) struct Secure<S> {
        stream: S,
        session: rustls::ClientConnection,
        /// Decrypted bytes may still wait inside the session. The socket's
        /// readiness says nothing about them.
        plaintext: bool,
    }

    impl<S: Wire> Secure<S> {
        /// A session that has not exchanged a byte yet.
        pub(in crate::http1) fn start(
            stream: S,
            tls: &Tls,
        ) -> io::Result<Self> {
            let session = rustls::ClientConnection::new(
                tls.config.clone(),
                tls.name.clone(),
            )
            .map_err(io::Error::other)?;
            Ok(Self { stream, session, plaintext: false })
        }

        /// Advance the handshake as far as the socket allows. `readable` and
        /// `writable` are what the owner knows of the socket and are cleared
        /// when it turns out to be empty or full. `Ok(true)` once the session
        /// carries application data.
        pub(in crate::http1) fn handshake_step(
            &mut self,
            readable: &mut bool,
            writable: &mut bool,
        ) -> io::Result<bool> {
            while self.session.is_handshaking() {
                let wants_write = self.session.wants_write();
                let wants_read = self.session.wants_read();
                if wants_write && *writable {
                    self.flush()?;
                    if self.session.wants_write() {
                        *writable = false;
                    }
                } else if wants_read && *readable {
                    match self.receive() {
                        Ok(()) => {}
                        Err(error) if error.kind() == ErrorKind::WouldBlock => {
                            *readable = false;
                        }
                        Err(error) => return Err(error),
                    }
                } else if wants_read || wants_write {
                    return Ok(false);
                } else {
                    break;
                }
            }
            self.flush()?;
            Ok(true)
        }

        pub(in crate::http1) fn socket(&mut self) -> &mut S {
            &mut self.stream
        }

        pub(in crate::http1) fn negotiated(&self) -> Option<crate::Negotiated> {
            Some(crate::Negotiated {
                version: self.session.protocol_version()?.as_str()?,
                cipher: self
                    .session
                    .negotiated_cipher_suite()?
                    .suite()
                    .as_str()?,
                group: self
                    .session
                    .negotiated_key_exchange_group()
                    .and_then(|group| group.name().as_str()),
            })
        }

        pub(in crate::http1) fn resumed(&self) -> bool {
            matches!(
                self.session.handshake_kind(),
                Some(rustls::HandshakeKind::Resumed)
            )
        }

        /// Move received TLS records into the session.
        fn receive(&mut self) -> io::Result<()> {
            if self.session.read_tls(&mut Socket(&self.stream))? == 0 {
                // The peer closed the socket without closing the session.
                return Err(ErrorKind::UnexpectedEof.into());
            }
            self.session.process_new_packets().map_err(io::Error::other)?;
            Ok(())
        }

        /// Send the records the session owes, as far as the socket takes them.
        fn flush(&mut self) -> io::Result<()> {
            while self.session.wants_write() {
                match self.session.write_tls(&mut Socket(&self.stream)) {
                    Ok(0) => return Err(ErrorKind::WriteZero.into()),
                    Ok(_) => {}
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        break;
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(())
        }

        pub(in crate::http1) fn try_read(
            &mut self,
            buffer: &mut [u8],
        ) -> io::Result<usize> {
            loop {
                match self.session.reader().read(buffer) {
                    Ok(size) => {
                        // A full buffer may have left more behind.
                        self.plaintext = size == buffer.len() && size > 0;
                        return Ok(size);
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        self.plaintext = false;
                    }
                    Err(error) => return Err(error),
                }
                self.receive()?;
            }
        }

        #[cfg(feature = "http2")]
        pub(in crate::http1) fn close_step(
            &mut self,
            writable: &mut bool,
        ) -> io::Result<bool> {
            self.session.send_close_notify();
            if *writable {
                self.flush()?;
            }
            if self.session.wants_write() {
                *writable = false;
                Ok(false)
            } else {
                Ok(true)
            }
        }

        pub(in crate::http1) fn try_write(
            &mut self,
            bytes: &[u8],
        ) -> io::Result<usize> {
            self.flush()?;
            if self.session.wants_write() {
                // The socket is full of records already owed: take no more.
                return Err(ErrorKind::WouldBlock.into());
            }
            let written = self.session.writer().write(bytes)?;
            self.flush()?;
            Ok(written)
        }
    }
}
