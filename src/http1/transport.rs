//! What carries one connection's bytes: a TCP stream as it is, or the same
//! stream under TLS. Both are driven by socket readiness and non-blocking
//! reads and writes, so the connection loop is the same for either.

use std::io;

use tokio::io::{Interest, Ready};
use tokio::net::TcpStream;

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

pub(crate) enum Transport {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<secure::Secure>),
}

impl Transport {
    /// Take an established TCP stream and, for an `https://` target, complete
    /// the TLS handshake on it. The caller's deadline covers both. `sessions`
    /// replaces the target's shared TLS sessions with one owner's own.
    pub(crate) async fn open(
        stream: TcpStream,
        client: &Client,
        sessions: Option<&Sessions>,
    ) -> io::Result<Self> {
        #[cfg(feature = "tls")]
        if let Some(tls) = sessions.or(client.tls.as_ref()) {
            return Ok(Self::Tls(Box::new(
                secure::Secure::handshake(stream, tls).await?,
            )));
        }
        let _ = (client, sessions);
        Ok(Self::Plain(stream))
    }

    /// Whether this connection's TLS handshake resumed an earlier session.
    pub(crate) fn resumed(&self) -> bool {
        match self {
            Self::Plain(_) => false,
            #[cfg(feature = "tls")]
            Self::Tls(secure) => secure.resumed(),
        }
    }

    pub(crate) async fn ready(
        &mut self,
        interest: Interest,
    ) -> io::Result<Ready> {
        match self {
            Self::Plain(stream) => stream.ready(interest).await,
            #[cfg(feature = "tls")]
            Self::Tls(secure) => secure.ready(interest).await,
        }
    }

    pub(crate) fn try_read(
        &mut self,
        buffer: &mut [u8],
    ) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.try_read(buffer),
            #[cfg(feature = "tls")]
            Self::Tls(secure) => secure.try_read(buffer),
        }
    }

    pub(crate) fn try_write(
        &mut self,
        bytes: &[u8],
    ) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.try_write(bytes),
            #[cfg(feature = "tls")]
            Self::Tls(secure) => secure.try_write(bytes),
        }
    }
}

#[cfg(feature = "tls")]
pub(crate) mod secure {
    use std::io::{self, ErrorKind, Read, Write};
    use std::sync::Arc;

    use rustls::pki_types::ServerName;
    use tokio::io::{Interest, Ready};
    use tokio::net::TcpStream;

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

    /// The socket as the TLS session reads and writes it: without blocking.
    struct Socket<'a>(&'a TcpStream);

    impl Read for Socket<'_> {
        fn read(
            &mut self,
            buffer: &mut [u8],
        ) -> io::Result<usize> {
            self.0.try_read(buffer)
        }
    }

    impl Write for Socket<'_> {
        fn write(
            &mut self,
            bytes: &[u8],
        ) -> io::Result<usize> {
            self.0.try_write(bytes)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    pub(crate) struct Secure {
        stream: TcpStream,
        session: rustls::ClientConnection,
        /// Decrypted bytes may still wait inside the session. The socket's
        /// readiness says nothing about them.
        plaintext: bool,
    }

    impl Secure {
        pub(super) async fn handshake(
            stream: TcpStream,
            tls: &Tls,
        ) -> io::Result<Self> {
            let session = rustls::ClientConnection::new(
                tls.config.clone(),
                tls.name.clone(),
            )
            .map_err(io::Error::other)?;
            let mut secure = Self { stream, session, plaintext: false };
            while secure.session.is_handshaking() {
                let interest = match (
                    secure.session.wants_read(),
                    secure.session.wants_write(),
                ) {
                    (true, true) => Interest::READABLE | Interest::WRITABLE,
                    (true, false) => Interest::READABLE,
                    (false, true) => Interest::WRITABLE,
                    (false, false) => break,
                };
                let ready = secure.stream.ready(interest).await?;
                if ready.is_writable() {
                    secure.flush()?;
                }
                if ready.is_readable() && secure.session.wants_read() {
                    match secure.receive() {
                        Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        }
                        other => other?,
                    }
                }
            }
            secure.flush()?;
            Ok(secure)
        }

        pub(super) fn resumed(&self) -> bool {
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

        pub(super) async fn ready(
            &mut self,
            interest: Interest,
        ) -> io::Result<Ready> {
            loop {
                if self.plaintext && interest.is_readable() {
                    return Ok(Ready::READABLE);
                }
                let owed = self.session.wants_write();
                let ready = self
                    .stream
                    .ready(if owed {
                        interest | Interest::WRITABLE
                    } else {
                        interest
                    })
                    .await?;
                if owed && ready.is_writable() {
                    self.flush()?;
                }
                // Writability first pays for records already owed; only what
                // is left of it is the caller's.
                let mut granted = Ready::EMPTY;
                if interest.is_readable() && ready.is_readable() {
                    granted |= Ready::READABLE;
                }
                if interest.is_writable()
                    && ready.is_writable()
                    && !self.session.wants_write()
                {
                    granted |= Ready::WRITABLE;
                }
                if !granted.is_empty() {
                    return Ok(granted);
                }
            }
        }

        pub(super) fn try_read(
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

        pub(super) fn try_write(
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
