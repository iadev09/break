//! Native Hyper reference responder. Both H2 transports use the same handler,
//! runtime and HTTP/2 builder; TLS adds only an in-memory, verified identity.
use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::TlsAcceptor;

use super::{Config, Protocol, Result, Server};
use crate::CancellationToken;

/// Fixed workload shared with external fixtures: consume the request, GET
/// `/plain` → 200, Content-Length 2, `ok`. Other paths cannot pass silently.
pub async fn handle<B>(request: Request<B>) -> Result<Response<Full<Bytes>>>
where
    B: http_body::Body<Data = Bytes>,
    B::Error: Into<super::Error>,
{
    let status = if request.method() != Method::GET {
        StatusCode::METHOD_NOT_ALLOWED
    } else if request.uri().path() != "/plain" {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::OK
    };
    request.into_body().collect().await.map_err(Into::into)?;
    let body = if status == StatusCode::OK {
        Bytes::from_static(b"ok")
    } else {
        Bytes::new()
    };
    Ok(Response::builder()
        .status(status)
        .header(http::header::CONTENT_LENGTH, body.len())
        .body(Full::new(body))?)
}

pub fn start(config: Config) -> Result<Server> {
    let (acceptor, trust_pem) = if config.protocol == Protocol::H2 {
        let identity = rcgen::generate_simple_self_signed(vec![
            "localhost".into(),
            "127.0.0.1".into(),
        ])?;
        let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(
            vec![identity.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(
                identity.signing_key.serialize_der(),
            )
            .into(),
        )?;
        tls.alpn_protocols = vec![b"h2".to_vec()];
        (Some(TlsAcceptor::from(Arc::new(tls))), Some(identity.cert.pem()))
    } else {
        (None, None)
    };
    let protocol = config.protocol;
    let max_streams = config.max_streams.get();
    Server::start(
        config,
        "native-hyper/shared",
        trust_pem,
        move |stream, _, stop| {
            let acceptor = acceptor.clone();
            async move {
                if let Some(acceptor) = acceptor {
                    let tls = tokio::select! {
                        () = stop.cancelled() => return,
                        accepted = acceptor.accept(stream) => match accepted { Ok(tls) => tls, Err(_) => return },
                    };
                    connection(tls, protocol, max_streams, stop).await;
                } else {
                    connection(stream, protocol, max_streams, stop).await;
                }
            }
        },
    )
}

async fn connection<I>(
    io: I,
    protocol: Protocol,
    max_streams: u32,
    stop: CancellationToken,
) where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // Errors draining incoming data close that connection. Successful replies
    // retain the concrete Full body; no boxed body or handler future is added.
    let service = service_fn(|request: Request<Incoming>| async move {
        handle(request)
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))
    });
    match protocol {
        Protocol::H1 => {
            let connection = hyper::server::conn::http1::Builder::new()
                .keep_alive(true)
                .header_read_timeout(None)
                .writev(true)
                .serve_connection(TokioIo::new(io), service);
            tokio::pin!(connection);
            tokio::select! {
                _ = &mut connection => {},
                () = stop.cancelled() => { connection.as_mut().graceful_shutdown(); let _ = connection.await; }
            }
        }
        Protocol::H2c | Protocol::H2 => {
            let connection =
                hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .max_concurrent_streams(max_streams)
                    .serve_connection(TokioIo::new(io), service);
            tokio::pin!(connection);
            tokio::select! {
                _ = &mut connection => {},
                () = stop.cancelled() => { connection.as_mut().graceful_shutdown(); let _ = connection.await; }
            }
        }
    }
}
