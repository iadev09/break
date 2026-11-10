#![cfg(unix)]

use std::process::Command;
use std::sync::Arc;

use brk::fixture::{Config, Process, Protocol};
use bytes::Bytes;
use http_body_util::BodyExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

fn start(protocol: Protocol) -> Process {
    Process::spawn(
        &mut Command::new(env!("CARGO_BIN_EXE_brk-fixture")),
        &Config {
            protocol,
            threads: 1.try_into().unwrap(),
            max_streams: 8.try_into().unwrap(),
        },
    )
    .unwrap()
}

async fn exchanges<I>(io: I)
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, connection) = h2::client::handshake(io).await.unwrap();
    let driver = tokio::spawn(connection);
    let mut responses = Vec::new();
    // Submit the full window before consuming any response.
    for _ in 0..8 {
        let request = http::Request::builder()
            .uri("https://localhost/plain")
            .body(())
            .unwrap();
        let (response, _) = sender.send_request(request, true).unwrap();
        responses.push(response);
    }
    for response in responses {
        let response = response.await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()[http::header::CONTENT_LENGTH], "2");
        let mut body = response.into_body();
        assert_eq!(body.data().await.unwrap().unwrap(), "ok");
        assert!(body.data().await.is_none());
    }
    drop(sender);
    // This task owns only the test client's connection. Closing its socket
    // precedes the fixture's separate, acknowledged server drain.
    driver.abort();
    let _ = driver.await;
}

#[test]
fn native_profiles_serve_the_same_workload_and_stop() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for protocol in [Protocol::H1, Protocol::H2c, Protocol::H2] {
        let mut child = start(protocol);
        let before = child.sample().unwrap();
        runtime.block_on(async {
            let stream = tokio::net::TcpStream::connect(child.ready().address).await.unwrap();
            match protocol {
                Protocol::H1 => {
                    let mut stream = stream;
                    stream.write_all(b"GET /plain HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").await.unwrap();
                    let mut response = String::new();
                    stream.read_to_string(&mut response).await.unwrap();
                    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
                    assert!(response.ends_with("\r\n\r\nok"));
                }
                Protocol::H2c => exchanges(stream).await,
                Protocol::H2 => {
                    use rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};
                    let certificate = CertificateDer::from_pem_slice(child.ready().trust_pem.as_ref().unwrap().as_bytes()).unwrap();
                    let mut roots = rustls::RootCertStore::empty();
                    roots.add(certificate).unwrap();
                    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
                        .with_safe_default_protocol_versions().unwrap().with_root_certificates(roots).with_no_client_auth();
                    tls.alpn_protocols = vec![b"h2".to_vec()];
                    let tls = tokio_rustls::TlsConnector::from(Arc::new(tls))
                        .connect(ServerName::try_from("localhost").unwrap(), stream).await.unwrap();
                    assert_eq!(tls.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));
                    exchanges(tls).await;
                }
            }
        });
        child.sample().unwrap().since(before).unwrap();
        child.stop().unwrap();
        // Acknowledgement means listener teardown and process exit, not just
        // receipt of the stop command.
        assert!(std::net::TcpStream::connect(child.ready().address).is_err());
    }
}

#[test]
fn shutdown_interrupts_an_unfinished_tls_handshake() {
    let mut child = start(Protocol::H2);
    let _unstarted_handshake =
        std::net::TcpStream::connect(child.ready().address).unwrap();
    child.stop().unwrap();
}

#[test]
fn workload_does_not_accept_the_wrong_path_or_method() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for (method, path, status) in
            [("GET", "/other", 404), ("POST", "/plain", 405)]
        {
            let response = brk::fixture::native::handle(
                http::Request::builder()
                    .method(method)
                    .uri(path)
                    .body(http_body_util::Full::new(Bytes::from_static(
                        b"input",
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(response.status(), status);
            assert!(
                response
                    .into_body()
                    .collect()
                    .await
                    .unwrap()
                    .to_bytes()
                    .is_empty()
            );
        }
    });
}
