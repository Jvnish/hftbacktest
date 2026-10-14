//! HTTP CONNECT tunnelling for the websocket connections.
//!
//! `reqwest` reads the proxy environment variables, but `tokio-tungstenite` does not. On a host
//! that reaches the exchange only through an HTTP proxy, the REST snapshots worked while the
//! websocket never connected. The proxy is resolved here the same way: `HTTPS_PROXY`,
//! `https_proxy`, `ALL_PROXY`, `all_proxy`, skipped for hosts listed in `NO_PROXY`/`no_proxy`.

use std::{env, io};

use reqwest::Url;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const PROXY_VARS: [&str; 4] = ["HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"];
const MAX_RESPONSE_HEAD: usize = 8192;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proxy {
    pub host: String,
    pub port: u16,
}

/// Returns the proxy to use for `host`, or `None` to connect directly.
pub fn proxy_for(host: &str) -> io::Result<Option<Proxy>> {
    resolve(host, |key| env::var(key).ok())
}

fn resolve(host: &str, var: impl Fn(&str) -> Option<String>) -> io::Result<Option<Proxy>> {
    let no_proxy = var("NO_PROXY")
        .or_else(|| var("no_proxy"))
        .unwrap_or_default();
    if bypass(host, &no_proxy) {
        return Ok(None);
    }
    match PROXY_VARS
        .iter()
        .find_map(|key| var(key).filter(|value| !value.is_empty()))
    {
        Some(url) => parse(&url).map(Some),
        None => Ok(None),
    }
}

fn bypass(host: &str, no_proxy: &str) -> bool {
    no_proxy
        .split(',')
        .map(|entry| entry.trim().trim_start_matches('.'))
        .filter(|entry| !entry.is_empty())
        .any(|entry| entry == "*" || host == entry || host.ends_with(&format!(".{entry}")))
}

fn parse(value: &str) -> io::Result<Proxy> {
    let url = if value.contains("://") {
        value.to_string()
    } else {
        format!("http://{value}")
    };
    let url = Url::parse(&url).map_err(|error| invalid(format!("proxy {value}: {error}")))?;
    if url.scheme() != "http" {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("proxy {value}: only http:// CONNECT proxies are supported"),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("proxy {value}: proxy credentials are not supported"),
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| invalid(format!("proxy {value}: no host")))?
        .trim_matches(|c| c == '[' || c == ']')
        .to_string();
    let port = url.port_or_known_default().unwrap_or(80);
    Ok(Proxy { host, port })
}

/// Opens a TCP connection through the proxy to `host:port`, ready for the TLS handshake.
pub async fn tunnel(proxy: &Proxy, host: &str, port: u16) -> io::Result<TcpStream> {
    let mut stream = TcpStream::connect((proxy.host.as_str(), proxy.port)).await?;
    stream
        .write_all(
            format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n").as_bytes(),
        )
        .await?;
    // Reads the response head one byte at a time. The proxy sends nothing after it until the TLS
    // handshake starts, and a buffered read must not swallow tunnelled bytes.
    let mut head = Vec::with_capacity(128);
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if head.len() >= MAX_RESPONSE_HEAD {
            return Err(invalid("proxy response head is too long".to_string()));
        }
        if stream.read(&mut byte).await? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "proxy closed the connection during CONNECT",
            ));
        }
        head.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&head);
    let status_line = head.lines().next().unwrap_or_default();
    if status_line.split_whitespace().nth(1) != Some("200") {
        return Err(io::Error::other(format!(
            "proxy CONNECT {host}:{port} failed: {status_line}"
        )));
    }
    Ok(stream)
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokio::net::TcpListener;

    use super::*;

    fn resolve_with(host: &str, vars: &[(&str, &str)]) -> io::Result<Option<Proxy>> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        resolve(host, |key| vars.get(key).cloned())
    }

    fn proxy(host: &str, port: u16) -> Option<Proxy> {
        Some(Proxy {
            host: host.to_string(),
            port,
        })
    }

    #[test]
    fn resolves_in_precedence_order() {
        let host = "fstream.binance.com";
        assert_eq!(resolve_with(host, &[]).unwrap(), None);
        assert_eq!(
            resolve_with(host, &[("https_proxy", "http://127.0.0.1:7890")]).unwrap(),
            proxy("127.0.0.1", 7890)
        );
        assert_eq!(
            resolve_with(
                host,
                &[
                    ("HTTPS_PROXY", "http://10.0.0.1:3128"),
                    ("ALL_PROXY", "http://127.0.0.1:7890")
                ]
            )
            .unwrap(),
            proxy("10.0.0.1", 3128)
        );
        assert_eq!(
            resolve_with(
                host,
                &[("HTTPS_PROXY", ""), ("ALL_PROXY", "127.0.0.1:7890")]
            )
            .unwrap(),
            proxy("127.0.0.1", 7890)
        );
        assert_eq!(
            resolve_with(host, &[("https_proxy", "http://[::1]:7890/")]).unwrap(),
            proxy("::1", 7890)
        );
        assert_eq!(
            resolve_with(host, &[("https_proxy", "http://proxy.local")]).unwrap(),
            proxy("proxy.local", 80)
        );
    }

    #[test]
    fn honours_no_proxy() {
        let vars = [
            ("https_proxy", "http://127.0.0.1:7890"),
            ("no_proxy", "127.0.0.1, .binance.com"),
        ];
        assert_eq!(resolve_with("fstream.binance.com", &vars).unwrap(), None);
        assert_eq!(resolve_with("binance.com", &vars).unwrap(), None);
        assert_eq!(
            resolve_with("notbinance.com", &vars).unwrap(),
            proxy("127.0.0.1", 7890)
        );
        let all = [("https_proxy", "http://127.0.0.1:7890"), ("NO_PROXY", "*")];
        assert_eq!(resolve_with("fstream.binance.com", &all).unwrap(), None);
    }

    #[test]
    fn rejects_what_it_cannot_tunnel() {
        let host = "fstream.binance.com";
        for value in ["socks5://127.0.0.1:1080", "http://user:pass@127.0.0.1:7890"] {
            let error = resolve_with(host, &[("https_proxy", value)]).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::Unsupported, "{value}");
        }
    }

    async fn serve_once(response: &'static [u8]) -> (Proxy, tokio::task::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 256];
            while !request.ends_with(b"\r\n\r\n") {
                let n = socket.read(&mut buf).await.unwrap();
                request.extend_from_slice(&buf[..n]);
            }
            socket.write_all(response).await.unwrap();
            // Bytes after the head belong to the tunnel and must stay unread by `tunnel`.
            socket.write_all(b"TUNNEL").await.unwrap();
            request
        });
        (proxy("127.0.0.1", port).unwrap(), handle)
    }

    #[tokio::test]
    async fn tunnels_on_200() {
        let (proxy, server) = serve_once(b"HTTP/1.1 200 Connection established\r\n\r\n").await;
        let mut stream = tunnel(&proxy, "fstream.binance.com", 443).await.unwrap();
        let request = server.await.unwrap();
        assert_eq!(
            request,
            b"CONNECT fstream.binance.com:443 HTTP/1.1\r\nHost: fstream.binance.com:443\r\n\r\n"
        );
        let mut rest = [0u8; 6];
        stream.read_exact(&mut rest).await.unwrap();
        assert_eq!(&rest, b"TUNNEL");
    }

    #[tokio::test]
    async fn fails_on_other_status() {
        let (proxy, _server) =
            serve_once(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n").await;
        let error = tunnel(&proxy, "fstream.binance.com", 443)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("502"), "{error}");
    }
}
