use std::time::{Duration, Instant};

use anyhow::{Error, anyhow};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use reqwest::{StatusCode, header::RETRY_AFTER};
use tokio::{
    net::TcpStream,
    select,
    sync::mpsc::{UnboundedSender, unbounded_channel},
    time::{interval, sleep, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls, connect_async,
    tungstenite::{Bytes, Message, Utf8Bytes, client::IntoClientRequest},
};
use tracing::{error, info, warn};

use crate::proxy;

/// Binance has routed USDⓈ-M market streams by category since 2026-04-23. Depth, bookTicker and
/// trade are public streams. markPrice and aggTrade are market streams and push nothing on the
/// public or on the legacy unrouted URL.
pub const PUBLIC_URL: &str = "wss://fstream.binance.com/public/stream?streams=";
pub const MARKET_URL: &str = "wss://fstream.binance.com/market/stream?streams=";

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const PING_TIMEOUT: Duration = Duration::from_secs(300);
/// After a connection that stayed up this long, the next reconnect is immediate.
const STABLE_CONNECTION: Duration = Duration::from_secs(60);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub async fn fetch_symbol_list() -> Result<Vec<String>, reqwest::Error> {
    Ok(reqwest::Client::new()
        .get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        .header("Accept", "application/json")
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?
        .get("symbols")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .filter(|j_symbol| j_symbol.get("contractType").unwrap().as_str().unwrap() == "PERPETUAL")
        .map(|j_symbol| {
            j_symbol
                .get("symbol")
                .unwrap()
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect())
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error(transparent)]
    Request(#[from] reqwest::Error),
    #[error("HTTP {status}: {body}")]
    Status {
        status: StatusCode,
        retry_after: Option<Duration>,
        body: String,
    },
    #[error("unexpected snapshot body: {0}")]
    Body(String),
}

impl SnapshotError {
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Status { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

/// Fetches a 1000-level depth snapshot. Only a successful response that looks like a snapshot is
/// returned; an error body written to the file would be read by the converter as a snapshot.
pub async fn fetch_depth_snapshot(
    client: &reqwest::Client,
    symbol: &str,
) -> Result<String, SnapshotError> {
    let response = client
        .get(format!(
            "https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000"
        ))
        .header("Accept", "application/json")
        .send()
        .await?;
    let status = response.status();
    let retry_after = response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs);
    let body = response.text().await?;
    if !status.is_success() {
        return Err(SnapshotError::Status {
            status,
            retry_after,
            body: truncate(&body),
        });
    }
    validate_snapshot(body)
}

/// Checks that the body has the fields the converter reads and keeps it on a single line.
fn validate_snapshot(body: String) -> Result<String, SnapshotError> {
    let value: serde_json::Value =
        serde_json::from_str(&body).map_err(|_| SnapshotError::Body(truncate(&body)))?;
    let valid = value.get("lastUpdateId").is_some_and(|v| v.is_u64())
        && value.get("T").is_some_and(|v| v.is_u64())
        && value.get("bids").is_some_and(|v| v.is_array())
        && value.get("asks").is_some_and(|v| v.is_array())
        && value.get("data").is_none();
    if !valid {
        return Err(SnapshotError::Body(truncate(&body)));
    }
    if body.contains('\n') {
        Ok(value.to_string())
    } else {
        Ok(body)
    }
}

fn truncate(body: &str) -> String {
    body.chars().take(200).collect()
}

/// A line written to every symbol's file when a connection opens or closes, so that a reader can
/// tell a quiet market from a disconnection. The converter skips it as an unknown event type.
fn marker(symbol: &str, conn: &str, event: &str, detail: &str) -> Utf8Bytes {
    serde_json::json!({
        "stream": format!("{}@collector", symbol.to_lowercase()),
        "data": {
            "e": "collectorEvent",
            "E": Utc::now().timestamp_millis(),
            "s": symbol.to_uppercase(),
            "conn": conn,
            "event": event,
            "detail": detail,
        }
    })
    .to_string()
    .into()
}

async fn open(url: &str) -> Result<WsStream, Error> {
    let request = url.into_client_request()?;
    let host = request
        .uri()
        .host()
        .ok_or_else(|| anyhow!("no host in {url}"))?
        .to_string();
    let port = request.uri().port_u16().unwrap_or(443);
    let (ws_stream, _) = match proxy::proxy_for(&host)? {
        Some(proxy) => {
            let stream = proxy::tunnel(&proxy, &host, port).await?;
            client_async_tls(request, stream).await?
        }
        None => connect_async(request).await?,
    };
    Ok(ws_stream)
}

/// Runs one connection until it fails. Returns `Ok` only when the receiver is gone.
pub async fn connect(
    url: &str,
    name: &str,
    symbols: &[String],
    ws_tx: &UnboundedSender<(DateTime<Utc>, Utf8Bytes)>,
    idle_timeout: Duration,
) -> Result<(), Error> {
    let ws_stream = timeout(CONNECT_TIMEOUT, open(url))
        .await
        .map_err(|_| anyhow!("connect timed out after {CONNECT_TIMEOUT:?}"))??;
    info!(conn = name, "connected");
    for symbol in symbols {
        if ws_tx
            .send((Utc::now(), marker(symbol, name, "connected", "")))
            .is_err()
        {
            return Ok(());
        }
    }
    let result = read_frames(ws_stream, ws_tx, idle_timeout).await;
    if let Err(error) = &result {
        for symbol in symbols {
            let _ = ws_tx.send((
                Utc::now(),
                marker(symbol, name, "disconnected", &error.to_string()),
            ));
        }
    }
    result
}

async fn read_frames(
    ws_stream: WsStream,
    ws_tx: &UnboundedSender<(DateTime<Utc>, Utf8Bytes)>,
    idle_timeout: Duration,
) -> Result<(), Error> {
    let (mut write, mut read) = ws_stream.split();
    let (tx, mut rx) = unbounded_channel::<Bytes>();

    tokio::spawn(async move {
        while let Some(data) = rx.recv().await {
            if write.send(Message::Pong(data)).await.is_err() {
                let _ = write.close().await;
                return;
            }
        }
    });

    let mut last_ping = Instant::now();
    let mut last_data = Instant::now();
    let mut checker = interval(Duration::from_secs(1));

    loop {
        select! {
            msg = read.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let recv_time = Utc::now();
                    last_data = Instant::now();
                    if ws_tx.send((recv_time, text)).is_err() {
                        return Ok(());
                    }
                }
                Some(Ok(Message::Binary(_))) => {}
                Some(Ok(Message::Ping(data))) => {
                    if tx.send(data).is_err() {
                        return Err(anyhow!("the pong writer has stopped"));
                    }
                    last_ping = Instant::now();
                }
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Close(close_frame))) => {
                    warn!(?close_frame, "closed");
                    return Err(anyhow!("closed by the server: {close_frame:?}"));
                }
                Some(Ok(Message::Frame(_))) => {}
                Some(Err(e)) => {
                    return Err(Error::from(e));
                }
                None => {
                    return Err(anyhow!("the stream ended"));
                }
            },
            _ = checker.tick() => {
                // The server's ping only proves that the socket is alive. A half-open connection
                // or a subscription that stopped pushing is caught by the data timeout.
                if last_data.elapsed() > idle_timeout {
                    return Err(anyhow!("no data for {idle_timeout:?}"));
                }
                if last_ping.elapsed() > PING_TIMEOUT {
                    return Err(anyhow!("no ping for {PING_TIMEOUT:?}"));
                }
            }
        }
    }
}

/// No delay after a connection that stayed up, then 1, 2, 4, ... seconds up to a minute, which
/// keeps an outage far below Binance's limit of 300 connection attempts per 5 minutes per IP.
fn backoff(failures: u32) -> Duration {
    if failures == 0 {
        Duration::ZERO
    } else {
        Duration::from_secs(1u64 << (failures - 1).min(6)).min(MAX_BACKOFF)
    }
}

pub async fn keep_connection(
    name: &'static str,
    base_url: &'static str,
    streams: Vec<String>,
    symbol_list: Vec<String>,
    ws_tx: UnboundedSender<(DateTime<Utc>, Utf8Bytes)>,
    idle_timeout: Duration,
) {
    let streams_str = symbol_list
        .iter()
        .flat_map(|pair| {
            streams
                .iter()
                .cloned()
                .map(|stream| {
                    stream
                        .replace("$symbol", pair.to_lowercase().as_str())
                        .to_string()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
        .join("/");
    let url = format!("{base_url}{streams_str}");
    if let Ok(request) = url.as_str().into_client_request()
        && let Some(host) = request.uri().host()
    {
        match proxy::proxy_for(host) {
            Ok(Some(proxy)) => info!(
                conn = name,
                proxy = %format!("{}:{}", proxy.host, proxy.port),
                "connecting through an HTTP proxy"
            ),
            Ok(None) => info!(conn = name, "connecting directly"),
            Err(error) => error!(conn = name, ?error, "invalid proxy setting"),
        }
    }

    let mut failures = 0;
    loop {
        let connect_time = Instant::now();
        match connect(&url, name, &symbol_list, &ws_tx, idle_timeout).await {
            Ok(()) => break,
            Err(error) => {
                error!(conn = name, ?error, "websocket error");
                if connect_time.elapsed() >= STABLE_CONNECTION {
                    failures = 0;
                } else {
                    failures += 1;
                }
                let delay = backoff(failures);
                if !delay.is_zero() {
                    warn!(conn = name, failures, ?delay, "reconnecting after a delay");
                    sleep(delay).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_to_a_minute() {
        let delays: Vec<u64> = (0..10).map(|n| backoff(n).as_secs()).collect();
        assert_eq!(delays, [0, 1, 2, 4, 8, 16, 32, 60, 60, 60]);
    }

    #[test]
    fn accepts_only_snapshots() {
        let snapshot = r#"{"lastUpdateId":11817405432965,"E":1791998056331,"T":1791998056330,"bids":[["4087.43","31.226"]],"asks":[["4087.44","65.037"]]}"#;
        assert_eq!(validate_snapshot(snapshot.to_string()).unwrap(), snapshot);
        let multiline = snapshot.replace(",\"asks\"", ",\n\"asks\"");
        assert!(!validate_snapshot(multiline).unwrap().contains('\n'));
        for body in [
            r#"{"code":-1003,"msg":"Too many requests"}"#,
            r#"{"lastUpdateId":1,"T":1,"bids":[]}"#,
            "<html>502 Bad Gateway</html>",
        ] {
            assert!(
                matches!(
                    validate_snapshot(body.to_string()),
                    Err(SnapshotError::Body(_))
                ),
                "{body}"
            );
        }
    }

    #[test]
    fn marker_is_routed_by_symbol() {
        let line = marker("xauusdt", "public", "disconnected", "no data for 30s");
        let value: serde_json::Value = serde_json::from_str(line.as_str()).unwrap();
        assert_eq!(value["stream"], "xauusdt@collector");
        assert_eq!(value["data"]["e"], "collectorEvent");
        assert_eq!(value["data"]["s"], "XAUUSDT");
        assert_eq!(value["data"]["event"], "disconnected");
        assert!(value["data"]["E"].is_i64());
    }
}
