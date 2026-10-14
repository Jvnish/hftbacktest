use std::{
    io,
    io::ErrorKind,
    time::{Duration, Instant},
};

use anyhow::Error;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    net::TcpStream,
    select,
    sync::mpsc::{UnboundedSender, unbounded_channel},
    time::interval,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls, connect_async,
    tungstenite::{Bytes, Message, Utf8Bytes, client::IntoClientRequest},
};
use tracing::{error, info, warn};

use crate::proxy;

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

pub async fn fetch_depth_snapshot(symbol: &str) -> Result<String, reqwest::Error> {
    reqwest::Client::new()
        .get(format!(
            "https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000"
        ))
        .header("Accept", "application/json")
        .send()
        .await?
        .text()
        .await
}

async fn open(url: &str) -> Result<WsStream, Error> {
    let request = url.into_client_request()?;
    let host = request
        .uri()
        .host()
        .ok_or_else(|| anyhow::anyhow!("no host in {url}"))?
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

pub async fn connect(
    url: &str,
    ws_tx: UnboundedSender<(DateTime<Utc>, Utf8Bytes)>,
) -> Result<(), anyhow::Error> {
    let ws_stream = open(url).await?;
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
    let mut checker = interval(Duration::from_secs(10));

    loop {
        select! {
            msg = read.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let recv_time = Utc::now();
                    if ws_tx.send((recv_time, text)).is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Binary(_))) => {}
                Some(Ok(Message::Ping(data))) => {
                    if tx.send(data).is_err() {
                        return Err(Error::from(io::Error::new(
                            ErrorKind::ConnectionAborted,
                            "closed",
                        )));
                    }
                    last_ping = Instant::now();
                }
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Close(close_frame))) => {
                    warn!(?close_frame, "closed");
                    return Err(Error::from(io::Error::new(
                        ErrorKind::ConnectionAborted,
                        "closed",
                    )));
                }
                Some(Ok(Message::Frame(_))) => {}
                Some(Err(e)) => {
                    return Err(Error::from(e));
                }
                None => {
                    break;
                }
            },
            _ = checker.tick() => {
                if last_ping.elapsed() > Duration::from_secs(300) {
                    warn!("Ping timeout.");
                    return Err(Error::from(io::Error::new(
                        ErrorKind::TimedOut,
                        "Ping",
                    )));
                }
            }
        }
    }
    Ok(())
}

pub async fn keep_connection(
    streams: Vec<String>,
    symbol_list: Vec<String>,
    ws_tx: UnboundedSender<(DateTime<Utc>, Utf8Bytes)>,
) {
    match proxy::proxy_for("fstream.binance.com") {
        Ok(Some(proxy)) => info!(
            proxy = %format!("{}:{}", proxy.host, proxy.port),
            "connecting through an HTTP proxy"
        ),
        Ok(None) => info!("connecting directly"),
        Err(error) => error!(?error, "invalid proxy setting"),
    }
    let mut error_count = 0;
    loop {
        let connect_time = Instant::now();
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
        if let Err(error) = connect(
            &format!("wss://fstream.binance.com/stream?streams={streams_str}"),
            ws_tx.clone(),
        )
        .await
        {
            error!(?error, "websocket error");
            error_count += 1;
            if connect_time.elapsed() > Duration::from_secs(30) {
                error_count = 0;
            }
            if error_count > 3 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            } else if error_count > 10 {
                tokio::time::sleep(Duration::from_secs(5)).await;
            } else if error_count > 20 {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        } else {
            break;
        }
    }
}
