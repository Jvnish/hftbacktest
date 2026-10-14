mod http;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::anyhow;
use chrono::{DateTime, Utc};
pub use http::{fetch_depth_snapshot, keep_connection};
use tokio::{
    sync::{
        Mutex,
        mpsc::{UnboundedSender, unbounded_channel},
    },
    time::sleep,
};
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::{error, info, warn};

use crate::{error::ConnectorError, throttler::Throttler};

const SNAPSHOT_ATTEMPTS: u32 = 6;
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(10);
/// Scheduled snapshots are requested this long after the period boundary, so that they are
/// received after it and land in the segment that starts there.
const SNAPSHOT_OFFSET: Duration = Duration::from_secs(1);

pub struct Config {
    /// Seconds between scheduled snapshots, aligned to the Unix epoch; 0 disables them.
    pub snapshot_secs: u64,
    /// Reconnects when a connection delivers no data frame for this long.
    pub idle_timeout: Duration,
}

/// Fetches depth snapshots in the background and writes each one as its own line.
#[derive(Clone)]
struct Snapshots {
    client: reqwest::Client,
    throttler: Throttler,
    writer_tx: UnboundedSender<(DateTime<Utc>, String, String)>,
    in_flight: Arc<Mutex<HashSet<String>>>,
}

impl Snapshots {
    fn new(
        writer_tx: UnboundedSender<(DateTime<Utc>, String, String)>,
    ) -> Result<Self, reqwest::Error> {
        // https://www.binance.com/en/support/faq/rate-limits-on-binance-futures-281596e222414cdd9051664ea621cdc3
        // The default rate limit per IP is 2,400/min and the weight is 20 at a depth of 1000.
        // The maximum request rate for fetching snapshots is 120 per minute.
        // Sets the rate limit with a margin to account for connection requests.
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(SNAPSHOT_TIMEOUT)
                .build()?,
            throttler: Throttler::new(100),
            writer_tx,
            in_flight: Default::default(),
        })
    }

    /// Requests a snapshot unless one is already being fetched for the symbol. A snapshot that is
    /// in flight when a new gap appears still repairs that gap, because it is taken after it.
    fn request(&self, symbol: String, reason: &'static str) {
        let this = self.clone();
        tokio::spawn(async move {
            if !this.in_flight.lock().await.insert(symbol.clone()) {
                return;
            }
            let result = this.fetch(&symbol, reason).await;
            this.in_flight.lock().await.remove(&symbol);
            match result {
                Ok(data) => {
                    let recv_time = Utc::now();
                    let _ = this.writer_tx.send((recv_time, symbol, data));
                }
                Err(error) => {
                    error!(%symbol, reason, ?error, "couldn't fetch the depth snapshot.");
                }
            }
        });
    }

    async fn fetch(&self, symbol: &str, reason: &str) -> Result<String, anyhow::Error> {
        let mut delay = Duration::from_secs(1);
        for attempt in 1..=SNAPSHOT_ATTEMPTS {
            // Waits for a slot instead of dropping the request when the local limit is reached.
            let result = loop {
                let mut throttler = self.throttler.clone();
                if let Some(result) = throttler
                    .execute(fetch_depth_snapshot(&self.client, symbol))
                    .await
                {
                    break result;
                }
                sleep(Duration::from_secs(1)).await;
            };
            match result {
                Ok(data) => {
                    if attempt > 1 {
                        info!(%symbol, reason, attempt, "depth snapshot fetched after a retry.");
                    }
                    return Ok(data);
                }
                Err(error) => {
                    let wait = error.retry_after().unwrap_or(delay);
                    warn!(%symbol, reason, attempt, ?error, ?wait, "depth snapshot failed.");
                    if attempt < SNAPSHOT_ATTEMPTS {
                        sleep(wait).await;
                        delay = (delay * 2).min(Duration::from_secs(60));
                    }
                }
            }
        }
        Err(anyhow!("gave up after {SNAPSHOT_ATTEMPTS} attempts"))
    }
}

/// Requests a snapshot of every symbol shortly after each multiple of `every_secs` since the Unix
/// epoch. With the same period as the output segments, each segment can be replayed on its own
/// from about a second after it starts, instead of depending on every earlier segment.
async fn scheduled_snapshots(snapshots: Snapshots, symbols: Vec<String>, every_secs: u64) {
    let period = every_secs as i64 * 1_000_000_000;
    loop {
        let now = Utc::now().timestamp_nanos_opt().unwrap();
        let next = (now.div_euclid(period) + 1) * period + SNAPSHOT_OFFSET.as_nanos() as i64;
        sleep(Duration::from_nanos((next - now) as u64)).await;
        for symbol in &symbols {
            snapshots.request(symbol.to_uppercase(), "scheduled");
        }
    }
}

fn handle(
    prev_u_map: &mut HashMap<String, i64>,
    snapshots: &Snapshots,
    writer_tx: &UnboundedSender<(DateTime<Utc>, String, String)>,
    recv_time: DateTime<Utc>,
    data: Utf8Bytes,
) -> Result<(), ConnectorError> {
    let j: serde_json::Value = serde_json::from_str(data.as_str())?;
    if let Some(j_data) = j.get("data")
        && let Some(j_symbol) = j_data
            .as_object()
            .ok_or(ConnectorError::FormatError)?
            .get("s")
    {
        let symbol = j_symbol.as_str().ok_or(ConnectorError::FormatError)?;
        let ev = j_data
            .get("e")
            .ok_or(ConnectorError::FormatError)?
            .as_str()
            .ok_or(ConnectorError::FormatError)?;
        if ev == "depthUpdate" {
            let u = j_data
                .get("u")
                .ok_or(ConnectorError::FormatError)?
                .as_i64()
                .ok_or(ConnectorError::FormatError)?;
            let pu = j_data
                .get("pu")
                .ok_or(ConnectorError::FormatError)?
                .as_i64()
                .ok_or(ConnectorError::FormatError)?;
            match prev_u_map.get(symbol) {
                None => {
                    info!(%symbol, "first depth update; fetching the initial snapshot.");
                    snapshots.request(symbol.to_string(), "initial");
                }
                Some(prev_u) if pu != *prev_u => {
                    warn!(%symbol, "missing depth feed has been detected.");
                    snapshots.request(symbol.to_string(), "depth gap");
                }
                Some(_) => {}
            }
            prev_u_map.insert(symbol.to_string(), u);
        }
        let _ = writer_tx.send((recv_time, symbol.to_string(), data.to_string()));
    }
    Ok(())
}

pub async fn run_collection(
    public_streams: Vec<String>,
    market_streams: Vec<String>,
    symbols: Vec<String>,
    writer_tx: UnboundedSender<(DateTime<Utc>, String, String)>,
    config: Config,
) -> Result<(), anyhow::Error> {
    let mut prev_u_map = HashMap::new();
    let snapshots = Snapshots::new(writer_tx.clone())?;
    let (ws_tx, mut ws_rx) = unbounded_channel();
    let mut connections = Vec::new();
    for (name, base_url, streams) in [
        ("public", http::PUBLIC_URL, public_streams),
        ("market", http::MARKET_URL, market_streams),
    ] {
        if !streams.is_empty() {
            connections.push(tokio::spawn(keep_connection(
                name,
                base_url,
                streams,
                symbols.clone(),
                ws_tx.clone(),
                config.idle_timeout,
            )));
        }
    }
    drop(ws_tx);
    if config.snapshot_secs > 0 {
        tokio::spawn(scheduled_snapshots(
            snapshots.clone(),
            symbols.clone(),
            config.snapshot_secs,
        ));
    }
    while let Some((recv_time, data)) = ws_rx.recv().await {
        if let Err(error) = handle(&mut prev_u_map, &snapshots, &writer_tx, recv_time, data) {
            error!(?error, "couldn't handle the received data.");
        }
    }
    for connection in connections {
        let _ = connection.await;
    }
    Ok(())
}
