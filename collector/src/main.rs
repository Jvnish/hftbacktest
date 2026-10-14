use std::time::Duration;

use anyhow::anyhow;
use clap::Parser;
use tokio::{
    self, select, signal,
    signal::unix::{SignalKind, signal as unix_signal},
    sync::mpsc::unbounded_channel,
    time::{MissedTickBehavior, interval},
};
use tracing::{error, info};

use crate::file::Writer;

mod binance;
mod binancefuturescm;
mod binancefuturesum;
mod bybit;
mod error;
mod file;
mod hyperliquid;
mod proxy;
mod throttler;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path for the files where collected data will be written.
    path: String,

    /// Name of the exchange
    exchange: String,

    /// Symbols for which data will be collected.
    symbols: Vec<String>,

    /// Seconds per output segment. Segments start at multiples of this since the Unix epoch, so
    /// 3600 gives one file per symbol and UTC hour.
    #[arg(long, default_value_t = 3600, value_parser = clap::value_parser!(u64).range(1..))]
    rotate_secs: u64,

    /// Seconds between flushes of the open segments; a killed process loses at most this much.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u64).range(1..))]
    flush_secs: u64,

    /// binancefutures only: seconds between scheduled depth snapshots, aligned like
    /// --rotate-secs. 0 disables them; snapshots are still taken at startup and after gaps.
    #[arg(long, default_value_t = 3600)]
    snapshot_secs: u64,

    /// binancefutures only: reconnect when a connection delivers no data for this many seconds.
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..))]
    idle_timeout_secs: u64,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let (writer_tx, mut writer_rx) = unbounded_channel();

    let _handle = match args.exchange.as_str() {
        "binancefutures" | "binancefuturesum" => {
            let public_streams = ["$symbol@trade", "$symbol@bookTicker", "$symbol@depth@0ms"]
                .iter()
                .map(|stream| stream.to_string())
                .collect();
            let market_streams = ["$symbol@markPrice@1s"]
                .iter()
                .map(|stream| stream.to_string())
                .collect();

            tokio::spawn(binancefuturesum::run_collection(
                public_streams,
                market_streams,
                args.symbols,
                writer_tx,
                binancefuturesum::Config {
                    snapshot_secs: args.snapshot_secs,
                    idle_timeout: Duration::from_secs(args.idle_timeout_secs),
                },
            ))
        }
        "binancefuturescm" => {
            let streams = [
                "$symbol@trade",
                "$symbol@bookTicker",
                "$symbol@depth@0ms",
                // "$symbol@@markPrice@1s"
            ]
            .iter()
            .map(|stream| stream.to_string())
            .collect();

            tokio::spawn(binancefuturescm::run_collection(
                streams,
                args.symbols,
                writer_tx,
            ))
        }
        "binance" | "binancespot" => {
            let streams = ["$symbol@trade", "$symbol@bookTicker", "$symbol@depth@100ms"]
                .iter()
                .map(|stream| stream.to_string())
                .collect();

            tokio::spawn(binance::run_collection(streams, args.symbols, writer_tx))
        }
        "bybit" => {
            let topics = [
                "orderbook.1.$symbol",
                "orderbook.50.$symbol",
                "orderbook.500.$symbol",
                "publicTrade.$symbol",
            ]
            .iter()
            .map(|topic| topic.to_string())
            .collect();

            tokio::spawn(bybit::run_collection(topics, args.symbols, writer_tx))
        }
        "hyperliquid" => {
            let subscriptions = ["trades", "l2Book", "bbo"]
                .iter()
                .map(|sub| sub.to_string())
                .collect();

            tokio::spawn(hyperliquid::run_collection(
                subscriptions,
                args.symbols,
                writer_tx,
            ))
        }
        exchange => {
            return Err(anyhow!("{exchange} is not supported."));
        }
    };

    // systemd and `kill` send SIGTERM. Without a handler the process died without finishing its
    // gzip segments.
    let mut sigterm = unix_signal(SignalKind::terminate())?;
    let mut flush = interval(Duration::from_secs(args.flush_secs));
    flush.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut writer = Writer::new(&args.path, args.rotate_secs);
    let mut result = Ok(());
    loop {
        select! {
            _ = signal::ctrl_c() => {
                info!("ctrl-c received");
                break;
            }
            _ = sigterm.recv() => {
                info!("SIGTERM received");
                break;
            }
            _ = flush.tick() => {
                if let Err(error) = writer.flush() {
                    error!(?error, "flush error");
                    result = Err(error.into());
                    break;
                }
            }
            r = writer_rx.recv() => match r {
                Some((recv_time, symbol, data)) => {
                    if let Err(error) = writer.write(recv_time, symbol, data) {
                        error!(?error, "write error");
                        result = Err(error);
                        break;
                    }
                }
                None => {
                    break;
                }
            }
        }
    }
    if result.is_ok() {
        // Writes what was already received before finishing the segments.
        while let Ok((recv_time, symbol, data)) = writer_rx.try_recv() {
            if let Err(error) = writer.write(recv_time, symbol, data) {
                error!(?error, "write error");
                result = Err(error);
                break;
            }
        }
    }
    if let Err(error) = writer.close() {
        error!(?error, "couldn't finish the segments");
        if result.is_ok() {
            result = Err(error.into());
        }
    }
    result
}
