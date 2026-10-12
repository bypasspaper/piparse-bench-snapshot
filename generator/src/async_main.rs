//! Async (tokio) TCP data generator for the connection-scaling / network latency experiment.
//!
//! WHY a second generator: the blocking `tcp-generator` uses ONE OS thread per client. On
//! localhost (no separate host / NIC available) that means N connections -> N producer threads
//! sharing the same 48-core machine as the engine. At many connections the producer's own thread
//! explosion oversubscribes the CPU and drags down the ENGINE too -- including the async engine
//! arm -- so it confounds the async-vs-blocking comparison we actually want to measure.
//!
//! This generator serves N connections as async TASKS on a FIXED small tokio worker pool
//! (`--threads`), so producer OS-thread count stays constant regardless of N. Loopback TCP still
//! does real per-packet CPU work (ordering, socket-buffer copies, no checksum offload on lo), but
//! that cost is symmetric across engine arms and bounded by throughput, not by N.
//!
//! tokio's `write_all().await` re-arms write interest correctly, so it does NOT suffer the
//! missed-EPOLLOUT stall the blocking generator's header warns about (that was about hand-rolled
//! epoll). Still, backpressure behaviour is stress-tested before trusting results.
//!
//! Wire protocol is identical to the blocking generator: raw file bytes streamed to each client,
//! `iterations` passes, uniform chunk-paced rate limiting. No framing/handshake.

use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::Barrier;
use tokio::time::{sleep_until, Instant};

#[derive(Parser, Debug)]
#[command(
    name = "async-tcp-generator",
    about = "Async thread-pool TCP data generator"
)]
struct Args {
    /// Path to the data file to serve
    #[arg(long)]
    file: PathBuf,
    /// Number of clients to wait for before starting (all released together)
    #[arg(short = 'n', long, default_value_t = 1)]
    clients: u32,
    /// Number of passes over the file per client
    #[arg(short = 'm', long, default_value_t = 1)]
    iterations: u32,
    /// Target TOTAL data rate in MB/s, split evenly across clients (per-client = rate/clients).
    /// Set this BELOW the engine's saturation ceiling to measure latency-under-controlled-load.
    #[arg(short = 'r', long, default_value_t = 100000)]
    rate: u64,
    /// Host to bind to
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    /// Port to bind to
    #[arg(long, default_value_t = 1111)]
    port: u16,
    /// Chunk size in KB for rate pacing (smaller = smoother offered load = cleaner latency)
    #[arg(long, default_value_t = 128)]
    chunk_size_kb: u64,
    /// tokio worker threads (the whole point: FIXED producer thread count, independent of N)
    #[arg(long, default_value_t = 4)]
    threads: usize,
}

fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let data = Arc::new(std::fs::read(&args.file).expect("failed to read data file"));
    // The file is loaded BEFORE the runtime binds, so a multi-GB file doesn't delay "Listening on".
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads)
        .enable_all()
        .build()
        .expect("build tokio runtime");
    rt.block_on(run(args, data));
}

async fn run(args: Args, data: Arc<Vec<u8>>) {
    let n = args.clients as usize;
    // pbench readiness + our harness both grep "Listening on".
    eprintln!(
        "Listening on {}:{}, waiting for {} client(s), threads={}, rate={} MB/s total, chunk={}KB",
        args.host, args.port, n, args.threads, args.rate, args.chunk_size_kb
    );
    let listener = TcpListener::bind(format!("{}:{}", args.host, args.port))
        .await
        .expect("bind");

    let barrier = Arc::new(Barrier::new(n));
    let rate_per_client_bps = (args.rate / n as u64).max(1) * 1_000_000;
    let chunk = (args.chunk_size_kb * 1024) as usize;
    let iters = args.iterations;

    let mut tasks = Vec::with_capacity(n);
    for id in 0..n {
        let (mut stream, _addr) = listener.accept().await.expect("accept");
        stream.set_nodelay(true).ok(); // avoid Nagle-induced latency on loopback
        let data = Arc::clone(&data);
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await; // all clients start streaming together (clean measurement window)
            let chunk_dur = Duration::from_secs_f64(chunk as f64 / rate_per_client_bps as f64);
            let mut next = Instant::now();
            for _ in 0..iters {
                let mut off = 0usize;
                while off < data.len() {
                    let now = Instant::now();
                    if now < next {
                        sleep_until(next).await;
                    }
                    next += chunk_dur;
                    if next < Instant::now() {
                        next = Instant::now(); // never accumulate a catch-up burst
                    }
                    let end = (off + chunk).min(data.len());
                    if stream.write_all(&data[off..end]).await.is_err() {
                        return; // receiver closed
                    }
                    off = end;
                }
            }
            let _ = stream.shutdown().await;
            let _ = id;
        }));
    }
    for t in tasks {
        let _ = t.await;
    }
    eprintln!("All clients done. Shutting down.");
}
