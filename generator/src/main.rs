//! TCP data generator for PiParse benchmarks.
//!
//! Reads a data file into memory and streams it to connected TCP clients
//! with configurable rate limiting in uniform or burst mode.
//!
//! When `--timestamp-field` is set, each replay iteration shifts the
//! named CSV column by the previous iteration's last rendered timestamp,
//! so windowed queries see strictly increasing timestamps across loops.
//!
//! I/O is intentionally BLOCKING (std::net + one thread per client). A data
//! blaster has no use for async concurrency, and async writes are fragile under
//! heavy receiver backpressure: when the receiver throttles the TCP window, an
//! async writer must be woken (EPOLLOUT) to resume, and that wakeup can be
//! missed, permanently stalling the send. A blocking `write_all` needs no
//! app-level wakeup — the kernel resumes it the instant the window reopens — so
//! it cannot stall this way. (Reproduced + diagnosed against NebulaStream's
//! backpressured TCP source; see engine_to_e2e HANDOVER 11b.)

use clap::Parser;
use std::io::Write;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(name = "tcp-generator", about = "TCP data generator for PiParse benchmarks")]
struct Args {
    /// Path to the data file to serve
    #[arg(long)]
    file: PathBuf,

    /// Number of clients to wait for before starting
    #[arg(short = 'n', long, default_value_t = 1)]
    clients: u32,

    /// Number of times to send the file to each client
    #[arg(short = 'm', long, default_value_t = 1)]
    iterations: u32,

    /// Target data size in MB per client. If set, overrides --iterations:
    /// full iterations over the file are performed until this size is surpassed.
    #[arg(long)]
    target_mb: Option<u64>,

    /// Target data rate in MB/s (per generator, split across clients)
    #[arg(short = 'r', long, default_value_t = 100000)]
    rate: u64,

    /// Sending mode: uniform (chunked pacing) or burst (send-then-pause)
    #[arg(long, default_value = "uniform")]
    mode: String,

    /// Host to bind to
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port to bind to
    #[arg(long, default_value_t = 1111)]
    port: u16,

    /// Chunk size in KB for rate limiting (uniform mode)
    #[arg(long, default_value_t = 1000)]
    chunk_size_kb: u64,

    /// 0-based index of the timestamp column in each CSV row. When set,
    /// every replay iteration i shifts that column by `i * last_ts`,
    /// where `last_ts` is the last row's timestamp in the source file,
    /// keeping timestamps strictly increasing across loops for windowed
    /// queries.
    #[arg(long)]
    timestamp_field: Option<usize>,

    /// CSV delimiter, single byte (default ',')
    #[arg(long, default_value = ",")]
    delimiter: String,
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Uniform,
    Burst,
}

impl Mode {
    fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "burst" => Mode::Burst,
            _ => Mode::Uniform,
        }
    }
}

/// Byte ranges around one parsed row's timestamp field, indexed into the
/// original file buffer. Rendering an iteration copies `prefix`, writes the
/// shifted timestamp, then copies `suffix` (which includes the trailing
/// delimiter and line terminator, if any).
#[derive(Clone, Debug)]
struct Row {
    prefix: Range<usize>,
    timestamp: i64,
    suffix: Range<usize>,
}

struct Server {
    data: Vec<u8>,
    rows: Option<Vec<Row>>,
    /// Last row's timestamp in the source file, used as the per-iteration
    /// offset multiplier. `None` when timestamp shifting is disabled.
    last_ts: Option<i64>,
    args: Args,
    barrier: Arc<Barrier>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let mut args = Args::parse();

    // Read file into memory
    info!("Reading file: {:?}", args.file);
    let data = std::fs::read(&args.file)?;
    let file_size_mb = data.len() as f64 / 1_000_000.0;
    info!(
        "File loaded: {:.1} MB ({} bytes)",
        file_size_mb,
        data.len()
    );

    // If --target-mb is set, compute iterations needed to surpass it (full passes only).
    if let Some(target_mb) = args.target_mb {
        if data.is_empty() {
            return Err("Cannot use --target-mb with an empty data file".into());
        }
        let target_bytes = target_mb.saturating_mul(1_000_000);
        let iters = target_bytes.div_ceil(data.len() as u64).max(1) as u32;
        info!(
            "target-mb={} → {} iteration(s) ({:.1} MB total, overrides --iterations={})",
            target_mb,
            iters,
            (iters as f64) * file_size_mb,
            args.iterations
        );
        args.iterations = iters;
    }

    let (rows, last_ts) = if let Some(field) = args.timestamp_field {
        let delim = parse_delimiter(&args.delimiter)?;
        let parsed = parse_rows(&data, field, delim)?;
        let last = parsed.last().map(|r| r.timestamp).ok_or_else(|| {
            "file has no rows but --timestamp-field is set".to_string()
        })?;
        info!(
            "Timestamp shifting enabled: field={}, last_ts={}, parsed {} rows",
            field,
            last,
            parsed.len()
        );
        (Some(parsed), Some(last))
    } else {
        (None, None)
    };

    let mode = Mode::parse(&args.mode);
    let n_clients = args.clients as usize;
    let barrier = Arc::new(Barrier::new(n_clients));

    info!(
        "Listening on {}:{}, waiting for {} client(s), mode={:?}, rate={} MB/s, iterations={}",
        args.host, args.port, n_clients, mode, args.rate, args.iterations
    );

    let listener = TcpListener::bind(format!("{}:{}", args.host, args.port))?;

    let server = Arc::new(Server {
        data,
        rows,
        last_ts,
        args,
        barrier,
    });

    // Accept clients (blocking accept), one OS thread per client.
    let mut handles = Vec::new();
    for client_id in 0..n_clients {
        let (stream, addr) = listener.accept()?;
        info!("Client {} connected from {}", client_id, addr);

        let srv = Arc::clone(&server);
        let handle = thread::spawn(move || {
            if let Err(e) = handle_client(srv, stream, client_id) {
                error!("Client {} error: {}", client_id, e);
            }
        });
        handles.push(handle);
    }

    // Wait for all clients to finish
    for handle in handles {
        let _ = handle.join();
    }

    info!("All clients done. Shutting down.");
    Ok(())
}

fn handle_client(
    server: Arc<Server>,
    mut stream: TcpStream,
    client_id: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let n_clients = server.args.clients as u64;
    let rate_per_client = server.args.rate / n_clients; // MB/s per client
    let mode = Mode::parse(&server.args.mode);
    let iterations = server.args.iterations;
    let chunk_size = (server.args.chunk_size_kb * 1024) as usize;

    // Wait for all clients to connect
    info!("Client {}: waiting for barrier", client_id);
    server.barrier.wait();
    info!("Client {}: starting transfer", client_id);

    let total_start = Instant::now();
    let mut iter_buf: Vec<u8> = Vec::new();
    let mut total_bytes: u64 = 0;

    for iter in 0..iterations {
        let iter_start = Instant::now();

        let payload: &[u8] = match (&server.rows, server.last_ts) {
            (Some(rows), Some(last_ts)) => {
                let offset = last_ts.wrapping_mul(iter as i64);
                render_iteration(&server.data, rows, offset, &mut iter_buf);
                &iter_buf
            }
            _ => &server.data,
        };

        match mode {
            Mode::Uniform => {
                send_uniform(&mut stream, payload, rate_per_client, chunk_size)?;
            }
            Mode::Burst => {
                send_burst(&mut stream, payload, rate_per_client)?;
            }
        }

        total_bytes += payload.len() as u64;
        let elapsed = iter_start.elapsed().as_secs_f64();
        let throughput = payload.len() as f64 / elapsed / 1_000_000.0;

        if (iter + 1) % 10 == 0 || iter == 0 || iter + 1 == iterations {
            info!(
                "Client {}: iteration {}/{}, {:.1} MB/s",
                client_id,
                iter + 1,
                iterations,
                throughput
            );
        }
    }

    let total_elapsed = total_start.elapsed().as_secs_f64();
    let avg_throughput = total_bytes as f64 / total_elapsed / 1_000_000.0;

    info!(
        "Client {}: done. {} iterations in {:.1}s, avg {:.1} MB/s",
        client_id, iterations, total_elapsed, avg_throughput
    );

    stream.flush()?;
    stream.shutdown(Shutdown::Write)?;
    Ok(())
}

fn parse_delimiter(s: &str) -> Result<u8, Box<dyn std::error::Error>> {
    let bytes = s.as_bytes();
    if bytes.len() != 1 {
        return Err(format!(
            "delimiter must be a single byte, got {:?} ({} bytes)",
            s,
            bytes.len()
        )
        .into());
    }
    Ok(bytes[0])
}

/// Parse the file into rows indexed by the timestamp field's byte range.
/// Supports '\n' and '\r\n' line terminators; blank lines are skipped.
fn parse_rows(
    data: &[u8],
    field_idx: usize,
    delimiter: u8,
) -> Result<Vec<Row>, Box<dyn std::error::Error>> {
    let mut rows = Vec::new();
    let mut pos = 0;
    let mut line_no: usize = 0;

    while pos < data.len() {
        line_no += 1;
        let (content_end, next_pos) = match data[pos..].iter().position(|&b| b == b'\n') {
            Some(p) => {
                let nl = pos + p;
                let content = if nl > pos && data[nl - 1] == b'\r' {
                    nl - 1
                } else {
                    nl
                };
                (content, nl + 1)
            }
            None => (data.len(), data.len()),
        };

        if content_end == pos {
            pos = next_pos;
            continue;
        }

        let line = &data[pos..content_end];
        let mut current_field = 0usize;
        let mut field_begin = 0usize;
        let mut ts_range: Option<(usize, usize)> = None;

        for (i, &b) in line.iter().enumerate() {
            if b == delimiter {
                if current_field == field_idx {
                    ts_range = Some((field_begin, i));
                    break;
                }
                current_field += 1;
                field_begin = i + 1;
            }
        }
        if ts_range.is_none() && current_field == field_idx {
            ts_range = Some((field_begin, line.len()));
        }

        let (ts_start_rel, ts_end_rel) = ts_range.ok_or_else(|| {
            format!(
                "line {}: timestamp field index {} out of range ({} fields in line)",
                line_no,
                field_idx,
                current_field + 1
            )
        })?;

        let ts_bytes = &line[ts_start_rel..ts_end_rel];
        let ts_str = std::str::from_utf8(ts_bytes)
            .map_err(|e| format!("line {}: timestamp is not valid UTF-8: {}", line_no, e))?;
        let timestamp: i64 = ts_str.trim().parse().map_err(|e| {
            format!(
                "line {}: cannot parse timestamp {:?} as i64: {}",
                line_no,
                ts_str.trim(),
                e
            )
        })?;

        let ts_start_abs = pos + ts_start_rel;
        let ts_end_abs = pos + ts_end_rel;
        rows.push(Row {
            prefix: pos..ts_start_abs,
            timestamp,
            suffix: ts_end_abs..next_pos,
        });

        pos = next_pos;
    }

    Ok(rows)
}

/// Render one iteration's bytes into `out` with every timestamp shifted by
/// `offset`. Reuses `out`'s buffer across calls.
fn render_iteration(data: &[u8], rows: &[Row], offset: i64, out: &mut Vec<u8>) {
    out.clear();
    // i64 formatted as decimal fits in 20 bytes (incl. sign).
    let capacity: usize = rows
        .iter()
        .map(|r| r.prefix.len() + 20 + r.suffix.len())
        .sum();
    if out.capacity() < capacity {
        out.reserve(capacity - out.capacity());
    }
    for row in rows {
        out.extend_from_slice(&data[row.prefix.clone()]);
        write!(out, "{}", row.timestamp.wrapping_add(offset)).unwrap();
        out.extend_from_slice(&data[row.suffix.clone()]);
    }
}

/// Uniform mode: send data in chunks with interval-based rate limiting (blocking).
fn send_uniform(
    stream: &mut TcpStream,
    data: &[u8],
    rate_mbps: u64,
    chunk_size: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let rate_bytes_per_sec = rate_mbps.saturating_mul(1_000_000).max(1);
    let chunk_duration = Duration::from_secs_f64(chunk_size as f64 / rate_bytes_per_sec as f64);

    let mut offset = 0;
    let mut next_tick = Instant::now();
    while offset < data.len() {
        let now = Instant::now();
        if now < next_tick {
            thread::sleep(next_tick - now);
        }
        // Advance the schedule; never let it fall behind (mirrors tokio's
        // MissedTickBehavior::Delay so a slow chunk doesn't trigger a catch-up burst).
        next_tick += chunk_duration;
        let now = Instant::now();
        if next_tick < now {
            next_tick = now;
        }

        let end = (offset + chunk_size).min(data.len());
        stream.write_all(&data[offset..end])?;
        offset = end;
    }

    Ok(())
}

/// Burst mode: send the entire file at once, then pause to maintain average rate (blocking).
fn send_burst(
    stream: &mut TcpStream,
    data: &[u8],
    rate_mbps: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let rate_bytes_per_sec = rate_mbps.saturating_mul(1_000_000).max(1);
    let expected_duration = Duration::from_secs_f64(data.len() as f64 / rate_bytes_per_sec as f64);

    let start = Instant::now();
    stream.write_all(data)?;
    let elapsed = start.elapsed();

    if elapsed < expected_duration {
        thread::sleep(expected_duration - elapsed);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_middle_field() {
        let data = b"a,100,x\nb,200,y\n";
        let rows = parse_rows(data, 1, b',').unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].timestamp, 100);
        assert_eq!(rows[1].timestamp, 200);
    }

    #[test]
    fn parses_last_field_without_trailing_newline() {
        let data = b"a,100\nb,200";
        let rows = parse_rows(data, 1, b',').unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].timestamp, 200);
    }

    #[test]
    fn parses_first_field() {
        let data = b"100,a\n200,b\n";
        let rows = parse_rows(data, 0, b',').unwrap();
        assert_eq!(rows[0].timestamp, 100);
        assert_eq!(rows[1].timestamp, 200);
    }

    #[test]
    fn handles_crlf_and_blank_lines() {
        let data = b"a,10,x\r\n\r\nb,20,y\r\n";
        let rows = parse_rows(data, 1, b',').unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].timestamp, 10);
        assert_eq!(rows[1].timestamp, 20);
    }

    #[test]
    fn out_of_range_field_errors() {
        let data = b"a,10\n";
        let err = parse_rows(data, 5, b',').unwrap_err();
        assert!(err.to_string().contains("out of range"));
    }

    #[test]
    fn render_shifts_timestamps_and_preserves_surroundings() {
        let data = b"a,100,x\nb,200,y\n".to_vec();
        let rows = parse_rows(&data, 1, b',').unwrap();
        let mut out = Vec::new();
        render_iteration(&data, &rows, 5, &mut out);
        assert_eq!(out, b"a,105,x\nb,205,y\n");
    }

    #[test]
    fn render_with_zero_offset_roundtrips_exact_bytes() {
        let data = b"foo,1,bar\nbaz,2,qux\n".to_vec();
        let rows = parse_rows(&data, 1, b',').unwrap();
        let mut out = Vec::new();
        render_iteration(&data, &rows, 0, &mut out);
        assert_eq!(out, data);
    }

    #[test]
    fn render_handles_different_digit_widths() {
        let data = b"x,9\n".to_vec();
        let rows = parse_rows(&data, 1, b',').unwrap();
        let mut out = Vec::new();
        render_iteration(&data, &rows, 91, &mut out);
        assert_eq!(out, b"x,100\n");
    }

    #[test]
    fn render_handles_negative_offset() {
        let data = b"x,100\n".to_vec();
        let rows = parse_rows(&data, 1, b',').unwrap();
        let mut out = Vec::new();
        render_iteration(&data, &rows, -50, &mut out);
        assert_eq!(out, b"x,50\n");
    }

    #[test]
    fn tab_delimiter_parses() {
        let data = b"a\t100\tx\n";
        let rows = parse_rows(data, 1, b'\t').unwrap();
        assert_eq!(rows[0].timestamp, 100);
    }

    #[test]
    fn parse_delimiter_rejects_multi_byte() {
        assert!(parse_delimiter(",,").is_err());
        assert!(parse_delimiter("").is_err());
    }
}
