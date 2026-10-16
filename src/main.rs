use anyhow::Result;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::header::HeaderMap;
use reqwest::{Client, Method};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::str::FromStr;
use std::sync::{atomic::AtomicBool, Mutex};
use tokio::time::{sleep, Duration, Instant};

// ─── ANSI color helpers ──────────────────────────────────────────────────────

fn c_red(s: &str) -> String {
    format!("\x1b[31m{}\x1b[0m", s)
}
fn c_green(s: &str) -> String {
    format!("\x1b[32m{}\x1b[0m", s)
}
fn c_yellow(s: &str) -> String {
    format!("\x1b[33m{}\x1b[0m", s)
}
fn c_cyan(s: &str) -> String {
    format!("\x1b[36m{}\x1b[0m", s)
}
fn c_white(s: &str) -> String {
    format!("\x1b[37m{}\x1b[0m", s)
}
fn c_gray(s: &str) -> String {
    format!("\x1b[90m{}\x1b[0m", s)
}
fn c_bold(s: &str) -> String {
    format!("\x1b[1m{}\x1b[0m", s)
}
// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "http_burner")]
#[command(about = "High-performance HTTP API load tester")]
#[command(long_about = "
Load test your API with configurable workers, duration, rate limiting,
and custom headers. Produces detailed latency percentile statistics.

Examples:
  http_burner http://localhost:3000/api -c 20 -d 30s
  http_burner https://api.example.com/users -c 50 -d 2m -X POST -b '{\"key\":\"val\"}'
  http_burner http://localhost:8080/health -c 10 -d 60s -r 100 -H 'Authorization: Bearer tok...'",)]
struct Args {
    /// Target URL (e.g., http://localhost:3000/api/test)
    #[arg()]
    url: String,

    /// Number of concurrent workers
    #[arg(short = 'c', long, default_value_t = 10)]
    workers: usize,

    /// Duration to run the test (e.g., 30s, 5m, 1h)
    #[arg(short = 'd', long)]
    duration: Option<String>,

    /// Global requests per second limit (0 = unlimited burst)
    #[arg(short = 'r', long, default_value_t = 0)]
    rate_limit: u64,

    /// HTTP method
    #[arg(short = 'X', long, default_value = "GET")]
    method: String,

    /// Request body (JSON string)
    #[arg(short = 'b', long)]
    body: Option<String>,

    /// Request header in "Key: Value" format (repeatable)
    #[arg(short = 'H', long)]
    headers: Vec<String>,

    /// Content-Type header
    #[arg(long, default_value = "application/json")]
    content_type: String,

    /// Request timeout in seconds
    #[arg(long, default_value_t = 30)]
    timeout: u64,

    /// Follow HTTP redirects
    #[arg(long)]
    follow_redirects: bool,

    /// Output results as JSON
    #[arg(long)]
    json: bool,
}

// ─── Stats collector ─────────────────────────────────────────────────────────

struct Stats {
    total: AtomicU64,
    success: AtomicU64,
    failed: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_recv: AtomicU64,
    status_codes: Mutex<BTreeMap<u16, u64>>,
    latencies: Mutex<Vec<f64>>,
}

impl Stats {
    fn new() -> Self {
        Self {
            total: AtomicU64::new(0),
            success: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_recv: AtomicU64::new(0),
            status_codes: Mutex::new(BTreeMap::new()),
            latencies: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, status: u16, latency_ms: f64, bytes_s: u64, bytes_r: u64) {
        self.total.fetch_add(1, Ordering::Relaxed);
        if (200..300).contains(&status) {
            self.success.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed.fetch_add(1, Ordering::Relaxed);
        }
        self.bytes_sent.fetch_add(bytes_s, Ordering::Relaxed);
        self.bytes_recv.fetch_add(bytes_r, Ordering::Relaxed);
        {
            let mut map = self.status_codes.lock().unwrap();
            *map.entry(status).or_default() += 1;
        }
        {
            let mut vec = self.latencies.lock().unwrap();
            vec.push(latency_ms);
        }
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.ends_with('s') {
        let n: u64 = s[..s.len() - 1].parse()?;
        Ok(Duration::from_secs(n))
    } else if s.ends_with('m') {
        let n: u64 = s[..s.len() - 1].parse()?;
        Ok(Duration::from_secs(n * 60))
    } else if s.ends_with('h') {
        let n: u64 = s[..s.len() - 1].parse()?;
        Ok(Duration::from_secs(n * 3600))
    } else {
        let n: u64 = s.parse()?;
        Ok(Duration::from_secs(n))
    }
}

fn build_headers(args: &Args) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        args.content_type.parse()?,
    );
    for h in &args.headers {
        if let Some((k, v)) = h.split_once(':') {
            headers.insert(
                reqwest::header::HeaderName::from_str(k.trim())?,
                reqwest::header::HeaderValue::from_str(v.trim())?,
            );
        }
    }
    Ok(headers)
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (p / 100.0) * (sorted.len() as f64 - 1.0);
    let lower = idx as usize;
    let upper = (lower + 1).min(sorted.len() - 1);
    let frac = idx - lower as f64;
    sorted[lower] + (sorted[upper] - sorted[lower]) * frac
}

fn bar(pct: f64, width: f64) -> String {
    let filled = (pct / 100.0 * width) as usize;
    let filled = filled.min(width as usize);
    let empty = (width - filled as f64) as usize;
    let color = if pct >= 99.0 {
        "\x1b[32m" // green
    } else if pct >= 95.0 {
        "\x1b[33m" // yellow
    } else {
        "\x1b[31m" // red
    };
    format!(
        "{}{}{}{}{}",
        color,
        "█".repeat(filled),
        "\x1b[0m",
        "░".repeat(empty),
        "\x1b[0m"
    )
}

fn human_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

// ─── Worker ──────────────────────────────────────────────────────────────────

async fn worker(
    client: Client,
    url: String,
    method: Method,
    body: Option<Vec<u8>>,
    headers: HeaderMap,
    stats: Arc<Stats>,
    rate_limit: u64,
    stopped: Arc<AtomicBool>,
) {
    loop {
        if stopped.load(Ordering::Relaxed) {
            break;
        }

        let start = Instant::now();

        let req_builder = client
            .request(method.clone(), &url)
            .headers(headers.clone())
            .timeout(Duration::from_secs(30));

        let req_builder = if let Some(ref b) = body {
            req_builder.body(b.clone())
        } else {
            req_builder
        };

        let resp = match req_builder.send().await {
            Ok(r) => r,
            Err(_) => {
                stats.record(0, start.elapsed().as_secs_f64() * 1000.0, 0, 0);
                continue;
            }
        };

        let status = resp.status().as_u16();
        let latency = start.elapsed().as_secs_f64() * 1000.0;
        let body_len = resp.content_length().unwrap_or(0) as u64;
        stats.record(status, latency, 0, body_len);

        // Rate limiting: sleep to maintain target RPS
        if rate_limit > 0 {
            let expected = start.elapsed().as_secs_f64() * rate_limit as f64;
            let next = Duration::from_secs_f64((expected.ceil() as f64) / rate_limit as f64);
            if next > start.elapsed() {
                sleep(next.saturating_sub(start.elapsed())).await;
            }
        }

        tokio::task::yield_now().await;
    }
}

// ─── Output ──────────────────────────────────────────────────────────────────

fn print_results(stats: &Stats, duration_secs: f64, url: &str, args: &Args) -> Result<()> {
    let total = stats.total.load(Ordering::Relaxed);
    if total == 0 {
        eprintln!("{}", c_red("No requests were completed."));
        return Ok(());
    }

    let success = stats.success.load(Ordering::Relaxed);
    let failed = stats.failed.load(Ordering::Relaxed);
    let rps = total as f64 / duration_secs;
    let bytes_sent = stats.bytes_sent.load(Ordering::Relaxed);
    let bytes_recv = stats.bytes_recv.load(Ordering::Relaxed);

    let mut latencies = stats.latencies.lock().unwrap().clone();
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let min = latencies.first().copied().unwrap_or(0.0);
    let max = latencies.last().copied().unwrap_or(0.0);
    let avg = latencies.iter().sum::<f64>() / latencies.len() as f64;
    let median = percentile(&latencies, 50.0);
    let p90 = percentile(&latencies, 90.0);
    let p95 = percentile(&latencies, 95.0);
    let p99 = percentile(&latencies, 99.0);

    if args.json {
        let status_codes = stats.status_codes.lock().unwrap().clone();
        let result = json!({
            "target": url,
            "duration_seconds": duration_secs,
            "workers": args.workers,
            "total_requests": total,
            "successful": success,
            "failed": failed,
            "rps": (rps * 100.0).round() / 100.0,
            "latency": {
                "min_ms": (min * 100.0).round() / 100.0,
                "avg_ms": (avg * 100.0).round() / 100.0,
                "median_ms": (median * 100.0).round() / 100.0,
                "p90_ms": (p90 * 100.0).round() / 100.0,
                "p95_ms": (p95 * 100.0).round() / 100.0,
                "p99_ms": (p99 * 100.0).round() / 100.0,
                "max_ms": (max * 100.0).round() / 100.0,
            },
            "status_codes": status_codes,
            "bytes_sent": bytes_sent,
            "bytes_received": bytes_recv,
        });
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(());
    }

    // ── Header ──
    println!();
    println!(
        "  {}",
        c_bold("╔══════════════════════════════════════════════════════╗")
    );
    println!(
        "  {}",
        c_cyan(&c_bold("║              http_burner — Results                   ║"))
    );
    println!(
        "  {}",
        c_bold("╚══════════════════════════════════════════════════════╝")
    );
    println!();

    // ── Target info ──
    println!("  {}", c_bold("Target:"));
    println!("    {}", c_cyan(url));
    println!();

    // ── Divider helper ──
    let div = c_gray("──────────────────────────────────────────────────────");

    // ── Summary ──
    let success_pct = success as f64 / total as f64 * 100.0;
    let failed_pct = failed as f64 / total as f64 * 100.0;

    println!("  {}", c_bold("Summary"));
    println!("  {}", div);
    println!("    Total Requests:   {}", c_bold(&total.to_string()));
    println!(
        "    Successful:       {} ({:.1}%)",
        c_green(&success.to_string()),
        success_pct
    );
    println!(
        "    Failed:           {} ({:.1}%)",
        c_red(&failed.to_string()),
        failed_pct
    );
    println!("    Duration:         {} s", c_bold(&format!("{:.2}", duration_secs)));
    let err_str = if failed_pct > 1.0 {
        c_red(&format!("{:.2}%", failed_pct))
    } else {
        c_green(&format!("{:.2}%", failed_pct))
    };
    println!("    Errors:           {}", err_str);
    println!();

    // ── RPS ──
    println!("  {}", c_bold("Requests/sec"));
    println!("  {}", div);
    let rps_str = format!("{:.2}", rps);
    let rps_fg = if rps >= 1000.0 {
        "\x1b[32m"
    } else if rps >= 100.0 {
        "\x1b[33m"
    } else {
        "\x1b[37m"
    };
    println!("    {}", format!("{} RPS", rps_str).with_ansi_code(rps_fg));
    println!();

    // ── Latency ──
    println!("  {}", c_bold("Latency Percentiles"));
    println!("  {}", div);
    let latency = vec![
        ("Average", format!("{:.2} ms", avg)),
        ("Median", format!("{:.2} ms", median)),
        ("90%", format!("{:.2} ms", p90)),
        ("95%", format!("{:.2} ms", p95)),
        ("99%", format!("{:.2} ms", p99)),
        ("Min", format!("{:.2} ms", min)),
        ("Max", format!("{:.2} ms", max)),
    ];
    for (label, val) in &latency {
        let val_out = if *label == "99%" {
            c_red(val)
        } else if *label == "Median" {
            c_yellow(val)
        } else {
            c_white(val)
        };
        println!("    {:<10} {}", label, val_out);
    }
    println!();

    // ── Status codes ──
    let status_codes = stats.status_codes.lock().unwrap().clone();
    if !status_codes.is_empty() {
        println!("  {}", c_bold("Status Code Distribution"));
        println!("  {}", div);
        for (code, count) in &status_codes {
            let pct = *count as f64 / total as f64 * 100.0;
            let code_fg = match code {
                200..=299 => "\x1b[32m",
                300..=399 => "\x1b[36m",
                400..=499 => "\x1b[33m",
                500..=599 => "\x1b[31m",
                _ => "\x1b[37m",
            };
            let code_bold = format!("\x1b[1m{}\x1b[0m", code);
            let count_str = format!("\x1b[1m{}\x1b[0m", count);
            println!(
                "    {}  {}  {:.2}%  {}",
                code_bold.with_ansi_code(code_fg),
                count_str,
                pct,
                bar(pct, 20.0)
            );
        }
        println!();
    }

    // ── Data transfer ──
    println!("  {}", c_bold("Data Transfer"));
    println!("  {}", div);
    println!("    Sent:         {}", c_bold(&human_size(bytes_sent)));
    println!("    Received:     {}", c_bold(&human_size(bytes_recv)));
    println!();

    Ok(())
}

trait AnsiExt {
    fn with_ansi_code(self, code: &str) -> String;
}

impl AnsiExt for &str {
    fn with_ansi_code(self, code: &str) -> String {
        format!("{}\x1b[0m", format!("{}{}", code, self))
    }
}

impl AnsiExt for String {
    fn with_ansi_code(self, code: &str) -> String {
        format!("{}\x1b[0m", format!("{}{}", code, self))
    }
}

// ─── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let duration = match &args.duration {
        Some(d) => parse_duration(d)?,
        None => {
            eprintln!("{}", c_red("Error: --duration (-d) is required."));
            std::process::exit(1);
        }
    };

    let headers = build_headers(&args)?;

    let client = Client::builder()
        .timeout(Duration::from_secs(args.timeout))
        .connect_timeout(Duration::from_secs(10))
        .redirect(if args.follow_redirects {
            reqwest::redirect::Policy::limited(10)
        } else {
            reqwest::redirect::Policy::none()
        })
        .build()?;

    // ── Print banner ──
    println!();
    println!(
        "  {}",
        c_bold("╔══════════════════════════════════════════════════════╗")
    );
    println!(
        "  {}",
        c_cyan("║             http_burner — Starting Test              ║")
    );
    println!(
        "  {}",
        c_bold("╚══════════════════════════════════════════════════════╝")
    );
    println!();
    println!("  Target:      {}", c_cyan(&args.url));
    println!("  Workers:     {}", c_bold(&args.workers.to_string()));
    println!(
        "  Duration:    {}",
        c_bold(&args.duration.clone().unwrap_or_default())
    );
    println!("  Method:      {}", c_bold(&args.method));
    if !args.headers.is_empty() {
        println!("  Custom Headers: {} headers", args.headers.len());
    }
    if args.rate_limit > 0 {
        println!("  Rate Limit:  {} req/s", args.rate_limit);
    }
    println!();

    let stats = Arc::new(Stats::new());
    let start = Instant::now();

    // Progress bar
    let pb = ProgressBar::new(duration.as_millis() as u64);
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {wide_msg}")
            .unwrap(),
    );
    pb.set_message("Running...");
    pb.enable_steady_tick(Duration::from_millis(200));

    // Spawn workers
    let stopped = Arc::new(AtomicBool::new(false));
    let mut handles = vec![];

    let method_str = args.method.to_uppercase();
    let method = match method_str.as_str() {
        "GET" => Method::GET,
        "POST" => Method::POST,
        "PUT" => Method::PUT,
        "DELETE" => Method::DELETE,
        "PATCH" => Method::PATCH,
        "HEAD" => Method::HEAD,
        "OPTIONS" => Method::OPTIONS,
        _ => {
            eprintln!("Unsupported method: {}. Using GET.", args.method);
            Method::GET
        }
    };

    let body_bytes = args.body.as_ref().map(|b| b.as_bytes().to_vec());

    for i in 0..args.workers {
        let s = Arc::clone(&stats);
        let c = client.clone();
        let url = args.url.clone();
        let hdrs = headers.clone();
        let rl = args.rate_limit;
        let st = Arc::clone(&stopped);

        let m = method.clone();
        let b = body_bytes.clone();
        let handle = tokio::spawn(async move {
            worker(c, url, m, b, hdrs, s, rl, st).await;
        });

        pb.set_message(format!("Worker {}: running...", i + 1));
        handles.push(handle);
    }

    // Wait for duration
    sleep(duration).await;

    // Signal all workers to stop
    stopped.store(true, Ordering::Relaxed);

    // Wait for all workers to finish
    for handle in handles {
        let _ = handle.await;
    }

    pb.finish_with_message("Complete!");

    let elapsed = start.elapsed().as_secs_f64();

    // Print results
    print_results(&stats, elapsed, &args.url, &args)?;

    Ok(())
}
