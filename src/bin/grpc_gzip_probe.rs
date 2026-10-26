//! gRPC 订阅压缩诊断：对比 gzip / 明文下的实际下行字节数。
//!
//! 为什么需要它：`accept_compressed(gzip)` 只是**告知**服务端「我可以收 gzip」，
//! 压不压由服务端（或中间代理）决定。这个 bin 用 `/proc/self/io` 的 `rchar`
//! 统计同一过滤器、同样时长下两种模式各读了多少字节（TLS 密文口径），
//! 直接看压没压。
//!
//! 用法（在仓库根目录、能连到 `YELLOWSTONE_GRPC_URL` 的机器上）：
//!
//! ```console
//! set -a; . pumpswap-snipe/.env; set +a
//! cargo run --release -p transaction-monitor --bin grpc_gzip_probe -- 15 both
//! # 自定义过滤器（逗号分隔多个账户）：
//! cargo run --release -p transaction-monitor --bin grpc_gzip_probe -- 20 both <pubkey1>,<pubkey2>
//! ```
//!
//! 参数：`[秒数=15] [gzip|none|both=both] [account_include 逗号分隔]`
//!
//! 判读：gzip 生效时 `B/msg` 通常比 none 低 1.5x~4x；两者接近说明服务端
//! 没开 compression.send（或中间层剥掉了 grpc-accept-encoding），此时客户端
//! 开着也不会更省流量，但不会报错。

use std::collections::HashMap;
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use grpc_client::YellowstoneGrpc;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeRequestPing,
    subscribe_update::UpdateOneof,
};

/// 不传账户时默认盯 pump.fun 程序（链上最忙的合约之一，样本量大）。
const DEFAULT_ACCOUNT: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

struct Stats {
    mode: &'static str,
    secs: f64,
    msgs: u64,
    pings: u64,
    bytes: u64,
}

impl Stats {
    fn bytes_per_msg(&self) -> f64 {
        self.bytes as f64 / self.msgs.max(1) as f64
    }
}

fn rchar() -> u64 {
    let s = std::fs::read_to_string("/proc/self/io").unwrap_or_default();
    for line in s.lines() {
        if let Some(v) = line.strip_prefix("rchar: ") {
            return v.trim().parse().unwrap_or(0);
        }
    }
    0
}

fn with_commas(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

async fn run_mode(mode: &'static str, secs: u64, accounts: &[String]) -> Result<Stats, String> {
    let url = std::env::var("YELLOWSTONE_GRPC_URL")
        .map_err(|_| "YELLOWSTONE_GRPC_URL 未设置（先 source .env）".to_string())?;
    let token = std::env::var("YELLOWSTONE_GRPC_TOKEN").ok();

    let grpc = YellowstoneGrpc::new(url, token);
    let grpc = if mode == "none" { grpc.no_compression() } else { grpc.gzip() };
    let client = grpc
        .build_client()
        .await
        .map_err(|e| format!("build_client 失败: {e:?}"))?;

    let filter = SubscribeRequestFilterTransactions {
        vote: Some(false),
        failed: None,
        signature: None,
        account_include: accounts.to_vec(),
        account_exclude: vec![],
        account_required: vec![],
        cuckoo_account_include: None,
        token_accounts: None,
    };
    let req = SubscribeRequest {
        transactions: HashMap::from([("probe".to_string(), filter)]),
        commitment: Some(CommitmentLevel::Processed.into()),
        ..Default::default()
    };

    let (mut tx, mut stream) = client
        .lock()
        .await
        .subscribe_with_request(Some(req))
        .await
        .map_err(|e| format!("subscribe 失败: {e:?}"))?;

    let before = rchar();
    let start = Instant::now();
    let deadline = start + Duration::from_secs(secs);
    let mut msgs = 0u64;
    let mut pings = 0u64;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        match tokio::time::timeout(left, stream.next()).await {
            Ok(Some(Ok(msg))) => match msg.update_oneof {
                Some(UpdateOneof::Ping(_)) => {
                    pings += 1;
                    let _ = tx
                        .send(SubscribeRequest {
                            ping: Some(SubscribeRequestPing { id: 1 }),
                            ..Default::default()
                        })
                        .await;
                }
                Some(_) => msgs += 1,
                None => {}
            },
            Ok(Some(Err(e))) => return Err(format!("stream error: {e:?}")),
            Ok(None) => return Err("stream ended".to_string()),
            Err(_) => break,
        }
    }
    let bytes = rchar().saturating_sub(before);
    Ok(Stats {
        mode,
        secs: start.elapsed().as_secs_f64(),
        msgs,
        pings,
        bytes,
    })
}

fn print_stats(s: &Stats) {
    let bps = (s.bytes as f64 / s.secs.max(1e-9)) as u64;
    println!(
        "  {:<4} {:>5.1}s  msgs={:<6} pings={:<3} bytes={:>12}  B/s={:>10}  B/msg={:>8.1}",
        s.mode,
        s.secs,
        s.msgs,
        s.pings,
        with_commas(s.bytes),
        with_commas(bps),
        s.bytes_per_msg()
    );
}

#[tokio::main]
async fn main() {
    let _ = dotenvy::dotenv();

    let mut args = std::env::args().skip(1);
    let secs: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(15);
    let mode = args.next().unwrap_or_else(|| "both".to_string());
    let accounts: Vec<String> = match args.next() {
        Some(a) => a
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect(),
        None => vec![DEFAULT_ACCOUNT.to_string()],
    };
    let modes: Vec<&'static str> = match mode.as_str() {
        "gzip" => vec!["gzip"],
        "none" => vec!["none"],
        "both" => vec!["none", "gzip"],
        other => {
            eprintln!("未知模式 {other}，可选 gzip | none | both");
            std::process::exit(2);
        }
    };

    println!("endpoint = {}", std::env::var("YELLOWSTONE_GRPC_URL").unwrap_or_default());
    println!("filter   = account_include {accounts:?}（vote=false, commitment=processed）");
    println!("duration = {secs}s / mode");

    let mut results: Vec<Stats> = Vec::new();
    for m in modes {
        println!("── 运行 {m} ...");
        match run_mode(m, secs, &accounts).await {
            Ok(stats) => {
                print_stats(&stats);
                results.push(stats);
            }
            Err(e) => {
                eprintln!("❌ {m} 失败: {e}");
                std::process::exit(1);
            }
        }
    }

    if results.len() == 2 {
        let (none, gzip) = (&results[0], &results[1]);
        if none.msgs == 0 || gzip.msgs == 0 {
            println!("\n⚠️ 样本不足（msgs={}/{}），换个更忙的 account_include 或加长秒数再测", none.msgs, gzip.msgs);
        } else {
            let ratio = none.bytes_per_msg() / gzip.bytes_per_msg();
            println!(
                "\n结论：none {:.1} B/msg vs gzip {:.1} B/msg，压缩比 {:.2}x",
                none.bytes_per_msg(),
                gzip.bytes_per_msg(),
                ratio
            );
            if ratio < 1.1 {
                println!("→ 服务端基本没压缩：可能没开 compression.send，或被中间层剥掉了 grpc-accept-encoding。");
            } else {
                println!("→ gzip 已生效，下行流量约为明文的 {:.0}%。", 100.0 / ratio);
            }
        }
    }
}