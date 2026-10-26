//! gRPC 订阅压缩诊断：对比 gzip / 明文下的实际下行字节数。
//!
//! 为什么需要它：`accept_compressed(gzip)` 只是**告知**服务端「我可以收 gzip」，
//! 压不压由服务端（或中间代理）决定。
//!
//! 统计方式：在本地起一个 TCP 代理（仅支持 `http://` 端点，TLS 拆不开），
//! 客户端连 `127.0.0.1`，代理逐字节转发并计数，得到**真实 TCP payload 字节**。
//! 不用 `/proc/self/io`：实测在容器/虚拟化环境里 `rchar` 根本不统计 socket，
//! 会得到个位数字节的假数据。
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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use grpc_client::YellowstoneGrpc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeRequestPing,
    subscribe_update::UpdateOneof,
};

/// 不传账户时默认盯 pump.fun 程序（链上最忙的合约之一，样本量大）。
const DEFAULT_ACCOUNT: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

struct Stats {
    mode: String,
    secs: f64,
    msgs: u64,
    pings: u64,
    down: u64,
    up: u64,
}

impl Stats {
    fn bytes_per_msg(&self) -> f64 {
        self.down as f64 / self.msgs.max(1) as f64
    }
}

/// 本地计数代理：`local_url` 给客户端用，`down`/`up` 是累计字节。
struct Proxy {
    local_url: String,
    down: Arc<AtomicU64>,
    up: Arc<AtomicU64>,
}

async fn copy_count<R, W>(reader: &mut R, writer: &mut W, counter: &AtomicU64) -> std::io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut buf = vec![0u8; 32 * 1024];
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        writer.write_all(&buf[..n]).await?;
        counter.fetch_add(n as u64, Ordering::Relaxed);
    }
}

/// 解析 `http://host:port` 并在 127.0.0.1 上起计数代理。
async fn start_proxy(url: &str) -> Result<Proxy, String> {
    let rest = url.strip_prefix("http://").ok_or_else(|| {
        "探针只能拆 http:// 端点（https 的 TLS 无法在本地代理里拆；请换内网 http 端点）".to_string()
    })?;
    let authority = match rest.find("/") {
        Some(i) => &rest[..i],
        None => rest,
    };
    let (host, port) = match authority.rfind(":") {
        Some(i) => (
            authority[..i].to_string(),
            authority[i + 1..]
                .parse::<u16>()
                .map_err(|e| format!("端口解析失败: {e}"))?,
        ),
        None => (authority.to_string(), 80),
    };
    let upstream = format!("{host}:{port}");

    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .map_err(|e| format!("代理监听失败: {e}"))?;
    let local_port = listener.local_addr().map_err(|e| e.to_string())?.port();
    let down = Arc::new(AtomicU64::new(0));
    let up = Arc::new(AtomicU64::new(0));
    let (down2, up2) = (down.clone(), up.clone());

    tokio::spawn(async move {
        loop {
            let Ok((inbound, _)) = listener.accept().await else {
                return;
            };
            let Ok(upstream_conn) = TcpStream::connect(&upstream).await else {
                continue;
            };
            let (client_r, client_w) = tokio::io::split(inbound);
            let (server_r, server_w) = tokio::io::split(upstream_conn);
            let up3 = up2.clone();
            tokio::spawn(async move {
                let (mut r, mut w) = (client_r, server_w);
                let _ = copy_count(&mut r, &mut w, &up3).await;
            });
            let down3 = down2.clone();
            tokio::spawn(async move {
                let (mut r, mut w) = (server_r, client_w);
                let _ = copy_count(&mut r, &mut w, &down3).await;
            });
        }
    });

    Ok(Proxy {
        local_url: format!("http://127.0.0.1:{local_port}"),
        down,
        up,
    })
}

async fn run_mode(mode: &str, secs: u64, accounts: &[String]) -> Result<Stats, String> {
    let url = std::env::var("YELLOWSTONE_GRPC_URL")
        .map_err(|_| "YELLOWSTONE_GRPC_URL 未设置（先 source .env）".to_string())?;
    let token = std::env::var("YELLOWSTONE_GRPC_TOKEN").ok();

    let proxy = start_proxy(&url).await?;
    println!("  本地计数代理: {} -> {url}", proxy.local_url);

    let grpc = YellowstoneGrpc::new(proxy.local_url.clone(), token);
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
    let secs_elapsed = start.elapsed().as_secs_f64();

    // 断开连接，让代理把在途字节记完，再读计数。
    drop(stream);
    drop(tx);
    drop(client);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let down = proxy.down.load(Ordering::Relaxed);
    let up = proxy.up.load(Ordering::Relaxed);
    Ok(Stats {
        mode: mode.to_string(),
        secs: secs_elapsed,
        msgs,
        pings,
        down,
        up,
    })
}

fn with_commas(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push_str(",");
        }
        out.push(c);
    }
    out
}

fn print_stats(s: &Stats) {
    let bps = (s.down as f64 / s.secs.max(1e-9)) as u64;
    println!(
        "  {:<4} {:>5.1}s  msgs={:<6} pings={:<3} down={:>12}  down/s={:>10}  B/msg={:>9.1}  up={:>9}",
        s.mode,
        s.secs,
        s.msgs,
        s.pings,
        with_commas(s.down),
        with_commas(bps),
        s.bytes_per_msg(),
        with_commas(s.up)
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
            .split(",")
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect(),
        None => vec![DEFAULT_ACCOUNT.to_string()],
    };
    let modes: Vec<String> = match mode.as_str() {
        "gzip" => vec!["gzip".to_string()],
        "none" => vec!["none".to_string()],
        "both" => vec!["none".to_string(), "gzip".to_string()],
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
        match run_mode(&m, secs, &accounts).await {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// 本地代理必须原样转发并分别计数两个方向。
    #[tokio::test]
    async fn proxy_counts_both_directions() {
        // 假上游：收 4 字节，回 100KB，然后关闭。
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4];
            s.read_exact(&mut buf).await.unwrap();
            s.write_all(&vec![7u8; 100_000]).await.unwrap();
            s.shutdown().await.unwrap();
        });

        let proxy = start_proxy(&format!("http://127.0.0.1:{port}")).await.unwrap();
        let addr = proxy.local_url.trim_start_matches("http://").to_string();
        let mut c = TcpStream::connect(&addr).await.unwrap();
        c.write_all(b"ping").await.unwrap();
        let mut out = Vec::new();
        c.read_to_end(&mut out).await.unwrap();
        assert_eq!(out.len(), 100_000);

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(proxy.up.load(Ordering::Relaxed), 4, "upstream 方向计数");
        assert_eq!(proxy.down.load(Ordering::Relaxed), 100_000, "downstream 方向计数");
    }
}