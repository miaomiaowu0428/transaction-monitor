//! 账户数据订阅模块。
//!
//! - `on_account_update(addr, callback)` 返回 `AccountHandle`，drop 时自动退订
//! - `get_account(addr, if_match)` 从缓存读取最新数据
//!
//! ⚠️ **gRPC 账户订阅只在账户被改动时推送，从不下发初始快照**。
//! 所以 `AmmConfig` 这类**永不改动**的账户即使订阅了也永远收不到。
//! 因此每次登记新订阅时，会额外用 RPC 主动拉一次初值（见 [`prime_account`]）。
//!
//! [`prime_account`]: TxDispatcherInner::prime_account

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;

use super::TxDispatcherInner;

static ACCOUNT_HANDLE_ID: AtomicU64 = AtomicU64::new(0);

/// 初值拉取的最大并发数。
///
/// 一次订阅几百个账户时逐个发 RPC，用信号量限流避免把节点打爆。
pub const PRIME_CONCURRENCY: usize = 32;

/// 单个账户初值拉取的超时。
const PRIME_TIMEOUT: Duration = Duration::from_secs(10);

/// 初值拉取用的 RPC 客户端（进程内单例，首次使用时按环境变量初始化）。
static RPC_CLIENT: OnceLock<Option<Arc<RpcClient>>> = OnceLock::new();

/// 取初值拉取用的 RPC 客户端；未配置则返回 `None`。
fn rpc_client() -> Option<Arc<RpcClient>> {
    RPC_CLIENT
        .get_or_init(|| {
            let url = std::env::var("JSON_RPC_URL")
                .or_else(|_| std::env::var("RPC_URL"))
                .ok()
                .filter(|u| !u.is_empty());
            match url {
                Some(u) => {
                    log::info!("🔌 账户初值拉取已启用（RPC = {}）", mask_url(&u));
                    Some(Arc::new(RpcClient::new(u)))
                }
                None => {
                    log::warn!(
                        "⚠️ 未设置 JSON_RPC_URL：无法主动拉取账户初值。\
                         gRPC 只在账户被改动时推送，永不改动的账户（如 CLMM AmmConfig）\
                         将永远收不到"
                    );
                    None
                }
            }
        })
        .clone()
}

/// 打码 URL 里的 `api-key`，避免日志泄露密钥。
fn mask_url(u: &str) -> String {
    match u.split_once("api-key=") {
        Some((head, _)) => format!("{head}api-key=***"),
        None => u.to_string(),
    }
}

// ── AccountHandle ─────────────────────────────────────────────────────────

pub struct AccountHandle {
    id: u64,
    addr: Pubkey,
    dispatcher: Weak<TxDispatcherInner>,
}

impl AccountHandle {
    fn new(id: u64, addr: Pubkey, dispatcher: Weak<TxDispatcherInner>) -> Self {
        Self {
            id,
            addr,
            dispatcher,
        }
    }
}

impl Drop for AccountHandle {
    fn drop(&mut self) {
        if let Some(inner) = self.dispatcher.upgrade() {
            inner.remove_account_entry(self.addr, self.id);
        }
    }
}

// ── 内部状态 ─────────────────────────────────────────────────────────────

#[derive(Default)]
struct AddrState {
    callbacks: HashMap<u64, Box<dyn Fn(&Pubkey, &[u8], u64) + Send + Sync + 'static>>,
    latest: Option<(Vec<u8>, u64)>,
    bare_sub_count: u64,
}

pub(crate) struct AccountSubs {
    states: Mutex<HashMap<Pubkey, AddrState>>,
}

impl AccountSubs {
    pub fn new() -> Self {
        Self {
            states: Mutex::new(HashMap::new()),
        }
    }

    pub fn active_addresses(&self) -> Vec<Pubkey> {
        self.states
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, s)| !s.callbacks.is_empty() || s.bare_sub_count > 0)
            .map(|(a, _)| *a)
            .collect()
    }

    pub fn update_and_fire(&self, addr: &Pubkey, data: Vec<u8>, slot: u64) {
        let mut states = self.states.lock().unwrap();
        if let Some(s) = states.get_mut(addr) {
            s.latest = Some((data.clone(), slot));
            for cb in s.callbacks.values() {
                cb(addr, &data, slot);
            }
        }
    }

    pub fn get_latest(&self, addr: &Pubkey) -> Option<(Vec<u8>, u64)> {
        self.states
            .lock()
            .unwrap()
            .get(addr)
            .and_then(|s| s.latest.clone())
    }

    /// 只在**缓存为空**时写入并触发回调，返回是否写入。
    ///
    /// 用于 RPC 初值拉取：它是异步的，期间可能已经有 gRPC 推送到达，
    /// 此时不能拿旧快照覆盖新数据。
    pub fn update_if_empty(&self, addr: &Pubkey, data: Vec<u8>, slot: u64) -> bool {
        let mut states = self.states.lock().unwrap();
        let Some(s) = states.get_mut(addr) else {
            return false;
        };
        if s.latest.is_some() {
            return false;
        }
        s.latest = Some((data.clone(), slot));
        for cb in s.callbacks.values() {
            cb(addr, &data, slot);
        }
        true
    }
}

// ── TxDispatcher 公开 API ────────────────────────────────────────────────

impl crate::tx_dispatcher::TxDispatcher {
    /// 注册账户更新回调。handle drop 时自动移除。
    pub fn on_account_update<F>(&self, addr: &Pubkey, callback: F) -> AccountHandle
    where
        F: Fn(&Pubkey, &[u8], u64) + Send + Sync + 'static,
    {
        let id = ACCOUNT_HANDLE_ID.fetch_add(1, Ordering::SeqCst);
        {
            let subs = &self.inner.account_subs;
            let mut states = subs.states.lock().unwrap();
            states
                .entry(*addr)
                .or_default()
                .callbacks
                .insert(id, Box::new(callback));
        }
        self.inner.notify_account_change();
        self.inner.prime_account(*addr);
        AccountHandle::new(id, *addr, Arc::downgrade(&self.inner))
    }

    /// 纯订阅（无回调），从缓存读取时用。handle drop 自动退订。
    pub fn subscribe_account(&self, addr: &Pubkey) -> AccountHandle {
        let id = ACCOUNT_HANDLE_ID.fetch_add(1, Ordering::SeqCst);
        {
            let subs = &self.inner.account_subs;
            let mut states = subs.states.lock().unwrap();
            states.entry(*addr).or_default().bare_sub_count += 1;
        }
        self.inner.notify_account_change();
        self.inner.prime_account(*addr);
        AccountHandle::new(id, *addr, Arc::downgrade(&self.inner))
    }

    /// 从缓存读取最近一次推送的账户数据。`if_match` 接收 `&[u8]` 返回 `Option<T>`。
    pub fn get_account<T>(
        &self,
        addr: &Pubkey,
        if_match: impl FnOnce(&[u8]) -> Option<T>,
    ) -> Option<T> {
        let subs = &self.inner.account_subs;
        let (data, _) = subs.get_latest(addr)?;
        if_match(&data)
    }
}

// ── TxDispatcherInner 内部 ───────────────────────────────────────────────

impl TxDispatcherInner {
    /// 订阅新账户时，主动用 RPC 拉一次**初值**。
    ///
    /// 背景：gRPC 账户订阅**只在账户被改动时推送，不下发初始快照**。
    /// 于是像 CLMM `AmmConfig` 这种永不改动的账户，订阅了也永远收不到数据。
    /// 这里补一次 RPC 读取（每个地址只拉一次）。
    ///
    /// 写入用 [`AccountSubs::update_if_empty`]：若期间已有 gRPC 推送到达，
    /// 则丢弃本次结果，保证不会用旧值覆盖新值。
    pub(crate) fn prime_account(self: &Arc<Self>, addr: Pubkey) {
        {
            let mut primed = self.primed.lock().unwrap();
            if !primed.insert(addr) {
                return;
            }
        }
        let Some(rpc) = rpc_client() else { return };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            log::warn!("⚠️ 当前不在 tokio 运行时内，跳过账户 {addr} 的初值拉取");
            return;
        };
        let inner = Arc::clone(self);
        let sem = Arc::clone(&self.prime_sem);
        handle.spawn(async move {
            let _permit = sem.acquire().await;
            match tokio::time::timeout(PRIME_TIMEOUT, rpc.get_account(&addr)).await {
                Ok(Ok(acct)) => {
                    let len = acct.data.len();
                    if inner.account_subs.update_if_empty(&addr, acct.data, 0) {
                        log::debug!("📥 账户 {addr} 初值已拉取（{len}B）");
                    }
                }
                // 账户不存在 / RPC 报错：正常，保持静默
                Ok(Err(e)) => log::debug!("账户 {addr} 初值拉取失败：{e}"),
                Err(_) => log::warn!("⚠️ 账户 {addr} 初值拉取超时"),
            }
        });
    }

    pub(crate) fn remove_account_entry(&self, addr: Pubkey, id: u64) {
        let emptied = {
            let mut states = self.account_subs.states.lock().unwrap();
            match states.get_mut(&addr) {
                Some(s) => {
                    s.callbacks.remove(&id);
                    s.bare_sub_count = s.bare_sub_count.saturating_sub(1);
                    if s.callbacks.is_empty() && s.bare_sub_count == 0 {
                        states.remove(&addr);
                        true
                    } else {
                        false
                    }
                }
                None => false,
            }
        };
        if emptied {
            // 完全退订后允许下次重新订阅时再拉一次初值
            self.primed.lock().unwrap().remove(&addr);
        }
        // 通知 gRPC 流重新发送订阅请求
        // 代际计数 +1 是**可靠**的信号（电平）；Notify 仅作为快速唤醒（边沿，会丢）
        self.account_change_gen.fetch_add(1, Ordering::Relaxed);
        self.account_change_notify.notify_one();
    }

    pub(crate) fn notify_account_change(&self) {
        self.account_change_gen.fetch_add(1, Ordering::Relaxed);
        self.account_change_notify.notify_one();
    }
}
