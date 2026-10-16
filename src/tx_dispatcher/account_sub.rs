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
use std::time::{Duration, SystemTime};

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;

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

/// lamports 缓存是**哪一路**写进来的（**只用于查询时打日志**，不参与任何判定）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LamportsSource {
    /// 订阅时的 RPC 初值拉取（`prime_account`）—— 不知道 slot、也没有交易签名
    PrimeRpc,
    /// gRPC 账户推送（`SubscribeUpdateAccount`）
    AccountUpdate,
    /// 交易流解析 `balance_changes_of_grpc`（保底通道，见 `TxDispatcher::set_lamports`）
    Transaction,
}

impl LamportsSource {
    /// 日志里用的短名
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PrimeRpc => "prime-rpc",
            Self::AccountUpdate => "account",
            Self::Transaction => "tx",
        }
    }
}

/// 一笔 lamports 缓存的**来源信息**：什么时候采到的、哪一路、链上 slot、交易签名。
///
/// 之所以要把这些留下来：`CheckBalanceDrop` 的语义完全取决于「这个读数有多新」，
/// 出问题时必须能回答「这一笔买入用的余额是什么时候、从哪拿的、对应哪笔交易」。
/// 采集时不打日志（热路径 + 刷屏），**查询时由调用方打**。
#[derive(Clone, Debug)]
pub struct LamportsMeta {
    /// 写入缓存的**本地墙钟时刻**（不是链上时间）—— 用来算「这个读数有多旧」
    pub observed_at: SystemTime,
    /// 链上 slot（`0` = 未知，初值 RPC 就是这种）
    pub slot: u64,
    pub source: LamportsSource,
    /// 产生这次变化的交易签名：交易流一定有；账户推送若带 `txn_signature` 也有
    pub signature: Option<Signature>,
}

#[derive(Default)]
struct AddrState {
    callbacks: HashMap<u64, Box<dyn Fn(&Pubkey, &[u8], u64) + Send + Sync + 'static>>,
    latest: Option<(Vec<u8>, u64)>,
    /// 最近一次更新的**原生 lamports**（SOL 余额）。
    ///
    /// 与 `latest` **同源、同一次更新**：gRPC 的 `SubscribeUpdateAccountInfo.lamports`
    /// 或初值拉取时 `Account.lamports` —— 两者都在同一条写入路径上，
    /// 不会出现「data 是新的、lamports 是旧的」这种错配。
    latest_lamports: Option<u64>,
    /// 写入 [`Self::latest_lamports`] 时的 slot（`0` = 未知）。
    ///
    /// **两路数据源共用一个缓存**时靠它做单调保护，见 [`AddrState::apply_lamports`]：
    /// 1. **账户流**：gRPC `SubscribeUpdateAccount`（改动即推，带 `slot`）；
    /// 2. **交易流**：订阅器解析 `balance_changes_of_grpc` 的 `after_balance`
    ///    （[`super::TxDispatcher::set_lamports`]，带交易 `slot`）。
    ///
    /// 两条流都是 Processed，但**跨通道到达顺序不保证** —— 没有这个 slot，一次迟到的
    /// 旧帧就能把缓存**回退**成一个更旧（通常更高）的余额，让 `CheckBalanceDrop`
    /// 误判成「掉太多」。
    lamports_slot: u64,
    /// [`Self::latest_lamports`] 的来源信息（诊断用，见 [`LamportsMeta`]）。
    lamports_meta: Option<LamportsMeta>,
    bare_sub_count: u64,
}

impl AddrState {
    /// 写入 lamports，带 **slot 单调保护**。
    ///
    /// - `slot != 0` 且已存 slot 更大 → **丢弃**（迟到的旧帧不回退缓存）；
    /// - `slot == 0`（初值 RPC 不知道 slot）→ 不做比较，直接写。
    fn apply_lamports(&mut self, lamports: u64, slot: u64, source: LamportsSource, signature: Option<Signature>) {
        if slot != 0 && self.lamports_slot != 0 && slot < self.lamports_slot {
            return;
        }
        self.latest_lamports = Some(lamports);
        if slot != 0 {
            self.lamports_slot = slot;
        }
        self.lamports_meta = Some(LamportsMeta {
            observed_at: SystemTime::now(),
            slot,
            source,
            signature,
        });
    }
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

    pub fn update_and_fire(&self, addr: &Pubkey, data: Vec<u8>, lamports: u64, slot: u64, txn_signature: Option<Signature>) {
        let mut states = self.states.lock().unwrap();
        if let Some(s) = states.get_mut(addr) {
            s.latest = Some((data.clone(), slot));
            s.apply_lamports(lamports, slot, LamportsSource::AccountUpdate, txn_signature);
            for cb in s.callbacks.values() {
                cb(addr, &data, slot);
            }
        }
    }

    /// **只写 lamports**（不动 data、不触发回调），给交易流那条保底通道用。
    ///
    /// 返回是否写入（地址没订阅过 = `false`，调用方无副作用）。
    pub fn set_lamports(&self, addr: &Pubkey, lamports: u64, slot: u64, signature: Option<Signature>) -> bool {
        let mut states = self.states.lock().unwrap();
        match states.get_mut(addr) {
            Some(s) => {
                s.apply_lamports(lamports, slot, LamportsSource::Transaction, signature);
                true
            }
            None => false,
        }
    }

    pub fn get_latest(&self, addr: &Pubkey) -> Option<(Vec<u8>, u64)> {
        self.states
            .lock()
            .unwrap()
            .get(addr)
            .and_then(|s| s.latest.clone())
    }

    /// 最近一次更新的**原生 lamports**；没订阅过 / 还没任何数据时为 `None`。
    pub fn get_latest_lamports(&self, addr: &Pubkey) -> Option<u64> {
        self.states.lock().unwrap().get(addr).and_then(|s| s.latest_lamports)
    }

    /// 上面那个余额的**来源信息**（采集时刻 / 哪一路 / slot / 交易签名），给查询方打日志用。
    pub fn get_latest_lamports_meta(&self, addr: &Pubkey) -> Option<LamportsMeta> {
        self.states
            .lock()
            .unwrap()
            .get(addr)
            .and_then(|s| s.lamports_meta.clone())
    }

    /// 只在**缓存为空**时写入并触发回调，返回是否写入。
    ///
    /// 用于 RPC 初值拉取：它是异步的，期间可能已经有 gRPC 推送到达，
    /// 此时不能拿旧快照覆盖新数据。
    pub fn update_if_empty(&self, addr: &Pubkey, data: Vec<u8>, lamports: u64, slot: u64) -> bool {
        let mut states = self.states.lock().unwrap();
        let Some(s) = states.get_mut(addr) else {
            return false;
        };
        if s.latest.is_some() {
            return false;
        }
        s.latest = Some((data.clone(), slot));
        s.apply_lamports(lamports, slot, LamportsSource::PrimeRpc, None);
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

    /// 从缓存读取最近一次推送的账户**原生 lamports（SOL 余额）**。
    ///
    /// # 谁会用到
    ///
    /// `dash` 程序的 `CheckBalanceDrop`（tag 11）哨兵：买入交易里要填一串
    /// 「链下缓存的余额」，这里就是那个缓存的读口 —— **纯内存、不打 RPC**。
    ///
    /// # 返回 `None` 的两种情况（调用方自己决定怎么兜底）
    ///
    /// 1. 这个地址**从没订阅过**（没调过 [`Self::subscribe_account`] / [`Self::on_account_update`]）；
    /// 2. 订阅了但**初值还没拉回来**、gRPC 也没推送过（`prime_account` 是异步的）。
    pub fn get_lamports(&self, addr: &Pubkey) -> Option<u64> {
        self.inner.account_subs.get_latest_lamports(addr)
    }

    /// 上面那个余额的**来源信息**（采集时刻 / 哪一路 / slot / 交易签名）。
    ///
    /// 专门给「查询时打一条日志」用 —— 采集侧（账户推送、交易流）**不打日志**
    /// （热路径 + 会刷屏），出问题时在这里一次问清楚。
    pub fn get_lamports_meta(&self, addr: &Pubkey) -> Option<LamportsMeta> {
        self.inner.account_subs.get_latest_lamports_meta(addr)
    }

    /// **从交易流写入 lamports**（第二路数据源，只写余额、不动账户 data、不触发回调）。
    ///
    /// # 为什么要这条路
    ///
    /// 账户订阅只在**节点把账户写进去**时推一帧；它是主路。但生产上总会遇到
    /// 「推送没到 / 连接刚重连」这类情况 —— 而交易流我们本来就在逐笔收，交易里
    /// `post_balances` 就是**同一份权威数据**（`utils::balance_change::balance_changes_of_grpc`）。
    /// 两条路写**同一个缓存**，靠 slot 单调保护合并（见 `AddrState::apply_lamports`），
    /// 谁更新用谁，迟到帧不回退。
    ///
    /// - `addr` **必须是已订阅地址**（订阅过才有 `AddrState`）；没订阅过返回 `false`。
    /// - `slot` 传该笔交易的 slot（合并用）；`0` = 未知，直接写不比较。
    /// - `signature` 传该笔交易的签名（只进诊断元数据，不参与判定）。
    ///
    /// 返回是否真的写入了缓存。
    pub fn set_lamports(&self, addr: &Pubkey, lamports: u64, slot: u64, signature: Option<Signature>) -> bool {
        self.inner.account_subs.set_lamports(addr, lamports, slot, signature)
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
                    if inner.account_subs.update_if_empty(&addr, acct.data, acct.lamports, 0) {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// **跨通道合并的核心不变量**：迟到的旧帧不能把缓存回退成更旧的（通常更高的）余额。
    ///
    /// 两路数据源（gRPC 账户流 / 交易流）都是 Processed，但到达顺序不保证 ——
    /// 一旦回退，`CheckBalanceDrop` 就会把「没掉」判成「掉太多」而误杀买单。
    #[test]
    fn lamports_never_regress_on_a_late_frame() {
        let mut s = AddrState::default();

        s.apply_lamports(100, 10, LamportsSource::AccountUpdate, None);
        assert_eq!(s.latest_lamports, Some(100));

        // 迟到的旧帧：slot 更小、值更高（典型场景：对手刚掉完，旧值晚到）
        s.apply_lamports(500, 9, LamportsSource::Transaction, None);
        assert_eq!(s.latest_lamports, Some(100), "旧 slot 的帧必须被丢弃");

        // 更新的帧照常写（对手又掉了）
        s.apply_lamports(50, 11, LamportsSource::Transaction, None);
        assert_eq!(s.latest_lamports, Some(50));

        // slot = 0（初值 RPC，不知道 slot）：不做比较，直接写
        s.apply_lamports(7, 0, LamportsSource::PrimeRpc, None);
        assert_eq!(s.latest_lamports, Some(7));
    }

    /// 采集元数据要跟着余额一起更新：来源 / slot / 签名 / 采集时刻。
    /// 查询侧（`get_lamports_meta`）就靠它打「这个读数哪来的、多旧」。
    #[test]
    fn lamports_meta_tracks_source_slot_and_signature() {
        let subs = AccountSubs::new();
        let addr = Pubkey::new_unique();
        {
            let mut states = subs.states.lock().unwrap();
            states.entry(addr).or_default();
        }

        // 交易流写入：带 slot + 签名
        let sig = Signature::from([7u8; 64]);
        assert!(subs.set_lamports(&addr, 12_000_000_000, 351_234_567, Some(sig)));
        let meta = subs.get_latest_lamports_meta(&addr).expect("meta 必须写进去了");
        assert_eq!(meta.source, LamportsSource::Transaction);
        assert_eq!(meta.slot, 351_234_567);
        assert_eq!(meta.signature, Some(sig));
        assert_eq!(meta.source.as_str(), "tx");

        // 账户推送写入：来源要变成 account
        subs.update_and_fire(&addr, vec![], 11_000_000_000, 351_234_570, None);
        let meta = subs.get_latest_lamports_meta(&addr).unwrap();
        assert_eq!(meta.source, LamportsSource::AccountUpdate);
        assert_eq!(meta.slot, 351_234_570);
        assert_eq!(meta.signature, None, "这帧没带 txn_signature");
    }

    /// 只写 lamports 的通道**不动 data、不触发回调**（交易流那条保底路）。
    #[test]
    fn set_lamports_does_not_touch_data_or_callbacks() {
        let subs = AccountSubs::new();
        let addr = Pubkey::new_unique();
        let fired = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // 注册一个带回调的订阅，先灌一帧 data
        {
            let fired = Arc::clone(&fired);
            let mut states = subs.states.lock().unwrap();
            let st = states.entry(addr).or_default();
            st.callbacks
                .insert(1, Box::new(move |_, _, _| {
                    fired.fetch_add(1, Ordering::SeqCst);
                }));
        }
        subs.update_and_fire(&addr, vec![1, 2, 3], 100, 10, None);
        assert_eq!(fired.load(Ordering::SeqCst), 1);

        // 交易流只写余额
        assert!(subs.set_lamports(&addr, 55, 11, None));
        assert_eq!(subs.get_latest_lamports(&addr), Some(55));
        assert_eq!(subs.get_latest(&addr).map(|(d, _)| d), Some(vec![1, 2, 3]), "data 不能被覆盖");
        assert_eq!(fired.load(Ordering::SeqCst), 1, "只写余额不该触发回调");

        // 没订阅过的地址：写不进去，返回 false
        assert!(!subs.set_lamports(&Pubkey::new_unique(), 1, 1, None));
    }
}
