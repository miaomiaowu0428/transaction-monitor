//! 交易分发器：管理 gRPC 连接并将交易广播给各 subscriber。
//!
//! 核心类型：
//! - [`TxDispatcher`]：线程安全的分发器，支持动态注册/注销 subscriber、主动重连
//! - [`SubscriberHandle`]：注册句柄，drop 时自动取消注册

pub mod account_sub;

use grpc_client::TransactionFormat;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use futures::{SinkExt, StreamExt};

use grpc_client::YellowstoneGrpc;
use log::{error, info, warn};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeRequestPing,
    subscribe_update::UpdateOneof,
};

use crate::tx_subscriber::TxSubscriber;
use account_sub::AccountSubs;

use arc_swap::ArcSwap;

/// 全局 subscriber ID 自增计数器，保证每个 subscriber 注册 ID 全局唯一。
static SUBSCRIBER_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Subscriber 注册句柄，可用于取消注册
///
/// 当 Handle 被 drop 时，会自动取消注册对应的 subscriber
pub struct SubscriberHandle {
    id: u64,
    subscriber: Arc<dyn TxSubscriber>,
    dispatcher: Weak<TxDispatcherInner>,
}

impl SubscriberHandle {
    /// 获取 subscriber 的唯一 ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// 获取 subscriber 的引用
    pub fn subscriber(&self) -> &Arc<dyn TxSubscriber> {
        &self.subscriber
    }

    /// 手动取消注册（提前释放）
    ///
    /// 返回 true 表示成功取消注册，false 表示已经被取消或 dispatcher 已释放
    pub fn unregister(self) -> bool {
        if let Some(dispatcher) = self.dispatcher.upgrade() {
            dispatcher.unregister_by_id(self.id)
        } else {
            false
        }
    }
}

impl Drop for SubscriberHandle {
    fn drop(&mut self) {
        if let Some(dispatcher) = self.dispatcher.upgrade() {
            dispatcher.unregister_by_id(self.id);
        }
    }
}

impl std::fmt::Debug for SubscriberHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriberHandle")
            .field("id", &self.id)
            .finish()
    }
}

/// 交易分发器的内部实现，通过 [`Arc`] 共享给 [`SubscriberHandle`]。
struct TxDispatcherInner {
    subscribers: ArcSwap<HashMap<u64, Arc<dyn TxSubscriber>>>,
    account_filters: ArcSwap<Option<Vec<String>>>,
    account_subs: AccountSubs,
    /// 账户订阅变更时通知 gRPC 循环重新发送 SubscribeRequest
    account_change_notify: tokio::sync::Notify,
}

/// 线程安全的交易分发器。
///
/// 支持多 subscriber 并发处理，并内置带自动重连的 gRPC 订阅循环。
/// 可安全地在多个线程中 clone 和共享。
pub struct TxDispatcher {
    inner: Arc<TxDispatcherInner>,
}

impl Default for TxDispatcher {
    fn default() -> Self {
        Self {
            inner: Arc::new(TxDispatcherInner {
                subscribers: ArcSwap::from_pointee(HashMap::new()),
                account_filters: ArcSwap::from_pointee(None),
                account_subs: AccountSubs::new(),
                account_change_notify: tokio::sync::Notify::new(),
            }),
        }
    }
}

impl Clone for TxDispatcher {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl TxDispatcher {
    /// 创建空的 dispatcher，不含任何 subscriber 和过滤器。
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置账户过滤器，只订阅涉及这些账户的交易
    ///
    /// # 参数
    /// - `accounts`: 要监听的账户列表
    ///
    /// # 示例
    /// ```rust,ignore
    /// let dispatcher = TxDispatcher::new();
    /// dispatcher.with_account_filters(vec![
    ///     Pubkey::from_str("...").unwrap(),
    /// ]);
    /// ```
    pub fn with_account_filters(&self, accounts: Vec<Pubkey>) -> &Self {
        let account_strings: Vec<String> = accounts.iter().map(|pk| pk.to_string()).collect();
        self.inner
            .account_filters
            .store(Arc::new(Some(account_strings)));
        info!(
            "✅ TxDispatcher 账户过滤器已设置，共 {} 个账户",
            accounts.len()
        );
        self
    }

    /// 清除账户过滤器，订阅全链交易（如果节点支持）
    pub fn clear_account_filters(&self) -> &Self {
        self.inner.account_filters.store(Arc::new(None));
        info!("🔄 TxDispatcher 账户过滤器已清除，订阅全链交易");
        self
    }

    /// 注册 subscriber（旧接口，保持向后兼容）
    ///
    /// 注意：使用此方法注册的 subscriber 无法取消注册
    /// 如需取消注册功能，请使用 `register_with_handle` 方法
    pub fn register(&self, sub: Arc<dyn TxSubscriber>) -> &Self {
        let id = SUBSCRIBER_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let old = self.inner.subscribers.load();
        let mut new = (**old).clone();
        new.insert(id, sub.clone());
        let count = new.len();
        self.inner.subscribers.store(Arc::new(new));

        info!(
            "✅ Subscriber 已注册 [{}] ID: {}，当前共 {} 个 subscriber",
            sub.name(),
            id,
            count
        );

        self
    }

    /// 注册 subscriber 并返回句柄（推荐使用）
    ///
    /// 返回的 `SubscriberHandle` 会在 drop 时自动取消注册
    /// 也可以手动调用 `handle.unregister()` 提前释放
    ///
    /// # 示例
    /// ```rust,ignore
    /// // 自动管理生命周期
    /// {
    ///     let handle = dispatcher.register_with_handle(Arc::new(my_subscriber));
    ///     // ... 使用
    /// } // handle drop 时自动取消注册
    ///
    /// // 或手动控制
    /// let handle = dispatcher.register_with_handle(Arc::new(my_subscriber));
    /// handle.unregister(); // 提前手动取消
    /// ```
    pub fn register_with_handle(&self, sub: Arc<dyn TxSubscriber>) -> SubscriberHandle {
        let id = SUBSCRIBER_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let old = self.inner.subscribers.load();
        let mut new = (**old).clone();
        new.insert(id, sub.clone());
        let count = new.len();
        self.inner.subscribers.store(Arc::new(new));

        info!(
            "✅ Subscriber 已注册 [{}] ID: {}，当前共 {} 个 subscriber",
            sub.name(),
            id,
            count
        );

        SubscriberHandle {
            id,
            subscriber: sub,
            dispatcher: Arc::downgrade(&self.inner),
        }
    }

    /// 获取当前注册的 subscriber 数量
    pub fn subscriber_count(&self) -> usize {
        self.inner.subscribers.load().len()
    }
}

impl TxDispatcherInner {
    /// 按 ID 超找并移除 subscriber，成功返回 `true`。
    fn unregister_by_id(&self, id: u64) -> bool {
        let old = self.subscribers.load();
        if let Some(sub) = old.get(&id) {
            let name = sub.name();
            let mut new = (**old).clone();
            new.remove(&id);
            let count = new.len();
            self.subscribers.store(Arc::new(new));

            info!(
                "✅ Subscriber 已取消注册 [{}] ID: {}，当前剩余 {} 个 subscriber",
                name, id, count
            );
            true
        } else {
            false
        }
    }
}

impl TxDispatcher {
    /// 将一笔交易广播给所有已注册的 subscriber。
    ///
    /// 每个 subscriber 在独立的 tokio 任务中并发运行 `interested` 和 `on_tx`。
    /// 返回 `None` 的 subscriber 会被自动移除。
    pub async fn dispatch(&self, tx: Arc<TransactionFormat>) {
        let subs = self.inner.subscribers.load();

        // 为每个 subscriber 独立 spawn 任务
        for (id, sub) in subs.iter() {
            let id = *id;
            let sub = sub.clone();
            let tx = tx.clone();
            let dispatcher = self.inner.clone();

            tokio::spawn(async move {
                match sub.interested(&tx).await {
                    Some(true) => {
                        // 感兴趣，spawn on_tx 处理
                        tokio::spawn(async move {
                            sub.on_tx(tx).await;
                        });
                    }
                    Some(false) => {
                        // 不感兴趣，跳过
                    }
                    None => {
                        // 请求注销，立即执行
                        dispatcher.unregister_by_id(id);
                    }
                }
            });
        }
    }

    /// 启动 gRPC 监听循环，自动重连直到进程退出。
    ///
    /// 使用指数退避（最大 60s）应对网络中断。
    /// gRPC URL 从环境变量 `YELLOWSTONE_GRPC_URL` 读取，token 从 `YELLOWSTONE_GRPC_TOKEN` 读取（可选）。
    pub async fn run(&self) {
        let url = std::env::var("YELLOWSTONE_GRPC_URL").expect("YELLOWSTONE_GRPC_URL must be set");
        let token = std::env::var("YELLOWSTONE_GRPC_TOKEN").ok();

        if let Some(ref t) = token {
            info!("✅ 使用 Yellowstone token 认证: {}", &t[..8]);
        } else {
            warn!("⚠️ 未设置 YELLOWSTONE_GRPC_TOKEN，请确保您使用的节点无需认证");
        }

        let mut reconnect_delay = tokio::time::Duration::from_secs(1);
        let max_reconnect_delay = tokio::time::Duration::from_secs(60);

        loop {
            info!("🔗 正在连接到 Yellowstone gRPC: {}", url);

            match self.run_once(&url, token.as_deref()).await {
                Ok(_) => {
                    warn!("⚠️ gRPC 流正常结束，准备重连...");
                    reconnect_delay = tokio::time::Duration::from_secs(1);
                }
                Err(e) => {
                    error!("❌ gRPC 连接错误: {}, 等待 {:?} 后重连", e, reconnect_delay);
                    tokio::time::sleep(reconnect_delay).await;

                    // 指数退避，最多到 max_reconnect_delay
                    reconnect_delay = (reconnect_delay * 2).min(max_reconnect_delay);
                }
            }

            info!("🔄 准备重新连接...");
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }

    /// 建立一次性 gRPC 订阅并持续处理直到流结束或出错。
    async fn run_once(
        &self,
        url: &str,
        token: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let grpc = YellowstoneGrpc::new(url.to_string(), token.map(|s| s.to_string()));
        let client = grpc
            .build_client()
            .await
            .map_err(|e| format!("Failed to build gRPC client: {:?}", e))?;

        // 获取账户过滤器
        let account_filters = self.inner.account_filters.load();
        let account_include = match account_filters.as_ref() {
            Some(accounts) => {
                info!("📋 使用账户过滤器，共 {} 个账户", accounts.len());
                accounts.clone()
            }
            None => {
                info!("🌐 订阅全链交易（无账户过滤）");
                vec![]
            }
        };

        // 构建交易订阅过滤器（在 account_change 重发时需要复用）
        let tx_filter = SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include,
            account_exclude: vec![],
            account_required: vec![],
        };

        let build_account_subs = |inner: &TxDispatcherInner| -> std::collections::HashMap<String, yellowstone_grpc_proto::geyser::SubscribeRequestFilterAccounts> {
            let subs = &inner.account_subs;
            let addrs: Vec<String> = subs.active_addresses().iter().map(|a| a.to_string()).collect();
            if addrs.is_empty() {
                std::collections::HashMap::new()
            } else {
                std::collections::HashMap::from([(
                    "acc".to_string(),
                    yellowstone_grpc_proto::geyser::SubscribeRequestFilterAccounts {
                        account: addrs,
                        owner: vec![],
                        filters: vec![],
                        nonempty_txn_signature: None,
                    },
                )])
            }
        };

        let subscribe_request = SubscribeRequest {
            transactions: std::collections::HashMap::from([(
                "trade-monitor".to_string(),
                tx_filter.clone(),
            )]),
            accounts: build_account_subs(&self.inner),
            commitment: Some(CommitmentLevel::Processed.into()),
            ..Default::default()
        };

        let (mut subscribe_tx, mut stream) = client
            .lock()
            .await
            .subscribe_with_request(Some(subscribe_request))
            .await?;

        info!("✅ gRPC 订阅成功，开始监听交易+账户");

        loop {
            tokio::select! {
                message = stream.next() => {
                    match message {
                        Some(Ok(msg)) => match msg.update_oneof {
                            Some(UpdateOneof::Transaction(sut)) => {
                                let tx: TransactionFormat = sut.into();
                                self.dispatch(Arc::new(tx)).await;
                            }
                            Some(UpdateOneof::Ping(_)) => {
                                let _ = subscribe_tx
                                    .send(SubscribeRequest {
                                        ping: Some(SubscribeRequestPing { id: 1 }),
                                        ..Default::default()
                                    })
                                    .await;
                            }
                            Some(UpdateOneof::Account(acct)) => {
                                if let Some(info) = acct.account {
                                    if let Ok(addr) = Pubkey::try_from(info.pubkey) {
                                        self.inner.account_subs.update_and_fire(&addr, info.data, 0);
                                    }
                                }
                            }
                            _ => {}
                        },
                        Some(Err(error)) => {
                            return Err(format!("gRPC stream error: {:?}", error).into());
                        }
                        None => {
                            warn!("gRPC stream ended");
                            return Ok(());
                        }
                    }
                }
                _ = self.inner.account_change_notify.notified() => {
                    // 重发订阅请求时必须同时保留 transactions 和 accounts，
                    // 否则 Yellowstone gRPC 替换模式下会丢失其中一个订阅。
                    let updated_accounts = build_account_subs(&self.inner);
                    let _ = subscribe_tx
                        .send(SubscribeRequest {
                            transactions: std::collections::HashMap::from([(
                                "trade-monitor".to_string(),
                                tx_filter.clone(),
                            )]),
                            accounts: updated_accounts,
                            ..Default::default()
                        })
                        .await;
                }
            }
        }
    }
}

