use grpc_client::TransactionFormat;
use std::sync::Arc;

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

use arc_swap::ArcSwap;

pub struct TxDispatcher {
    subscribers: ArcSwap<Vec<Arc<dyn TxSubscriber>>>,
    account_filters: ArcSwap<Option<Vec<String>>>,
}

impl Default for TxDispatcher {
    fn default() -> Self {
        Self {
            subscribers: ArcSwap::from_pointee(Vec::new()),
            account_filters: ArcSwap::from_pointee(None),
        }
    }
}

impl TxDispatcher {
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
        self.account_filters.store(Arc::new(Some(account_strings)));
        info!(
            "✅ TxDispatcher 账户过滤器已设置，共 {} 个账户",
            accounts.len()
        );
        self
    }

    /// 清除账户过滤器，订阅全链交易（如果节点支持）
    pub fn clear_account_filters(&self) -> &Self {
        self.account_filters.store(Arc::new(None));
        info!("🔄 TxDispatcher 账户过滤器已清除，订阅全链交易");
        self
    }

    pub fn register(&self, sub: Arc<dyn TxSubscriber>) -> &Self {
        // 克隆当前 Vec
        let old = self.subscribers.load(); // load_full() 会返回 Arc<Vec<_>>
        let mut new = (**old).clone(); // 先克隆 Vec
        new.push(sub); // 现在可以 mut
        self.subscribers.store(Arc::new(new)); // 存回 ArcSwap
        self
    }
}

impl TxDispatcher {
    pub async fn dispatch(&self, tx: Arc<TransactionFormat>) {
        let subs = self.subscribers.load();

        for sub in subs.iter() {
            if sub.interested(&tx).await {
                sub.clone().on_tx(tx.clone()).await;
            }
        }
    }

    pub async fn run(&self) {
        let url = std::env::var("YELLOWSTONE_GRPC_URL").expect("YELLOWSTONE_GRPC_URL must be set");
        let token = std::env::var("YELLOWSTONE_GRPC_TOKEN").ok();

        if let Some(ref t) = token {
            info!("✅ 使用 Yellowstone token 认证");
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
        let account_filters = self.account_filters.load();
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

        let subscribe_request = SubscribeRequest {
            transactions: HashMap::from([(
                "trade-monitor".to_string(),
                SubscribeRequestFilterTransactions {
                    vote: Some(false),
                    failed: Some(false),
                    signature: None,
                    account_include,
                    account_exclude: vec![],
                    account_required: vec![],
                },
            )]),
            commitment: Some(CommitmentLevel::Processed.into()),
            ..Default::default()
        };

        let (mut subscribe_tx, mut stream) = client
            .lock()
            .await
            .subscribe_with_request(Some(subscribe_request))
            .await?;

        info!("✅ gRPC 订阅成功，开始监听交易");

        while let Some(message) = stream.next().await {
            match message {
                Ok(msg) => match msg.update_oneof {
                    Some(UpdateOneof::Transaction(sut)) => {
                        let tx: TransactionFormat = sut.into();
                        self.dispatch(Arc::new(tx));
                    }
                    Some(UpdateOneof::Ping(_)) => {
                        let _ = subscribe_tx
                            .send(SubscribeRequest {
                                ping: Some(SubscribeRequestPing { id: 1 }),
                                ..Default::default()
                            })
                            .await;
                    }
                    _ => {}
                },
                Err(error) => {
                    return Err(format!("gRPC stream error: {:?}", error).into());
                }
            }
        }

        Ok(())
    }
}
