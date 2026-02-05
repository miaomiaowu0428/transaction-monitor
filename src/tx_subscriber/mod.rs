use grpc_client::TransactionFormat;

pub mod scatter_gather;
pub mod trace_integration;
pub mod utils;

#[async_trait::async_trait]
pub trait TxSubscriber: Send + Sync + 'static {
    /// 模块名字，仅用于日志
    fn name(&self) -> &'static str;

    /// 是否对这笔交易感兴趣（必须非常快）
    async fn interested(&self, tx: &TransactionFormat) -> bool;

    /// 真正的处理逻辑
    async fn on_tx(self: Arc<Self>, tx: Arc<TransactionFormat>);
}

use arc_swap::ArcSwap;
use log::info;
use rand::seq::IndexedRandom;
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashSet, sync::Arc};

pub struct SubscriberDemo {
    watch: ArcSwap<HashSet<Pubkey>>,
}
impl SubscriberDemo {
    pub fn new() -> Self {
        Self {
            watch: ArcSwap::new(Arc::new(HashSet::new())),
        }
    }
}

#[async_trait::async_trait]
impl TxSubscriber for SubscriberDemo {
    fn name(&self) -> &'static str {
        "subscriber demo"
    }

    async fn interested(&self, tx: &TransactionFormat) -> bool {
        let watch_set = self.watch.load(); // Arc<HashSet<_>>

        // 是否已有关注账户
        let res = tx.account_keys.iter().any(|k| {
            if watch_set.contains(k) {
                info!("Account {} is already in watchlist", k);
                true
            } else {
                false
            }
        });

        // 随机添加一个账户到 watchlist
        if let Some(random_account) = tx.account_keys.choose(&mut rand::rng()) {
            // 克隆当前 HashSet
            let mut new_set = (**watch_set).clone();
            new_set.insert(*random_account);

            // 存回 ArcSwap
            self.watch.store(Arc::new(new_set));

            info!(
                "[{}] Added account {} to watchlist",
                self.name(),
                random_account
            );
        }

        res
    }

    async fn on_tx(self: Arc<Self>, _tx: Arc<TransactionFormat>) {
        // 这里可以做处理或者直接 spawn tokio 任务异步处理
        info!("[{}] Received tx: {}", self.name(), _tx.signature);
        panic!("should panic");
    }
}
