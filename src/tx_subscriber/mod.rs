use grpc_client::TransactionFormat;

pub trait TxSubscriber: Send + Sync + 'static {
    /// 模块名字，仅用于日志
    fn name(&self) -> &'static str;

    /// 是否对这笔交易感兴趣（必须非常快）
    fn interested(&self, tx: &TransactionFormat) -> bool;

    /// 真正的处理逻辑
    fn on_tx(&self, tx: Arc<TransactionFormat>);
}

use arc_swap::ArcSwap;
use log::info;
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

impl TxSubscriber for SubscriberDemo {
    fn name(&self) -> &'static str {
        "spam_detector"
    }

    fn interested(&self, tx: &TransactionFormat) -> bool {
        let set = self.watch.load();
        tx.account_keys.iter().any(|k| set.contains(k))
    }

    fn on_tx(&self, tx: Arc<TransactionFormat>) {
        info!("Received transaction: {:?}", tx);
        panic!("Not implemented")
    }
}
