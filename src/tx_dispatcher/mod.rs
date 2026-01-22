use grpc_client::TransactionFormat;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};

use grpc_client::YellowstoneGrpc;
use log::info;
use std::collections::HashMap;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeRequestPing,
    subscribe_update::UpdateOneof,
};

use crate::tx_subscriber::TxSubscriber;

use arc_swap::ArcSwap;

pub struct TxDispatcher {
    subscribers: ArcSwap<Vec<Arc<dyn TxSubscriber>>>,
}

impl Default for TxDispatcher {
    fn default() -> Self {
        Self {
            subscribers: ArcSwap::from_pointee(Vec::new()),
        }
    }
}

impl TxDispatcher {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn register(&self, sub: Arc<dyn TxSubscriber>) -> &Self {
        // 克隆当前 Vec
        let new = self.subscribers.load_full(); // load_full() 会返回 Arc<Vec<_>>
        let mut new_vec = (*new).clone(); // 先克隆 Vec
        new_vec.push(sub); // 现在可以 mut
        self.subscribers.store(Arc::new(new_vec)); // 存回 ArcSwap
        self
    }
}

impl TxDispatcher {
    pub fn dispatch(&self, tx: Arc<TransactionFormat>) {
        let subs = self.subscribers.load();

        for sub in subs.iter() {
            if sub.interested(&tx) {
                sub.on_tx(tx.clone());
            }
        }
    }

    pub async fn run(&self) {
        let url = std::env::var("YELLOWSTONE_GRPC_URL").expect("YELLOWSTONE_GRPC_URL must be set");
        info!("Connecting to Yellowstone gRPC at {}", url);
        let grpc = YellowstoneGrpc::new(url, None);
        let client = grpc.build_client().await.expect("grpc build failed");
        let subscribe_request = SubscribeRequest {
            transactions: HashMap::from([(
                "trade-monitor".to_string(),
                SubscribeRequestFilterTransactions {
                    vote: Some(false),
                    failed: Some(false),
                    signature: None,
                    account_include: vec![],
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
            .await
            .expect("subscribe_with_request failed");
        
        info!("subscribe_with_request succeeded");
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
                    info!("pump-migrate-monitor error: {:?}", error);
                    break;
                }
            }
        }
    }
}
