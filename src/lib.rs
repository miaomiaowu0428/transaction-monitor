#[cfg(feature = "shred")]
use crate::abc_manipulator::analyze_abc_pattern;
#[cfg(feature = "shred")]
use crate::shred::{
    commondef, commondef::ShredTxTemplate, lookuptable_handler::LookupTableMgr,
    shred_mgr::ShredClientMgr, shred_tx_parse, solana::SolanaWrapper,
};
use futures::{SinkExt, StreamExt};
use grpc_client::TransactionFormat;
use grpc_client::YellowstoneGrpc;
use log::error;
use log::info;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::VersionedTransaction;
use solana_sdk::{pubkey, pubkey::Pubkey};
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use std::sync::LazyLock;
use std::time::Instant;
use tokio::sync::RwLock;
#[cfg(feature = "shred")]
use tokio::task::JoinHandle;
use utils::IndexedInstruction;
use utils::SolToLamport;
use utils::flatten_instructions;
use utils::flatten_main_instructions;
use whirlwind::ShardMap;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeRequestPing,
    subscribe_update::UpdateOneof,
};

pub async fn tx_monitor_task() {
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

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => match msg.update_oneof {
                Some(UpdateOneof::Transaction(sut)) => {
                    let tx: TransactionFormat = sut.into();
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
