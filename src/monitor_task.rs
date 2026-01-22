use futures::{SinkExt, StreamExt};
use grpc_client::TransactionFormat;
use grpc_client::YellowstoneGrpc;
use log::info;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
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
                    if tx.account_keys.contains(&Pubkey::default()) {
                        panic!("found tx: {}", tx.signature)
                    }
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
