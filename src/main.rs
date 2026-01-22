use std::sync::Arc;

use transaction_monitor::{tx_dispatcher::TxDispatcher, tx_subscriber::SubscriberDemo};

#[tokio::main]
async fn main() {
    rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider())
        .unwrap();
    dotenvy::dotenv().ok();
    utils::init_logger();
    TxDispatcher::new()
        .register(Arc::new(SubscriberDemo::new()))
        .run()
        .await;
}
