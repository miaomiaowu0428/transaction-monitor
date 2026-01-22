use transaction_monitor::tx_monitor_task;

#[tokio::main]
async fn main(){
        rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider())
        .unwrap();
    dotenvy::dotenv().ok();
    utils::init_logger();
    tx_monitor_task().await;
}