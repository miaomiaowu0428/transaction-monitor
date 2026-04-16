//! subscriber trait 定义及示例实现。
//!
//! 实现 [`TxSubscriber`] trait 并注册到 [`crate::tx_dispatcher::TxDispatcher`]
//! 即可接收并处理实时交易。

use grpc_client::TransactionFormat;
use std::sync::Arc;


#[async_trait::async_trait]
pub trait TxSubscriber: Send + Sync + 'static {
    /// 模块名字，仅用于日志
    fn name(&self) -> &'static str;

    /// 是否对这笔交易感兴趣（必须非常快）
    ///
    /// 返回值：
    /// - Some(true): 感兴趣，会调用 on_tx
    /// - Some(false): 不感兴趣，跳过
    /// - None: 请求注销，dispatcher 会移除此 subscriber
    async fn interested(&self, tx: &TransactionFormat) -> Option<bool>;

    /// 真正的处理逻辑
    async fn on_tx(self: Arc<Self>, tx: Arc<TransactionFormat>);
}
