//! subscriber trait 定义及示例实现。
//!
//! 实现 [`TxSubscriber`] trait 并注册到 [`crate::tx_dispatcher::TxDispatcher`]
//! 即可接收并处理实时交易。

use grpc_client::TransactionFormat;
use std::sync::Arc;
use utils::tx_ctx::TxCtx;

/// 注册时缓存的 subscriber 元数据，dispatch 热路径避免虚函数调用。
#[derive(Clone)]
pub(crate) struct SubscriberEntry {
    pub sub: Arc<dyn TxSubscriber>,
    pub accept_failed: bool,
}

#[async_trait::async_trait]
pub trait TxSubscriber: Send + Sync + 'static {
    /// 模块名字，仅用于日志
    fn name(&self) -> &'static str;

    /// 是否接收失败交易。默认 false。需要失败 tx 的 subscriber 覆盖为 true。
    fn accept_failed(&self) -> bool {
        false
    }

    /// 是否对这笔交易感兴趣（必须非常快）
    ///
    /// 返回值：
    /// - Some(true): 感兴趣，会调用 on_tx
    /// - Some(false): 不感兴趣，跳过
    /// - None: 请求注销，dispatcher 会移除此 subscriber
    async fn interested(&self, tx: &TransactionFormat) -> Option<bool>;

    /// 真正的处理逻辑。
    ///
    /// `ctx` 由 dispatcher **每笔交易只创建一次**，然后 `Arc` 分发给所有命中的
    /// subscriber。需要指令时调 [`TxCtx::instructions`]：首次访问计算，之后共享，
    /// 同一笔交易最多展平一次（改造前是每个 subscriber 各展平一遍）。
    ///
    /// 只要交易本体时可以直接用 `ctx.transaction` / `ctx.meta` / `ctx.slot`
    /// （[`TxCtx`] 有 `Deref<Target = TransactionFormat>`）。
    async fn on_tx(self: Arc<Self>, ctx: Arc<TxCtx>);
}
