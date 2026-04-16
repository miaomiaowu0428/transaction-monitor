//! # transaction-monitor
//!
//! 基于 Yellowstone gRPC 的实时链上交易监控框架。
//!
//! # 核心概念
//!
//! - [`tx_dispatcher::TxDispatcher`]：gRPC 连接管理与交易分发器，支持多 subscriber 并发处理
//! - [`tx_subscriber::TxSubscriber`]：subscriber trait，实现后注册到 dispatcher 即可接收交易
//! - [`tx_subscriber::scatter_gather`]：散聚分析订阅器，追踪地址间的 SOL 转账关系图
//! - [`get_global_dispatcher`]：获取进程内全局 dispatcher 单例
//!
//! # 快速开始
//!
//! ```rust,ignore
//! use transaction_monitor::get_global_dispatcher;
//!
//! let disp = get_global_dispatcher();
//! disp.with_account_filters(vec![my_pubkey]);
//! let _handle = disp.register_with_handle(Arc::new(my_subscriber));
//! disp.run().await;
//! ```

pub mod tx_dispatcher;
pub mod tx_subscriber;

use std::sync::LazyLock;
use tx_dispatcher::TxDispatcher;

/// 进程内全局 [`TxDispatcher`] 单例，用于在任意位置注册/注销 subscriber。
static GLOBAL_DISPATCHER: LazyLock<TxDispatcher> = LazyLock::new(|| TxDispatcher::new());

/// 获取全局 dispatcher 引用
///
/// # 示例
/// ```rust,ignore
/// use transaction_monitor::get_global_dispatcher;
///
/// let handle = get_global_dispatcher().register_with_handle(Arc::new(my_subscriber));
/// // ... 使用
/// // handle drop 时自动注销
/// ```
pub fn get_global_dispatcher() -> &'static TxDispatcher {
    &GLOBAL_DISPATCHER
}
