pub mod monitor_task;
pub mod tx_dispatcher;
pub mod tx_subscriber;

use std::sync::LazyLock;
use tx_dispatcher::TxDispatcher;

/// 全局 TxDispatcher 实例
/// 用于在应用程序的任何地方动态注册/注销 subscriber
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
