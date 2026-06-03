//! 账户数据订阅模块。
//!
//! - `on_account_update(addr, callback)` 返回 `AccountHandle`，drop 时自动退订
//! - `get_account(addr, if_match)` 从缓存读取最新数据
//!
//! gRPC 在首次订阅时推送当前数据，后续只推送变化。

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::sync::atomic::{AtomicU64, Ordering};

use solana_sdk::pubkey::Pubkey;

use super::TxDispatcherInner;

static ACCOUNT_HANDLE_ID: AtomicU64 = AtomicU64::new(0);

// ── AccountHandle ─────────────────────────────────────────────────────────

pub struct AccountHandle {
    id: u64,
    addr: Pubkey,
    dispatcher: Weak<TxDispatcherInner>,
}

impl AccountHandle {
    fn new(id: u64, addr: Pubkey, dispatcher: Weak<TxDispatcherInner>) -> Self {
        Self { id, addr, dispatcher }
    }
}

impl Drop for AccountHandle {
    fn drop(&mut self) {
        if let Some(inner) = self.dispatcher.upgrade() {
            inner.remove_account_entry(self.addr, self.id);
        }
    }
}

// ── 内部状态 ─────────────────────────────────────────────────────────────

#[derive(Default)]
struct AddrState {
    callbacks: HashMap<u64, Box<dyn Fn(&Pubkey, &[u8], u64) + Send + Sync + 'static>>,
    latest: Option<(Vec<u8>, u64)>,
    bare_sub_count: u64,
}

pub(crate) struct AccountSubs {
    states: Mutex<HashMap<Pubkey, AddrState>>,
}

impl AccountSubs {
    pub fn new() -> Self {
        Self { states: Mutex::new(HashMap::new()) }
    }

    pub fn active_addresses(&self) -> Vec<Pubkey> {
        self.states.lock().unwrap()
            .iter()
            .filter(|(_, s)| !s.callbacks.is_empty() || s.bare_sub_count > 0)
            .map(|(a, _)| *a)
            .collect()
    }

    pub fn update_and_fire(&self, addr: &Pubkey, data: Vec<u8>, slot: u64) {
        let mut states = self.states.lock().unwrap();
        if let Some(s) = states.get_mut(addr) {
            s.latest = Some((data.clone(), slot));
            for cb in s.callbacks.values() {
                cb(addr, &data, slot);
            }
        }
    }

    pub fn get_latest(&self, addr: &Pubkey) -> Option<(Vec<u8>, u64)> {
        self.states.lock().unwrap()
            .get(addr)
            .and_then(|s| s.latest.clone())
    }
}

// ── TxDispatcher 公开 API ────────────────────────────────────────────────

impl crate::tx_dispatcher::TxDispatcher {
    /// 注册账户更新回调。handle drop 时自动移除。
    pub fn on_account_update<F>(&self, addr: &Pubkey, callback: F) -> AccountHandle
    where F: Fn(&Pubkey, &[u8], u64) + Send + Sync + 'static
    {
        let id = ACCOUNT_HANDLE_ID.fetch_add(1, Ordering::SeqCst);
        {
            let subs = &self.inner.account_subs;
            let mut states = subs.states.lock().unwrap();
            states.entry(*addr).or_default().callbacks.insert(id, Box::new(callback));
        }
        self.inner.notify_account_change();
        AccountHandle::new(id, *addr, Arc::downgrade(&self.inner))
    }

    /// 纯订阅（无回调），从缓存读取时用。handle drop 自动退订。
    pub fn subscribe_account(&self, addr: &Pubkey) -> AccountHandle {
        let id = ACCOUNT_HANDLE_ID.fetch_add(1, Ordering::SeqCst);
        {
            let subs = &self.inner.account_subs;
            let mut states = subs.states.lock().unwrap();
            states.entry(*addr).or_default().bare_sub_count += 1;
        }
        self.inner.notify_account_change();
        AccountHandle::new(id, *addr, Arc::downgrade(&self.inner))
    }

    /// 从缓存读取最近一次推送的账户数据。`if_match` 接收 `&[u8]` 返回 `Option<T>`。
    pub fn get_account<T>(&self, addr: &Pubkey, if_match: impl FnOnce(&[u8]) -> Option<T>) -> Option<T> {
        let subs = &self.inner.account_subs;
        let (data, _) = subs.get_latest(addr)?;
        if_match(&data)
    }
}

// ── TxDispatcherInner 内部 ───────────────────────────────────────────────

impl TxDispatcherInner {
    pub(crate) fn remove_account_entry(&self, addr: Pubkey, id: u64) {
        let mut states = self.account_subs.states.lock().unwrap();
        if let Some(s) = states.get_mut(&addr) {
            s.callbacks.remove(&id);
            s.bare_sub_count = s.bare_sub_count.saturating_sub(1);
            if s.callbacks.is_empty() && s.bare_sub_count == 0 {
                states.remove(&addr);
            }
        }
        // 通知 gRPC 流重新发送订阅请求
        self.account_change_notify.notify_one();
    }

    pub(crate) fn notify_account_change(&self) {
        self.account_change_notify.notify_one();
    }
}
