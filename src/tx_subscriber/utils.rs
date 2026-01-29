//! Utility functions for easy integration of scatter-gather monitoring
//!
//! This module provides convenience functions to simplify common tasks:
//! - Converting trace results to address lists
//! - Creating pre-configured monitoring sessions
//! - Event filtering and handling helpers

use super::scatter_gather::{ScatterGatherSubscriber, TraceConfig, TraceEvent};
use log::{info, warn};
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::mpsc;

/// Simple event handler that logs all events
pub async fn log_all_events(mut event_rx: mpsc::UnboundedReceiver<TraceEvent>) {
    while let Some(event) = event_rx.recv().await {
        match event {
            TraceEvent::NewAddress {
                address,
                depth,
                source,
            } => {
                info!(
                    "📍 New address at depth {}: {} (from: {:?})",
                    depth, address, source
                );
            }

            TraceEvent::Transfer {
                from,
                to,
                amount,
                signature,
                slot,
            } => {
                info!(
                    "💸 {} -> {} ({:.6} SOL) [slot: {}, sig: {}]",
                    truncate_address(&from),
                    truncate_address(&to),
                    amount,
                    slot,
                    truncate_signature(&signature)
                );
            }

            TraceEvent::Convergence {
                address,
                incoming_count,
                sources,
            } => {
                info!(
                    "🎯 CONVERGENCE: {} with {} sources",
                    address, incoming_count
                );
                info!("   Top sources:");
                for (i, src) in sources.iter().take(5).enumerate() {
                    info!("     {}. {}", i + 1, src);
                }
                if sources.len() > 5 {
                    info!("     ... and {} more", sources.len() - 5);
                }
                info!("");
            }

            TraceEvent::Scatter {
                address,
                outgoing_count,
                destinations,
            } => {
                info!("");
                info!("💥 SCATTER: {} to {} destinations", address, outgoing_count);
                info!("   Top destinations:");
                for (i, dst) in destinations.iter().take(5).enumerate() {
                    info!("     {}. {}", i + 1, dst);
                }
                if destinations.len() > 5 {
                    info!("     ... and {} more", destinations.len() - 5);
                }
                info!("");
            }

            TraceEvent::LimitReached { current, limit } => {
                warn!("⚠️  Address limit reached: {}/{}", current, limit);
            }
        }
    }
}

/// Event handler that filters for high-priority events only
pub async fn handle_high_priority_events(
    mut event_rx: mpsc::UnboundedReceiver<TraceEvent>,
    convergence_threshold: usize,
    scatter_threshold: usize,
) {
    while let Some(event) = event_rx.recv().await {
        match event {
            TraceEvent::Convergence {
                address,
                incoming_count,
                sources,
            } if incoming_count >= convergence_threshold => {
                warn!(
                    "🚨 HIGH-PRIORITY CONVERGENCE: {} ({} sources)",
                    address, incoming_count
                );
                // Here you could trigger alerts, store to DB, etc.
            }

            TraceEvent::Scatter {
                address,
                outgoing_count,
                destinations,
            } if outgoing_count >= scatter_threshold => {
                warn!(
                    "🚨 HIGH-PRIORITY SCATTER: {} ({} destinations)",
                    address, outgoing_count
                );
                // Here you could trigger alerts, store to DB, etc.
            }

            _ => {
                // Ignore low-priority events
            }
        }
    }
}

/// Event handler with callbacks
pub struct EventHandler<F1, F2, F3>
where
    F1: Fn(Pubkey, usize, Vec<Pubkey>) + Send + 'static,
    F2: Fn(Pubkey, usize, Vec<Pubkey>) + Send + 'static,
    F3: Fn(Pubkey, Pubkey, f64, String, u64) + Send + 'static,
{
    on_convergence: F1,
    on_scatter: F2,
    on_transfer: F3,
}

impl<F1, F2, F3> EventHandler<F1, F2, F3>
where
    F1: Fn(Pubkey, usize, Vec<Pubkey>) + Send + 'static,
    F2: Fn(Pubkey, usize, Vec<Pubkey>) + Send + 'static,
    F3: Fn(Pubkey, Pubkey, f64, String, u64) + Send + 'static,
{
    pub fn new(on_convergence: F1, on_scatter: F2, on_transfer: F3) -> Self {
        Self {
            on_convergence,
            on_scatter,
            on_transfer,
        }
    }

    pub async fn run(self, mut event_rx: mpsc::UnboundedReceiver<TraceEvent>) {
        while let Some(event) = event_rx.recv().await {
            match event {
                TraceEvent::Convergence {
                    address,
                    incoming_count,
                    sources,
                } => {
                    (self.on_convergence)(address, incoming_count, sources);
                }

                TraceEvent::Scatter {
                    address,
                    outgoing_count,
                    destinations,
                } => {
                    (self.on_scatter)(address, outgoing_count, destinations);
                }

                TraceEvent::Transfer {
                    from,
                    to,
                    amount,
                    signature,
                    slot,
                } => {
                    (self.on_transfer)(from, to, amount, signature, slot);
                }

                _ => {}
            }
        }
    }
}

/// Helper to truncate addresses for display
pub fn truncate_address(address: &Pubkey) -> String {
    let s = address.to_string();
    if s.len() > 12 {
        format!("{}...{}", &s[..6], &s[s.len() - 6..])
    } else {
        s
    }
}

/// Helper to truncate signatures for display
pub fn truncate_signature(sig: &str) -> String {
    if sig.len() > 16 {
        format!("{}...{}", &sig[..8], &sig[sig.len() - 8..])
    } else {
        sig.to_string()
    }
}

/// Quick setup: create and start monitoring in one call
pub async fn quick_monitor(
    addresses: Vec<Pubkey>,
    config: TraceConfig,
) -> Arc<ScatterGatherSubscriber> {
    let (subscriber, events) = ScatterGatherSubscriber::new(config);
    let subscriber = Arc::new(subscriber);

    subscriber.add_watch_addresses(addresses);

    // Spawn default event logger
    tokio::spawn(log_all_events(events));

    subscriber
}

/// Create a monitoring session that collects events into vectors
pub fn create_collector() -> (
    Arc<ScatterGatherSubscriber>,
    EventCollector,
    mpsc::UnboundedReceiver<TraceEvent>,
) {
    let (subscriber, event_rx) = ScatterGatherSubscriber::new(TraceConfig::default());

    let collector = EventCollector::new();

    (Arc::new(subscriber), collector, event_rx)
}

/// Collects events for later analysis
#[derive(Clone)]
pub struct EventCollector {
    pub convergences: Arc<std::sync::RwLock<Vec<ConvergenceEvent>>>,
    pub scatters: Arc<std::sync::RwLock<Vec<ScatterEvent>>>,
    pub transfers: Arc<std::sync::RwLock<Vec<TransferEvent>>>,
}

#[derive(Debug, Clone)]
pub struct ConvergenceEvent {
    pub address: Pubkey,
    pub incoming_count: usize,
    pub sources: Vec<Pubkey>,
    pub timestamp: SystemTime,
}

#[derive(Debug, Clone)]
pub struct ScatterEvent {
    pub address: Pubkey,
    pub outgoing_count: usize,
    pub destinations: Vec<Pubkey>,
    pub timestamp: SystemTime,
}

#[derive(Debug, Clone)]
pub struct TransferEvent {
    pub from: Pubkey,
    pub to: Pubkey,
    pub amount: f64,
    pub signature: String,
    pub slot: u64,
    pub timestamp: SystemTime,
}

impl EventCollector {
    pub fn new() -> Self {
        Self {
            convergences: Arc::new(std::sync::RwLock::new(Vec::new())),
            scatters: Arc::new(std::sync::RwLock::new(Vec::new())),
            transfers: Arc::new(std::sync::RwLock::new(Vec::new())),
        }
    }

    /// Start collecting events
    pub async fn collect(&self, mut event_rx: mpsc::UnboundedReceiver<TraceEvent>) {
        while let Some(event) = event_rx.recv().await {
            match event {
                TraceEvent::Convergence {
                    address,
                    incoming_count,
                    sources,
                } => {
                    self.convergences.write().unwrap().push(ConvergenceEvent {
                        address,
                        incoming_count,
                        sources,
                        timestamp: SystemTime::now(),
                    });
                }

                TraceEvent::Scatter {
                    address,
                    outgoing_count,
                    destinations,
                } => {
                    self.scatters.write().unwrap().push(ScatterEvent {
                        address,
                        outgoing_count,
                        destinations,
                        timestamp: SystemTime::now(),
                    });
                }

                TraceEvent::Transfer {
                    from,
                    to,
                    amount,
                    signature,
                    slot,
                } => {
                    self.transfers.write().unwrap().push(TransferEvent {
                        from,
                        to,
                        amount,
                        signature,
                        slot,
                        timestamp: SystemTime::now(),
                    });
                }

                _ => {}
            }
        }
    }

    /// Get all collected convergence events
    pub fn get_convergences(&self) -> Vec<ConvergenceEvent> {
        self.convergences.read().unwrap().clone()
    }

    /// Get all collected scatter events
    pub fn get_scatters(&self) -> Vec<ScatterEvent> {
        self.scatters.read().unwrap().clone()
    }

    /// Get all collected transfer events
    pub fn get_transfers(&self) -> Vec<TransferEvent> {
        self.transfers.read().unwrap().clone()
    }

    /// Get summary statistics
    pub fn summary(&self) -> CollectorSummary {
        let convergences = self.convergences.read().unwrap();
        let scatters = self.scatters.read().unwrap();
        let transfers = self.transfers.read().unwrap();

        CollectorSummary {
            total_convergences: convergences.len(),
            total_scatters: scatters.len(),
            total_transfers: transfers.len(),
            max_convergence_degree: convergences
                .iter()
                .map(|c| c.incoming_count)
                .max()
                .unwrap_or(0),
            max_scatter_degree: scatters.iter().map(|s| s.outgoing_count).max().unwrap_or(0),
            total_volume: transfers.iter().map(|t| t.amount).sum(),
        }
    }

    /// Clear all collected events
    pub fn clear(&self) {
        self.convergences.write().unwrap().clear();
        self.scatters.write().unwrap().clear();
        self.transfers.write().unwrap().clear();
    }
}

impl Default for EventCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct CollectorSummary {
    pub total_convergences: usize,
    pub total_scatters: usize,
    pub total_transfers: usize,
    pub max_convergence_degree: usize,
    pub max_scatter_degree: usize,
    pub total_volume: f64,
}

/// Address set builder helper
pub struct AddressSetBuilder {
    addresses: HashSet<Pubkey>,
}

impl AddressSetBuilder {
    pub fn new() -> Self {
        Self {
            addresses: HashSet::new(),
        }
    }

    /// Add a single address
    pub fn add(mut self, address: Pubkey) -> Self {
        self.addresses.insert(address);
        self
    }

    /// Add multiple addresses
    pub fn add_many(mut self, addresses: Vec<Pubkey>) -> Self {
        self.addresses.extend(addresses);
        self
    }

    /// Add from string (with error handling)
    pub fn add_str(mut self, address_str: &str) -> Self {
        if let Ok(addr) = address_str.parse::<Pubkey>() {
            self.addresses.insert(addr);
        } else {
            warn!("Invalid address string: {}", address_str);
        }
        self
    }

    /// Add from file (one address per line)
    pub fn add_from_file(mut self, path: &str) -> std::io::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        for line in content.lines() {
            let line = line.trim();
            if !line.is_empty() && !line.starts_with('#') {
                if let Ok(addr) = line.parse::<Pubkey>() {
                    self.addresses.insert(addr);
                } else {
                    warn!("Skipping invalid address in file: {}", line);
                }
            }
        }
        Ok(self)
    }

    /// Build the final set
    pub fn build(self) -> Vec<Pubkey> {
        self.addresses.into_iter().collect()
    }

    /// Get count without consuming
    pub fn len(&self) -> usize {
        self.addresses.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.addresses.is_empty()
    }
}

impl Default for AddressSetBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics tracker for monitoring performance
#[derive(Debug, Clone)]
pub struct PerformanceTracker {
    start_time: SystemTime,
    event_count: Arc<std::sync::atomic::AtomicUsize>,
    convergence_count: Arc<std::sync::atomic::AtomicUsize>,
    scatter_count: Arc<std::sync::atomic::AtomicUsize>,
}

impl PerformanceTracker {
    pub fn new() -> Self {
        Self {
            start_time: SystemTime::now(),
            event_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            convergence_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            scatter_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// Track an event
    pub fn track_event(&self, event: &TraceEvent) {
        use std::sync::atomic::Ordering;

        self.event_count.fetch_add(1, Ordering::Relaxed);

        match event {
            TraceEvent::Convergence { .. } => {
                self.convergence_count.fetch_add(1, Ordering::Relaxed);
            }
            TraceEvent::Scatter { .. } => {
                self.scatter_count.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Get current statistics
    pub fn get_stats(&self) -> PerformanceStats {
        use std::sync::atomic::Ordering;

        let elapsed = SystemTime::now()
            .duration_since(self.start_time)
            .unwrap_or(Duration::from_secs(0));

        let total_events = self.event_count.load(Ordering::Relaxed);

        PerformanceStats {
            elapsed,
            total_events,
            convergence_count: self.convergence_count.load(Ordering::Relaxed),
            scatter_count: self.scatter_count.load(Ordering::Relaxed),
            events_per_second: if elapsed.as_secs() > 0 {
                total_events as f64 / elapsed.as_secs() as f64
            } else {
                0.0
            },
        }
    }

    /// Print statistics
    pub fn print_stats(&self) {
        let stats = self.get_stats();
        info!("📊 Performance Stats:");
        info!("   Elapsed: {:?}", stats.elapsed);
        info!("   Total events: {}", stats.total_events);
        info!("   Convergences: {}", stats.convergence_count);
        info!("   Scatters: {}", stats.scatter_count);
        info!("   Events/sec: {:.2}", stats.events_per_second);
    }
}

impl Default for PerformanceTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceStats {
    pub elapsed: Duration,
    pub total_events: usize,
    pub convergence_count: usize,
    pub scatter_count: usize,
    pub events_per_second: f64,
}

/// Periodic stats printer
pub async fn periodic_stats_printer(subscriber: Arc<ScatterGatherSubscriber>, interval_secs: u64) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

    loop {
        interval.tick().await;
        let stats = subscriber.get_stats();

        info!("📊 Periodic Stats:");
        info!("   Addresses: {}", stats.total_addresses);
        info!("   Edges: {}", stats.total_edges);
        info!("   Convergence points: {}", stats.convergence_points);
        info!("   Scatter points: {}", stats.scatter_points);
        info!(
            "   Max degrees: in={}, out={}",
            stats.max_incoming, stats.max_outgoing
        );
        info!("");
    }
}

/// Helper: Load addresses from a text file (one per line)
pub fn load_addresses_from_file(path: &str) -> std::io::Result<Vec<Pubkey>> {
    let addresses = AddressSetBuilder::new().add_from_file(path)?.build();
    Ok(addresses)
}

/// Helper: Save addresses to a text file (one per line)
pub fn save_addresses_to_file(addresses: &[Pubkey], path: &str) -> std::io::Result<()> {
    let content: String = addresses
        .iter()
        .map(|addr| addr.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    std::fs::write(path, content)
}

/// Helper: Deduplicate addresses
pub fn deduplicate_addresses(addresses: Vec<Pubkey>) -> Vec<Pubkey> {
    let set: HashSet<_> = addresses.into_iter().collect();
    set.into_iter().collect()
}

/// Helper: Filter addresses by activity (requires historical check)
pub fn filter_recent_addresses(
    addresses: Vec<Pubkey>,
    first_seen_times: &std::collections::HashMap<Pubkey, SystemTime>,
    max_age: Duration,
) -> Vec<Pubkey> {
    let now = SystemTime::now();

    addresses
        .into_iter()
        .filter(|addr| {
            if let Some(first_seen) = first_seen_times.get(addr) {
                now.duration_since(*first_seen).unwrap_or(Duration::MAX) <= max_age
            } else {
                true // Include if we don't have timing info
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_address_set_builder() {
        let addr1 = Pubkey::new_unique();
        let addr2 = Pubkey::new_unique();

        let addresses = AddressSetBuilder::new()
            .add(addr1)
            .add(addr2)
            .add(addr1) // Duplicate
            .build();

        assert_eq!(addresses.len(), 2);
    }

    #[test]
    fn test_truncate_address() {
        let addr = Pubkey::new_unique();
        let truncated = truncate_address(&addr);
        assert!(truncated.contains("..."));
        assert!(truncated.len() < addr.to_string().len());
    }

    #[test]
    fn test_truncate_signature() {
        let sig = "5YNmS1R9nNSCDzb1a7yJSXGpDKx8hqTLKDG2vV9AZu9h";
        let truncated = truncate_signature(sig);
        assert!(truncated.contains("..."));
    }

    #[test]
    fn test_deduplicate() {
        let addr1 = Pubkey::new_unique();
        let addr2 = Pubkey::new_unique();

        let addresses = vec![addr1, addr2, addr1, addr2, addr1];
        let deduped = deduplicate_addresses(addresses);

        assert_eq!(deduped.len(), 2);
    }

    #[test]
    fn test_event_collector() {
        let collector = EventCollector::new();

        // Initially empty
        assert_eq!(collector.get_convergences().len(), 0);

        // Add a convergence
        collector
            .convergences
            .write()
            .unwrap()
            .push(ConvergenceEvent {
                address: Pubkey::new_unique(),
                incoming_count: 5,
                sources: vec![],
                timestamp: SystemTime::now(),
            });

        assert_eq!(collector.get_convergences().len(), 1);

        // Check summary
        let summary = collector.summary();
        assert_eq!(summary.total_convergences, 1);
    }

    #[test]
    fn test_performance_tracker() {
        let tracker = PerformanceTracker::new();

        // Track some events
        tracker.track_event(&TraceEvent::Convergence {
            address: Pubkey::new_unique(),
            incoming_count: 5,
            sources: vec![],
        });

        let stats = tracker.get_stats();
        assert_eq!(stats.total_events, 1);
        assert_eq!(stats.convergence_count, 1);
    }

    #[tokio::test]
    async fn test_quick_monitor() {
        let addresses = vec![Pubkey::new_unique()];
        let config = TraceConfig::default();

        let subscriber = quick_monitor(addresses.clone(), config).await;

        let stats = subscriber.get_stats();
        assert_eq!(stats.total_addresses, addresses.len());
    }
}
