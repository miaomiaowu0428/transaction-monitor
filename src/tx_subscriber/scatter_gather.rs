use arc_swap::ArcSwap;
use grpc_client::TransactionFormat;
use log::{debug, info, warn};
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::SystemTime,
};
use tokio::sync::mpsc;

use super::TxSubscriber;

/// Configuration for scatter-gather tracing
#[derive(Debug, Clone)]
pub struct TraceConfig {
    /// Minimum SOL amount to consider a transfer (filter spam)
    pub spam_threshold: f64,

    /// Minimum number of incoming edges to consider an address as a convergence point
    pub min_convergence_degree: usize,

    /// Maximum addresses to track (prevent memory explosion)
    pub max_tracked_addresses: usize,

    /// Whether to auto-expand monitoring when new transfers are detected
    pub auto_expand: bool,

    /// Maximum depth for auto-expansion (0 = no limit)
    pub max_expansion_depth: usize,
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            spam_threshold: 0.0001,
            min_convergence_degree: 5,
            max_tracked_addresses: 100_000,
            auto_expand: true,
            max_expansion_depth: 10,
        }
    }
}

/// Events emitted by the scatter-gather subscriber
#[derive(Debug, Clone)]
pub enum TraceEvent {
    /// A new address has been added to the watch list
    NewAddress {
        address: Pubkey,
        depth: usize,
        source: Option<Pubkey>,
    },

    /// A transfer was detected between monitored addresses
    Transfer {
        from: Pubkey,
        to: Pubkey,
        amount: f64,
        signature: String,
        slot: u64,
    },

    /// A convergence point was detected (multiple sources -> one destination)
    Convergence {
        address: Pubkey,
        incoming_count: usize,
        sources: Vec<Pubkey>,
    },

    /// A scatter point was detected (one source -> multiple destinations)
    Scatter {
        address: Pubkey,
        outgoing_count: usize,
        destinations: Vec<Pubkey>,
    },

    /// Maximum address limit reached
    LimitReached { current: usize, limit: usize },
}

/// Internal graph structure for tracking transfers
#[derive(Debug, Clone)]
pub struct TransferGraph {
    /// For each address: (incoming edges, outgoing edges)
    edges: HashMap<Pubkey, (HashSet<Pubkey>, HashSet<Pubkey>)>,

    /// First time each address was seen
    first_seen: HashMap<Pubkey, SystemTime>,

    /// Depth of each address from initial roots (0 = root)
    depths: HashMap<Pubkey, usize>,

    /// Detected convergence points
    convergence_points: HashSet<Pubkey>,

    /// Detected scatter points
    scatter_points: HashSet<Pubkey>,
}

impl TransferGraph {
    fn new() -> Self {
        Self {
            edges: HashMap::new(),
            first_seen: HashMap::new(),
            depths: HashMap::new(),
            convergence_points: HashSet::new(),
            scatter_points: HashSet::new(),
        }
    }

    fn add_edge(&mut self, from: Pubkey, to: Pubkey, now: SystemTime) {
        // Add to edges
        self.edges
            .entry(from)
            .or_insert_with(|| (HashSet::new(), HashSet::new()))
            .1
            .insert(to);

        self.edges
            .entry(to)
            .or_insert_with(|| (HashSet::new(), HashSet::new()))
            .0
            .insert(from);

        // Record first seen time
        self.first_seen.entry(from).or_insert(now);
        self.first_seen.entry(to).or_insert(now);
    }

    fn get_incoming_count(&self, address: &Pubkey) -> usize {
        self.edges
            .get(address)
            .map(|(incoming, _)| incoming.len())
            .unwrap_or(0)
    }

    fn get_outgoing_count(&self, address: &Pubkey) -> usize {
        self.edges
            .get(address)
            .map(|(_, outgoing)| outgoing.len())
            .unwrap_or(0)
    }

    fn get_incoming_addresses(&self, address: &Pubkey) -> Vec<Pubkey> {
        self.edges
            .get(address)
            .map(|(incoming, _)| incoming.iter().copied().collect())
            .unwrap_or_default()
    }

    fn get_outgoing_addresses(&self, address: &Pubkey) -> Vec<Pubkey> {
        self.edges
            .get(address)
            .map(|(_, outgoing)| outgoing.iter().copied().collect())
            .unwrap_or_default()
    }

    fn mark_convergence(&mut self, address: Pubkey) {
        self.convergence_points.insert(address);
    }

    fn mark_scatter(&mut self, address: Pubkey) {
        self.scatter_points.insert(address);
    }
}

/// Main scatter-gather subscriber
pub struct ScatterGatherSubscriber {
    /// Addresses currently being monitored (for fast lookup in interested())
    watch_set: ArcSwap<HashSet<Pubkey>>,

    /// Transfer graph (for tracking relationships)
    graph: Arc<RwLock<TransferGraph>>,

    /// Configuration
    config: TraceConfig,

    /// Event channel sender
    event_tx: mpsc::UnboundedSender<TraceEvent>,
}

impl ScatterGatherSubscriber {
    /// Create a new scatter-gather subscriber
    pub fn new(config: TraceConfig) -> (Self, mpsc::UnboundedReceiver<TraceEvent>) {
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let subscriber = Self {
            watch_set: ArcSwap::new(Arc::new(HashSet::new())),
            graph: Arc::new(RwLock::new(TransferGraph::new())),
            config,
            event_tx,
        };

        (subscriber, event_rx)
    }

    /// Add initial addresses to monitor (e.g., roots or starting points)
    pub fn add_watch_addresses(&self, addresses: Vec<Pubkey>) {
        let mut graph = self.graph.write().unwrap();
        let watch_set = self.watch_set.load();
        let mut new_set = (**watch_set).clone();

        for addr in addresses {
            if new_set.insert(addr) {
                // Initialize depth to 0 for manually added addresses
                graph.depths.entry(addr).or_insert(0);
                graph.first_seen.entry(addr).or_insert_with(SystemTime::now);

                let _ = self.event_tx.send(TraceEvent::NewAddress {
                    address: addr,
                    depth: 0,
                    source: None,
                });

                debug!("Added address {} to watch list", addr);
            }
        }

        self.watch_set.store(Arc::new(new_set));
    }

    /// Add a single address with depth tracking
    fn add_address_with_depth(
        &self,
        address: Pubkey,
        depth: usize,
        source: Option<Pubkey>,
    ) -> bool {
        let watch_set = self.watch_set.load();

        // Check limit
        if watch_set.len() >= self.config.max_tracked_addresses {
            if watch_set.len() == self.config.max_tracked_addresses {
                let _ = self.event_tx.send(TraceEvent::LimitReached {
                    current: watch_set.len(),
                    limit: self.config.max_tracked_addresses,
                });
                warn!(
                    "Address limit reached: {}",
                    self.config.max_tracked_addresses
                );
            }
            return false;
        }

        // Check if already exists
        if watch_set.contains(&address) {
            return false;
        }

        // Add to watch set
        let mut new_set = (**watch_set).clone();
        new_set.insert(address);
        self.watch_set.store(Arc::new(new_set));

        // Update graph
        let mut graph = self.graph.write().unwrap();
        graph.depths.entry(address).or_insert(depth);
        graph
            .first_seen
            .entry(address)
            .or_insert_with(SystemTime::now);

        drop(graph);

        let _ = self.event_tx.send(TraceEvent::NewAddress {
            address,
            depth,
            source,
        });

        debug!(
            "Added address {} at depth {} from {:?}",
            address, depth, source
        );
        true
    }

    /// Get a snapshot of the current graph
    pub fn get_graph_snapshot(&self) -> TransferGraph {
        self.graph.read().unwrap().clone()
    }

    /// Get all detected convergence points
    pub fn get_convergence_points(&self) -> Vec<Pubkey> {
        let graph = self.graph.read().unwrap();
        graph.convergence_points.iter().copied().collect()
    }

    /// Get all detected scatter points
    pub fn get_scatter_points(&self) -> Vec<Pubkey> {
        let graph = self.graph.read().unwrap();
        graph.scatter_points.iter().copied().collect()
    }

    /// Get statistics about current monitoring state
    pub fn get_stats(&self) -> MonitorStats {
        let watch_set = self.watch_set.load();
        let graph = self.graph.read().unwrap();

        MonitorStats {
            total_addresses: watch_set.len(),
            total_edges: graph.edges.values().map(|(_, out)| out.len()).sum(),
            convergence_points: graph.convergence_points.len(),
            scatter_points: graph.scatter_points.len(),
            max_incoming: graph
                .edges
                .values()
                .map(|(inc, _)| inc.len())
                .max()
                .unwrap_or(0),
            max_outgoing: graph
                .edges
                .values()
                .map(|(_, out)| out.len())
                .max()
                .unwrap_or(0),
        }
    }

    /// Process a SOL transfer within the transaction
    fn process_transfer(&self, from: Pubkey, to: Pubkey, amount: f64, tx: &TransactionFormat) {
        // Filter spam
        if amount < self.config.spam_threshold {
            return;
        }

        let now = SystemTime::now();
        let watch_set = self.watch_set.load();

        // Check if either address is in watch set
        let from_watched = watch_set.contains(&from);
        let to_watched = watch_set.contains(&to);

        if !from_watched && !to_watched {
            return; // Neither address is interesting
        }

        // Update graph
        let mut graph = self.graph.write().unwrap();
        graph.add_edge(from, to, now);

        // Get counts before releasing lock
        let incoming_count = graph.get_incoming_count(&to);
        let outgoing_count = graph.get_outgoing_count(&from);
        let from_depth = graph.depths.get(&from).copied().unwrap_or(0);

        // Check for convergence
        let is_new_convergence = if incoming_count >= self.config.min_convergence_degree {
            if graph.convergence_points.insert(to) {
                let sources = graph.get_incoming_addresses(&to);
                Some((to, incoming_count, sources))
            } else {
                None
            }
        } else {
            None
        };

        // Check for scatter
        let is_new_scatter = if outgoing_count >= self.config.min_convergence_degree {
            if graph.scatter_points.insert(from) {
                let destinations = graph.get_outgoing_addresses(&from);
                Some((from, outgoing_count, destinations))
            } else {
                None
            }
        } else {
            None
        };

        drop(graph);

        // Send events
        let _ = self.event_tx.send(TraceEvent::Transfer {
            from,
            to,
            amount,
            signature: tx.signature.to_string(),
            slot: tx.slot,
        });

        if let Some((addr, count, sources)) = is_new_convergence {
            info!(
                "🎯 Convergence detected at {} with {} incoming edges",
                addr, count
            );
            let _ = self.event_tx.send(TraceEvent::Convergence {
                address: addr,
                incoming_count: count,
                sources,
            });
        }

        if let Some((addr, count, destinations)) = is_new_scatter {
            info!(
                "💥 Scatter detected at {} with {} outgoing edges",
                addr, count
            );
            let _ = self.event_tx.send(TraceEvent::Scatter {
                address: addr,
                outgoing_count: count,
                destinations,
            });
        }

        // Auto-expand: add destination to watch list if from is watched
        if from_watched && !to_watched && self.config.auto_expand {
            let new_depth = from_depth + 1;

            // Check depth limit
            if self.config.max_expansion_depth == 0 || new_depth <= self.config.max_expansion_depth
            {
                self.add_address_with_depth(to, new_depth, Some(from));
            }
        }
    }

    /// Extract SOL transfers from a transaction
    fn extract_sol_transfers(&self, tx: &TransactionFormat) -> Vec<(Pubkey, Pubkey, f64)> {
        let mut transfers = Vec::new();

        // Get balances from meta
        let meta = match &tx.meta {
            Some(m) => m,
            None => {
                debug!("No meta in tx {}", tx.signature);
                return transfers;
            }
        };

        let pre_balances = &meta.pre_balances;
        let post_balances = &meta.post_balances;

        // Parse pre/post balances to find SOL transfers
        if pre_balances.len() != post_balances.len() || pre_balances.len() != tx.account_keys.len()
        {
            warn!(
                "Balance/account_keys length mismatch in tx {}",
                tx.signature
            );
            return transfers;
        }

        // Build balance changes map
        let mut balance_changes: HashMap<Pubkey, i64> = HashMap::new();
        for i in 0..tx.account_keys.len() {
            let key = tx.account_keys[i];
            let pre = pre_balances[i] as i64;
            let post = post_balances[i] as i64;
            let delta = post - pre;

            if delta != 0 {
                balance_changes.insert(key, delta);
            }
        }

        // Find pairs: negative (sender) and positive (receiver)
        let senders: Vec<_> = balance_changes
            .iter()
            .filter(|&(_, &delta)| delta < 0)
            .map(|(&k, &delta)| (k, -delta))
            .collect();

        let receivers: Vec<_> = balance_changes
            .iter()
            .filter(|&(_, &delta)| delta > 0)
            .map(|(&k, &delta)| (k, delta))
            .collect();

        // Simple heuristic: pair largest sender with largest receiver
        // (A more sophisticated approach would analyze instruction logs)
        for (sender, sent_lamports) in senders {
            for (receiver, received_lamports) in &receivers {
                // Approximate: transfer amount is min of sent/received
                let transfer_lamports = sent_lamports.min(*received_lamports);
                let amount_sol = transfer_lamports as f64 / 1_000_000_000.0;

                if amount_sol >= self.config.spam_threshold {
                    transfers.push((sender, *receiver, amount_sol));
                }
            }
        }

        transfers
    }
}


#[async_trait::async_trait]
impl TxSubscriber for ScatterGatherSubscriber {
    fn name(&self) -> &'static str {
        "scatter-gather-monitor"
    }

    async fn interested(&self, tx: &TransactionFormat) -> bool {
        let watch_set = self.watch_set.load();

        // Fast path: check if any account_key is in our watch set
        tx.account_keys.iter().any(|k| watch_set.contains(k))
    }

    async fn on_tx(self: Arc<Self>, tx: Arc<TransactionFormat>) {
        // Extract SOL transfers
        let transfers = self.extract_sol_transfers(&tx);

        if transfers.is_empty() {
            return;
        }

        // Process each transfer
        for (from, to, amount) in transfers {
            debug!(
                "[{}] Transfer: {} -> {} ({:.6} SOL) in tx {}",
                self.name(),
                from,
                to,
                amount,
                tx.signature
            );

            self.process_transfer(from, to, amount, &tx);
        }
    }
}

/// Statistics about the monitoring state
#[derive(Debug, Clone)]
pub struct MonitorStats {
    pub total_addresses: usize,
    pub total_edges: usize,
    pub convergence_points: usize,
    pub scatter_points: usize,
    pub max_incoming: usize,
    pub max_outgoing: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transfer_graph_basic() {
        let mut graph = TransferGraph::new();
        let now = SystemTime::now();

        let addr_a = Pubkey::new_unique();
        let addr_b = Pubkey::new_unique();
        let addr_c = Pubkey::new_unique();

        // A -> B
        graph.add_edge(addr_a, addr_b, now);
        assert_eq!(graph.get_outgoing_count(&addr_a), 1);
        assert_eq!(graph.get_incoming_count(&addr_b), 1);

        // A -> C
        graph.add_edge(addr_a, addr_c, now);
        assert_eq!(graph.get_outgoing_count(&addr_a), 2);

        // B -> C (convergence at C)
        graph.add_edge(addr_b, addr_c, now);
        assert_eq!(graph.get_incoming_count(&addr_c), 2);
    }

    #[test]
    fn test_subscriber_add_addresses() {
        let config = TraceConfig::default();
        let (subscriber, mut event_rx) = ScatterGatherSubscriber::new(config);

        let addr = Pubkey::new_unique();
        subscriber.add_watch_addresses(vec![addr]);

        // Should receive NewAddress event
        let event = event_rx.try_recv().unwrap();
        match event {
            TraceEvent::NewAddress {
                address,
                depth,
                source,
            } => {
                assert_eq!(address, addr);
                assert_eq!(depth, 0);
                assert!(source.is_none());
            }
            _ => panic!("Expected NewAddress event"),
        }
    }
}
