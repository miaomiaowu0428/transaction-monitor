//! Example: How to use ScatterGatherSubscriber with offline trace results
//!
//! This example demonstrates:
//! 1. Running offline trace to find initial paths/roots
//! 2. Setting up real-time monitoring on those addresses
//! 3. Handling events (new addresses, convergence, scatter)
//! 4. Combining offline and online analysis

use log::{debug, info, warn};
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::mpsc;

use crate::tx_dispatcher::TxDispatcher;
use crate::tx_subscriber::scatter_gather::{ScatterGatherSubscriber, TraceConfig, TraceEvent};

/// Example 1: Monitor a known scatter-gather pattern
pub async fn example_monitor_from_roots() {
    // Step 1: Use offline trace tool to find roots and initial paths
    // (假设这些是从 line.rs trace_backward 得到的结果)
    let known_roots = vec![
        Pubkey::new_unique(), // Root 1
        Pubkey::new_unique(), // Root 2
        Pubkey::new_unique(), // Root 3
    ];

    let convergence_point = Pubkey::new_unique();

    // Step 2: Create scatter-gather subscriber with config
    let config = TraceConfig {
        spam_threshold: 0.001,         // Ignore transfers < 0.001 SOL
        min_convergence_degree: 5,     // Need 5+ incoming edges for convergence
        max_tracked_addresses: 50_000, // Track up to 50k addresses
        auto_expand: true,             // Auto-add new addresses
        max_expansion_depth: 10,       // Max 10 hops from roots
    };

    let (subscriber, mut event_rx) = ScatterGatherSubscriber::new(config);
    let subscriber = Arc::new(subscriber);

    // Step 3: Add initial monitoring addresses
    subscriber.add_watch_addresses(known_roots.clone());
    subscriber.add_watch_addresses(vec![convergence_point]);

    info!("Started monitoring {} root addresses", known_roots.len());

    // Step 4: Set up event handler task
    let event_handler = tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            match event {
                TraceEvent::NewAddress {
                    address,
                    depth,
                    source,
                } => {
                    info!(
                        "📍 New address added: {} (depth: {}, from: {:?})",
                        address, depth, source
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
                        "💸 Transfer: {} -> {} ({:.6} SOL) [slot: {}, sig: {}]",
                        from, to, amount, slot, signature
                    );
                }

                TraceEvent::Convergence {
                    address,
                    incoming_count,
                    sources,
                } => {
                    info!(
                        "🎯 CONVERGENCE DETECTED at {} with {} sources",
                        address, incoming_count
                    );
                    info!("   Sources: {:?}", sources);

                    // TODO: Trigger alert, store to DB, etc.
                }

                TraceEvent::Scatter {
                    address,
                    outgoing_count,
                    destinations,
                } => {
                    info!(
                        "💥 SCATTER DETECTED at {} with {} destinations",
                        address, outgoing_count
                    );
                    info!("   Destinations: {:?}", destinations);
                }

                TraceEvent::LimitReached { current, limit } => {
                    info!(
                        "⚠️  Address limit reached: {}/{}. Stopping auto-expansion.",
                        current, limit
                    );
                }
            }
        }
    });

    // Step 5: Register subscriber and run dispatcher
    let dispatcher = TxDispatcher::new();
    dispatcher.register(subscriber.clone());

    // Run the dispatcher (this will block)
    dispatcher.run().await;

    // Clean up
    event_handler.abort();
}

/// Example 2: Two-phase monitoring (forward then backward)
pub async fn example_two_phase_monitoring() {
    // Phase 1: Monitor forward scatter from a known root
    let suspicious_root = Pubkey::new_unique();

    let config = TraceConfig {
        spam_threshold: 0.0005,
        min_convergence_degree: 3,
        max_tracked_addresses: 10_000,
        auto_expand: true,
        max_expansion_depth: 5, // Only go 5 hops deep
    };

    let (subscriber, mut event_rx) = ScatterGatherSubscriber::new(config);
    let subscriber = Arc::new(subscriber);

    // Start monitoring the root
    subscriber.add_watch_addresses(vec![suspicious_root]);

    info!(
        "Phase 1: Monitoring forward scatter from root {}",
        suspicious_root
    );

    // Monitor for convergence points
    let _convergence_detector = tokio::spawn(async move {
        let mut detected_convergence = Vec::new();

        while let Some(event) = event_rx.recv().await {
            if let TraceEvent::Convergence {
                address,
                incoming_count,
                ..
            } = event
            {
                info!(
                    "Found convergence point: {} with {} sources",
                    address, incoming_count
                );
                detected_convergence.push(address);

                // Once we have some convergence points, we could switch to phase 2
                if detected_convergence.len() >= 3 {
                    info!(
                        "Detected {} convergence points, could start phase 2",
                        detected_convergence.len()
                    );
                    break;
                }
            }
        }

        detected_convergence
    });

    // Register and run
    let dispatcher = TxDispatcher::new();
    dispatcher.register(subscriber.clone());

    // In real usage, you'd run this in a background task
    // dispatcher.run().await;

    info!("Monitoring active. Waiting for convergence points...");
}

/// Example 3: Integration with offline trace tool
pub async fn example_hybrid_trace(root: Pubkey, convergence: Pubkey) {
    // Step 1: Use offline trace to find initial path
    // (这里调用你的 line.rs 中的函数)
    /*
    use your_crate::trace_tool::{trace_forward, trace_backward};

    let forward_result = trace_forward(root).await;
    let backward_result = trace_backward(convergence).await;
    */

    // Step 2: Extract all addresses from trace results
    let mut monitored_addresses = HashSet::new();
    monitored_addresses.insert(root);
    monitored_addresses.insert(convergence);

    // From forward trace: add all intermediate nodes
    // forward_result.path.iter().for_each(|addr| { monitored_addresses.insert(*addr); });

    // From backward trace: add all found roots and paths
    // backward_result.roots.iter().for_each(|root_info| {
    //     root_info.path.iter().for_each(|addr| { monitored_addresses.insert(*addr); });
    // });

    // Step 3: Start real-time monitoring
    let config = TraceConfig {
        spam_threshold: 0.0001,
        min_convergence_degree: 5,
        max_tracked_addresses: 100_000,
        auto_expand: true,
        max_expansion_depth: 8,
    };

    let (subscriber, event_rx) = ScatterGatherSubscriber::new(config);
    let subscriber = Arc::new(subscriber);

    // Add all addresses from offline trace
    let addresses: Vec<_> = monitored_addresses.into_iter().collect();
    subscriber.add_watch_addresses(addresses.clone());

    info!(
        "Hybrid monitoring: {} addresses from offline trace + real-time expansion",
        addresses.len()
    );

    // Handle events
    handle_events(event_rx, subscriber.clone()).await;
}

/// Event handler that processes trace events
async fn handle_events(
    mut event_rx: mpsc::UnboundedReceiver<TraceEvent>,
    subscriber: Arc<ScatterGatherSubscriber>,
) {
    let mut convergence_count = 0;
    let mut scatter_count = 0;

    while let Some(event) = event_rx.recv().await {
        match event {
            TraceEvent::NewAddress {
                address,
                depth,
                source,
            } => {
                debug!(
                    "New address at depth {}: {} (from {:?})",
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
                // Log or store interesting transfers
                if amount > 1.0 {
                    info!(
                        "Large transfer: {} SOL from {} to {} [{}]",
                        amount, from, to, signature
                    );
                }
            }

            TraceEvent::Convergence {
                address,
                incoming_count,
                sources,
            } => {
                convergence_count += 1;
                info!(
                    "🎯 Convergence #{}: {} ({} sources)",
                    convergence_count, address, incoming_count
                );

                // Here you could:
                // 1. Alert on suspicious convergence patterns
                // 2. Trigger backward trace from this point
                // 3. Store to database for analysis
                // 4. Flag for manual review
            }

            TraceEvent::Scatter {
                address,
                outgoing_count,
                destinations: _,
            } => {
                scatter_count += 1;
                info!(
                    "💥 Scatter #{}: {} ({} destinations)",
                    scatter_count, address, outgoing_count
                );

                // Here you could:
                // 1. Analyze scatter patterns (equal amounts? timing?)
                // 2. Compare with known laundering patterns
                // 3. Calculate risk scores
            }

            TraceEvent::LimitReached { current, limit } => {
                warn!("⚠️  Monitoring limit reached: {}/{}", current, limit);

                // Get stats and decide what to do
                let stats = subscriber.get_stats();
                info!("Current stats: {:?}", stats);

                // Could remove old/inactive addresses or increase limit
            }
        }

        // Periodic stats logging
        if (convergence_count + scatter_count) % 10 == 0 {
            let stats = subscriber.get_stats();
            info!("📊 Stats: {:?}", stats);
        }
    }
}

/// Example 4: Targeted monitoring - only track specific patterns
pub async fn example_targeted_monitoring() {
    let config = TraceConfig {
        spam_threshold: 0.01,         // Higher threshold - only significant transfers
        min_convergence_degree: 10,   // Need 10+ sources (strong convergence signal)
        max_tracked_addresses: 5_000, // Smaller footprint
        auto_expand: false,           // Manual control only
        max_expansion_depth: 0,
    };

    let (subscriber, mut event_rx) = ScatterGatherSubscriber::new(config);
    let subscriber = Arc::new(subscriber);

    // Only monitor a specific set of addresses (e.g., known exchange wallets)
    let exchange_wallets = vec![
        // Add known wallets
    ];

    subscriber.add_watch_addresses(exchange_wallets);

    // Manual expansion: when you see interesting patterns, manually add more addresses
    tokio::spawn({
        let subscriber = subscriber.clone();
        async move {
            while let Some(event) = event_rx.recv().await {
                if let TraceEvent::Convergence { address, .. } = event {
                    // Manually decide to expand monitoring
                    info!(
                        "Manually adding convergence point to monitoring: {}",
                        address
                    );
                    subscriber.add_watch_addresses(vec![address]);
                }
            }
        }
    });
}

/// Helper to create a full monitoring pipeline
pub struct MonitoringPipeline {
    dispatcher: Arc<TxDispatcher>,
    subscriber: Arc<ScatterGatherSubscriber>,
}

impl MonitoringPipeline {
    pub fn new(config: TraceConfig) -> (Self, mpsc::UnboundedReceiver<TraceEvent>) {
        let dispatcher = Arc::new(TxDispatcher::new());
        let (subscriber, event_rx) = ScatterGatherSubscriber::new(config);
        let subscriber = Arc::new(subscriber);

        dispatcher.register(subscriber.clone());

        (
            Self {
                dispatcher,
                subscriber,
            },
            event_rx,
        )
    }

    pub fn add_addresses(&self, addresses: Vec<Pubkey>) {
        self.subscriber.add_watch_addresses(addresses);
    }

    pub async fn run(&self) {
        self.dispatcher.run().await;
    }

    pub fn get_stats(&self) -> crate::tx_subscriber::scatter_gather::MonitorStats {
        self.subscriber.get_stats()
    }

    pub fn get_convergence_points(&self) -> Vec<Pubkey> {
        self.subscriber.get_convergence_points()
    }

    pub fn get_scatter_points(&self) -> Vec<Pubkey> {
        self.subscriber.get_scatter_points()
    }
}

/// Main usage example
#[allow(dead_code)]
async fn main_example() {
    // Initialize
    dotenvy::dotenv().ok();
    env_logger::init();

    // Create monitoring pipeline
    let config = TraceConfig::default();
    let (pipeline, mut events) = MonitoringPipeline::new(config);

    // Add addresses from offline trace
    let root = Pubkey::new_unique();
    pipeline.add_addresses(vec![root]);

    // Spawn event handler
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            match event {
                TraceEvent::Convergence {
                    address,
                    incoming_count,
                    ..
                } => {
                    info!(
                        "🚨 ALERT: Convergence at {} with {} sources - possible laundering!",
                        address, incoming_count
                    );

                    // Here you could:
                    // - Store to database
                    // - Send alert to monitoring system
                    // - Trigger deeper analysis
                    // - Flag for manual review
                }
                TraceEvent::Scatter {
                    address,
                    outgoing_count,
                    ..
                } => {
                    info!(
                        "🚨 ALERT: Scatter from {} to {} destinations",
                        address, outgoing_count
                    );
                }
                _ => {}
            }
        }
    });

    // Run the monitoring (blocks until stopped)
    pipeline.run().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx_subscriber::scatter_gather::TraceConfig;

    #[tokio::test]
    async fn test_pipeline_creation() {
        let config = TraceConfig::default();
        let (pipeline, _event_rx) = MonitoringPipeline::new(config);

        let test_addr = Pubkey::new_unique();
        pipeline.add_addresses(vec![test_addr]);

        let stats = pipeline.get_stats();
        assert_eq!(stats.total_addresses, 1);
    }

    #[tokio::test]
    async fn test_event_flow() {
        let config = TraceConfig {
            min_convergence_degree: 2, // Lower threshold for testing
            ..Default::default()
        };

        let (pipeline, mut event_rx) = MonitoringPipeline::new(config);

        let root = Pubkey::new_unique();
        pipeline.add_addresses(vec![root]);

        // Should receive NewAddress event
        let event = event_rx.try_recv().unwrap();
        match event {
            TraceEvent::NewAddress { address, depth, .. } => {
                assert_eq!(address, root);
                assert_eq!(depth, 0);
            }
            _ => panic!("Expected NewAddress event"),
        }
    }
}
