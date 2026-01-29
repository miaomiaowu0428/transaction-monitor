//! Integration module for combining offline trace analysis with real-time monitoring
//!
//! This module provides a bridge between:
//! - Offline trace tools (line.rs) that analyze historical transactions
//! - Real-time monitoring (ScatterGatherSubscriber) that tracks live transactions
//!
//! # Usage Flow
//!
//! 1. Run offline trace to find initial scatter-gather patterns
//! 2. Extract addresses from trace results
//! 3. Start real-time monitoring on those addresses
//! 4. Dynamically expand monitoring as new patterns emerge
//! 5. Detect and alert on new convergence/scatter points

use super::scatter_gather::{MonitorStats, ScatterGatherSubscriber, TraceConfig, TraceEvent};
use log::{info, warn};
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::mpsc;

/// Result from combining offline trace with real-time monitoring
#[derive(Debug, Clone)]
pub struct HybridTraceResult {
    /// Initial addresses from offline trace
    pub seed_addresses: Vec<Pubkey>,

    /// Addresses discovered during real-time monitoring
    pub discovered_addresses: Vec<Pubkey>,

    /// Convergence points found
    pub convergence_points: Vec<ConvergenceInfo>,

    /// Scatter points found
    pub scatter_points: Vec<ScatterInfo>,

    /// Time when monitoring started
    pub started_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct ConvergenceInfo {
    pub address: Pubkey,
    pub incoming_count: usize,
    pub sources: Vec<Pubkey>,
    pub detected_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct ScatterInfo {
    pub address: Pubkey,
    pub outgoing_count: usize,
    pub destinations: Vec<Pubkey>,
    pub detected_at: SystemTime,
}

/// Hybrid monitoring session that combines offline + real-time analysis
pub struct HybridMonitor {
    subscriber: Arc<ScatterGatherSubscriber>,
    event_rx: mpsc::UnboundedReceiver<TraceEvent>,

    // Tracking
    discovered_addresses: Arc<std::sync::RwLock<HashSet<Pubkey>>>,
    convergence_history: Arc<std::sync::RwLock<Vec<ConvergenceInfo>>>,
    scatter_history: Arc<std::sync::RwLock<Vec<ScatterInfo>>>,
    started_at: SystemTime,
}

impl HybridMonitor {
    /// Create a new hybrid monitor with custom config
    pub fn new(config: TraceConfig) -> Self {
        let (subscriber, event_rx) = ScatterGatherSubscriber::new(config);

        Self {
            subscriber: Arc::new(subscriber),
            event_rx,
            discovered_addresses: Arc::new(std::sync::RwLock::new(HashSet::new())),
            convergence_history: Arc::new(std::sync::RwLock::new(Vec::new())),
            scatter_history: Arc::new(std::sync::RwLock::new(Vec::new())),
            started_at: SystemTime::now(),
        }
    }

    /// Create with default config
    pub fn with_defaults() -> Self {
        Self::new(TraceConfig::default())
    }

    /// Add initial addresses from offline trace
    pub fn add_seed_addresses(&self, addresses: Vec<Pubkey>) {
        info!(
            "Adding {} seed addresses from offline trace",
            addresses.len()
        );
        self.subscriber.add_watch_addresses(addresses);
    }

    /// Get the underlying subscriber (to register with dispatcher)
    pub fn subscriber(&self) -> Arc<ScatterGatherSubscriber> {
        self.subscriber.clone()
    }

    /// Process events and collect results
    pub async fn process_events(&mut self) -> HybridTraceResult {
        while let Some(event) = self.event_rx.recv().await {
            match event {
                TraceEvent::NewAddress { address, .. } => {
                    self.discovered_addresses.write().unwrap().insert(address);
                }

                TraceEvent::Convergence {
                    address,
                    incoming_count,
                    sources,
                } => {
                    info!("🎯 Convergence at {}: {} sources", address, incoming_count);

                    self.convergence_history
                        .write()
                        .unwrap()
                        .push(ConvergenceInfo {
                            address,
                            incoming_count,
                            sources,
                            detected_at: SystemTime::now(),
                        });
                }

                TraceEvent::Scatter {
                    address,
                    outgoing_count,
                    destinations,
                } => {
                    info!(
                        "💥 Scatter from {}: {} destinations",
                        address, outgoing_count
                    );

                    self.scatter_history.write().unwrap().push(ScatterInfo {
                        address,
                        outgoing_count,
                        destinations,
                        detected_at: SystemTime::now(),
                    });
                }

                TraceEvent::LimitReached { current, limit } => {
                    warn!("⚠️  Limit reached: {}/{}", current, limit);
                }

                _ => {}
            }
        }

        self.build_result()
    }

    /// Process events with a timeout
    pub async fn process_events_with_timeout(&mut self, duration: Duration) -> HybridTraceResult {
        let timeout = tokio::time::sleep(duration);
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                Some(event) = self.event_rx.recv() => {
                    self.handle_event(event);
                }
                _ = &mut timeout => {
                    info!("Monitoring timeout reached");
                    break;
                }
            }
        }

        self.build_result()
    }

    fn handle_event(&mut self, event: TraceEvent) {
        match event {
            TraceEvent::NewAddress { address, .. } => {
                self.discovered_addresses.write().unwrap().insert(address);
            }

            TraceEvent::Convergence {
                address,
                incoming_count,
                sources,
            } => {
                self.convergence_history
                    .write()
                    .unwrap()
                    .push(ConvergenceInfo {
                        address,
                        incoming_count,
                        sources,
                        detected_at: SystemTime::now(),
                    });
            }

            TraceEvent::Scatter {
                address,
                outgoing_count,
                destinations,
            } => {
                self.scatter_history.write().unwrap().push(ScatterInfo {
                    address,
                    outgoing_count,
                    destinations,
                    detected_at: SystemTime::now(),
                });
            }

            _ => {}
        }
    }

    fn build_result(&self) -> HybridTraceResult {
        HybridTraceResult {
            seed_addresses: Vec::new(), // Could track this if needed
            discovered_addresses: self
                .discovered_addresses
                .read()
                .unwrap()
                .iter()
                .copied()
                .collect(),
            convergence_points: self.convergence_history.read().unwrap().clone(),
            scatter_points: self.scatter_history.read().unwrap().clone(),
            started_at: self.started_at,
        }
    }

    /// Get current monitoring statistics
    pub fn get_stats(&self) -> MonitorStats {
        self.subscriber.get_stats()
    }
}

/// Builder for creating a monitoring session from offline trace results
pub struct TraceMonitorBuilder {
    config: TraceConfig,
    seed_addresses: Vec<Pubkey>,
    callbacks: Vec<Box<dyn Fn(&TraceEvent) + Send + Sync>>,
}

impl TraceMonitorBuilder {
    pub fn new() -> Self {
        Self {
            config: TraceConfig::default(),
            seed_addresses: Vec::new(),
            callbacks: Vec::new(),
        }
    }

    /// Set configuration
    pub fn with_config(mut self, config: TraceConfig) -> Self {
        self.config = config;
        self
    }

    /// Add seed addresses from offline trace
    pub fn with_seed_addresses(mut self, addresses: Vec<Pubkey>) -> Self {
        self.seed_addresses.extend(addresses);
        self
    }

    /// Add a single seed address
    pub fn add_seed_address(mut self, address: Pubkey) -> Self {
        self.seed_addresses.push(address);
        self
    }

    /// Add callback for convergence detection
    pub fn on_convergence<F>(mut self, callback: F) -> Self
    where
        F: Fn(&Pubkey, usize, &[Pubkey]) + Send + Sync + 'static,
    {
        self.callbacks.push(Box::new(move |event| {
            if let TraceEvent::Convergence {
                address,
                incoming_count,
                sources,
            } = event
            {
                callback(address, *incoming_count, sources);
            }
        }));
        self
    }

    /// Add callback for scatter detection
    pub fn on_scatter<F>(mut self, callback: F) -> Self
    where
        F: Fn(&Pubkey, usize, &[Pubkey]) + Send + Sync + 'static,
    {
        self.callbacks.push(Box::new(move |event| {
            if let TraceEvent::Scatter {
                address,
                outgoing_count,
                destinations,
            } = event
            {
                callback(address, *outgoing_count, destinations);
            }
        }));
        self
    }

    /// Build and start monitoring
    pub fn build(
        self,
    ) -> (
        Arc<ScatterGatherSubscriber>,
        mpsc::UnboundedReceiver<TraceEvent>,
    ) {
        let (subscriber, event_rx) = ScatterGatherSubscriber::new(self.config);
        let subscriber = Arc::new(subscriber);

        // Add seed addresses
        if !self.seed_addresses.is_empty() {
            subscriber.add_watch_addresses(self.seed_addresses);
        }

        (subscriber, event_rx)
    }
}

impl Default for TraceMonitorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to run a monitoring session that collects results
pub async fn run_monitoring_session(
    seed_addresses: Vec<Pubkey>,
    config: TraceConfig,
    duration: Duration,
) -> HybridTraceResult {
    let mut monitor = HybridMonitor::new(config);
    monitor.add_seed_addresses(seed_addresses);

    // Process events for the specified duration
    monitor.process_events_with_timeout(duration).await
}

/// Analysis helper: detect if a pattern matches known laundering signatures
pub struct PatternAnalyzer {
    convergence_history: Vec<ConvergenceInfo>,
    scatter_history: Vec<ScatterInfo>,
}

impl PatternAnalyzer {
    pub fn new() -> Self {
        Self {
            convergence_history: Vec::new(),
            scatter_history: Vec::new(),
        }
    }

    pub fn from_result(result: &HybridTraceResult) -> Self {
        Self {
            convergence_history: result.convergence_points.clone(),
            scatter_history: result.scatter_points.clone(),
        }
    }

    /// Detect scatter-gather chains (scatter -> converge -> scatter -> converge)
    pub fn find_chains(&self) -> Vec<Vec<Pubkey>> {
        let mut chains = Vec::new();

        // Build index: which addresses are both scatter and convergence points
        let scatter_set: HashSet<_> = self.scatter_history.iter().map(|s| s.address).collect();
        let convergence_set: HashSet<_> =
            self.convergence_history.iter().map(|c| c.address).collect();

        // Find addresses that are BOTH scatter and convergence (chain links)
        let chain_links: Vec<_> = scatter_set
            .intersection(&convergence_set)
            .copied()
            .collect();

        if !chain_links.is_empty() {
            info!("Found {} chain link addresses", chain_links.len());
            chains.push(chain_links);
        }

        chains
    }

    /// Calculate a suspicion score based on patterns
    pub fn calculate_suspicion_score(&self) -> f64 {
        let mut score = 0.0;

        // More convergence points = higher score
        score += self.convergence_history.len() as f64 * 2.0;

        // More scatter points = higher score
        score += self.scatter_history.len() as f64 * 1.5;

        // High-degree convergence is very suspicious
        for conv in &self.convergence_history {
            if conv.incoming_count > 20 {
                score += (conv.incoming_count as f64 - 20.0) * 0.5;
            }
        }

        // Scatter-then-converge chains are very suspicious
        let chain_count = self.find_chains().len();
        score += chain_count as f64 * 10.0;

        score
    }

    /// Analyze timing patterns (rapid scatter-gather = suspicious)
    pub fn analyze_timing(&self) -> TimingAnalysis {
        let mut min_gap = Duration::from_secs(u64::MAX);
        let mut max_gap = Duration::from_secs(0);
        let mut total_gap = Duration::from_secs(0);
        let mut gap_count = 0;

        // Analyze gaps between scatter and convergence events
        for scatter in &self.scatter_history {
            for convergence in &self.convergence_history {
                if convergence.detected_at > scatter.detected_at {
                    if let Ok(gap) = convergence.detected_at.duration_since(scatter.detected_at) {
                        min_gap = min_gap.min(gap);
                        max_gap = max_gap.max(gap);
                        total_gap += gap;
                        gap_count += 1;
                    }
                }
            }
        }

        let avg_gap = if gap_count > 0 {
            total_gap / gap_count as u32
        } else {
            Duration::from_secs(0)
        };

        TimingAnalysis {
            min_gap,
            max_gap,
            avg_gap,
            sample_count: gap_count,
        }
    }
}

impl Default for PatternAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct TimingAnalysis {
    pub min_gap: Duration,
    pub max_gap: Duration,
    pub avg_gap: Duration,
    pub sample_count: usize,
}

/// Complete workflow: offline trace -> real-time monitoring -> analysis
pub struct TraceWorkflow {
    monitor: HybridMonitor,
    seed_addresses: Vec<Pubkey>,
}

impl TraceWorkflow {
    /// Create from offline trace results
    ///
    /// # Arguments
    /// * `trace_addresses` - Addresses from offline trace (roots, intermediates, convergence points)
    /// * `config` - Monitoring configuration
    pub fn from_offline_trace(trace_addresses: Vec<Pubkey>, config: TraceConfig) -> Self {
        let monitor = HybridMonitor::new(config);
        monitor.add_seed_addresses(trace_addresses.clone());

        Self {
            monitor,
            seed_addresses: trace_addresses,
        }
    }

    /// Get subscriber for registration with dispatcher
    pub fn subscriber(&self) -> Arc<ScatterGatherSubscriber> {
        self.monitor.subscriber.clone()
    }

    /// Run monitoring for a fixed duration and return results
    pub async fn run_timed(mut self, duration: Duration) -> HybridTraceResult {
        info!(
            "Starting hybrid monitoring for {} seconds with {} seed addresses",
            duration.as_secs(),
            self.seed_addresses.len()
        );

        self.monitor.process_events_with_timeout(duration).await
    }

    /// Run monitoring until a condition is met
    pub async fn run_until<F>(mut self, mut condition: F) -> HybridTraceResult
    where
        F: FnMut(&MonitorStats, &[ConvergenceInfo], &[ScatterInfo]) -> bool,
    {
        loop {
            tokio::select! {
                Some(event) = self.monitor.event_rx.recv() => {
                    self.monitor.handle_event(event);

                    // Check condition
                    let stats = self.monitor.get_stats();
                    let convergence = self.monitor.convergence_history.read().unwrap();
                    let scatter = self.monitor.scatter_history.read().unwrap();

                    if condition(&stats, &convergence, &scatter) {
                        info!("Monitoring condition met, stopping");
                        break;
                    }
                }
                else => {
                    info!("Event channel closed");
                    break;
                }
            }
        }

        self.monitor.build_result()
    }

    /// Get current stats
    pub fn stats(&self) -> MonitorStats {
        self.monitor.get_stats()
    }
}

/// Preset monitoring strategies
pub mod strategies {
    use super::*;

    /// Strategy: High-frequency laundering detection
    /// - Low spam threshold to catch micro-transactions
    /// - Low convergence degree (even 3-5 sources is suspicious)
    /// - Deep expansion to follow the chain
    pub fn high_frequency_laundering() -> TraceConfig {
        TraceConfig {
            spam_threshold: 0.00001,   // Very low - catch micro-transactions
            min_convergence_degree: 3, // Even 3 sources is worth investigating
            max_tracked_addresses: 200_000,
            auto_expand: true,
            max_expansion_depth: 15, // Follow deep chains
        }
    }

    /// Strategy: Large-scale mixer detection
    /// - Higher spam threshold (ignore tiny amounts)
    /// - High convergence degree (looking for big mixers)
    /// - Moderate expansion
    pub fn mixer_detection() -> TraceConfig {
        TraceConfig {
            spam_threshold: 0.01,       // Ignore < 0.01 SOL
            min_convergence_degree: 20, // Need significant convergence
            max_tracked_addresses: 100_000,
            auto_expand: true,
            max_expansion_depth: 8,
        }
    }

    /// Strategy: Targeted investigation
    /// - Manual control, no auto-expansion
    /// - Focus on specific known addresses
    pub fn targeted_investigation() -> TraceConfig {
        TraceConfig {
            spam_threshold: 0.001,
            min_convergence_degree: 5,
            max_tracked_addresses: 10_000,
            auto_expand: false, // Manual only
            max_expansion_depth: 0,
        }
    }

    /// Strategy: Quick scan
    /// - Fast, shallow monitoring for initial assessment
    pub fn quick_scan() -> TraceConfig {
        TraceConfig {
            spam_threshold: 0.01,
            min_convergence_degree: 5,
            max_tracked_addresses: 5_000,
            auto_expand: true,
            max_expansion_depth: 3, // Only 3 hops
        }
    }
}

/// Report generator for monitoring results
pub struct MonitoringReport {
    result: HybridTraceResult,
    stats: MonitorStats,
    analyzer: PatternAnalyzer,
}

impl MonitoringReport {
    pub fn from_result(result: HybridTraceResult, stats: MonitorStats) -> Self {
        let analyzer = PatternAnalyzer::from_result(&result);

        Self {
            result,
            stats,
            analyzer,
        }
    }

    /// Generate a text summary
    pub fn summary(&self) -> String {
        let mut output = String::new();

        output.push_str("=== Scatter-Gather Monitoring Report ===\n\n");

        // Overview
        output.push_str(&format!(
            "Monitoring Duration: {:?}\n",
            SystemTime::now()
                .duration_since(self.result.started_at)
                .unwrap_or_default()
        ));
        output.push_str(&format!(
            "Seed Addresses: {}\n",
            self.result.seed_addresses.len()
        ));
        output.push_str(&format!(
            "Discovered Addresses: {}\n",
            self.result.discovered_addresses.len()
        ));
        output.push_str(&format!(
            "Total Tracked: {}\n\n",
            self.stats.total_addresses
        ));

        // Patterns
        output.push_str(&format!(
            "Convergence Points: {}\n",
            self.result.convergence_points.len()
        ));
        output.push_str(&format!(
            "Scatter Points: {}\n",
            self.result.scatter_points.len()
        ));
        output.push_str(&format!("Total Edges: {}\n\n", self.stats.total_edges));

        // Top convergence points
        output.push_str("Top Convergence Points:\n");
        let mut convergence = self.result.convergence_points.clone();
        convergence.sort_by(|a, b| b.incoming_count.cmp(&a.incoming_count));
        for (i, conv) in convergence.iter().take(5).enumerate() {
            output.push_str(&format!(
                "  {}. {} ({} sources)\n",
                i + 1,
                conv.address,
                conv.incoming_count
            ));
        }
        output.push_str("\n");

        // Analysis
        let suspicion_score = self.analyzer.calculate_suspicion_score();
        output.push_str(&format!("Suspicion Score: {:.2}\n", suspicion_score));

        let timing = self.analyzer.analyze_timing();
        output.push_str(&format!(
            "Timing: avg={:?}, min={:?}, max={:?} (n={})\n",
            timing.avg_gap, timing.min_gap, timing.max_gap, timing.sample_count
        ));

        let chains = self.analyzer.find_chains();
        output.push_str(&format!("Detected Chains: {}\n", chains.len()));

        output
    }

    /// Check if result is suspicious based on heuristics
    pub fn is_suspicious(&self) -> bool {
        let score = self.analyzer.calculate_suspicion_score();

        // Thresholds (tune based on your data)
        if score > 50.0 {
            return true;
        }

        // Multiple convergence points
        if self.result.convergence_points.len() >= 3 {
            return true;
        }

        // Chain pattern detected
        if !self.analyzer.find_chains().is_empty() {
            return true;
        }

        // Very rapid scatter-gather (< 1 minute average)
        let timing = self.analyzer.analyze_timing();
        if timing.sample_count > 0 && timing.avg_gap < Duration::from_secs(60) {
            return true;
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder() {
        let addr1 = Pubkey::new_unique();
        let addr2 = Pubkey::new_unique();

        let _builder = TraceMonitorBuilder::new()
            .with_config(TraceConfig::default())
            .add_seed_address(addr1)
            .add_seed_address(addr2)
            .on_convergence(|addr, count, _sources| {
                println!("Convergence: {} with {}", addr, count);
            })
            .on_scatter(|addr, count, _dests| {
                println!("Scatter: {} with {}", addr, count);
            });

        // Builder is ready to build()
    }

    #[test]
    fn test_pattern_analyzer() {
        let analyzer = PatternAnalyzer::new();
        let score = analyzer.calculate_suspicion_score();
        assert_eq!(score, 0.0); // No patterns yet
    }

    #[tokio::test]
    async fn test_workflow() {
        let addresses = vec![Pubkey::new_unique(), Pubkey::new_unique()];
        let config = TraceConfig::default();

        let workflow = TraceWorkflow::from_offline_trace(addresses.clone(), config);
        let stats = workflow.stats();

        // Should have seed addresses in watch set
        assert_eq!(stats.total_addresses, addresses.len());
    }

    #[tokio::test]
    async fn test_monitoring_session() {
        let addresses = vec![Pubkey::new_unique()];
        let config = TraceConfig::default();

        // Run for 1 second (will timeout since no real transactions)
        let result =
            run_monitoring_session(addresses.clone(), config, Duration::from_secs(1)).await;

        // Should have started
        assert!(result.started_at <= SystemTime::now());
    }

    #[test]
    fn test_strategies() {
        let _hf = strategies::high_frequency_laundering();
        let _mixer = strategies::mixer_detection();
        let _targeted = strategies::targeted_investigation();
        let _quick = strategies::quick_scan();

        // Just verify they all construct without panic
    }
}
