//! Scatter-Gather Monitoring Demo
//!
//! This binary demonstrates how to use the scatter-gather monitoring system
//! to detect and track dispersion-convergence patterns in real-time.
//!
//! Usage:
//!   cargo run --bin scatter_gather_demo -- <mode> [addresses...]
//!
//! Modes:
//!   - monitor-root <address>         : Monitor scatter from a root address
//!   - monitor-convergence <address>  : Monitor convergence point
//!   - hybrid <addr1> <addr2> ...     : Monitor multiple addresses from offline trace
//!   - demo                           : Run demo mode with random addresses

use log::{info, warn};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;
use std::{sync::Arc, time::Duration};
use transaction_monitor::{
    tx_dispatcher::TxDispatcher,
    tx_subscriber::{
        scatter_gather::{MonitorStats, ScatterGatherSubscriber, TraceConfig, TraceEvent},
        trace_integration::{
            HybridMonitor, MonitoringReport, PatternAnalyzer, TraceWorkflow, strategies,
        },
    },
};

#[tokio::main]
async fn main() {
    // Initialize
    rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider())
        .unwrap();
    dotenvy::dotenv().ok();
    utils::init_logger();

    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        print_usage();
        return;
    }

    let mode = &args[1];

    match mode.as_str() {
        "monitor-root" => {
            if args.len() < 3 {
                eprintln!("Error: monitor-root requires an address");
                print_usage();
                return;
            }
            let root = parse_pubkey(&args[2]);
            monitor_root(root).await;
        }
        "monitor-convergence" => {
            if args.len() < 3 {
                eprintln!("Error: monitor-convergence requires an address");
                print_usage();
                return;
            }
            let convergence = parse_pubkey(&args[2]);
            monitor_convergence(convergence).await;
        }
        "hybrid" => {
            if args.len() < 3 {
                eprintln!("Error: hybrid requires at least one address");
                print_usage();
                return;
            }
            let addresses: Vec<Pubkey> = args[2..].iter().map(|s| parse_pubkey(s)).collect();
            hybrid_monitoring(addresses).await;
        }
        "demo" => {
            demo_mode().await;
        }
        _ => {
            eprintln!("Error: unknown mode '{}'", mode);
            print_usage();
        }
    }
}

fn print_usage() {
    eprintln!("Scatter-Gather Monitoring Demo");
    eprintln!();
    eprintln!("Usage:");
    eprintln!("  cargo run --bin scatter_gather_demo -- <mode> [addresses...]");
    eprintln!();
    eprintln!("Modes:");
    eprintln!("  monitor-root <address>        - Monitor scatter from a root address");
    eprintln!("  monitor-convergence <address> - Monitor around a convergence point");
    eprintln!("  hybrid <addr1> <addr2> ...    - Monitor multiple addresses (from offline trace)");
    eprintln!("  demo                          - Run interactive demo mode");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  # Monitor a suspicious root address");
    eprintln!(
        "  cargo run --bin scatter_gather_demo -- monitor-root 7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"
    );
    eprintln!();
    eprintln!("  # Hybrid monitoring from offline trace results");
    eprintln!("  cargo run --bin scatter_gather_demo -- hybrid 7xKX... 9jKL... 4aBc...");
}

fn parse_pubkey(s: &str) -> Pubkey {
    s.parse()
        .unwrap_or_else(|_| panic!("Invalid pubkey: {}", s))
}

/// Mode 1: Monitor scatter from a known root address
async fn monitor_root(root: Pubkey) {
    info!("🔍 Mode: Monitor Root Address");
    info!("Root: {}", root);
    info!("Strategy: High-frequency laundering detection");
    info!("---");

    // Use high-frequency strategy for root monitoring
    let config = strategies::high_frequency_laundering();

    let (subscriber, mut event_rx) = ScatterGatherSubscriber::new(config);
    let subscriber = Arc::new(subscriber);

    // Add root to monitoring
    subscriber.add_watch_addresses(vec![root]);

    // Spawn event handler
    let stats_subscriber = subscriber.clone();
    let event_handler = tokio::spawn(async move {
        let mut event_count = 0;
        let mut convergence_count = 0;
        let mut scatter_count = 0;

        while let Some(event) = event_rx.recv().await {
            event_count += 1;

            match event {
                TraceEvent::NewAddress {
                    address,
                    depth,
                    source,
                } => {
                    info!(
                        "📍 [Depth {}] New address: {} (from: {:?})",
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
                        "💸 Transfer: {} -> {} ({:.6} SOL)",
                        truncate_pubkey(&from),
                        truncate_pubkey(&to),
                        amount
                    );
                    info!("   Slot: {}, Sig: {}", slot, truncate_sig(&signature));
                }

                TraceEvent::Convergence {
                    address,
                    incoming_count,
                    sources,
                } => {
                    convergence_count += 1;
                    info!("");
                    info!(
                        "🎯 CONVERGENCE #{}: {} ({} sources)",
                        convergence_count, address, incoming_count
                    );
                    info!("   Sources:");
                    for (i, src) in sources.iter().take(10).enumerate() {
                        info!("     {}. {}", i + 1, src);
                    }
                    if sources.len() > 10 {
                        info!("     ... and {} more", sources.len() - 10);
                    }
                    info!("");
                }

                TraceEvent::Scatter {
                    address,
                    outgoing_count,
                    destinations,
                } => {
                    scatter_count += 1;
                    info!("");
                    info!(
                        "💥 SCATTER #{}: {} ({} destinations)",
                        scatter_count, address, outgoing_count
                    );
                    info!("   Destinations:");
                    for (i, dst) in destinations.iter().take(10).enumerate() {
                        info!("     {}. {}", i + 1, dst);
                    }
                    if destinations.len() > 10 {
                        info!("     ... and {} more", destinations.len() - 10);
                    }
                    info!("");
                }

                TraceEvent::LimitReached { current, limit } => {
                    warn!("⚠️  Address limit reached: {}/{}", current, limit);
                    warn!("   Auto-expansion stopped. Current stats:");
                    let stats = stats_subscriber.get_stats();
                    print_stats(&stats);
                }
            }

            // Print stats every 100 events
            if event_count % 100 == 0 {
                info!("📊 Stats after {} events:", event_count);
                let stats = stats_subscriber.get_stats();
                print_stats(&stats);
            }
        }

        info!("Event handler finished");
    });

    // Register and run dispatcher
    let dispatcher = TxDispatcher::new();
    dispatcher.register(subscriber);

    info!("Starting transaction dispatcher...");
    info!("Press Ctrl+C to stop");
    info!("");

    dispatcher.run().await;

    // Cleanup
    event_handler.abort();
}

/// Mode 2: Monitor around a convergence point
async fn monitor_convergence(convergence: Pubkey) {
    info!("🔍 Mode: Monitor Convergence Point");
    info!("Convergence: {}", convergence);
    info!("Strategy: Mixer detection");
    info!("---");

    // Use mixer detection strategy
    let config = strategies::mixer_detection();

    let (subscriber, mut event_rx) = ScatterGatherSubscriber::new(config);
    let subscriber = Arc::new(subscriber);

    // Add convergence point to monitoring
    subscriber.add_watch_addresses(vec![convergence]);

    // Event handler
    let stats_subscriber = subscriber.clone();
    let event_handler = tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            match event {
                TraceEvent::Transfer {
                    from,
                    to,
                    amount,
                    signature,
                    ..
                } => {
                    if to == convergence {
                        info!(
                            "📥 Incoming to convergence: {} -> {} ({:.6} SOL) [{}]",
                            truncate_pubkey(&from),
                            truncate_pubkey(&to),
                            amount,
                            truncate_sig(&signature)
                        );
                    } else if from == convergence {
                        info!(
                            "📤 Outgoing from convergence: {} -> {} ({:.6} SOL) [{}]",
                            truncate_pubkey(&from),
                            truncate_pubkey(&to),
                            amount,
                            truncate_sig(&signature)
                        );
                    }
                }

                TraceEvent::Scatter {
                    address,
                    outgoing_count,
                    ..
                } if address == convergence => {
                    warn!(
                        "⚠️  Convergence point is now scattering! {} destinations",
                        outgoing_count
                    );
                }

                _ => {}
            }
        }
    });

    // Register and run
    let dispatcher = TxDispatcher::new();
    dispatcher.register(subscriber.clone());

    info!("Monitoring convergence point and its neighbors...");
    info!("Press Ctrl+C to stop");
    info!("");

    // Spawn a stats printer
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let stats = stats_subscriber.get_stats();
            info!("📊 Periodic stats:");
            print_stats(&stats);
        }
    });

    dispatcher.run().await;
    event_handler.abort();
}

/// Mode 3: Hybrid monitoring from offline trace results
async fn hybrid_monitoring(addresses: Vec<Pubkey>) {
    info!("🔍 Mode: Hybrid Monitoring");
    info!("Seed addresses: {}", addresses.len());
    for (i, addr) in addresses.iter().enumerate() {
        info!("  {}. {}", i + 1, addr);
    }
    info!("Strategy: Mixer detection with moderate expansion");
    info!("---");

    // Create hybrid monitor
    let config = strategies::mixer_detection();
    let mut monitor = HybridMonitor::new(config);
    monitor.add_seed_addresses(addresses.clone());

    let subscriber = monitor.subscriber();

    // Spawn event processor
    let result_handle = tokio::spawn(async move {
        monitor
            .process_events_with_timeout(Duration::from_secs(600))
            .await
    });

    // Register and run dispatcher
    let dispatcher = TxDispatcher::new();
    dispatcher.register(subscriber.clone());

    // Spawn stats printer
    tokio::spawn({
        let subscriber = subscriber.clone();
        async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                let stats = subscriber.get_stats();
                info!("📊 Current monitoring stats:");
                print_stats(&stats);
            }
        }
    });

    info!("Starting hybrid monitoring for 10 minutes...");
    info!("Press Ctrl+C to stop early");
    info!("");

    // Run dispatcher in background
    let dispatcher_handle = tokio::spawn(async move {
        dispatcher.run().await;
    });

    // Wait for monitoring to complete or timeout
    tokio::select! {
        result = result_handle => {
            match result {
                Ok(hybrid_result) => {
                    info!("✅ Monitoring completed!");
                    info!("");

                    // Generate report
                    let stats = subscriber.get_stats();
                    let report = MonitoringReport::from_result(hybrid_result.clone(), stats);

                    println!("{}", report.summary());

                    if report.is_suspicious() {
                        warn!("🚨 SUSPICIOUS PATTERN DETECTED!");
                        let analyzer = PatternAnalyzer::from_result(&hybrid_result);
                        warn!("   Suspicion score: {:.2}", analyzer.calculate_suspicion_score());

                        let chains = analyzer.find_chains();
                        if !chains.is_empty() {
                            warn!("   Scatter-gather chains found: {}", chains.len());
                        }

                        let timing = analyzer.analyze_timing();
                        warn!("   Average timing gap: {:?}", timing.avg_gap);
                    }
                }
                Err(e) => {
                    eprintln!("Error in monitoring: {:?}", e);
                }
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Interrupted by user");
        }
    }

    dispatcher_handle.abort();
}

/// Mode 4: Demo mode with simulated workflow
async fn demo_mode() {
    info!("🎮 Demo Mode");
    info!("This demonstrates the workflow with example addresses");
    info!("(Note: These are random addresses, no real activity expected)");
    info!("---");

    // Generate some demo addresses
    let root1 = Pubkey::new_unique();
    let root2 = Pubkey::new_unique();
    let convergence = Pubkey::new_unique();

    info!("Simulated scenario:");
    info!("  Root 1: {}", root1);
    info!("  Root 2: {}", root2);
    info!("  Convergence: {}", convergence);
    info!("");

    // Create workflow
    let config = TraceConfig {
        spam_threshold: 0.0001,
        min_convergence_degree: 2, // Low threshold for demo
        max_tracked_addresses: 10_000,
        auto_expand: true,
        max_expansion_depth: 5,
    };

    let workflow = TraceWorkflow::from_offline_trace(vec![root1, root2, convergence], config);

    let subscriber = workflow.subscriber();

    // Setup event monitoring
    info!("Setting up event monitoring...");
    info!("Monitoring will run for 60 seconds or until we find patterns");
    info!("");

    // Register with dispatcher
    let dispatcher = TxDispatcher::new();
    dispatcher.register(subscriber.clone());

    // Spawn stats printer
    tokio::spawn({
        let subscriber = subscriber.clone();
        async move {
            let mut tick = 0;
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                tick += 1;

                let stats = subscriber.get_stats();
                info!("📊 Stats ({}0s):", tick);
                print_stats(&stats);

                let convergence_points = subscriber.get_convergence_points();
                if !convergence_points.is_empty() {
                    info!("   Convergence points found: {}", convergence_points.len());
                }

                let scatter_points = subscriber.get_scatter_points();
                if !scatter_points.is_empty() {
                    info!("   Scatter points found: {}", scatter_points.len());
                }

                info!("");
            }
        }
    });

    // Run workflow with timeout
    let result_handle =
        tokio::spawn(async move { workflow.run_timed(Duration::from_secs(60)).await });

    // Run dispatcher
    let dispatcher_handle = tokio::spawn(async move {
        dispatcher.run().await;
    });

    // Wait for completion
    tokio::select! {
        result = result_handle => {
            match result {
                Ok(hybrid_result) => {
                    info!("✅ Demo monitoring completed!");
                    info!("");
                    info!("Results:");
                    info!("  Discovered addresses: {}", hybrid_result.discovered_addresses.len());
                    info!("  Convergence points: {}", hybrid_result.convergence_points.len());
                    info!("  Scatter points: {}", hybrid_result.scatter_points.len());
                }
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                }
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Interrupted by user");
        }
    }

    dispatcher_handle.abort();
}

/// Helper: Print monitoring statistics
fn print_stats(stats: &MonitorStats) {
    info!("   Total addresses: {}", stats.total_addresses);
    info!("   Total edges: {}", stats.total_edges);
    info!("   Convergence points: {}", stats.convergence_points);
    info!("   Scatter points: {}", stats.scatter_points);
    info!(
        "   Max in/out degree: {}/{}",
        stats.max_incoming, stats.max_outgoing
    );
}

/// Helper: Truncate pubkey for display
fn truncate_pubkey(pk: &Pubkey) -> String {
    let s = pk.to_string();
    format!("{}...{}", &s[..4], &s[s.len() - 4..])
}

/// Helper: Truncate signature for display
fn truncate_sig(sig: &str) -> String {
    if sig.len() > 16 {
        format!("{}...{}", &sig[..8], &sig[sig.len() - 8..])
    } else {
        sig.to_string()
    }
}

// ============================================================================
// Advanced Examples
// ============================================================================

/// Example: Integration with offline trace (commented - requires line.rs)
#[allow(dead_code)]
async fn example_with_offline_trace() {
    // Step 1: Run offline backward trace to find roots
    // (Assuming you have access to the trace_backward function from line.rs)
    /*
    use your_crate::trace_tool::trace_backward;

    let convergence_point = "YourConvergenceAddress".parse().unwrap();
    let backward_result = trace_backward(convergence_point).await;

    // Extract all addresses from trace results
    let mut trace_addresses = Vec::new();
    trace_addresses.push(convergence_point);

    for root_info in backward_result.roots {
        trace_addresses.push(root_info.address);
        trace_addresses.extend(root_info.path);
    }

    info!("Offline trace found {} addresses", trace_addresses.len());
    */

    // Step 2: Start real-time monitoring
    let trace_addresses = vec![Pubkey::new_unique()]; // Placeholder

    let config = strategies::mixer_detection();
    let workflow = TraceWorkflow::from_offline_trace(trace_addresses.clone(), config);

    let subscriber = workflow.subscriber();

    // Step 3: Run with condition (stop when we find enough convergence points)
    let result = workflow
        .run_until(|_stats, convergence, _scatter| {
            // Stop when we find 5 convergence points
            convergence.len() >= 5
        })
        .await;

    // Step 4: Analyze
    let stats = subscriber.get_stats();
    let report = MonitoringReport::from_result(result, stats);

    println!("{}", report.summary());
}

/// Example: Custom event processing with database storage
#[allow(dead_code)]
async fn example_with_database() {
    let config = TraceConfig::default();
    let (subscriber, mut event_rx) = ScatterGatherSubscriber::new(config);
    let subscriber = Arc::new(subscriber);

    // Add initial addresses
    subscriber.add_watch_addresses(vec![Pubkey::new_unique()]);

    // Custom event processor with DB storage
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            match event {
                TraceEvent::Convergence {
                    address,
                    incoming_count,
                    sources: _,
                } => {
                    // Store to database
                    // db.insert_convergence_event(address, incoming_count, sources).await;

                    // Send alert if high degree
                    if incoming_count >= 20 {
                        // send_alert(&format!("High-degree convergence: {}", address)).await;
                        info!(
                            "🚨 ALERT: High-degree convergence at {} with {} sources",
                            address, incoming_count
                        );
                    }
                }

                TraceEvent::Transfer {
                    from: _,
                    to: _,
                    amount,
                    signature: _,
                    slot: _,
                } => {
                    // Store transfer to DB
                    // db.insert_transfer(from, to, amount, signature, slot).await;

                    // Check for suspicious patterns
                    if amount > 10.0 {
                        info!("💰 Large transfer detected: {:.2} SOL", amount);
                    }
                }

                TraceEvent::Scatter {
                    address,
                    outgoing_count,
                    ..
                } => {
                    // db.insert_scatter_event(address, outgoing_count).await;

                    if outgoing_count >= 50 {
                        info!("🚨 ALERT: Large scatter from {}", address);
                    }
                }

                _ => {}
            }
        }
    });

    // Run monitoring...
}

/// Example: Chained investigation (convergence -> backward trace -> monitor roots)
#[allow(dead_code)]
async fn example_chained_investigation() {
    let initial_convergence = Pubkey::new_unique();

    info!("🔍 Phase 1: Backward trace from convergence point");
    // let backward_result = trace_backward(initial_convergence).await;
    // let roots = backward_result.roots;

    // Simulate
    let roots = vec![Pubkey::new_unique(), Pubkey::new_unique()];

    info!("   Found {} roots", roots.len());

    info!("🔍 Phase 2: Real-time monitoring of roots");
    let config = strategies::high_frequency_laundering();
    let (subscriber, mut event_rx) = ScatterGatherSubscriber::new(config);
    let subscriber = Arc::new(subscriber);

    subscriber.add_watch_addresses(roots.clone());

    // When we discover new convergence points, trigger backward trace
    let subscriber_clone = subscriber.clone();
    tokio::spawn(async move {
        let mut investigated = HashSet::new();

        while let Some(event) = event_rx.recv().await {
            if let TraceEvent::Convergence {
                address,
                incoming_count,
                ..
            } = event
            {
                if incoming_count >= 10 && !investigated.contains(&address) {
                    investigated.insert(address);

                    info!(
                        "🔍 Phase 3: New convergence {}, triggering backward trace",
                        address
                    );

                    // Spawn backward trace
                    tokio::spawn({
                        let subscriber = subscriber_clone.clone();
                        async move {
                            // let new_backward = trace_backward(address).await;
                            // let new_roots: Vec<Pubkey> = new_backward.roots
                            //     .iter()
                            //     .map(|r| r.address)
                            //     .collect();

                            let new_roots = vec![]; // Placeholder

                            if !new_roots.is_empty() {
                                info!(
                                    "   Found {} new roots, adding to monitoring",
                                    new_roots.len()
                                );
                                subscriber.add_watch_addresses(new_roots);
                            }
                        }
                    });
                }
            }
        }
    });

    // Run dispatcher...
    let dispatcher = TxDispatcher::new();
    dispatcher.register(subscriber);
    dispatcher.run().await;
}

/// Example: Generate monitoring report
#[allow(dead_code)]
async fn example_generate_report() {
    let addresses = vec![Pubkey::new_unique()];
    let config = strategies::quick_scan();

    let workflow = TraceWorkflow::from_offline_trace(addresses, config);
    let subscriber = workflow.subscriber();

    // Run for 5 minutes
    let result = workflow.run_timed(Duration::from_secs(300)).await;

    // Generate comprehensive report
    let stats = subscriber.get_stats();
    let report = MonitoringReport::from_result(result.clone(), stats);

    // Print summary
    println!("{}", report.summary());

    // Detailed analysis
    let analyzer = PatternAnalyzer::from_result(&result);

    println!("\n=== Detailed Analysis ===\n");
    println!(
        "Suspicion Score: {:.2}",
        analyzer.calculate_suspicion_score()
    );

    let chains = analyzer.find_chains();
    println!("Chain Patterns: {}", chains.len());
    for (i, chain) in chains.iter().enumerate() {
        println!("  Chain {}: {} addresses", i + 1, chain.len());
    }

    let timing = analyzer.analyze_timing();
    println!("\nTiming Analysis:");
    println!("  Samples: {}", timing.sample_count);
    println!("  Average gap: {:?}", timing.avg_gap);
    println!("  Min gap: {:?}", timing.min_gap);
    println!("  Max gap: {:?}", timing.max_gap);

    println!("\nConvergence Points:");
    for (i, conv) in result.convergence_points.iter().enumerate() {
        println!(
            "  {}. {} ({} sources) at {:?}",
            i + 1,
            conv.address,
            conv.incoming_count,
            conv.detected_at.duration_since(result.started_at)
        );
    }

    println!("\nScatter Points:");
    for (i, scatter) in result.scatter_points.iter().enumerate() {
        println!(
            "  {}. {} ({} destinations) at {:?}",
            i + 1,
            scatter.address,
            scatter.outgoing_count,
            scatter.detected_at.duration_since(result.started_at)
        );
    }

    // Risk assessment
    if report.is_suspicious() {
        println!("\n⚠️  RISK ASSESSMENT: HIGH");
        println!("Recommended action: Manual review and possible escalation");
    } else {
        println!("\n✅ RISK ASSESSMENT: LOW");
        println!("Pattern appears normal for monitored addresses");
    }
}
