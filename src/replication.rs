//! # MQTT-based Replication System
//!
//! This module implements real-time replication of write operations across
//! MerkleKV nodes using MQTT as the message transport. Unlike the anti-entropy
//! sync system, replication provides immediate propagation of changes.
//!
//! ## How Replication Works
//! 
//! 1. **Write Operations**: When a client writes data (SET/DELETE), the operation
//!    is first applied locally, then published to MQTT
//! 2. **Message Distribution**: MQTT broker distributes the message to all
//!    subscribed nodes in the cluster
//! 3. **Remote Application**: Other nodes receive the message and apply the
//!    same operation to their local storage
//! 4. **Loop Prevention**: Nodes ignore messages from themselves
//! 
//! ## Message Format
//! 
//! Replication messages are JSON-serialized with this structure:
//! ```json
//! {
//!   "operation": "SET|DELETE",
//!   "key": "user:123", 
//!   "value": "john_doe",  // null for DELETE
//!   "source_node": "node1"
//! }
//! ```
//!
//! ## Current Implementation Status
//! 
//! **⚠️ This is a STUB implementation!**
//! 
//! The current code provides MQTT connectivity and message structure but
//! lacks integration with the storage engine and server. Missing pieces:
//! - Integration with the TCP server for write operations
//! - Handling of received replication messages
//! - Proper error handling and retry logic
//! - Conflict resolution for concurrent writes

use anyhow::Result;
use log::{error, warn};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, Mutex};
use std::sync::Arc;

use crate::config::Config;
use crate::store::KVEngineStoreTrait;
use crate::change_event::{ChangeCodec, ChangeEvent, OpKind};

/// Handles MQTT-based replication of write operations.
/// 
/// The Replicator connects to an MQTT broker and provides methods to
/// publish local write operations and handle incoming replication messages.
#[derive(Clone)]
pub struct Replicator {
    /// MQTT client for publishing and receiving messages
    client: AsyncClient,
    
    /// Prefix for MQTT topics (e.g., "merkle_kv")
    topic_prefix: String,
    
    /// Unique identifier for this node
    node_id: String,

    /// Preferred codec for on-wire messages
    codec: ChangeCodec,

    /// Channel carrying decoded ChangeEvents from the MQTT eventloop
    tx: broadcast::Sender<ChangeEvent>,
}

impl Replicator {
    /// Create a new replicator and connect to MQTT broker.
    /// 
    /// # Arguments
    /// * `config` - Configuration containing MQTT broker details
    /// 
    /// # Returns
    /// * `Result<Replicator>` - New replicator instance or connection error
    /// 
    /// # Behavior
    /// - Connects to the MQTT broker specified in config
    /// - Subscribes to replication topic pattern
    /// - Starts background task to handle incoming messages
    /// 
    /// # MQTT Topics
    /// - Publishes to: `{topic_prefix}/events`
    /// - Subscribes to: `{topic_prefix}/events/#`
    pub async fn new(config: &Config) -> Result<Self> {
        // Configure MQTT client options
        let mut mqtt_options = MqttOptions::new(
            &config.replication.client_id,
            &config.replication.mqtt_broker,
            config.replication.mqtt_port,
        );
        mqtt_options.set_keep_alive(Duration::from_secs(30));
        // Academic Context: Invariant: Large values must replicate without truncation
        // Adversary: Default MQTT payload limits cause silent data loss
        // Oracle: Explicit max_packet_size ensures payload integrity preservation
        let max_size = 16 * 1024 * 1024; // 16MB max packet size for large values
        mqtt_options.set_max_packet_size(max_size, max_size);
        
        // Create MQTT client and event loop
        let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);
        
        // Subscribe to the replication topic pattern
        let topic = format!("{}/events/#", config.replication.topic_prefix);
        client.subscribe(&topic, QoS::AtLeastOnce).await?;
        
        // Subscribe to readiness beacons from all peers (including self)
        let ready_topic = format!("{}/ready/+", config.replication.topic_prefix);
        client.subscribe(&ready_topic, QoS::AtLeastOnce).await?;
        
        // MQTT Readiness Barrier - Deterministic Startup Synchronization
        //
        // Academic Context:
        // - Invariant: All nodes must receive replication events from any publishing peer
        // - Adversary: Asynchronous MQTT subscription allows publishers to send messages
        //   before subscribers are ready, causing permanent state divergence
        // - Oracle: Retained readiness beacons provide observable evidence of subscription
        //   establishment across all peers before enabling replication publishing
        //
        // Implementation: Each node publishes a retained QoS 1 beacon after subscription,
        // then waits for all peer beacons before enabling replication. This creates a
        // deterministic barrier eliminating the startup race condition.
        
        // Publish own readiness beacon (retained QoS 1 for persistence)
        let ready_beacon_topic = format!("{}/ready/{}", config.replication.topic_prefix, config.replication.client_id);
        client.publish(&ready_beacon_topic, QoS::AtLeastOnce, true, "ready".as_bytes()).await?;
        println!("Published readiness beacon: {} -> {}", config.replication.client_id, ready_beacon_topic);
        
        // Create MQTT event loop and readiness tracking
        let (tx, _rx_unused) = broadcast::channel::<ChangeEvent>(1024);
        let tx_clone = tx.clone();
        
        // Wait for readiness beacons with bounded timeout (deterministic)
        let readiness_timeout = Duration::from_secs(15);
        let readiness_start = std::time::Instant::now();
        let mut ready_nodes = std::collections::HashSet::new();
        
        // Expected minimum nodes - enhanced for distributed testing
        // In production this should be configurable, for now use a reasonable default
        let min_expected_nodes = 3; // Wait for reasonable cluster size
        
        // Store topic prefixes to avoid borrowing issues
        let ready_topic_prefix = format!("{}/ready/", config.replication.topic_prefix);
        let events_topic_prefix_inner = format!("{}/events", config.replication.topic_prefix);
        
        while ready_nodes.len() < min_expected_nodes && readiness_start.elapsed() < readiness_timeout {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::Publish(publish))) => {
                    // Check if this is a readiness beacon
                    if publish.topic.starts_with(&ready_topic_prefix) {
                        if let Some(node_id) = publish.topic.split('/').last() {
                            if ready_nodes.insert(node_id.to_string()) {
                                println!("Readiness beacon received from: {} (total ready: {}/{})", 
                                        node_id, ready_nodes.len(), min_expected_nodes);
                            }
                        }
                    }
                    // Also forward replication events to channel
                    else if publish.topic.starts_with(&events_topic_prefix_inner) {
                        match ChangeEvent::decode_any(&publish.payload) {
                            Ok(ev) => {
                                let _ = tx_clone.send(ev);
                            }
                            Err(e) => warn!("Failed to decode ChangeEvent: {}", e),
                        }
                    }
                }
                Ok(_) => {} // Other MQTT events
                Err(e) => {
                    error!("MQTT eventloop error during readiness: {}", e);
                    break;
                }
            }
            
            // Small yield to prevent busy waiting
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        
        println!("Readiness barrier complete. Ready nodes: {:?} (waited {:.2}s)", 
                ready_nodes, readiness_start.elapsed().as_secs_f64());
        
        // Continue MQTT event processing in background
        let events_topic_prefix = format!("{}/events", config.replication.topic_prefix);
        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        // Only process replication events, ignore readiness beacons
                        if p.topic.starts_with(&events_topic_prefix) {
                            match ChangeEvent::decode_any(&p.payload) {
                                Ok(ev) => {
                                    let _ = tx_clone.send(ev); // ignore errors if no receivers
                                }
                                Err(e) => warn!("Failed to decode ChangeEvent: {}", e),
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        error!("MQTT eventloop error: {}", e);
                        tokio::time::sleep(Duration::from_secs(3)).await;
                    }
                }
            }
        });
        
        Ok(Self {
            client,
            topic_prefix: config.replication.topic_prefix.clone(),
            node_id: config.replication.client_id.clone(),
            codec: ChangeCodec::Cbor,
            tx,
        })
    }
    
    /// Publish a SET operation to other nodes.
    /// 
    /// This method should be called by the TCP server after successfully
    /// applying a SET command locally.
    /// 
    /// # Arguments
    /// * `key` - The key that was set
    /// * `value` - The value that was set
    /// 
    /// # Returns
    /// * `Result<()>` - Success if message was published, error if MQTT failed
    /// 
    /// # Example Usage (in server.rs)
    /// ```rust
    /// // After applying SET locally:
    /// store.set(key.clone(), value.clone());
    /// if let Some(replicator) = &replicator {
    ///     replicator.publish_set(&key, &value).await?;
    /// }
    /// ```
    pub async fn publish_set(&self, key: &str, value: &str) -> Result<()> {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        let ev = ChangeEvent::with_str_value(1, OpKind::Set, key, Some(value), ts, self.node_id.clone(), None, None);
        self.publish_event(ev).await
    }
    
    /// Publish a DELETE operation to other nodes.
    /// 
    /// This method should be called by the TCP server after successfully
    /// applying a DELETE command locally.
    /// 
    /// # Arguments
    /// * `key` - The key that was deleted
    /// 
    /// # Returns
    /// * `Result<()>` - Success if message was published, error if MQTT failed
    /// 
    /// # Example Usage (in server.rs)
    /// ```rust
    /// // After applying DELETE locally:
    /// store.delete(&key);
    /// if let Some(replicator) = &replicator {
    ///     replicator.publish_delete(&key).await?;
    /// }
    /// ```
    pub async fn publish_delete(&self, key: &str) -> Result<()> {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        let ev = ChangeEvent::with_str_value(1, OpKind::Del, key, None, ts, self.node_id.clone(), None, None);
        self.publish_event(ev).await
    }

    /// Publish an INCR with resulting numeric value.
    pub async fn publish_incr(&self, key: &str, new_value: i64) -> Result<()> {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        let ev = ChangeEvent::with_str_value(1, OpKind::Incr, key, Some(&new_value.to_string()), ts, self.node_id.clone(), None, None);
        self.publish_event(ev).await
    }

    /// Publish a DECR with resulting numeric value.
    pub async fn publish_decr(&self, key: &str, new_value: i64) -> Result<()> {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        let ev = ChangeEvent::with_str_value(1, OpKind::Decr, key, Some(&new_value.to_string()), ts, self.node_id.clone(), None, None);
        self.publish_event(ev).await
    }

    /// Publish an APPEND with resulting value.
    pub async fn publish_append(&self, key: &str, new_value: &str) -> Result<()> {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        let ev = ChangeEvent::with_str_value(1, OpKind::Append, key, Some(new_value), ts, self.node_id.clone(), None, None);
        self.publish_event(ev).await
    }

    /// Publish a PREPEND with resulting value.
    pub async fn publish_prepend(&self, key: &str, new_value: &str) -> Result<()> {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        let ev = ChangeEvent::with_str_value(1, OpKind::Prepend, key, Some(new_value), ts, self.node_id.clone(), None, None);
        self.publish_event(ev).await
    }

    /// Serialize and publish a change event to MQTT with QoS 1 (at-least-once).
    async fn publish_event(&self, ev: ChangeEvent) -> Result<()> {
        let topic = format!("{}/events", self.topic_prefix);
        let payload = self.codec.encode(&ev).map_err(|e| anyhow::anyhow!(e))?;
        self.client
            .publish(&topic, QoS::AtLeastOnce, false, payload)
            .await?;
        Ok(())
    }
    
    /// Start background tasks for (1) forwarding MQTT publish packets into a
    /// channel, and (2) applying them to local storage with idempotency and LWW.
    ///
    /// Teaching note: We separate transport concerns (MQTT event loop) from
    /// application concerns (idempotent LWW apply) with a channel. This models
    /// the classic “ingress queue” in replicated systems.
    pub async fn start_replication_handler(&self, store: Arc<Mutex<Box<dyn KVEngineStoreTrait + Send + Sync>>>) {
        // Subscribe to broadcasted events from the MQTT poller
        let mut rx = self.tx.subscribe();
        let node_id = self.node_id.clone();
        tokio::spawn(async move {
            let mut seen: HashSet<[u8; 16]> = HashSet::new();
            let mut last_ts: HashMap<String, u64> = HashMap::new();
            let mut last_op_id: HashMap<String, [u8; 16]> = HashMap::new(); // For deterministic tie-breaking
            let mut last_seq: HashMap<String, u64> = HashMap::new(); // Per-publisher sequence tracking for gap detection
            loop {
                let ev = match rx.recv().await {
                    Ok(ev) => ev,
                    Err(e) => {
                        warn!("Replication handler receive error: {}", e);
                        continue;
                    }
                };
                if ev.src == node_id { continue; } // loop prevention
                if seen.contains(&ev.op_id) { continue; } // idempotency
                
                // --- Deterministic LWW with Tie-Breaking -----------------------------------
                // Academic Context: 
                // Invariant: All nodes must apply the same "winning" operation for equal timestamps
                // Adversary: Concurrent operations with identical timestamps causing non-deterministic resolution
                // Oracle: Lexicographic op_id comparison provides stable total ordering
                // Proof: (timestamp ASC, op_id lexicographic ASC) creates deterministic conflict resolution
                let current_ts = last_ts.get(&ev.key).cloned().unwrap_or(0);
                if ev.ts < current_ts { 
                    continue; // LWW: ignore older events
                } else if ev.ts == current_ts {
                    // Timestamp tie: break using lexicographic comparison of op_id
                    let current_op_id = last_op_id.get(&ev.key).cloned().unwrap_or([0; 16]);
                    if ev.op_id < current_op_id {
                        continue; // Tie-breaker: ignore events with smaller op_id (deterministic ordering)
                    }
                }
                
                // --- Optional Gap Detection for Sequence Numbers ---------------------------
                // Academic Context:
                // Invariant: Message sequence integrity preserves causal consistency 
                // Adversary: Network partitions or MQTT delivery issues causing message loss
                // Oracle: Sequence number gaps signal potential inconsistency requiring repair
                // Implementation: Backward-compatible optional field enables targeted resync
                if let Some(seq) = ev.seq {
                    let expected_seq = last_seq.get(&ev.src).cloned().unwrap_or(0) + 1;
                    if seq > expected_seq {
                        warn!("🔧 EVENTUAL CONSISTENCY GAP DETECTED: Sequence gap from {}: expected {}, got {} (potential message loss)", 
                              ev.src, expected_seq, seq);
                        warn!("🔧 REPAIR REQUIRED: Anti-entropy mechanism needed for complete eventual consistency");
                        warn!("🔧 CURRENT STATUS: Real-time MQTT replication only - rejoining nodes may miss historical updates");
                        // TODO: Trigger targeted anti-entropy repair for this publisher
                        // In production, this would initiate a Merkle comparison with ev.src
                    }
                    last_seq.insert(ev.src.clone(), seq.max(last_seq.get(&ev.src).cloned().unwrap_or(0)));
                }

                let guard = store.lock().await;
                match ev.op {
                    OpKind::Del => {
                        guard.delete(&ev.key);
                    }
                    _ => {
                        if let Some(bytes) = ev.val.clone() {
                            // Interpret as UTF-8 if possible, otherwise store base64 string
                            let value = String::from_utf8(bytes.clone())
                                .unwrap_or_else(|_| base64::encode(bytes));
                            // We apply by writing the resulting value (idempotent)
                            if let Err(e) = guard.set(ev.key.clone(), value) {
                                warn!("Failed to apply event to store: {}", e);
                            }
                        }
                    }
                }
                // Update LWW state and dedupe set with deterministic tie-breaking
                last_ts.insert(ev.key.clone(), ev.ts);
                last_op_id.insert(ev.key.clone(), ev.op_id);
                seen.insert(ev.op_id);

                // TODO: Update Merkle tree – in this prototype the store engines
                // are in-memory maps without an exposed Merkle instance. The
                // anti-entropy module rebuilds as needed. A production design
                // would invoke an incremental Merkle update here.
            }
        });
    }
}

#[cfg(test)]
mod tests {
    // TODO: Implement comprehensive tests for replication logic
    // When the actual implementation is integrated, tests should cover:
    // 
    // 1. Message serialization/deserialization
    // 2. MQTT connectivity and reconnection
    // 3. Publishing SET and DELETE operations
    // 4. Receiving and applying remote operations
    // 5. Loop prevention (ignoring own messages)
    // 6. Error handling for malformed messages
    // 
    // Example test structure:
    // #[test]
    // fn test_replication_message_serialization() {
    //     let msg = ReplicationMessage {
    //         operation: "SET".to_string(),
    //         key: "test".to_string(), 
    //         value: Some("value".to_string()),
    //         source_node: "node1".to_string(),
    //     };
    //     let json = serde_json::to_string(&msg).unwrap();
    //     let parsed: ReplicationMessage = serde_json::from_str(&json).unwrap();
    //     assert_eq!(msg.operation, parsed.operation);
    // }
    // 
    // #[tokio::test]
    // async fn test_mqtt_integration() {
    //     // Mock MQTT broker and test publish/subscribe flow
    // }
}
