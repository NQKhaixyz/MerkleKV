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
        // -----------------------------------------------------------------------------
        // Design Note (Security & Operability)
        // This block implements environment-first resolution for identity (Client ID)
        // and secret (password). The objective is twofold: (i) enable secure injection
        // of credentials via deployment tooling, and (ii) preserve configuration
        // determinism when environment variables are absent. The approach deliberately
        // avoids widening the configuration module's responsibilities.
        // -----------------------------------------------------------------------------
        
        // Environment-first resolution of identity and credentials
        // Client ID: env var CLIENT_ID overrides config
        let effective_client_id = std::env::var("CLIENT_ID")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| config.replication.client_id.clone());

        // Password: env var CLIENT_PASSWORD overrides config.replication.client_password
        let effective_password = std::env::var("CLIENT_PASSWORD")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| config.replication.client_password.clone());

        // Configure MQTT client options
        let mut mqtt_options = MqttOptions::new(
            &effective_client_id,
            &config.replication.mqtt_broker,
            config.replication.mqtt_port,
        );
        mqtt_options.set_keep_alive(Duration::from_secs(30));

        // -----------------------------------------------------------------------------
        // Rationale (Compatibility)
        // We re-use the effective Client ID as the MQTT username when a password is
        // provided. This conservative choice avoids a schema expansion and sustains
        // backwards compatibility. If a distinct username becomes necessary, it can be
        // added later without perturbing the present call sites.
        // -----------------------------------------------------------------------------
        
        // Some brokers accept "username + password". In the absence of a dedicated
        // username field in configuration, we conservatively re-use the Client ID
        // as the username. This preserves the existing configuration surface.
        // If a future username field is introduced, it can replace this parameter.
        if let Some(pw) = effective_password {
            mqtt_options.set_credentials(effective_client_id.clone(), pw);
        }
        
    // Create MQTT client and event loop
    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);
        
        // Subscribe to the replication topic pattern
        let topic = format!("{}/events/#", config.replication.topic_prefix);
        client.subscribe(&topic, QoS::AtLeastOnce).await?;

        // Create broadcast channel and spawn the MQTT poller
        let (tx, _rx_unused) = broadcast::channel::<ChangeEvent>(1024);
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        match ChangeEvent::decode_any(&p.payload) {
                            Ok(ev) => {
                                let _ = tx_clone.send(ev); // ignore errors if no receivers
                            }
                            Err(e) => warn!("Failed to decode ChangeEvent: {}", e),
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
                let current_ts = last_ts.get(&ev.key).cloned().unwrap_or(0);
                if ev.ts < current_ts { continue; } // LWW

                let mut guard = store.lock().await;
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
                // Update LWW state and dedupe set
                last_ts.insert(ev.key.clone(), ev.ts);
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
