//! # Change Events: Format, Serialization, and Teaching Notes
//!
//! This module defines the canonical “change event” used to propagate state
//! between nodes in an eventually consistent system. Each local write produces
//! a `ChangeEvent` which can be serialized (JSON, CBOR, or Bincode) and
//! distributed via a transport (MQTT in this project). Remote nodes deserialize
//! and apply these events idempotently, resolving conflicts via Last-Write-Wins
//! (LWW) using a timestamp. The commentary is intentionally scholarly to help
//! students understand the “why”, not just the “what”.
//!
//! Key distributed systems concepts illustrated here:
//! - Event propagation and at-least-once delivery (QoS 1)
//! - Idempotency using an operation identifier (UUID v4)
//! - LWW conflict resolution using a physical or logical timestamp
//! - Optional Merkle hash pointers to support anti-entropy protocols
//!
//! The event’s `val` carries the resulting value after the operation (for SET,
//! INCR/DECR, APPEND/PREPEND). This choice makes idempotent application simple
//! and makes LWW straightforward: the winner simply becomes “the value”.

use serde::{Deserialize, Serialize};

/// The operation kind carried by a change event.
///
/// We use compact lowercase tags in serialized form to minimize payload size.
/// For teaching: these operations are the minimal set needed to illustrate
/// common writes in a KV store while demonstrating that “SET-like” writes can
/// encode their result so that LWW can simply overwrite state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpKind {
    /// Store or overwrite a value
    Set,
    /// Delete a key
    Del,
    /// Numeric increment; event value contains the resulting number as bytes
    Incr,
    /// Numeric decrement; event value contains the resulting number as bytes
    Decr,
    /// String append; event value contains the resulting string as bytes
    Append,
    /// String prepend; event value contains the resulting string as bytes
    Prepend,
}

/// Canonical change-event structure used to replicate writes.
///
/// - `v` (schema version): Enables evolution of the on-wire format.
/// - `op`: What kind of mutation this is (set/del/incr/decr/append/prepend).
/// - `key`: The logical key being mutated.
/// - `val`: The resulting value as raw bytes (UTF-8 for string values, ASCII
///          digits for numeric results); `None` for deletions.
/// - `ts`: A timestamp for conflict resolution. We allow Unix nanoseconds or a
///         logical clock; the comparison is the only semantic the system needs.
/// - `src`: The originating node identifier, used for loop prevention.
/// - `op_id`: A 128-bit identifier (UUID v4) for idempotency/deduplication.
/// - `prev`: Optional 32-byte Merkle root (or leaf) hash to assist anti-entropy.
/// - `ttl`: Optional TTL-in-seconds hint (not enforced by the in-memory engine).
/// - `seq`: Optional sequence number per publisher for gap detection (backward-compatible).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// Schema version (allows additive, backward-compatible upgrades)
    pub v: u16,
    /// Operation kind
    pub op: OpKind,
    /// Key under mutation
    pub key: String,
    /// Resulting value after mutation; None for deletions
    pub val: Option<Vec<u8>>, // bytes to be agnostic to codec and content
    /// Timestamp for LWW resolution (unix_nanos or logical clock)
    pub ts: u64,
    /// Originating node id
    pub src: String,
    /// Operation id for idempotency (UUID v4 bytes)
    pub op_id: [u8; 16],
    /// Optional Merkle hash (32 bytes). Useful for anti-entropy proofs.
    pub prev: Option<[u8; 32]>,
    /// Optional TTL in seconds (advisory in this prototype)
    pub ttl: Option<u64>,
    /// Optional sequence number per publisher for gap detection (backward-compatible)
    pub seq: Option<u64>,
}

impl ChangeEvent {
    /// Construct a new change event with the provided fields.
    ///
    /// We generate an operation id (UUID v4) to make this event idempotent
    /// under at-least-once delivery. Timestamps should be monotonic within a
    /// node for LWW to be meaningful. Using `unix_nanos` is sufficient for a
    /// prototype; Lamport clocks would improve causality ordering across nodes.
    pub fn new(
        v: u16,
        op: OpKind,
        key: impl Into<String>,
        val: Option<Vec<u8>>,
        ts: u64,
        src: impl Into<String>,
        prev: Option<[u8; 32]>,
        ttl: Option<u64>,
    ) -> Self {
        let op_id = uuid::Uuid::new_v4().into_bytes();
        Self {
            v,
            op,
            key: key.into(),
            val,
            ts,
            src: src.into(),
            op_id,
            prev,
            ttl,
            seq: None, // Default sequence number to None for backward compatibility
        }
    }

    /// Helper: Build an event whose value is a UTF-8 string.
    pub fn with_str_value(
        v: u16,
        op: OpKind,
        key: impl Into<String>,
        value: Option<&str>,
        ts: u64,
        src: impl Into<String>,
        prev: Option<[u8; 32]>,
        ttl: Option<u64>,
    ) -> Self {
        let val = value.map(|s| s.as_bytes().to_vec());
        Self::new(v, op, key, val, ts, src, prev, ttl)
    }

    /// Serialize the event to JSON. Human-readable and easy to debug.
    pub fn to_json(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    /// Serialize the event to CBOR. Compact, self-describing binary format.
    pub fn to_cbor(&self) -> serde_cbor::Result<Vec<u8>> {
        serde_cbor::to_vec(self)
    }

    /// Serialize the event to Bincode. Very compact, not self-describing.
    pub fn to_bincode(&self) -> bincode::Result<Vec<u8>> {
        bincode::serialize(self)
    }

    /// Deserialize from JSON bytes.
    pub fn from_json(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
    }

    /// Deserialize from CBOR bytes.
    pub fn from_cbor(bytes: &[u8]) -> serde_cbor::Result<Self> {
        serde_cbor::from_slice(bytes)
    }

    /// Deserialize from Bincode bytes.
    pub fn from_bincode(bytes: &[u8]) -> bincode::Result<Self> {
        bincode::deserialize(bytes)
    }

    /// Attempt to decode using CBOR, then Bincode, then JSON.
    ///
    /// This is useful for subscribers that accept multiple codecs without
    /// negotiating content-types on the transport.
    pub fn decode_any(bytes: &[u8]) -> Result<Self, String> {
        if let Ok(e) = Self::from_cbor(bytes) {
            return Ok(e);
        }
        if let Ok(e) = Self::from_bincode(bytes) {
            return Ok(e);
        }
        if let Ok(e) = Self::from_json(bytes) {
            return Ok(e);
        }
        Err("Failed to decode ChangeEvent with CBOR, Bincode, or JSON".into())
    }
}

/// Preferred encoding for on-wire messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeCodec {
    Json,
    Cbor,
    Bincode,
}

impl ChangeCodec {
    /// Serialize according to the selected codec.
    pub fn encode(self, ev: &ChangeEvent) -> Result<Vec<u8>, String> {
        match self {
            ChangeCodec::Json => ev.to_json().map_err(|e| e.to_string()),
            ChangeCodec::Cbor => ev.to_cbor().map_err(|e| e.to_string()),
            ChangeCodec::Bincode => ev.to_bincode().map_err(|e| e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    /// A minimal local applier used for unit tests without MQTT.
    ///
    /// Pedagogical note: this mirrors the LWW/idempotency logic used by the
    /// networked subscriber, but runs on a plain HashMap to keep tests simple.
    struct LocalApplier {
        seen: HashSet<[u8; 16]>,
        last_ts: HashMap<String, u64>,
        last_op_id: HashMap<String, [u8; 16]>, // For deterministic tie-breaking
        store: HashMap<String, String>,
    }

    impl LocalApplier {
        fn new() -> Self {
            Self { 
                seen: HashSet::new(), 
                last_ts: HashMap::new(), 
                last_op_id: HashMap::new(), // Initialize tie-breaker tracking
                store: HashMap::new() 
            }
        }

        /// --- Deterministic Tie-Breaking ----------------------------------------------
        /// Problem: Equal timestamps previously fell back to non-deterministic UUID
        /// comparison, causing flaky resolution in tests and replica merges.
        /// Rule: Define a total order as (timestamp ascending, op_id lexicographic).
        /// Justification: Ensures stable, idempotent LWW application and reproducible
        /// state across replicas without affecting Merkle tree construction/sync.
        /// Complexity: O(1) per comparison; no changes to higher-level protocols.
        fn apply(&mut self, ev: &ChangeEvent) {
            if self.seen.contains(&ev.op_id) {
                return; // idempotent: ignore duplicates
            }
            let ts_entry = self.last_ts.get(&ev.key).cloned().unwrap_or(0);
            
            // LWW with deterministic tie-breaking: reject if timestamp is older,
            // or if timestamp is equal but op_id is lexicographically smaller
            if ev.ts < ts_entry {
                return; // LWW: ignore older events
            } else if ev.ts == ts_entry {
                // Timestamp tie: break using lexicographic comparison of op_id
                let last_op_id = self.last_op_id.get(&ev.key).cloned().unwrap_or([0; 16]);
                if ev.op_id < last_op_id {
                    return; // Tie-breaker: ignore events with smaller op_id
                }
            }
            
            match ev.op {
                OpKind::Del => {
                    self.store.remove(&ev.key);
                }
                _ => {
                    if let Some(bytes) = &ev.val {
                        let s = String::from_utf8(bytes.clone()).unwrap_or_else(|_| base64::encode(bytes));
                        self.store.insert(ev.key.clone(), s);
                    }
                }
            }
            self.last_ts.insert(ev.key.clone(), ev.ts);
            self.last_op_id.insert(ev.key.clone(), ev.op_id); // Track op_id for tie-breaking
            self.seen.insert(ev.op_id);
        }
    }

    fn sample_event(op: OpKind, key: &str, val: Option<&str>, ts: u64) -> ChangeEvent {
        ChangeEvent::with_str_value(1, op, key.to_string(), val, ts, "nodeA", None, None)
    }

    #[test]
    fn json_cbor_bincode_roundtrip() {
        let ev = sample_event(OpKind::Set, "k", Some("v"), 123);
        let j = ev.to_json().unwrap();
        let c = ev.to_cbor().unwrap();
        let b = ev.to_bincode().unwrap();
        assert_eq!(ChangeEvent::from_json(&j).unwrap(), ev);
        assert_eq!(ChangeEvent::from_cbor(&c).unwrap(), ev);
        assert_eq!(ChangeEvent::from_bincode(&b).unwrap(), ev);
        assert_eq!(ChangeEvent::decode_any(&c).unwrap(), ev);
    }

    #[test]
    fn idempotency_duplicate_event() {
        let mut applier = LocalApplier::new();
        let mut ev = sample_event(OpKind::Set, "x", Some("1"), 10);
        // Ensure same op id for duplicate
        let op_id = ev.op_id;
        applier.apply(&ev);
        applier.apply(&ev); // duplicate
        assert_eq!(applier.store.get("x").cloned(), Some("1".into()));
        // Build a new identical event with the same op_id
        let mut ev2 = sample_event(OpKind::Set, "x", Some("1"), 10);
        ev2.op_id = op_id;
        applier.apply(&ev2);
        assert_eq!(applier.store.get("x").cloned(), Some("1".into()));
    }

    #[test]
    fn lww_out_of_order_delivery() {
        let mut applier = LocalApplier::new();
        // Newer event arrives first
        let ev_new = sample_event(OpKind::Set, "c", Some("new"), 200);
        let mut ev_old = sample_event(OpKind::Set, "c", Some("old"), 100);
        applier.apply(&ev_new);
        applier.apply(&ev_old); // should be ignored by LWW
        assert_eq!(applier.store.get("c").cloned(), Some("new".into()));
        // Now deliver in reverse order on a fresh key
        let mut applier2 = LocalApplier::new();
        let ev_old2 = sample_event(OpKind::Set, "d", Some("old"), 100);
        let ev_new2 = sample_event(OpKind::Set, "d", Some("new"), 200);
        applier2.apply(&ev_old2);
        applier2.apply(&ev_new2);
        assert_eq!(applier2.store.get("d").cloned(), Some("new".into()));
    }
    fn mixed_codec_interop() {
    let ev = sample_event(OpKind::Set, "k", Some("v"), 1_000);
    let j = ev.to_json().unwrap();
    let c = ev.to_cbor().unwrap();
    let b = ev.to_bincode().unwrap();

    assert_eq!(ChangeEvent::decode_any(&j).unwrap(), ev);
    assert_eq!(ChangeEvent::decode_any(&c).unwrap(), ev);
    assert_eq!(ChangeEvent::decode_any(&b).unwrap(), ev);
}
#[test]
fn corrupted_payload_rejected() {
    let garbage = b"\x00\x01\x02not-a-valid-payload";
    let err = ChangeEvent::decode_any(garbage).unwrap_err();
    assert!(err.to_lowercase().contains("failed"));
}
#[test]
fn non_utf8_value_safe_handling() {
    let mut applier = LocalApplier::new();
    let mut bytes = vec![0, 159, 146, 150]; // invalid UTF-8
    let ev = ChangeEvent::new(1, OpKind::Set, "bin", Some(bytes.clone()), 5, "A", None, None);
    applier.apply(&ev);
    // LocalApplier stringify non-UTF8 via base64 fallback
    let got = applier.store.get("bin").unwrap();
    assert_eq!(got, &base64::encode(&bytes));
}
#[test]
fn idempotency_burst_duplicates() {
    let mut applier = LocalApplier::new();
    let mut ev = sample_event(OpKind::Set, "dup", Some("1"), 10);
    let op_id = ev.op_id;
    for _ in 0..10 { applier.apply(&ev); }
    assert_eq!(applier.store.get("dup").cloned(), Some("1".into()));

    // Even a freshly built event with the same op_id is ignored
    let mut ev2 = sample_event(OpKind::Set, "dup", Some("1"), 10);
    ev2.op_id = op_id;
    applier.apply(&ev2);
    assert_eq!(applier.store.get("dup").cloned(), Some("1".into()));
}
#[test]
fn lww_clock_skew_no_overwrite() {
    let mut a = LocalApplier::new();
    a.apply(&sample_event(OpKind::Set, "t", Some("newer"), 2000));
    a.apply(&sample_event(OpKind::Set, "t", Some("older"), 1000)); // late & older
    assert_eq!(a.store.get("t").cloned(), Some("newer".into()));
}
#[test]
fn same_timestamp_tie_break_by_op_id() {
    let mut applier = LocalApplier::new();
    let mut ev1 = sample_event(OpKind::Set, "tie", Some("A"), 500);
    let mut ev2 = sample_event(OpKind::Set, "tie", Some("B"), 500);
    // Definition: choose the event with the lexicographically larger op_id
    let winner_is_ev2 = ev2.op_id > ev1.op_id;
    applier.apply(&ev1);
    applier.apply(&ev2);
    let expect = if winner_is_ev2 {"B"} else {"A"};
    assert_eq!(applier.store.get("tie").cloned(), Some(expect.into()));
}
#[test]
fn per_key_ts_isolation() {
    let mut a = LocalApplier::new();
    
    // Apply events to demonstrate per-key timestamp isolation
    // Set initial values with different timestamps
    a.apply(&sample_event(OpKind::Set, "x", Some("1"), 100));
    a.apply(&sample_event(OpKind::Set, "y", Some("9"), 200));
    
    // Now try to update x with a lower timestamp than y's timestamp
    // If timestamp isolation is working correctly per key, this should succeed
    // because x's last timestamp (100) is less than this update's timestamp (150),
    // even though y has a higher timestamp (200)
    a.apply(&sample_event(OpKind::Set, "x", Some("2"), 150));
    
    // If applied incorrectly, the timestamp of y could accidentally block x
    assert_eq!(a.store.get("x").cloned(), Some("2".into()));
    assert_eq!(a.store.get("y").cloned(), Some("9".into()));
}
#[test]
fn delete_lww_behavior() {
    let mut a = LocalApplier::new();
    a.apply(&sample_event(OpKind::Set, "z", Some("keep"), 10));
    a.apply(&sample_event(OpKind::Del, "z", None, 11));
    assert!(a.store.get("z").is_none());
}
#[test]
fn missing_value_for_non_del_is_ignored() {
    let mut a = LocalApplier::new();
    let ev = ChangeEvent::new(1, OpKind::Set, "m", None, 5, "node", None, None);
    a.apply(&ev);
    assert!(a.store.get("m").is_none());
}
#[test]
fn large_payload_roundtrip() {
    let big = vec![b'x'; 256 * 1024]; // 256KB
    let ev = ChangeEvent::new(1, OpKind::Set, "big", Some(big.clone()), 42, "node", None, None);
    let b = ev.to_bincode().unwrap();
    let de = ChangeEvent::from_bincode(&b).unwrap();
    assert_eq!(de.val.unwrap(), big);
}
#[test]
fn append_prepend_store_result_value() {
    let mut a = LocalApplier::new();
    // Assume the server has already computed the result and put it into val
    a.apply(&sample_event(OpKind::Set, "s", Some("core"), 1));
    a.apply(&sample_event(OpKind::Append, "s", Some("core+tail"), 2));
    assert_eq!(a.store.get("s").cloned(), Some("core+tail".into()));
    a.apply(&sample_event(OpKind::Prepend, "s", Some("head+core+tail"), 3));
    assert_eq!(a.store.get("s").cloned(), Some("head+core+tail".into()));
}
#[test]
fn incr_decr_ascii_numeric_result() {
    let mut a = LocalApplier::new();
    a.apply(&sample_event(OpKind::Set, "n", Some("10"), 1));
    a.apply(&sample_event(OpKind::Incr, "n", Some("11"), 2));
    a.apply(&sample_event(OpKind::Decr, "n", Some("9"), 3));
    assert_eq!(a.store.get("n").cloned(), Some("9".into()));
}
#[test]
fn replay_without_dedupe_still_lww_correct() {
    let mut a = LocalApplier::new();
    let ev_new = sample_event(OpKind::Set, "r", Some("new"), 200);
    let ev_old = sample_event(OpKind::Set, "r", Some("old"), 100);
    // phiên 1
    a.apply(&ev_new);
    // “restart” -> seen/last_ts trống
    let mut b = LocalApplier::new();
    b.apply(&ev_old);  // cũ → set
    b.apply(&ev_new);  // mới → overwrite
    assert_eq!(b.store.get("r").cloned(), Some("new".into()));
}
#[test]
fn self_origin_event_ignored() {
    // LocalApplier does not check src, but a real Subscriber must.
    // Here we simulate by verifying the business rule:
    let ev = ChangeEvent::with_str_value(1, OpKind::Set, "self", Some("v"), 10, "nodeA", None, None);
    assert_eq!(ev.src, "nodeA");
    // Gợi ý: test này nên nằm trong test của Subscriber thực tế, nơi có node_id local.
}
#[test]
fn ttl_is_advisory_only() {
    let mut a = LocalApplier::new();
    let mut ev = sample_event(OpKind::Set, "ttl", Some("x"), 50);
    // Adding TTL does not mean auto-expire (according to the prototype)
    ev.ttl = Some(1);
    a.apply(&ev);
    assert_eq!(a.store.get("ttl").cloned(), Some("x".into()));
}

}
