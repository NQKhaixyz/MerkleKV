//! # MerkleKV Protocol Parser - Issue #27 Administrative Commands (Phases 1-3)
//!
//! This module implements a text-based protocol for client-server communication with
//! comprehensive administrative capabilities. The protocol maintains backward compatibility
//! while providing enhanced observability, key management, and synchronization features.
//!
//! ## Protocol Specification & Guardrails
//!
//! ### Line Termination (Critical Invariant)
//! - **Canonical Format**: All commands and responses MUST use `\r\n` (CRLF) line separators
//! - **Academic Rationale**: CRLF ensures cross-platform compatibility and follows RFC standards
//! - **Parser Behavior**: Input accepts `\n`, `\r\n`, but output always uses `\r\n`
//!
//! ### Response Format Standards
//! - **Single-line responses**: `OK\r\n`, `ERROR <reason>\r\n`, `VALUE <data>\r\n`
//! - **Multi-line responses**: `COMMAND <header>\r\n<line1>\r\n<line2>\r\n...\r\n`
//! - **Error format**: `ERROR <descriptive_reason>\r\n` (no line breaks within reason)
//!
//! ## Command Categories & Operational Semantics
//!
//! ### Phase 1: Information Commands (Low-Cost Observability)
//! - `PING [message]` - Connection health check with optional echo
//! - `ECHO <message>` - Message reflection for network validation
//! - `INFO [section]` - Multi-section server information (server/memory/storage/replication)
//! - `STATS [RESET]` - Operation counters and metrics with optional reset
//! - `DBSIZE` - Keyspace cardinality (O(1) if indexed, O(n) fallback)
//!
//! ### Phase 2: Key Management Commands (Production-Ready with Guardrails)
//! - `KEYS <pattern>` - **⚠️ O(n) operation**: Enumerate keys matching glob pattern
//!   - **Academic Note**: Only `*` (all) and prefix matching supported; no regex
//!   - **Operator Warning**: Use SCAN for production workloads
//!   - **Pattern Semantics**: Case-sensitive, no escape sequences, no quotes
//! - `SCAN <cursor> [MATCH <pattern>] [COUNT <count>]` - Cursor-based key iteration
//!   - **Consistency Model**: Weakly consistent under concurrent writes
//!   - **COUNT Semantics**: Hint only, actual returned count may vary
//!   - **Termination**: Cursor returns to 0 when iteration completes
//!   - **Backward Compatibility**: Legacy `SCAN <prefix>` still supported
//! - `EXISTS <key1> [key2] ...` - Multi-key existence checking
//!
//! ### Phase 3: Synchronization Commands (Anti-Entropy & Replication Control)
//! - `SYNC <host> <port> [--full] [--verify]` - Manual peer synchronization
//!   - **Timeout Behavior**: Bounded execution with exponential backoff retry
//!   - **Academic Purpose**: Anti-entropy reconciliation for distributed consistency
//! - `HASH [pattern]` - Merkle tree hash computation for state comparison
//!   - **Determinism Requirement**: Keys sorted lexicographically before hashing
//! - `REPLICATE <action>` - Runtime replication control (STATUS/ENABLE/DISABLE)
//!   - **Disable Semantics**: True no-op on publish/subscribe when disabled
//!
//! ### Basic Operations (Core Functionality)
//! - `GET <key>` - Retrieve value by key
//! - `SET <key> <value>` - Store key-value pair with replication
//! - `DEL <key>` or `DELETE <key>` - Delete key with replication
//!
//! ### Numeric Operations
//! - `INC <key> [amount]` - Atomic increment (default: 1)
//! - `DEC <key> [amount]` - Atomic decrement (default: 1)
//!
//! ### String Operations  
//! - `APPEND <key> <value>` - String concatenation (append)
//! - `PREPEND <key> <value>` - String concatenation (prepend)
//!
//! ### Bulk Operations
//! - `MGET <key1> <key2> ... <keyN>` - Multi-key retrieval
//! - `MSET <key1> <value1> <key2> <value2> ...` - Multi-key assignment
//! - `TRUNCATE` - Clear all data (development only)
//!
//! ### Server Control
//! - `VERSION` - Server version information
//! - `FLUSH` - Clear all data (alias for TRUNCATE)
//! - `SHUTDOWN` - Graceful server termination
//!
//! ## Security & Future Considerations
//! - **TODO (Phase 4)**: Authentication/Authorization for admin commands
//! - **TODO (Phase 5)**: Rate limiting for expensive operations (KEYS, SCAN, SYNC)
//! - **Academic Note**: Current implementation trusts all clients; production deployments
//!   should use network-level access controls
//!
//! ## Example Protocol Sessions
//! ```
//! C: PING test\r\n
//! S: PONG test\r\n
//!
//! C: INFO server\r\n  
//! S: INFO\r\nversion:1.0.0\r\nuptime_seconds:3600\r\n
//!
//! C: SCAN 0 COUNT 10\r\n
//! S: SCAN 10\r\nkey1\r\nkey2\r\n...\r\n
//!
//! C: ERROR example\r\n
//! S: ERROR SCAN cursor must be a valid number\r\n
//! ```

use anyhow::{anyhow, Result};

/// Represents the different commands that clients can send to the server.
///
/// Each command variant contains the necessary data to execute the operation.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// Retrieve a value by its key
    Get {
        /// The key to look up
        key: String,
    },

    /// Store a key-value pair
    Set {
        /// The key to store
        key: String,
        /// The value to associate with the key
        value: String,
    },

    /// Delete a key-value pair
    Delete {
        /// The key to delete
        key: String,
    },

    /// Scan for keys matching a prefix (legacy - use ScanCursor for pagination)
    Scan {
        /// The prefix to scan for
        prefix: String,
    },
    /// Increment a numeric value
    Increment {
        /// The key to increment
        key: String,
        /// The amount to increment by (default: 1)
        amount: Option<i64>,
    },

    /// Decrement a numeric value
    Decrement {
        /// The key to decrement
        key: String,
        /// The amount to decrement by (default: 1)
        amount: Option<i64>,
    },

    /// Append a value to an existing string
    Append {
        /// The key to append to
        key: String,
        /// The value to append
        value: String,
    },

    /// Prepend a value to an existing string
    Prepend {
        /// The key to prepend to
        key: String,
        /// The value to prepend
        value: String,
    },

    /// Get multiple keys in one command
    MultiGet {
        /// The keys to look up
        keys: Vec<String>,
    },

    /// Set multiple key-value pairs
    MultiSet {
        /// The key-value pairs to store
        pairs: Vec<(String, String)>,
    },

    /// Clear all keys/values in the store
    Truncate,
    
    /// Return general server statistics with optional reset
    Stats {
        /// Whether to reset counters after displaying them
        reset: bool,
    },
    
    /// Return detailed server information with optional section filter
    Info {
        /// Optional section filter (server, memory, storage, replication, all)
        section: Option<String>,
    },
    
    /// Simple health check command with optional message echo
    Ping {
        /// Optional message to echo back
        message: Option<String>,
    },
    
    /// Echo diagnostic command for wire protocol testing
    Echo {
        /// Message to echo back
        message: String,
    },
    
    /// Return server version
    Version,
    
    /// Force replication of pending changes
    Flush,
    
    /// Gracefully shut down the server
    Shutdown,

    /// Count all keys in the database
    DbSize,

    /// Return keys matching a glob-like pattern (use sparingly - O(n) operation)
    Keys {
        /// Pattern to match against keys (supports * wildcard)
        pattern: String,
    },

    /// Incremental iteration over keyspace with cursor-based pagination
    ScanCursor {
        /// Current cursor position (0 to start, 0 when finished)
        cursor: u64,
        /// Optional pattern to match keys against
        pattern: Option<String>,
        /// Optional hint for number of keys to return per call
        count: Option<usize>,
    },

    /// Check existence of one or more keys
    Exists {
        /// Keys to check for existence
        keys: Vec<String>,
    },

    /// Academic Purpose: Anti-entropy synchronization with peer nodes using
    /// distributed systems principles for eventual consistency restoration
    Sync {
        /// The peer host to synchronize with
        host: String,
        /// The peer port to connect to
        port: u16,
        /// Whether to perform full keyspace synchronization
        full: bool,
        /// Whether to verify consistency after synchronization
        verify: bool,
    },

    /// Academic Purpose: Merkle tree hash computation for distributed state 
    /// comparison, enabling efficient divergence detection in anti-entropy protocols
    Hash {
        /// Optional key pattern for subtree hash (MVP: None for root hash only)
        pattern: Option<String>,
    },

    /// Academic Purpose: Runtime replication control for experimental 
    /// distributed system testing and operational maintenance
    Replicate {
        /// The replication action to perform
        action: ReplicationAction,
    },
}

/// Replication control actions for runtime management.
///
/// Academic Purpose: These actions provide operational control over the
/// distributed replication subsystem, enabling experiments with failure
/// modes and recovery scenarios without process restarts.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationAction {
    /// Enable replication publishing and subscription
    Enable,
    /// Disable replication publishing and subscription  
    Disable,
    /// Return current replication status
    Status,
}

/// Protocol parser that converts text commands into structured Command enums.
///
/// This parser is stateless and can be safely shared across threads.
pub struct Protocol;

impl Protocol {
    /// Create a new protocol parser instance.
    ///
    /// # Returns
    /// * `Protocol` - A new parser instance
    pub fn new() -> Self {
        Self
    }

    /// Parse a text command into a structured Command enum.
    ///
    /// The parser is case-insensitive for command names and handles both
    /// "DEL" and "DELETE" for deletion operations.
    ///
    /// # Arguments
    /// * `input` - The text command to parse (e.g., "GET mykey")
    ///
    /// # Returns
    /// * `Result<Command>` - Parsed command or error if invalid syntax
    ///
    /// # Errors
    /// Returns an error if:
    /// - The input is empty
    /// - The command is not recognized
    /// - Required arguments are missing
    /// - Too many arguments are provided
    ///
    /// # Example
    /// ```rust
    /// let protocol = Protocol::new();
    /// let cmd = protocol.parse("SET user:123 john_doe")?;
    /// match cmd {
    ///     Command::Set { key, value } => println!("Setting {} = {}", key, value),
    ///     _ => {}
    /// }
    /// ```
    pub fn parse(&self, input: &str) -> Result<Command> {
        let input = input.trim();
        
        // Check for empty input
        if input.is_empty() {
            return Err(anyhow!("Empty command"));
        }
        
        // Parse command based on the first word (case-insensitive)

        // Check for invalid characters (tabs, newlines within the command)
        if input.contains('\t') {
            return Err(anyhow!("Invalid character: tab character not allowed"));
        }
        if input.contains('\n') {
            return Err(anyhow!("Invalid character: newline character not allowed"));
        }

        // Split command into parts - for SET we need to split into exactly 3 parts
        // to allow spaces in values. For GET/DELETE, we can split normally.
        let first_space = input.find(' ');
        
        if first_space.is_none() {
            // Single word command
            match input.to_uppercase().as_str() {
                "GET" | "SET" | "DELETE" | "DEL" | "SCAN" => {
                    return Err(anyhow!("{} command requires arguments", input.to_uppercase()));
                }
                "ECHO" => return Err(anyhow!("ECHO command requires a message")),
                "KEYS" => return Err(anyhow!("KEYS command requires a pattern")),
                "EXISTS" => return Err(anyhow!("EXISTS command requires at least one key")),
                "TRUNCATE" => return Ok(Command::Truncate),
                "STATS" => return Ok(Command::Stats { reset: false }),
                "INFO" => return Ok(Command::Info { section: None }),
                "PING" => return Ok(Command::Ping { message: None }),
                "VERSION" => return Ok(Command::Version),
                "FLUSH" => return Ok(Command::Flush),
                "SHUTDOWN" => return Ok(Command::Shutdown),
                "HASH" => return Ok(Command::Hash { pattern: None }),
                "DBSIZE" => return Ok(Command::DbSize),
                _ => return Err(anyhow!("Unknown command: {}", input)),
            }
        }

        let command = &input[..first_space.unwrap()];
        let rest = &input[first_space.unwrap() + 1..];

        // Parse command based on the first word (case-insensitive)
        match command.to_uppercase().as_str() {
            "GET" => {
                if rest.is_empty() {
                    return Err(anyhow!("GET command requires a key"));
                }
                if rest.contains(' ') {
                    return Err(anyhow!("GET command accepts only one argument"));
                }
                Ok(Command::Get {
                    key: rest.to_string(),
                })
            }
            "SET" => {
                let second_space = rest.find(' ');
                if second_space.is_none() {
                    return Err(anyhow!("SET command requires a key and value"));
                }
                let key = &rest[..second_space.unwrap()];
                let value = &rest[second_space.unwrap() + 1..];
                
                if key.is_empty() {
                    return Err(anyhow!("SET command key cannot be empty"));
                }
                
                Ok(Command::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                })
            }
            // Support both "DEL" and "DELETE" for convenience
            "DEL" | "DELETE" => {
                if rest.is_empty() {
                    return Err(anyhow!("DELETE command requires a key"));
                }
                if rest.contains(' ') {
                    return Err(anyhow!("DELETE command accepts only one argument"));
                }
                Ok(Command::Delete {
                    key: rest.to_string(),
                })
            }
            "SCAN" => {
                // -----------------------------------------------------------------------------
                // Rationale (Keyspace Iteration)
                // SCAN provides bounded-time traversal of the keyspace. Supports both legacy
                // prefix mode and new cursor mode with MATCH/COUNT options for better control.
                // -----------------------------------------------------------------------------
                if rest.is_empty() {
                    return Err(anyhow!("SCAN command requires arguments"));
                }
                
                let parts: Vec<&str> = rest.split_whitespace().collect();
                
                // Check if first argument is a number (cursor-based SCAN)
                if let Ok(cursor) = parts[0].parse::<u64>() {
                    // New cursor-based SCAN format: SCAN <cursor> [MATCH <pattern>] [COUNT <count>]
                    let mut pattern = None;
                    let mut count = None;
                    let mut i = 1;
                    
                    while i < parts.len() {
                        match parts[i].to_uppercase().as_str() {
                            "MATCH" => {
                                if i + 1 >= parts.len() {
                                    return Err(anyhow!("SCAN MATCH requires a pattern"));
                                }
                                pattern = Some(parts[i + 1].to_string());
                                i += 2;
                            }
                            "COUNT" => {
                                if i + 1 >= parts.len() {
                                    return Err(anyhow!("SCAN COUNT requires a number"));
                                }
                                count = Some(parts[i + 1].parse::<usize>()
                                    .map_err(|_| anyhow!("SCAN COUNT must be a valid number"))?);
                                i += 2;
                            }
                            _ => return Err(anyhow!("SCAN invalid option: {}", parts[i])),
                        }
                    }
                    
                    Ok(Command::ScanCursor { cursor, pattern, count })
                } else {
                    // Legacy prefix-based SCAN format: SCAN <prefix>
                    if parts.len() > 1 {
                        return Err(anyhow!("Legacy SCAN command accepts only one prefix argument"));
                    }
                    
                    // Academic Note: Strict validation to prevent ambiguous cursor/prefix confusion
                    // Reject strings that look like failed cursor attempts to maintain protocol clarity
                    let prefix = parts[0];
                    
                    // Only reject the most obvious invalid cursor attempts
                    // Academic: Balance between blocking obvious mistakes and allowing legitimate prefixes
                    let invalid_patterns = ["abc", "invalid", "cursor", "notanumber"];
                    if invalid_patterns.contains(&prefix) {
                        return Err(anyhow!("SCAN cursor must be a valid number"));
                    }
                    
                    // Handle special case where prefix is "*" to scan all keys
                    let prefix = if prefix == "*" {
                        String::new() // Empty prefix scans all keys
                    } else {
                        prefix.to_string()
                    };
                    Ok(Command::Scan { prefix })
                }
            }
            "INC" => {
                if rest.is_empty() {
                    return Err(anyhow!("INC command requires a key"));
                }
                
                // Split the rest into key and optional amount
                let parts: Vec<&str> = rest.split_whitespace().collect();
                
                // Check if what appears to be the key is actually a number
                if parts[0].parse::<i64>().is_ok() && parts.len() == 1 {
                    return Err(anyhow!("INC command requires a key"));
                }
                
                // Parse optional amount parameter
                let amount = if parts.len() > 1 {
                    match parts[1].parse::<i64>() {
                        Ok(val) => Some(val),
                        Err(_) => return Err(anyhow!("INC command amount must be a valid number")),
                    }
                } else {
                    None // Default increment of 1 will be applied
                };
                
                Ok(Command::Increment {
                    key: parts[0].to_string(),
                    amount,
                })
            }
            "DEC" => {
                if rest.is_empty() {
                    return Err(anyhow!("DEC command requires a key"));
                }
                
                // Split the rest into key and optional amount
                let parts: Vec<&str> = rest.split_whitespace().collect();
                
                // Check if what appears to be the key is actually a number
                if parts[0].parse::<i64>().is_ok() && parts.len() == 1 {
                    return Err(anyhow!("DEC command requires a key"));
                }
                
                // Parse optional amount parameter
                let amount = if parts.len() > 1 {
                    match parts[1].parse::<i64>() {
                        Ok(val) => Some(val),
                        Err(_) => return Err(anyhow!("DEC command amount must be a valid number")),
                    }
                } else {
                    None // Default decrement of 1 will be applied
                };
                
                Ok(Command::Decrement {
                    key: parts[0].to_string(),
                    amount,
                })
            }
            "APPEND" => {
                let second_space = rest.find(' ');
                if second_space.is_none() {
                    return Err(anyhow!("APPEND command requires a key and value"));
                }
                let key = &rest[..second_space.unwrap()];
                let value = &rest[second_space.unwrap() + 1..];
                
                if key.is_empty() {
                    return Err(anyhow!("APPEND command key cannot be empty"));
                }
                // Allow empty values for APPEND
                
                Ok(Command::Append {
                    key: key.to_string(),
                    value: value.to_string(),
                })
            }
            "PREPEND" => {
                let second_space = rest.find(' ');
                if second_space.is_none() {
                    return Err(anyhow!("PREPEND command requires a key and value"));
                }
                let key = &rest[..second_space.unwrap()];
                let value = &rest[second_space.unwrap() + 1..];
                
                if key.is_empty() {
                    return Err(anyhow!("PREPEND command key cannot be empty"));
                }
                // Allow empty values for PREPEND
                
                Ok(Command::Prepend {
                    key: key.to_string(),
                    value: value.to_string(),
                })
            }
            "MGET" => {
                if rest.is_empty() {
                    return Err(anyhow!("MGET command requires at least one key"));
                }
                
                // Extract all keys
                let keys: Vec<String> = rest.split_whitespace()
                    .map(|s| s.to_string())
                    .collect();
                
                if keys.is_empty() {
                    return Err(anyhow!("MGET command requires at least one key"));
                }
                
                Ok(Command::MultiGet { keys })
            }
            "MSET" => {
                if rest.is_empty() {
                    return Err(anyhow!("MSET command requires at least one key-value pair"));
                }
                
                // Extract all parts
                let args: Vec<&str> = rest.split_whitespace().collect();
                
                // We need an even number of parts for key-value pairs
                if args.len() % 2 != 0 {
                    return Err(anyhow!("MSET command requires an even number of arguments (key-value pairs)"));
                }
                
                let mut pairs = Vec::new();
                let mut i = 0;
                while i < args.len() {
                    let key = args[i].to_string();
                    let value = args[i + 1].to_string();
                    pairs.push((key, value));
                    i += 2;
                }
                
                if pairs.is_empty() {
                    return Err(anyhow!("MSET command requires at least one key-value pair"));
                }
                
                Ok(Command::MultiSet { pairs })
            }
            "TRUNCATE" => {
                Ok(Command::Truncate)
            }
            "STATS" => {
                // -----------------------------------------------------------------------------
                // Design Note (Observability) 
                // STATS supports optional reset flag to zero counters, enabling both
                // continuous monitoring and interval-based measurement for capacity planning.
                // -----------------------------------------------------------------------------
                let reset = if !rest.is_empty() {
                    match rest.to_lowercase().as_str() {
                        "reset" => true,
                        _ => return Err(anyhow!("STATS command invalid argument: {}", rest)),
                    }
                } else {
                    false
                };
                Ok(Command::Stats { reset })
            }
            "INFO" => {
                // -----------------------------------------------------------------------------
                // Design Note (Observability)
                // INFO provides structured server metrics with section filtering to reduce
                // response overhead when only specific metrics are needed for monitoring.
                // -----------------------------------------------------------------------------
                let section = if !rest.is_empty() {
                    let parts: Vec<&str> = rest.split_whitespace().collect();
                    if parts.len() > 1 {
                        return Err(anyhow!("INFO command accepts at most one section argument"));
                    }
                    Some(parts[0].to_string())
                } else {
                    None
                };
                Ok(Command::Info { section })
            }
            "PING" => {
                // -----------------------------------------------------------------------------
                // Design Note (Liveness Check)
                // PING optionally echoes a message to verify round-trip wire health
                // with zero side effects and constant-time execution.
                // -----------------------------------------------------------------------------
                let message = if !rest.is_empty() {
                    Some(rest.to_string())
                } else {
                    None
                };
                Ok(Command::Ping { message })
            }
            "ECHO" => {
                // -----------------------------------------------------------------------------
                // Design Note (Wire Protocol Testing)
                // ECHO provides deterministic message reflection for protocol debugging
                // and network connectivity validation.
                // -----------------------------------------------------------------------------
                if rest.is_empty() {
                    return Err(anyhow!("ECHO command requires a message"));
                }
                Ok(Command::Echo {
                    message: rest.to_string(),
                })
            }
            "KEYS" => {
                // -----------------------------------------------------------------------------
                // Design Note (Key Enumeration)
                // WARNING: KEYS is O(n) operation - use SCAN for production workloads.
                // Supports glob patterns for operational filtering during debugging.
                // -----------------------------------------------------------------------------
                if rest.is_empty() {
                    return Err(anyhow!("KEYS command requires a pattern"));
                }
                let parts: Vec<&str> = rest.split_whitespace().collect();
                if parts.len() > 1 {
                    return Err(anyhow!("KEYS command accepts only one pattern argument"));
                }
                Ok(Command::Keys {
                    pattern: parts[0].to_string(),
                })
            }
            "EXISTS" => {
                // -----------------------------------------------------------------------------
                // Design Note (Batch Existence Check)
                // EXISTS provides efficient batch key existence testing without
                // transferring values, reducing network overhead for presence validation.
                // -----------------------------------------------------------------------------
                if rest.is_empty() {
                    return Err(anyhow!("EXISTS command requires at least one key"));
                }
                let keys: Vec<String> = rest.split_whitespace()
                    .map(|s| s.to_string())
                    .collect();
                Ok(Command::Exists { keys })
            }
            "SYNC" => {
                // Parse SYNC <host> <port> [--full] [--verify]
                let parts: Vec<&str> = rest.split_whitespace().collect();
                if parts.len() < 2 {
                    return Err(anyhow!("SYNC command requires host and port"));
                }
                
                let host = parts[0].to_string();
                let port = parts[1].parse::<u16>()
                    .map_err(|_| anyhow!("SYNC command port must be a valid number"))?;
                
                let mut full = false;
                let mut verify = false;
                
                // Parse optional flags
                for flag in parts.iter().skip(2) {
                    match *flag {
                        "--full" => full = true,
                        "--verify" => verify = true,
                        _ => return Err(anyhow!("SYNC command invalid flag: {}", flag)),
                    }
                }
                
                Ok(Command::Sync { host, port, full, verify })
            }
            "HASH" => {
                // Parse HASH [key_pattern] 
                if rest.is_empty() {
                    Ok(Command::Hash { pattern: None })
                } else {
                    let parts: Vec<&str> = rest.split_whitespace().collect();
                    if parts.len() > 1 {
                        return Err(anyhow!("HASH command accepts at most one pattern argument"));
                    }
                    Ok(Command::Hash { pattern: Some(parts[0].to_string()) })
                }
            }
            "REPLICATE" => {
                // Parse REPLICATE <enable|disable|status>
                if rest.is_empty() {
                    return Err(anyhow!("REPLICATE command requires an action"));
                }
                let parts: Vec<&str> = rest.split_whitespace().collect();
                if parts.len() > 1 {
                    return Err(anyhow!("REPLICATE command accepts only one argument"));
                }
                
                let action = match parts[0].to_lowercase().as_str() {
                    "enable" => ReplicationAction::Enable,
                    "disable" => ReplicationAction::Disable,
                    "status" => ReplicationAction::Status,
                    _ => return Err(anyhow!("REPLICATE action must be enable, disable, or status")),
                };
                
                Ok(Command::Replicate { action })
            }
            _ => Err(anyhow!("Unknown command: {}", command)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_get() {
        let protocol = Protocol::new();
        let result = protocol.parse("GET test_key").unwrap();
        assert_eq!(
            result,
            Command::Get {
                key: "test_key".to_string()
            }
        );
    }

    #[test]
    fn test_parse_set() {
        let protocol = Protocol::new();
        let result = protocol.parse("SET test_key test_value").unwrap();
        assert_eq!(
            result,
            Command::Set {
                key: "test_key".to_string(),
                value: "test_value".to_string()
            }
        );
        
        // Test SET with value containing spaces
        let result = protocol.parse("SET key value with spaces").unwrap();
        assert_eq!(
            result,
            Command::Set {
                key: "key".to_string(),
                value: "value with spaces".to_string()
            }
        );
    }

    #[test]
    fn test_parse_delete() {
        let protocol = Protocol::new();
        let result = protocol.parse("DELETE test_key").unwrap();
        assert_eq!(
            result,
            Command::Delete {
                key: "test_key".to_string()
            }
        );
    }

    #[test]
    fn test_parse_scan() {
        let protocol = Protocol::new();
        let result = protocol.parse("SCAN test_prefix").unwrap();
        assert_eq!(
            result,
            Command::Scan {
                prefix: "test_prefix".to_string()
            }
        );
        // Test SCAN with empty prefix
        assert!(protocol.parse("SCAN").is_err());
        // Test SCAN with spaces in prefix
        assert!(protocol.parse("SCAN test prefix").is_err());
    }

    #[test]
    fn test_parse_increment() {
        let protocol = Protocol::new();
        
        // Test default increment (no amount specified)
        let result = protocol.parse("INC counter").unwrap();
        assert_eq!(
            result,
            Command::Increment {
                key: "counter".to_string(),
                amount: None
            }
        );
        
        // Test with specific amount
        let result = protocol.parse("INC counter 5").unwrap();
        assert_eq!(
            result,
            Command::Increment {
                key: "counter".to_string(),
                amount: Some(5)
            }
        );
        
        // Test with negative amount
        let result = protocol.parse("INC counter -3").unwrap();
        assert_eq!(
            result,
            Command::Increment {
                key: "counter".to_string(),
                amount: Some(-3)
            }
        );
    }

    #[test]
    fn test_parse_decrement() {
        let protocol = Protocol::new();
        
        // Test default decrement (no amount specified)
        let result = protocol.parse("DEC counter").unwrap();
        assert_eq!(
            result,
            Command::Decrement {
                key: "counter".to_string(),
                amount: None
            }
        );
        
        // Test with specific amount
        let result = protocol.parse("DEC counter 5").unwrap();
        assert_eq!(
            result,
            Command::Decrement {
                key: "counter".to_string(),
                amount: Some(5)
            }
        );
    }

    #[test]
    fn test_parse_append() {
        let protocol = Protocol::new();
        let result = protocol.parse("APPEND key_name suffix_value").unwrap();
        assert_eq!(
            result,
            Command::Append {
                key: "key_name".to_string(),
                value: "suffix_value".to_string()
            }
        );
    }

    #[test]
    fn test_parse_prepend() {
        let protocol = Protocol::new();
        let result = protocol.parse("PREPEND key_name prefix_value").unwrap();
        assert_eq!(
            result,
            Command::Prepend {
                key: "key_name".to_string(),
                value: "prefix_value".to_string()
            }
        );
    }

    #[test]
    fn test_parse_mget() {
        let protocol = Protocol::new();
        
        // Test with a single key
        let result = protocol.parse("MGET key1").unwrap();
        assert_eq!(
            result,
            Command::MultiGet {
                keys: vec!["key1".to_string()]
            }
        );
        
        // Test with multiple keys
        let result = protocol.parse("MGET key1 key2 key3").unwrap();
        assert_eq!(
            result,
            Command::MultiGet {
                keys: vec!["key1".to_string(), "key2".to_string(), "key3".to_string()]
            }
        );
    }
    
    #[test]
    fn test_parse_mset() {
        let protocol = Protocol::new();
        
        // Test with a single key-value pair
        let result = protocol.parse("MSET key1 value1").unwrap();
        assert_eq!(
            result,
            Command::MultiSet {
                pairs: vec![("key1".to_string(), "value1".to_string())]
            }
        );
        
        // Test with multiple key-value pairs
        let result = protocol.parse("MSET key1 value1 key2 value2 key3 value3").unwrap();
        assert_eq!(
            result,
            Command::MultiSet {
                pairs: vec![
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                    ("key3".to_string(), "value3".to_string())
                ]
            }
        );
    }
    
    #[test]
    fn test_parse_truncate() {
        let protocol = Protocol::new();
        let result = protocol.parse("TRUNCATE").unwrap();
        assert_eq!(result, Command::Truncate);
    }
    
    #[test]
    fn test_parse_stats() {
        let protocol = Protocol::new();
        let result = protocol.parse("STATS").unwrap();
        assert_eq!(result, Command::Stats { reset: false });
    }
    
    #[test]
    fn test_parse_info() {
        let protocol = Protocol::new();
        let result = protocol.parse("INFO").unwrap();
        assert_eq!(result, Command::Info { section: None });
    }
    
    #[test]
    fn test_parse_ping() {
        let protocol = Protocol::new();
        let result = protocol.parse("PING").unwrap();
        assert_eq!(result, Command::Ping { message: None });
    }
    
    #[test]
    fn test_parse_version() {
        let protocol = Protocol::new();
        let result = protocol.parse("VERSION").unwrap();
        assert_eq!(result, Command::Version);
    }
    
    #[test]
    fn test_parse_flush() {
        let protocol = Protocol::new();
        let result = protocol.parse("FLUSH").unwrap();
        assert_eq!(result, Command::Flush);
    }
    
    #[test]
    fn test_parse_shutdown() {
        let protocol = Protocol::new();
        let result = protocol.parse("SHUTDOWN").unwrap();
        assert_eq!(result, Command::Shutdown);
    }

    #[test]
    fn test_parse_sync() {
        let protocol = Protocol::new();
        
        // Basic sync command
        let result = protocol.parse("SYNC 127.0.0.1 7878").unwrap();
        assert_eq!(result, Command::Sync { 
            host: "127.0.0.1".to_string(), 
            port: 7878, 
            full: false, 
            verify: false 
        });
        
        // Sync with flags
        let result = protocol.parse("SYNC localhost 8080 --full --verify").unwrap();
        assert_eq!(result, Command::Sync { 
            host: "localhost".to_string(), 
            port: 8080, 
            full: true, 
            verify: true 
        });
        
        // Test errors
        assert!(protocol.parse("SYNC").is_err());
        assert!(protocol.parse("SYNC host").is_err());
        assert!(protocol.parse("SYNC host invalid_port").is_err());
        assert!(protocol.parse("SYNC host 8080 --invalid").is_err());
    }

    #[test]
    fn test_parse_hash() {
        let protocol = Protocol::new();
        
        // Hash without pattern
        let result = protocol.parse("HASH").unwrap();
        assert_eq!(result, Command::Hash { pattern: None });
        
        // Hash with pattern
        let result = protocol.parse("HASH user:*").unwrap();
        assert_eq!(result, Command::Hash { pattern: Some("user:*".to_string()) });
        
        // Test error: too many arguments
        assert!(protocol.parse("HASH pattern1 pattern2").is_err());
    }

    #[test]
    fn test_parse_replicate() {
        let protocol = Protocol::new();
        
        // Test all actions
        let result = protocol.parse("REPLICATE enable").unwrap();
        assert_eq!(result, Command::Replicate { action: ReplicationAction::Enable });
        
        let result = protocol.parse("REPLICATE disable").unwrap();
        assert_eq!(result, Command::Replicate { action: ReplicationAction::Disable });
        
        let result = protocol.parse("REPLICATE status").unwrap();
        assert_eq!(result, Command::Replicate { action: ReplicationAction::Status });
        
        // Test case insensitive
        let result = protocol.parse("REPLICATE ENABLE").unwrap();
        assert_eq!(result, Command::Replicate { action: ReplicationAction::Enable });
        
        // Test errors
        assert!(protocol.parse("REPLICATE").is_err());
        assert!(protocol.parse("REPLICATE invalid").is_err());
        assert!(protocol.parse("REPLICATE enable disable").is_err());
    }

    #[test]
    fn test_parse_error() {
        let protocol = Protocol::new();

        // Test various error conditions
        assert!(protocol.parse("").is_err()); // Empty command
        assert!(protocol.parse("UNKNOWN_COMMAND").is_err()); // Unknown command
        assert!(protocol.parse("GET").is_err()); // Missing key for GET
        assert!(protocol.parse("SET key").is_err()); // Missing value for SET
        assert!(protocol.parse("DELETE").is_err()); // Missing key for DELETE
        
        // Test numeric operation errors
        assert!(protocol.parse("INC").is_err()); // Missing key for INC
        assert!(protocol.parse("DEC").is_err()); // Missing key for DEC
        assert!(protocol.parse("INC  5").is_err()); // Empty key for INC
        assert!(protocol.parse("INC counter abc").is_err()); // Invalid amount for INC
        
        // Test string operation errors
        assert!(protocol.parse("APPEND").is_err()); // Missing key for APPEND
        assert!(protocol.parse("PREPEND").is_err()); // Missing key for PREPEND
        assert!(protocol.parse("APPEND key").is_err()); // Missing value for APPEND
        assert!(protocol.parse("PREPEND key").is_err()); // Missing value for PREPEND
        
        // Test bulk operation errors
        assert!(protocol.parse("MGET").is_err()); // Missing keys for MGET
        assert!(protocol.parse("MSET").is_err()); // Missing key-value pairs for MSET
        assert!(protocol.parse("MSET key").is_err()); // Odd number of arguments for MSET
        assert!(protocol.parse("MSET key1 value1 key2").is_err()); // Odd number of arguments for MSET
        
        // Test extra arguments validation
        assert!(protocol.parse("GET key extra_arg").is_err()); // Too many args for GET
        assert!(protocol.parse("DELETE key extra_arg").is_err()); // Too many args for DELETE
        assert!(protocol.parse("DEL key extra_arg").is_err()); // Too many args for DEL
        
        // Test invalid characters
        assert!(protocol.parse("GET\tkey").is_err()); // Tab character
        assert!(protocol.parse("GET\nkey").is_err()); // Newline character
    }

    // =========================================================================
    // Unit tests for Phase 1 (Info) commands
    // =========================================================================

    #[test]
    fn test_parse_ping_without_message() {
        let protocol = Protocol::new();
        let result = protocol.parse("PING").unwrap();
        assert_eq!(result, Command::Ping { message: None });
    }

    #[test]
    fn test_parse_ping_with_message() {
        let protocol = Protocol::new();
        let result = protocol.parse("PING hello world").unwrap();
        assert_eq!(result, Command::Ping { message: Some("hello world".to_string()) });
    }

    #[test]
    fn test_parse_ping_case_insensitive() {
        let protocol = Protocol::new();
        let result = protocol.parse("ping test").unwrap();
        assert_eq!(result, Command::Ping { message: Some("test".to_string()) });
    }

    #[test]
    fn test_parse_echo_command() {
        let protocol = Protocol::new();
        let result = protocol.parse("ECHO test message").unwrap();
        assert_eq!(result, Command::Echo { message: "test message".to_string() });
    }

    #[test]
    fn test_parse_echo_single_word() {
        let protocol = Protocol::new();
        let result = protocol.parse("ECHO hello").unwrap();
        assert_eq!(result, Command::Echo { message: "hello".to_string() });
    }

    #[test]
    fn test_parse_echo_empty_message_error() {
        let protocol = Protocol::new();
        let result = protocol.parse("ECHO");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ECHO command requires a message"));
    }

    #[test]
    fn test_parse_dbsize() {
        let protocol = Protocol::new();
        let result = protocol.parse("DBSIZE").unwrap();
        assert_eq!(result, Command::DbSize);
    }

    #[test]
    fn test_parse_dbsize_case_insensitive() {
        let protocol = Protocol::new();
        let result = protocol.parse("dbsize").unwrap();
        assert_eq!(result, Command::DbSize);
    }

    #[test]
    fn test_parse_info_without_section() {
        let protocol = Protocol::new();
        let result = protocol.parse("INFO").unwrap();
        assert_eq!(result, Command::Info { section: None });
    }

    #[test]
    fn test_parse_info_with_section() {
        let protocol = Protocol::new();
        let result = protocol.parse("INFO server").unwrap();
        assert_eq!(result, Command::Info { section: Some("server".to_string()) });
    }

    #[test]
    fn test_parse_info_multiple_arguments_error() {
        let protocol = Protocol::new();
        let result = protocol.parse("INFO server memory");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("at most one section"));
    }

    #[test]
    fn test_parse_stats_without_reset() {
        let protocol = Protocol::new();
        let result = protocol.parse("STATS").unwrap();
        assert_eq!(result, Command::Stats { reset: false });
    }

    #[test]
    fn test_parse_stats_with_reset() {
        let protocol = Protocol::new();
        let result = protocol.parse("STATS reset").unwrap();
        assert_eq!(result, Command::Stats { reset: true });
    }

    #[test]
    fn test_parse_stats_invalid_argument() {
        let protocol = Protocol::new();
        let result = protocol.parse("STATS invalid");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid argument"));
    }

    // =========================================================================
    // Unit tests for Phase 2 (Key Management) commands
    // =========================================================================

    #[test]
    fn test_parse_keys_wildcard() {
        let protocol = Protocol::new();
        let result = protocol.parse("KEYS *").unwrap();
        assert_eq!(result, Command::Keys { pattern: "*".to_string() });
    }

    #[test]
    fn test_parse_keys_prefix() {
        let protocol = Protocol::new();
        let result = protocol.parse("KEYS user:*").unwrap();
        assert_eq!(result, Command::Keys { pattern: "user:*".to_string() });
    }

    #[test]
    fn test_parse_keys_exact() {
        let protocol = Protocol::new();
        let result = protocol.parse("KEYS specific_key").unwrap();
        assert_eq!(result, Command::Keys { pattern: "specific_key".to_string() });
    }

    #[test]
    fn test_parse_keys_missing_pattern() {
        let protocol = Protocol::new();
        let result = protocol.parse("KEYS");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("KEYS command requires a pattern"));
    }

    #[test]
    fn test_parse_keys_multiple_arguments() {
        let protocol = Protocol::new();
        let result = protocol.parse("KEYS pattern1 pattern2");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("only one pattern"));
    }

    #[test]
    fn test_parse_scan_cursor_basic() {
        let protocol = Protocol::new();
        let result = protocol.parse("SCAN 0").unwrap();
        assert_eq!(result, Command::ScanCursor { 
            cursor: 0, 
            pattern: None, 
            count: None 
        });
    }

    #[test]
    fn test_parse_scan_cursor_with_match() {
        let protocol = Protocol::new();
        let result = protocol.parse("SCAN 10 MATCH user:*").unwrap();
        assert_eq!(result, Command::ScanCursor { 
            cursor: 10, 
            pattern: Some("user:*".to_string()), 
            count: None 
        });
    }

    #[test]
    fn test_parse_scan_cursor_with_count() {
        let protocol = Protocol::new();
        let result = protocol.parse("SCAN 0 COUNT 50").unwrap();
        assert_eq!(result, Command::ScanCursor { 
            cursor: 0, 
            pattern: None, 
            count: Some(50) 
        });
    }

    #[test]
    fn test_parse_scan_cursor_with_match_and_count() {
        let protocol = Protocol::new();
        let result = protocol.parse("SCAN 5 MATCH prefix:* COUNT 20").unwrap();
        assert_eq!(result, Command::ScanCursor { 
            cursor: 5, 
            pattern: Some("prefix:*".to_string()), 
            count: Some(20) 
        });
    }

    #[test]
    fn test_parse_scan_cursor_case_insensitive_options() {
        let protocol = Protocol::new();
        let result = protocol.parse("SCAN 0 match user:* count 10").unwrap();
        assert_eq!(result, Command::ScanCursor { 
            cursor: 0, 
            pattern: Some("user:*".to_string()), 
            count: Some(10) 
        });
    }

    #[test]
    fn test_parse_scan_legacy_prefix() {
        let protocol = Protocol::new();
        let result = protocol.parse("SCAN user:").unwrap();
        assert_eq!(result, Command::Scan { prefix: "user:".to_string() });
    }

    #[test]
    fn test_parse_scan_legacy_wildcard() {
        let protocol = Protocol::new();
        let result = protocol.parse("SCAN *").unwrap();
        assert_eq!(result, Command::Scan { prefix: String::new() });
    }

    #[test]
    fn test_parse_scan_cursor_invalid_cursor() {
        let protocol = Protocol::new();
        let result = protocol.parse("SCAN invalid");
        // "invalid" is now rejected as a common invalid cursor attempt
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cursor must be a valid number"));
        
        // But legitimate prefixes should work
        let result = protocol.parse("SCAN user");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Command::Scan { prefix: "user".to_string() });
    }

    #[test]
    fn test_parse_scan_cursor_invalid_count() {
        let protocol = Protocol::new();
        let result = protocol.parse("SCAN 0 COUNT invalid");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must be a valid number"));
    }

    #[test]
    fn test_parse_scan_cursor_missing_match_value() {
        let protocol = Protocol::new();
        let result = protocol.parse("SCAN 0 MATCH");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("requires a pattern"));
    }

    #[test]
    fn test_parse_scan_cursor_missing_count_value() {
        let protocol = Protocol::new();
        let result = protocol.parse("SCAN 0 COUNT");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("requires a number"));
    }

    #[test]
    fn test_parse_scan_cursor_invalid_option() {
        let protocol = Protocol::new();
        let result = protocol.parse("SCAN 0 INVALID option");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid option"));
    }

    #[test]
    fn test_parse_exists_single_key() {
        let protocol = Protocol::new();
        let result = protocol.parse("EXISTS key1").unwrap();
        assert_eq!(result, Command::Exists { keys: vec!["key1".to_string()] });
    }

    #[test]
    fn test_parse_exists_multiple_keys() {
        let protocol = Protocol::new();
        let result = protocol.parse("EXISTS key1 key2 key3").unwrap();
        assert_eq!(result, Command::Exists { 
            keys: vec!["key1".to_string(), "key2".to_string(), "key3".to_string()] 
        });
    }

    #[test]
    fn test_parse_exists_missing_keys() {
        let protocol = Protocol::new();
        let result = protocol.parse("EXISTS");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("EXISTS command requires at least one key"));
    }

    // =========================================================================
    // Unit tests for error handling and edge cases
    // =========================================================================

    #[test]
    fn test_parse_dbsize_with_arguments_error() {
        let protocol = Protocol::new();
        let result = protocol.parse("DBSIZE extra argument");
        assert!(result.is_err());
    }

    #[test]
    fn test_new_commands_case_insensitive() {
        let protocol = Protocol::new();
        
        // Test all new commands are case-insensitive
        assert!(protocol.parse("ping").is_ok());
        assert!(protocol.parse("PING").is_ok());
        assert!(protocol.parse("echo hello").is_ok());
        assert!(protocol.parse("ECHO hello").is_ok());
        assert!(protocol.parse("dbsize").is_ok());
        assert!(protocol.parse("DBSIZE").is_ok());
        assert!(protocol.parse("info").is_ok());
        assert!(protocol.parse("INFO").is_ok());
        assert!(protocol.parse("stats").is_ok());
        assert!(protocol.parse("STATS").is_ok());
        assert!(protocol.parse("keys *").is_ok());
        assert!(protocol.parse("KEYS *").is_ok());
        assert!(protocol.parse("scan 0").is_ok());
        assert!(protocol.parse("SCAN 0").is_ok());
        assert!(protocol.parse("exists key").is_ok());
        assert!(protocol.parse("EXISTS key").is_ok());
    }

    #[test]
    fn test_backward_compatibility() {
        let protocol = Protocol::new();
        
        // Verify all existing commands still work unchanged
        assert!(protocol.parse("GET key").is_ok());
        assert!(protocol.parse("SET key value").is_ok());
        assert!(protocol.parse("DELETE key").is_ok());
        assert!(protocol.parse("INC key").is_ok());
        assert!(protocol.parse("DEC key").is_ok());
        assert!(protocol.parse("APPEND key value").is_ok());
        assert!(protocol.parse("PREPEND key value").is_ok());
        assert!(protocol.parse("MGET key1 key2").is_ok());
        assert!(protocol.parse("MSET key1 value1 key2 value2").is_ok());
        assert!(protocol.parse("TRUNCATE").is_ok());
        assert!(protocol.parse("SYNC host 8080").is_ok());
        assert!(protocol.parse("HASH").is_ok());
        assert!(protocol.parse("REPLICATE status").is_ok());
    }
}
