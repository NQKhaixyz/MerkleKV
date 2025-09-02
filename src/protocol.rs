//! # Protocol Parser
//!
//! This module implements a simple text-based protocol for client-server communication.
//! The protocol supports basic operations, numeric operations, and string operations.
//!
//! ## Protocol Format
//!
//! Commands are text-based and terminated with line endings:
//!
//! ### Basic Operations
//! - `GET <key>` - Retrieve a value by key
//! - `SET <key> <value>` - Store a key-value pair  
//! - `DEL <key>` or `DELETE <key>` - Delete a key
//!
//! ### Numeric Operations
//! - `INC <key> [amount]` - Increment a numeric value (default: 1)
//! - `DEC <key> [amount]` - Decrement a numeric value (default: 1)
//!
//! ### String Operations
//! - `APPEND <key> <value>` - Append value to existing string
//! - `PREPEND <key> <value>` - Prepend value to existing string
//!
//! ### Bulk Operations
//! - `MGET <key1> <key2> ... <keyN>` - Get multiple keys in one command
//! - `MSET <key1> <value1> <key2> <value2> ...` - Set multiple key-value pairs
//! - `TRUNCATE` - Clear all keys/values in the store
//!
//! ### Statistical Commands
//! - `STATS` - Return general server statistics (connections, operations, memory usage)
//! - `INFO` - Return detailed server information (version, uptime, config)
//! - `PING` - Simple health check command
//!
//! ## Example Usage
//! ```
//! GET user:123
//! SET user:123 john_doe
//! DELETE user:123
//! INC counter
//! INC counter 5
//! DEC counter 2
//! APPEND greeting " World!"
//! PREPEND greeting "Hello,"
//! MGET user:123 user:456 user:789
//! MSET user:123 john_doe user:456 jane_smith
//! TRUNCATE
//! ```
//!
//! ## Response Format
//! - Success responses: `VALUE <data>`, `OK`
//! - Error responses: `ERROR <message>`, `NOT_FOUND`

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

    /// Scan for keys matching a prefix
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
    
    /// Return general server statistics (connections, operations, memory usage)
    Stats,
    
    /// Return detailed server information (version, uptime, config)
    Info,
    
    /// Simple health check command
    Ping,
    
    /// Return server version
    Version,
    
    /// Force replication of pending changes
    Flush,
    
    /// Gracefully shut down the server
    Shutdown,
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
                "TRUNCATE" => return Ok(Command::Truncate),
                "STATS" => return Ok(Command::Stats),
                "INFO" => return Ok(Command::Info),
                "PING" => return Ok(Command::Ping),
                "VERSION" => return Ok(Command::Version),
                "FLUSH" => return Ok(Command::Flush),
                "SHUTDOWN" => return Ok(Command::Shutdown),
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
                if rest.is_empty() {
                    return Err(anyhow!("SCAN command requires a prefix"));
                }
                if rest.contains(' ') {
                    return Err(anyhow!("SCAN command accepts only one argument"));
                }
                Ok(Command::Scan {
                    prefix: rest.to_string(),
                })
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
                Ok(Command::Stats)
            }
            "INFO" => {
                Ok(Command::Info)
            }
            "PING" => {
                Ok(Command::Ping)
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
        assert_eq!(result, Command::Stats);
    }
    
    #[test]
    fn test_parse_info() {
        let protocol = Protocol::new();
        let result = protocol.parse("INFO").unwrap();
        assert_eq!(result, Command::Info);
    }
    
    #[test]
    fn test_parse_ping() {
        let protocol = Protocol::new();
        let result = protocol.parse("PING").unwrap();
        assert_eq!(result, Command::Ping);
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
}
