//! # Key-Value Storage Engine
//!
//! This module provides the core storage functionality for MerkleKV.
//! Currently implements an in-memory storage engine using HashMap.
//!
//! ## Current Implementation
//!
//! The current implementation is a simple in-memory store that:
//! - Uses `Arc<HashMap<String, String>>` for thread-safe access
//! - Creates new HashMap instances on every write (copy-on-write pattern)
//! - Provides basic get/set/delete operations
//! - Supports numeric operations (increment/decrement)
//! - Supports string operations (append/prepend)
//! - Returns all keys for iteration
//!
//! ## Future Implementation Plans
//!
//! This is a placeholder implementation. A production version should:
//! - Use a persistent storage engine (e.g., RocksDB, Sled)
//! - Support transactions and atomic operations
//! - Implement Write-Ahead Logging (WAL)
//! - Add compression and efficient serialization
//! - Support range queries and iteration
//! - Implement proper error handling for I/O operations

use anyhow::Result;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::kv_trait::KVEngineStoreTrait;

/// In-memory key-value storage engine.
///
/// This is a simplified storage implementation that keeps all data in memory.
/// The `Arc<HashMap>` allows for efficient cloning of the engine while sharing
/// the underlying data until a write operation occurs.
///
/// **Note**: This implementation is not persistent! All data is lost when
/// the process terminates.
#[derive(Clone)]
pub struct KvEngine {
    /// Shared reference to the key-value data with thread-safe interior mutability
    /// Using Arc<RwLock<HashMap>> provides safe shared mutability across threads
    data: Arc<RwLock<HashMap<String, String>>>,
    // TODO: Add persistent storage implementation
    // In a real implementation, this would use a persistent storage engine like Sled:
    // storage_path: PathBuf,
    // sled_db: sled::Db,
}

impl KvEngine {
    /// Create a new storage engine instance.
    ///
    /// # Arguments
    /// * `_storage_path` - Path where data should be stored (currently unused)
    ///
    /// # Returns
    /// * `Result<KvEngine>` - New storage engine instance or error
    ///
    /// # Current Behavior
    /// Creates an empty in-memory HashMap. The storage_path is ignored.
    ///
    /// # Future Implementation
    /// Should initialize or open a persistent storage engine at the given path.
    pub fn new(_storage_path: &str) -> Result<Self> {
        // TODO: Initialize persistent storage engine here
        // For example, with Sled:
        // let db = sled::open(storage_path)?;
        // Ok(Self { storage_path: storage_path.into(), sled_db: db })

        Ok(Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Retrieve a value by its key.
    ///
    /// # Arguments
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// * `Option<String>` - The value if found, None otherwise
    ///
    /// # Example
    /// ```rust
    /// let engine = KvEngine::new("./data")?;
    /// if let Some(value) = engine.get("user:123") {
    ///     println!("Found user: {}", value);
    /// }
    /// ```
    pub fn get(&self, key: &str) -> Option<String> {
        self.data.read().unwrap().get(key).cloned()
    }

    /// Store a key-value pair.
    ///
    /// This operation creates a new HashMap with the updated data due to the
    /// immutable nature of the `Arc<HashMap>` design. This is inefficient but
    /// simple for the current prototype.
    ///
    /// # Arguments
    /// * `key` - The key to store
    /// * `value` - The value to associate with the key
    ///
    /// # Example
    /// ```rust
    /// let mut engine = KvEngine::new("./data")?;
    /// engine.set("user:123".to_string(), "john_doe".to_string());
    /// ```
    ///
    /// # Performance Note
    /// This implementation is O(n) in the number of keys due to HashMap cloning.
    /// A persistent storage engine would be much more efficient.
    pub fn set(&mut self, key: String, value: String) {
        // Use RwLock for thread-safe interior mutability instead of HashMap cloning
        self.data.write().unwrap().insert(key, value);
    }

    /// Delete a key-value pair.
    ///
    /// Like `set`, this creates a new HashMap without the deleted key.
    ///
    /// # Arguments
    /// * `key` - The key to delete
    ///
    /// # Example
    /// ```rust
    /// let mut engine = KvEngine::new("./data")?;
    /// engine.delete("user:123");
    /// ```
    pub fn delete(&mut self, key: &str) {
        // Use RwLock for safe deletion instead of HashMap cloning
        self.data.write().unwrap().remove(key);
    }

    /// Get all keys currently stored in the engine.
    ///
    /// This is primarily used by the Merkle tree to rebuild its state
    /// and for debugging purposes.
    ///
    /// # Returns
    /// * `Vec<String>` - Vector of all keys in the store
    ///
    /// # Performance Note
    /// This operation is O(n) and creates a new vector. In a production
    /// system, this should be replaced with an iterator-based approach.
    pub fn keys(&self) -> Vec<String> {
        self.data.read().unwrap().keys().cloned().collect()
    }

    pub fn scan(&self, prefix: &str) -> Vec<String> {
        let map = self.data.read().unwrap();
        if prefix.is_empty() {
            return map.keys().cloned().collect();
        }
        map.keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }

    /// Increment a numeric value stored at the given key.
    /// 
    /// If the key doesn't exist, it will be created with the increment amount.
    /// If the key exists but doesn't contain a valid number, an error message is returned.
    /// 
    /// # Arguments
    /// * `key` - The key to increment
    /// * `amount` - The amount to increment by (defaults to 1 if None)
    /// 
    /// # Returns
    /// * `Result<i64, String>` - The new value after incrementing, or an error message
    /// 
    /// # Example
    /// ```rust
    /// let mut engine = KvEngine::new("./data")?;
    /// match engine.increment("counter", Some(5)) {
    ///     Ok(new_value) => println!("New counter value: {}", new_value),
    ///     Err(e) => println!("Error: {}", e),
    /// }
    /// ```
    pub fn increment(&mut self, key: &str, amount: Option<i64>) -> Result<i64, String> {
        let increment_by = amount.unwrap_or(1);
        
        let new_value = match self.data.read().unwrap().get(key) {
            Some(value) => {
                // Try to parse the existing value as a number
                match value.parse::<i64>() {
                    Ok(num) => num + increment_by,
                    Err(_) => return Err(format!("Value for key '{}' is not a valid number", key)),
                }
            }
            None => increment_by, // Key doesn't exist, start with the increment amount
        };
        
        // Store the new value using RwLock
        self.data.write().unwrap().insert(key.to_string(), new_value.to_string());
        Ok(new_value)
    }

    /// Decrement a numeric value stored at the given key.
    /// 
    /// If the key doesn't exist, it will be created with the negative of the decrement amount.
    /// If the key exists but doesn't contain a valid number, an error message is returned.
    /// 
    /// # Arguments
    /// * `key` - The key to decrement
    /// * `amount` - The amount to decrement by (defaults to 1 if None)
    /// 
    /// # Returns
    /// * `Result<i64, String>` - The new value after decrementing, or an error message
    /// 
    /// # Example
    /// ```rust
    /// let mut engine = KvEngine::new("./data")?;
    /// match engine.decrement("counter", Some(3)) {
    ///     Ok(new_value) => println!("New counter value: {}", new_value),
    ///     Err(e) => println!("Error: {}", e),
    /// }
    /// ```
    pub fn decrement(&mut self, key: &str, amount: Option<i64>) -> Result<i64, String> {
        let decrement_by = amount.unwrap_or(1);
        // Decrement is just a negative increment
        self.increment(key, Some(-decrement_by))
    }

    /// Append a value to an existing string.
    /// 
    /// If the key doesn't exist, it will be created with the value.
    /// 
    /// # Arguments
    /// * `key` - The key to append to
    /// * `value` - The value to append
    /// 
    /// # Returns
    /// * `String` - The new value after appending
    /// 
    /// # Example
    /// ```rust
    /// let mut engine = KvEngine::new("./data")?;
    /// let new_value = engine.append("greeting", " World!");
    /// println!("New value: {}", new_value); // e.g., "Hello World!"
    /// ```
    pub fn append(&mut self, key: &str, value: &str) -> String {
        let new_value = match self.data.read().unwrap().get(key) {
            Some(existing) => {
                let mut result = existing.clone();
                result.push_str(value);
                result
            }
            None => value.to_string(), // Key doesn't exist, use the value as is
        };
        
        // Store the new value using RwLock
        self.data.write().unwrap().insert(key.to_string(), new_value.clone());
        new_value
    }

    /// Prepend a value to an existing string.
    /// 
    /// If the key doesn't exist, it will be created with the value.
    /// 
    /// # Arguments
    /// * `key` - The key to prepend to
    /// * `value` - The value to prepend
    /// 
    /// # Returns
    /// * `String` - The new value after prepending
    /// 
    /// # Example
    /// ```rust
    /// let mut engine = KvEngine::new("./data")?;
    /// let new_value = engine.prepend("greeting", "Hello, ");
    /// println!("New value: {}", new_value); // e.g., "Hello, World!"
    /// ```
    pub fn prepend(&mut self, key: &str, value: &str) -> String {
        let new_value = match self.data.read().unwrap().get(key) {
            Some(existing) => {
                let mut result = value.to_string();
                result.push_str(existing);
                result
            }
            None => value.to_string(), // Key doesn't exist, use the value as is
        };
        
        // Store the new value using RwLock
        self.data.write().unwrap().insert(key.to_string(), new_value.clone());
        new_value
    }

    /// Clear all keys/values in the store.
    /// 
    /// This operation removes all data from the store, effectively resetting it
    /// to an empty state.
    /// 
    /// # Example
    /// ```rust
    /// let mut engine = KvEngine::new("./data")?;
    /// engine.truncate();
    /// assert_eq!(engine.keys().len(), 0);
    /// ```
    pub fn truncate(&mut self) {
        // Clear all data using RwLock
        self.data.write().unwrap().clear();
    }
}

impl KVEngineStoreTrait for KvEngine {
    /// Retrieve a value by its key.
    ///
    /// # Arguments
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// * `Option<String>` - The value if found, None otherwise
    fn get(&self, key: &str) -> Option<String> {
        self.data.read().unwrap().get(key).cloned()
    }

    /// Store a key-value pair.
    ///
    /// ⚠️ **WARNING**: This method is NOT thread-safe!
    ///
    /// This operation creates a new HashMap with the updated data due to the
    /// immutable nature of the `Arc<HashMap>` design. This is inefficient but
    /// simple for the current prototype.
    ///
    /// # Arguments
    /// * `key` - The key to store
    /// * `value` - The value to associate with the key
    ///
    /// # Returns
    /// * `Result<()>` - Success or error
    ///
    /// # Thread Safety
    /// ⚠️ This method is NOT safe for concurrent access!
    /// Concurrent writes can lead to data corruption or lost updates.
    fn set(&self, key: String, value: String) -> Result<()> {
        // --- Memory Safety Fix -------------------------------------------------------
        // Problem: Prior implementation used unsafe raw pointer casts violating 
        // Rust's aliasing rules and causing potential undefined behavior.
        // Solution: Use RwLock for thread-safe interior mutability following Copilot's
        // recommendation. Maintains existing API and LWW semantics while eliminating
        // unsafe code that could cause data corruption or memory safety violations.
        self.data.write().unwrap().insert(key, value);
        Ok(())
    }

    /// Delete a key-value pair.
    ///
    /// ⚠️ **WARNING**: This method is NOT thread-safe!
    ///
    /// Like `set`, this creates a new HashMap without the deleted key.
    ///
    /// # Arguments
    /// * `key` - The key to delete
    ///
    /// # Returns
    /// * `bool` - True if the key existed and was deleted, false otherwise
    ///
    /// # Thread Safety
    /// ⚠️ This method is NOT safe for concurrent access!
    fn delete(&self, key: &str) -> bool {
        // Memory-safe deletion: use RwLock for thread-safe interior mutability,
        // eliminating unsafe raw pointer operations while preserving LWW
        // semantics and existing API behavior for anti-entropy protocols.
        self.data.write().unwrap().remove(key).is_some()
    }

    /// Get all keys currently stored in the engine.
    ///
    /// # Returns
    /// * `Vec<String>` - Vector of all keys in the store
    fn keys(&self) -> Vec<String> {
        self.data.read().unwrap().keys().cloned().collect()
    }

    fn scan(&self, prefix: &str) -> Vec<String> {
        if prefix.is_empty() {
            return self.keys();
        }
        self.keys()
            .into_iter()
            .filter(|k| k.starts_with(prefix))
            .collect()
    }
    /// Get the number of key-value pairs in the store.
    ///
    /// # Returns
    /// * `usize` - Number of key-value pairs
    fn len(&self) -> usize {
        self.data.read().unwrap().len()
    }

    /// Check if the store is empty.
    ///
    /// # Returns
    /// * `bool` - True if the store is empty, false otherwise
    fn is_empty(&self) -> bool {
        self.data.read().unwrap().is_empty()
    }
    
    /// Increment a numeric value.
    ///
    /// # Arguments
    /// * `key` - The key to increment
    /// * `amount` - The amount to increment by (default: 1)
    ///
    /// # Returns
    /// * `Result<i64>` - The new value after incrementing, or error if not a valid number
    fn increment(&self, key: &str, amount: Option<i64>) -> Result<i64> {
        // Default increment amount is 1
        let increment_by = amount.unwrap_or(1);
        
        // Use write lock to ensure exclusive access for atomic read-modify-write
        let mut data = self.data.write().unwrap();
        let current_value = match data.get(key) {
            Some(value) => {
                // Try to parse the current value as a number
                value.parse::<i64>().map_err(|_| {
                    anyhow::anyhow!("Value for key '{}' is not a valid number", key)
                })?
            }
            None => 0, // Key doesn't exist, start from 0
        };
        
        // Calculate and store the new value
        let new_value = current_value + increment_by;
        data.insert(key.to_string(), new_value.to_string());
        
        Ok(new_value)
    }
    
    /// Decrement a numeric value.
    ///
    /// # Arguments
    /// * `key` - The key to decrement
    /// * `amount` - The amount to decrement by (default: 1)
    ///
    /// # Returns
    /// * `Result<i64>` - The new value after decrementing, or error if not a valid number
    fn decrement(&self, key: &str, amount: Option<i64>) -> Result<i64> {
        // Decrement is just a negative increment
        let decrement_by = amount.unwrap_or(1);
        self.increment(key, Some(-decrement_by))
    }
    
    /// Append a value to an existing string.
    ///
    /// # Arguments
    /// * `key` - The key to append to
    /// * `value` - The value to append
    ///
    /// # Returns
    /// * `Result<String>` - The new value after appending
    fn append(&self, key: &str, value: &str) -> Result<String> {
        // Use write lock exclusively to avoid read/write lock deadlock
        let mut data = self.data.write().unwrap();
        if let Some(current_value) = data.get(key) {
            // Append the new value
            let new_value = format!("{}{}", current_value, value);
            data.insert(key.to_string(), new_value.clone());
            Ok(new_value)
        } else {
            // Key doesn't exist, return error as per trait documentation
            Err(anyhow::anyhow!("Key '{}' does not exist", key))
        }
    }
    
    /// Prepend a value to an existing string.
    ///
    /// # Arguments
    /// * `key` - The key to prepend to
    /// * `value` - The value to prepend
    ///
    /// # Returns
    /// * `Result<String>` - The new value after prepending
    fn prepend(&self, key: &str, value: &str) -> Result<String> {
        // Acquire a write lock to execute the read–modify–write as a single linearizable
        // critical section. std::sync::RwLock has no upgrade path from read→write; taking
        // a read lock first would require dropping and re-acquiring, creating a TOCTOU
        // window where the observed value may change. We hold the write lock from the
        // start for correctness and LWW consistency—not for deadlock avoidance.  
        let mut data = self.data.write().unwrap();
        if let Some(current_value) = data.get(key) {
            // Prepend the new value
            let new_value = format!("{}{}", value, current_value);
            data.insert(key.to_string(), new_value.clone());
            Ok(new_value)
        } else {
            // Key doesn't exist, return error as per trait documentation
            Err(anyhow::anyhow!("Key '{}' does not exist", key))
        }
    }
    
    /// Clear all keys/values in the store.
    ///
    /// # Returns
    /// * `Result<()>` - Success or error
    fn truncate(&self) -> Result<()> {
        // Memory-safe truncation using RwLock: clears all data while preserving
        // anti-entropy reconciliation semantics without unsafe pointer operations.
        self.data.write().unwrap().clear();
        Ok(())
    }
    
    /// Get the number of key-value pairs in the store.
    ///
    /// # Returns
    /// * `Result<u64>` - Number of key-value pairs or error
    fn count_keys(&self) -> Result<u64> {
        Ok(self.data.read().unwrap().len() as u64)
    }
    
    /// Force synchronization of pending changes to persistent storage.
    /// For this in-memory engine, this is a no-op.
    ///
    /// # Returns
    /// * `Result<()>` - Always returns Ok(())
    fn sync(&self) -> Result<()> {
        // This is an in-memory engine, so there's nothing to sync
        // In a persistent storage engine, this would flush data to disk
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_kv_operations() {
        // Use a temporary directory for testing (even though it's not used yet)
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().to_str().unwrap();

        let mut engine = KvEngine::new(storage_path).unwrap();

        // Test basic set and get operations
        engine.set("key1".to_string(), "value1".to_string());
        assert_eq!(engine.get("key1"), Some("value1".to_string()));

        // Test overwriting an existing key
        engine.set("key1".to_string(), "new_value".to_string());
        assert_eq!(engine.get("key1"), Some("new_value".to_string()));

        // Test delete operation
        engine.delete("key1");
        assert_eq!(engine.get("key1"), None);

        // Test keys() method with multiple entries
        engine.set("key2".to_string(), "value2".to_string());
        engine.set("key3".to_string(), "value3".to_string());

        let keys = engine.keys();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"key2".to_string()));
        assert!(keys.contains(&"key3".to_string()));
    }
    
    #[test]
    fn test_increment_operations() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().to_str().unwrap();
        let mut engine = KvEngine::new(storage_path).unwrap();
        
        // Test incrementing a non-existent key (should create with value 1)
        let result = engine.increment("counter1", None).unwrap();
        assert_eq!(result, 1);
        assert_eq!(engine.get("counter1"), Some("1".to_string()));
        
        // Test incrementing with a specific amount
        let result = engine.increment("counter1", Some(5)).unwrap();
        assert_eq!(result, 6);
        assert_eq!(engine.get("counter1"), Some("6".to_string()));
        
        // Test incrementing with a negative amount
        let result = engine.increment("counter1", Some(-2)).unwrap();
        assert_eq!(result, 4);
        assert_eq!(engine.get("counter1"), Some("4".to_string()));
        
        // Test incrementing a key with non-numeric value
        engine.set("text".to_string(), "hello".to_string());
        let result = engine.increment("text", None);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_decrement_operations() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().to_str().unwrap();
        let mut engine = KvEngine::new(storage_path).unwrap();
        
        // Test decrementing a non-existent key (should create with value -1)
        let result = engine.decrement("counter1", None).unwrap();
        assert_eq!(result, -1);
        assert_eq!(engine.get("counter1"), Some("-1".to_string()));
        
        // Test decrementing with a specific amount
        let result = engine.decrement("counter1", Some(3)).unwrap();
        assert_eq!(result, -4);
        assert_eq!(engine.get("counter1"), Some("-4".to_string()));
        
        // Test decrementing a key with non-numeric value
        engine.set("text".to_string(), "hello".to_string());
        let result = engine.decrement("text", None);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_string_operations() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().to_str().unwrap();
        let mut engine = KvEngine::new(storage_path).unwrap();
        
        // Set up a key for testing
        engine.set("greeting".to_string(), "World!".to_string());
        
        // Test append to existing key
        let result = engine.append("greeting", " Hello!").unwrap();
        assert_eq!(result, "World! Hello!");
        assert_eq!(engine.get("greeting"), Some("World! Hello!".to_string()));
        
        // Test prepend to existing key
        let result = engine.prepend("greeting", "Hey! ").unwrap();
        assert_eq!(result, "Hey! World! Hello!");
        assert_eq!(engine.get("greeting"), Some("Hey! World! Hello!".to_string()));
        
        // Test append to non-existent key (should error)
        let result = engine.append("nonexistent", "value");
        assert!(result.is_err());
        
        // Test prepend to non-existent key (should error)
        let result = engine.prepend("another_nonexistent", "value");
        assert!(result.is_err());
        
        // Set up a new key for testing
        engine.set("new_key".to_string(), "Start: ".to_string());
        assert_eq!(engine.get("new_key"), Some("Start: ".to_string()));
    }
    
    #[test]
    fn test_truncate_operation() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().to_str().unwrap();
        let mut engine = KvEngine::new(storage_path).unwrap();
        
        // Add some data
        engine.set("key1".to_string(), "value1".to_string());
        engine.set("key2".to_string(), "value2".to_string());
        engine.set("key3".to_string(), "value3".to_string());
        
        // Verify data exists
        assert_eq!(engine.keys().len(), 3);
        assert_eq!(engine.get("key1"), Some("value1".to_string()));
        assert_eq!(engine.get("key2"), Some("value2".to_string()));
        assert_eq!(engine.get("key3"), Some("value3".to_string()));
        
        // Truncate the store
        engine.truncate();
        
        // Verify all data is gone
        assert_eq!(engine.keys().len(), 0);
        assert_eq!(engine.get("key1"), None);
        assert_eq!(engine.get("key2"), None);
        assert_eq!(engine.get("key3"), None);
        
        // Verify we can add new data after truncate
        engine.set("new_key".to_string(), "new_value".to_string());
        assert_eq!(engine.keys().len(), 1);
        assert_eq!(engine.get("new_key"), Some("new_value".to_string()));
    }
}
