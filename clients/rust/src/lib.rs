//! MerkleKV Rust Client Library
//!
//! This library provides both synchronous and asynchronous clients for interacting 
//! with MerkleKV servers. MerkleKV is a distributed key-value store with Merkle tree
//! verification capabilities.
//!
//! # Features
//!
//! - **Synchronous Client**: Simple, blocking operations perfect for scripts and simple applications
//! - **Asynchronous Client**: High-performance async operations with connection pooling for concurrent workloads
//! - **Connection Management**: Automatic connection handling and pooling
//! - **Error Handling**: Comprehensive error types with detailed context
//! - **Protocol Compliance**: Full support for MerkleKV TCP protocol
//! - **Unicode Support**: Complete UTF-8 support for keys and values
//! - **Timeout Control**: Configurable timeouts for all operations
//!
//! # Quick Start
//!
//! ## Synchronous Client
//!
//! ```rust,no_run
//! use merklekv_client::{Client, Result};
//!
//! fn main() -> Result<()> {
//!     let mut client = Client::connect("127.0.0.1:7878")?;
//!     
//!     // Set a key-value pair
//!     client.set("hello", "world")?;
//!     
//!     // Get a value
//!     let value = client.get("hello")?;
//!     println!("Value: {}", value); // Output: Value: world
//!     
//!     // Delete a key
//!     let deleted = client.delete("hello")?;
//!     println!("Deleted: {}", deleted); // Output: Deleted: true
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Asynchronous Client
//!
//! ```rust,no_run
//! use merklekv_client::{AsyncClient, Result};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let mut client = AsyncClient::connect("127.0.0.1:7878").await?;
//!     
//!     // Set a key-value pair
//!     client.set("async_hello", "async_world").await?;
//!     
//!     // Get a value
//!     let value = client.get("async_hello").await?;
//!     println!("Value: {}", value); // Output: Value: async_world
//!     
//!     // Concurrent operations
//!     let keys = vec!["key1", "key2", "key3"];
//!     let results = client.mget(keys).await;
//!     
//!     Ok(())
//! }
//! ```

pub mod error;
mod client;
mod async_client;

// Re-export main types
pub use error::{Error, Result};
pub use client::Client;
pub use async_client::AsyncClient;
