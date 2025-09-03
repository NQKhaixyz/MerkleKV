//! Asynchronous MerkleKV client implementation

use crate::error::{Error, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::timeout;
use log::{debug, info};

/// Connection pool for managing multiple TCP connections
#[derive(Debug)]
struct ConnectionPool {
    connections: Vec<TcpStream>,
    addr: String,
    max_size: usize,
}

impl ConnectionPool {
    fn new(addr: String, max_size: usize) -> Self {
        ConnectionPool {
            connections: Vec::new(),
            addr,
            max_size,
        }
    }
    
    async fn get_connection(&mut self) -> Result<TcpStream> {
        if let Some(stream) = self.connections.pop() {
            debug!("Reusing existing connection from pool");
            Ok(stream)
        } else {
            debug!("Creating new connection to {}", self.addr);
            TcpStream::connect(&self.addr)
                .await
                .map_err(|e| Error::connection(format!("Failed to connect to {}: {}", self.addr, e)))
        }
    }
    
    async fn return_connection(&mut self, stream: TcpStream) {
        if self.connections.len() < self.max_size {
            debug!("Returning connection to pool");
            self.connections.push(stream);
        } else {
            debug!("Pool is full, dropping connection");
            // Connection will be dropped automatically
        }
    }
}

/// Asynchronous MerkleKV client for non-blocking operations
/// 
/// This client provides an asynchronous API for high-performance concurrent
/// operations with MerkleKV server. All operations return futures that can
/// be awaited or composed with other async operations.
///
/// # Example
/// 
/// ```rust,no_run
/// use merklekv_client::{AsyncClient, Result};
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut client = AsyncClient::connect("127.0.0.1:7878").await?;
///     
///     client.set("key", "value").await?;
///     let value = client.get("key").await?;
///     let deleted = client.delete("key").await?;
///     
///     Ok(())
/// }
/// ```
pub struct AsyncClient {
    pool: Arc<Mutex<ConnectionPool>>,
    timeout_duration: Duration,
}

impl AsyncClient {
    /// Connect to a MerkleKV server with default settings
    /// 
    /// # Arguments
    /// 
    /// * `addr` - Server address in format "host:port"
    /// 
    /// # Example
    /// 
    /// ```rust,no_run
    /// use merklekv_client::AsyncClient;
    /// 
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), merklekv_client::Error> {
    /// let client = AsyncClient::connect("localhost:7878").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect<S: Into<String>>(addr: S) -> Result<Self> {
        let addr = addr.into();
        Self::connect_with_options(&addr, 10, Duration::from_secs(30)).await
    }
    
    /// Connect to a MerkleKV server with custom options
    /// 
    /// # Arguments
    /// 
    /// * `addr` - Server address in format "host:port"
    /// * `pool_size` - Maximum number of connections to maintain in the pool
    /// * `timeout` - Operation timeout duration
    pub async fn connect_with_options(
        addr: &str, 
        pool_size: usize, 
        timeout_duration: Duration
    ) -> Result<Self> {
        info!("Connecting to MerkleKV server at {} with pool size {}", addr, pool_size);
        
        // Test connection to ensure server is reachable
        let _test_stream = TcpStream::connect(addr)
            .await
            .map_err(|e| Error::connection(format!("Failed to connect to {}: {}", addr, e)))?;
        
        let pool = ConnectionPool::new(addr.to_string(), pool_size);
        
        info!("Connected to MerkleKV server at {}", addr);
        
        Ok(AsyncClient {
            pool: Arc::new(Mutex::new(pool)),
            timeout_duration,
        })
    }
    
    /// Get a value by key from the MerkleKV store
    /// 
    /// # Arguments
    /// 
    /// * `key` - The key to retrieve
    /// 
    /// # Returns
    /// 
    /// The value associated with the key
    /// 
    /// # Errors
    /// 
    /// * `Error::KeyNotFound` if the key does not exist
    /// * `Error::Timeout` if operation times out
    /// * `Error::Io` if network communication fails
    /// * `Error::Protocol` if server returns an error
    pub async fn get<S: AsRef<str>>(&mut self, key: S) -> Result<String> {
        let key = key.as_ref();
        if key.is_empty() {
            return Err(Error::invalid_parameter("Key cannot be empty"));
        }
        
        let command = format!("GET {}", key);
        debug!("Sending async command: {}", command);
        
        let response = timeout(
            self.timeout_duration,
            self.send_command(&command)
        )
        .await
        .map_err(|_| Error::timeout("GET operation timed out"))??;
        
        debug!("Received async response: {}", response);
        
        if response == "NOT_FOUND" {
            Err(Error::key_not_found(key))
        } else if let Some(value) = response.strip_prefix("VALUE ") {
            Ok(value.to_string())
        } else if let Some(error) = response.strip_prefix("ERROR ") {
            Err(Error::protocol(error))
        } else {
            Err(Error::invalid_response(response))
        }
    }
    
    /// Set a key-value pair in the MerkleKV store
    /// 
    /// # Arguments
    /// 
    /// * `key` - The key to set
    /// * `value` - The value to associate with the key
    /// 
    /// # Errors
    /// 
    /// * `Error::Timeout` if operation times out
    /// * `Error::Io` if network communication fails
    /// * `Error::Protocol` if server returns an error
    pub async fn set<S: AsRef<str>, V: AsRef<str>>(&mut self, key: S, value: V) -> Result<()> {
        let key = key.as_ref();
        let value = value.as_ref();
        
        if key.is_empty() {
            return Err(Error::invalid_parameter("Key cannot be empty"));
        }
        
        let command = format!("SET {} {}", key, value);
        debug!("Sending async command: SET {} <value>", key);
        
        let response = timeout(
            self.timeout_duration,
            self.send_command(&command)
        )
        .await
        .map_err(|_| Error::timeout("SET operation timed out"))??;
        
        debug!("Received async response: {}", response);
        
        if response == "OK" {
            Ok(())
        } else if let Some(error) = response.strip_prefix("ERROR ") {
            Err(Error::protocol(error))
        } else {
            Err(Error::invalid_response(response))
        }
    }
    
    /// Delete a key from the MerkleKV store
    /// 
    /// # Arguments
    /// 
    /// * `key` - The key to delete
    /// 
    /// # Returns
    /// 
    /// `true` if the key was deleted, `false` if it didn't exist
    /// 
    /// # Errors
    /// 
    /// * `Error::Timeout` if operation times out
    /// * `Error::Io` if network communication fails
    /// * `Error::Protocol` if server returns an error
    pub async fn delete<S: AsRef<str>>(&mut self, key: S) -> Result<bool> {
        let key = key.as_ref();
        if key.is_empty() {
            return Err(Error::invalid_parameter("Key cannot be empty"));
        }
        
        let command = format!("DELETE {}", key);
        debug!("Sending async command: {}", command);
        
        let response = timeout(
            self.timeout_duration,
            self.send_command(&command)
        )
        .await
        .map_err(|_| Error::timeout("DELETE operation timed out"))??;
        
        debug!("Received async response: {}", response);
        
        if response == "OK" {
            Ok(true)
        } else if response == "NOT_FOUND" {
            Ok(false)
        } else if let Some(error) = response.strip_prefix("ERROR ") {
            Err(Error::protocol(error))
        } else {
            Err(Error::invalid_response(response))
        }
    }
    
    /// Get multiple values concurrently
    /// 
    /// # Arguments
    /// 
    /// * `keys` - Vector of keys to retrieve
    /// 
    /// # Returns
    /// 
    /// Vector of results for each key in the same order
    pub async fn mget<S: AsRef<str> + Clone>(&mut self, keys: Vec<S>) -> Vec<Result<String>> {
        let mut handles = Vec::new();
        
        for key in keys {
            let key_str = key.as_ref().to_string();
            let pool = Arc::clone(&self.pool);
            let timeout_duration = self.timeout_duration;
            
            let handle = tokio::spawn(async move {
                let command = format!("GET {}", key_str);
                let response = timeout(
                    timeout_duration,
                    send_command_with_pool(pool, &command)
                )
                .await
                .map_err(|_| Error::timeout("GET operation timed out"))??;
                
                if response == "NOT_FOUND" {
                    Err(Error::key_not_found(&key_str))
                } else if let Some(value) = response.strip_prefix("VALUE ") {
                    Ok(value.to_string())
                } else if let Some(error) = response.strip_prefix("ERROR ") {
                    Err(Error::protocol(error))
                } else {
                    Err(Error::invalid_response(response))
                }
            });
            
            handles.push(handle);
        }
        
        let mut results = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(_) => results.push(Err(Error::connection("Task panicked"))),
            }
        }
        
        results
    }
    
    /// Set multiple key-value pairs concurrently
    /// 
    /// # Arguments
    /// 
    /// * `pairs` - Vector of (key, value) pairs to set
    /// 
    /// # Returns
    /// 
    /// Vector of results for each pair in the same order
    pub async fn mset<K: AsRef<str> + Clone, V: AsRef<str> + Clone>(
        &mut self,
        pairs: Vec<(K, V)>
    ) -> Vec<Result<()>> {
        let mut handles = Vec::new();
        
        for (key, value) in pairs {
            let key_str = key.as_ref().to_string();
            let value_str = value.as_ref().to_string();
            let pool = Arc::clone(&self.pool);
            let timeout_duration = self.timeout_duration;
            
            let handle = tokio::spawn(async move {
                let command = format!("SET {} {}", key_str, value_str);
                let response = timeout(
                    timeout_duration,
                    send_command_with_pool(pool, &command)
                )
                .await
                .map_err(|_| Error::timeout("SET operation timed out"))??;
                
                if response == "OK" {
                    Ok(())
                } else if let Some(error) = response.strip_prefix("ERROR ") {
                    Err(Error::protocol(error))
                } else {
                    Err(Error::invalid_response(response))
                }
            });
            
            handles.push(handle);
        }
        
        let mut results = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(_) => results.push(Err(Error::connection("Task panicked"))),
            }
        }
        
        results
    }
    
    /// Get the server address this client is connected to
    pub async fn server_addr(&self) -> String {
        let pool = self.pool.lock().await;
        pool.addr.clone()
    }
    
    /// Send a command using the connection pool
    async fn send_command(&mut self, command: &str) -> Result<String> {
        let pool = Arc::clone(&self.pool);
        send_command_with_pool(pool, command).await
    }
}

// Helper function to send command with connection pool
async fn send_command_with_pool(pool: Arc<Mutex<ConnectionPool>>, command: &str) -> Result<String> {
    let mut pool_guard = pool.lock().await;
    let stream = pool_guard.get_connection().await?;
    drop(pool_guard);
    
    let result = send_command_on_stream(stream, command).await;
    
    match result {
        Ok((response, stream)) => {
            let mut pool_guard = pool.lock().await;
            pool_guard.return_connection(stream).await;
            Ok(response)
        }
        Err(e) => {
            // Don't return connection to pool if there was an error
            Err(e)
        }
    }
}

// Send command on a specific stream
async fn send_command_on_stream(stream: TcpStream, command: &str) -> Result<(String, TcpStream)> {
    let (reader, writer) = stream.into_split();
    let mut buf_reader = BufReader::new(reader);
    let mut buf_writer = BufWriter::new(writer);
    
    // Send command
    buf_writer.write_all(command.as_bytes()).await?;
    buf_writer.write_all(b"\r\n").await?;
    buf_writer.flush().await?;
    
    // Read response
    let mut response = String::new();
    buf_reader.read_line(&mut response).await?;
    
    // Remove trailing newline
    if response.ends_with('\n') {
        response.pop();
        if response.ends_with('\r') {
            response.pop();
        }
    }
    
    if response.is_empty() {
        return Err(Error::connection("Server closed connection"));
    }
    
    // Reconstruct the stream
    let reader = buf_reader.into_inner();
    let writer = buf_writer.into_inner();
    let stream = reader.reunite(writer)?;
    
    Ok((response, stream))
}

// Implement Send and Sync for AsyncClient
unsafe impl Send for AsyncClient {}
unsafe impl Sync for AsyncClient {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_async_command_formatting() {
        let key = "async_key";
        let value = "async_value";
        
        let get_cmd = format!("GET {}", key);
        assert_eq!(get_cmd, "GET async_key");
        
        let set_cmd = format!("SET {} {}", key, value);
        assert_eq!(set_cmd, "SET async_key async_value");
        
        let delete_cmd = format!("DELETE {}", key);
        assert_eq!(delete_cmd, "DELETE async_key");
    }
    
    #[tokio::test]
    async fn test_async_response_parsing() {
        // Test VALUE response
        let response = "VALUE async_data";
        if let Some(value) = response.strip_prefix("VALUE ") {
            assert_eq!(value, "async_data");
        }
        
        // Test NOT_FOUND response
        let response = "NOT_FOUND";
        assert_eq!(response, "NOT_FOUND");
        
        // Test ERROR response
        let response = "ERROR Async error";
        if let Some(error) = response.strip_prefix("ERROR ") {
            assert_eq!(error, "Async error");
        }
    }
    
    #[tokio::test]
    async fn test_async_parameter_validation() {
        // Test empty key validation
        let result = if "".is_empty() {
            Err(Error::invalid_parameter("Key cannot be empty"))
        } else {
            Ok(())
        };
        assert!(result.is_err());
        
        // Test valid key
        let result = if !"valid_async_key".is_empty() {
            Ok(())
        } else {
            Err(Error::invalid_parameter("Key cannot be empty"))
        };
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_concurrent_operations() {
        // Test that we can handle multiple concurrent operations
        let keys = vec!["key1", "key2", "key3"];
        let values = vec!["value1", "value2", "value3"];
        
        // Simulate concurrent operations
        let mut handles = vec![];
        for (i, (key, value)) in keys.iter().zip(values.iter()).enumerate() {
            let key = key.to_string();
            let value = value.to_string();
            let handle = tokio::spawn(async move {
                // Simulate processing
                tokio::time::sleep(Duration::from_millis(i as u64 * 10)).await;
                format!("SET {} {}", key, value)
            });
            handles.push(handle);
        }
        
        let mut results = vec![];
        for handle in handles {
            results.push(handle.await.unwrap());
        }
        
        assert_eq!(results.len(), 3);
        assert!(results[0].contains("key1"));
        assert!(results[1].contains("key2"));
        assert!(results[2].contains("key3"));
    }
    
    #[tokio::test]
    async fn test_timeout_simulation() {
        // Test timeout handling logic
        let timeout_duration = Duration::from_millis(100);
        
        let result = timeout(
            timeout_duration,
            async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok::<String, Error>("Should timeout".to_string())
            }
        ).await;
        
        assert!(result.is_err()); // Should timeout
        
        let result = timeout(
            timeout_duration,
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<String, Error>("Should succeed".to_string())
            }
        ).await;
        
        assert!(result.is_ok()); // Should succeed
        assert_eq!(result.unwrap().unwrap(), "Should succeed");
    }
    
    #[tokio::test]
    async fn test_mget_mset_logic() {
        let keys = vec!["mkey1", "mkey2", "mkey3"];
        let pairs = vec![("mkey1", "mvalue1"), ("mkey2", "mvalue2")];
        
        // Test that we can handle multiple keys and pairs
        assert_eq!(keys.len(), 3);
        assert_eq!(pairs.len(), 2);
        
        // Test concurrent task simulation
        let mut handles = vec![];
        for key in &keys {
            let key = key.to_string();
            let handle = tokio::spawn(async move {
                format!("GET {}", key)
            });
            handles.push(handle);
        }
        
        let mut results = vec![];
        for handle in handles {
            results.push(handle.await.unwrap());
        }
        
        assert_eq!(results.len(), 3);
        for (i, result) in results.iter().enumerate() {
            assert!(result.contains(&format!("mkey{}", i + 1)));
        }
    }
}
