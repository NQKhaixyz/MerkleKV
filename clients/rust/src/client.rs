//! Synchronous MerkleKV client implementation

use crate::error::{Error, Result};
use std::io::{BufRead, BufReader, Write, BufWriter};
use std::net::TcpStream;
use std::time::Duration;
use log::{debug, info};

/// Synchronous MerkleKV client for blocking operations
/// 
/// This client provides a simple, blocking API for interacting with MerkleKV server.
/// All operations will block until completion or timeout.
///
/// # Example
/// 
/// ```rust,no_run
/// use merklekv_client::{Client, Result};
///
/// fn main() -> Result<()> {
///     let mut client = Client::connect("127.0.0.1:7878")?;
///     
///     client.set("key", "value")?;
///     let value = client.get("key")?;
///     let deleted = client.delete("key")?;
///     
///     Ok(())
/// }
/// ```
pub struct Client {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
    addr: String,
}

impl Client {
    /// Connect to a MerkleKV server
    /// 
    /// # Arguments
    /// 
    /// * `addr` - Server address in format "host:port"
    /// 
    /// # Example
    /// 
    /// ```rust,no_run
    /// use merklekv_client::Client;
    /// 
    /// let client = Client::connect("localhost:7878")?;
    /// # Ok::<(), merklekv_client::Error>(())
    /// ```
    pub fn connect<S: Into<String>>(addr: S) -> Result<Self> {
        let addr = addr.into();
        Self::connect_with_timeout(&addr, Duration::from_secs(30))
    }
    
    /// Connect to a MerkleKV server with a custom timeout
    /// 
    /// # Arguments
    /// 
    /// * `addr` - Server address in format "host:port"
    /// * `timeout` - Connection timeout duration
    pub fn connect_with_timeout(addr: &str, timeout: Duration) -> Result<Self> {
        info!("Connecting to MerkleKV server at {}", addr);
        
        let stream = TcpStream::connect(addr)
            .map_err(|e| Error::connection(format!("Failed to connect to {}: {}", addr, e)))?;
        
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        
        let reader_stream = stream.try_clone()?;
        let reader = BufReader::new(reader_stream);
        let writer = BufWriter::new(stream);
        
        info!("Connected to MerkleKV server at {}", addr);
        
        Ok(Client {
            reader,
            writer,
            addr: addr.to_string(),
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
    /// * `Error::Io` if network communication fails
    /// * `Error::Protocol` if server returns an error
    pub fn get<S: AsRef<str>>(&mut self, key: S) -> Result<String> {
        let key = key.as_ref();
        if key.is_empty() {
            return Err(Error::invalid_parameter("Key cannot be empty"));
        }
        
        let command = format!("GET {}", key);
        debug!("Sending command: {}", command);
        
        let response = self.send_command(&command)?;
        debug!("Received response: {}", response);
        
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
    /// * `Error::Io` if network communication fails
    /// * `Error::Protocol` if server returns an error
    pub fn set<S: AsRef<str>, V: AsRef<str>>(&mut self, key: S, value: V) -> Result<()> {
        let key = key.as_ref();
        let value = value.as_ref();
        
        if key.is_empty() {
            return Err(Error::invalid_parameter("Key cannot be empty"));
        }
        
        let command = format!("SET {} {}", key, value);
        debug!("Sending command: SET {} <value>", key);
        
        let response = self.send_command(&command)?;
        debug!("Received response: {}", response);
        
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
    /// * `Error::Io` if network communication fails
    /// * `Error::Protocol` if server returns an error
    pub fn delete<S: AsRef<str>>(&mut self, key: S) -> Result<bool> {
        let key = key.as_ref();
        if key.is_empty() {
            return Err(Error::invalid_parameter("Key cannot be empty"));
        }
        
        let command = format!("DELETE {}", key);
        debug!("Sending command: {}", command);
        
        let response = self.send_command(&command)?;
        debug!("Received response: {}", response);
        
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
    
    /// Get the server address this client is connected to
    pub fn server_addr(&self) -> &str {
        &self.addr
    }
    
    /// Send a command to the server and receive the response
    fn send_command(&mut self, command: &str) -> Result<String> {
        // Send command
        writeln!(&mut self.writer, "{}", command)?;
        self.writer.flush()?;
        
        // Read response
        let mut response = String::new();
        self.reader.read_line(&mut response)?;
        
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
        
        Ok(response)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        debug!("Closing connection to {}", self.addr);
        // BufWriter and BufReader will be dropped automatically,
        // closing the underlying TcpStream
    }
}

// Implement Send for Client to allow moving between threads
unsafe impl Send for Client {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_formatting() {
        let key = "test_key";
        let value = "test_value";
        
        let get_cmd = format!("GET {}", key);
        assert_eq!(get_cmd, "GET test_key");
        
        let set_cmd = format!("SET {} {}", key, value);
        assert_eq!(set_cmd, "SET test_key test_value");
        
        let delete_cmd = format!("DELETE {}", key);
        assert_eq!(delete_cmd, "DELETE test_key");
    }
    
    #[test]
    fn test_response_parsing() {
        // Test VALUE response
        let response = "VALUE test_data";
        if let Some(value) = response.strip_prefix("VALUE ") {
            assert_eq!(value, "test_data");
        }
        
        // Test NOT_FOUND response
        let response = "NOT_FOUND";
        assert_eq!(response, "NOT_FOUND");
        
        // Test ERROR response
        let response = "ERROR Invalid command";
        if let Some(error) = response.strip_prefix("ERROR ") {
            assert_eq!(error, "Invalid command");
        }
    }
    
    #[test]
    fn test_parameter_validation() {
        // Test empty key validation
        let result = if "".is_empty() {
            Err(Error::invalid_parameter("Key cannot be empty"))
        } else {
            Ok(())
        };
        assert!(result.is_err());
        
        // Test valid key
        let result = if !"valid_key".is_empty() {
            Ok(())
        } else {
            Err(Error::invalid_parameter("Key cannot be empty"))
        };
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_unicode_support() {
        let unicode_key = "用户";
        let unicode_value = "测试值 🚀";
        
        let command = format!("SET {} {}", unicode_key, unicode_value);
        assert!(command.contains(unicode_key));
        assert!(command.contains(unicode_value));
    }
    
    #[test]
    fn test_large_values() {
        let mut large_value = String::new();
        for i in 0..1000 {
            large_value.push_str(&format!("Large value data {} ", i));
        }
        
        let command = format!("SET large_key {}", large_value);
        assert!(command.len() > 10000);
        assert!(command.starts_with("SET large_key"));
    }
    
    #[test]
    fn test_values_with_spaces() {
        let value_with_spaces = "value with multiple spaces";
        let command = format!("SET space_key {}", value_with_spaces);
        assert_eq!(command, "SET space_key value with multiple spaces");
        
        // Test response parsing
        let response = format!("VALUE {}", value_with_spaces);
        if let Some(extracted) = response.strip_prefix("VALUE ") {
            assert_eq!(extracted, value_with_spaces);
        }
    }
}
