//! Error types for MerkleKV client operations

use std::fmt;
use thiserror::Error;

/// Result type alias for MerkleKV operations
pub type Result<T> = std::result::Result<T, Error>;

/// Comprehensive error types for MerkleKV client operations
#[derive(Error, Debug)]
pub enum Error {
    /// Connection-related errors
    #[error("Connection error: {message}")]
    Connection { message: String },
    
    /// Operation timeout errors
    #[error("Timeout error: {message}")]
    Timeout { message: String },
    
    /// I/O operation errors
    #[error("I/O error: {source}")]
    Io {
        #[from]
        source: std::io::Error,
    },
    
    /// Protocol-level errors from server
    #[error("Protocol error: {message}")]
    Protocol { message: String },
    
    /// Key not found errors
    #[error("Key not found: '{key}'")]
    KeyNotFound { key: String },
    
    /// Invalid parameter errors
    #[error("Invalid parameter: {message}")]
    InvalidParameter { message: String },
    
    /// Invalid server response errors
    #[error("Invalid response: {message}")]
    InvalidResponse { message: String },
}

impl Error {
    /// Create a connection error
    pub fn connection<S: Into<String>>(msg: S) -> Self {
        Error::Connection { message: msg.into() }
    }
    
    /// Create a timeout error
    pub fn timeout<S: Into<String>>(msg: S) -> Self {
        Error::Timeout { message: msg.into() }
    }
    
    /// Create a protocol error
    pub fn protocol<S: Into<String>>(msg: S) -> Self {
        Error::Protocol { message: msg.into() }
    }
    
    /// Create a key not found error
    pub fn key_not_found<S: Into<String>>(key: S) -> Self {
        Error::KeyNotFound { key: key.into() }
    }
    
    /// Create an invalid response error
    pub fn invalid_response<S: Into<String>>(msg: S) -> Self {
        Error::InvalidResponse { message: msg.into() }
    }
    
    /// Create an invalid parameter error
    pub fn invalid_parameter<S: Into<String>>(msg: S) -> Self {
        Error::InvalidParameter { message: msg.into() }
    }
    
    /// Create an I/O error
    pub fn io(err: std::io::Error) -> Self {
        Error::Io { source: err }
    }
}

impl From<tokio::net::tcp::ReuniteError> for Error {
    fn from(err: tokio::net::tcp::ReuniteError) -> Self {
        Error::Connection { 
            message: format!("Failed to reunite TCP stream: {}", err)
        }
    }
}
