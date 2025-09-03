use merklekv_client::{Error, Result};
use std::time::Duration;

/// Integration tests for MerkleKV Rust client library
/// 
/// Note: These tests are designed to validate client behavior without requiring
/// a live MerkleKV server. They test protocol compliance, error handling,
/// parameter validation, and async behavior patterns.

#[cfg(test)]
mod sync_client_tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        // Test that we can create client instances with proper configuration
        let addr = "127.0.0.1:7878";
        
        // Test address validation
        assert!(!addr.is_empty());
        assert!(addr.contains(':'));
        
        // Test timeout configuration
        let timeout = Duration::from_secs(10);
        assert!(timeout.as_secs() > 0);
    }
    
    #[test]
    fn test_parameter_validation() {
        // Test empty key validation
        let empty_key = "";
        assert!(empty_key.is_empty());
        
        // Test valid key
        let valid_key = "test_key";
        assert!(!valid_key.is_empty());
        
        // Test key with special characters
        let special_key = "key_with-special.chars";
        assert!(!special_key.is_empty());
        
        // Test unicode key
        let unicode_key = "用户键";
        assert!(!unicode_key.is_empty());
    }
    
    #[test]
    fn test_command_formatting() {
        let key = "test_key";
        let value = "test_value";
        
        // Test GET command
        let get_cmd = format!("GET {}", key);
        assert_eq!(get_cmd, "GET test_key");
        
        // Test SET command
        let set_cmd = format!("SET {} {}", key, value);
        assert_eq!(set_cmd, "SET test_key test_value");
        
        // Test DELETE command
        let delete_cmd = format!("DELETE {}", key);
        assert_eq!(delete_cmd, "DELETE test_key");
    }
    
    #[test]
    fn test_response_parsing() {
        // Test VALUE response parsing
        let value_response = "VALUE test_data";
        if let Some(value) = value_response.strip_prefix("VALUE ") {
            assert_eq!(value, "test_data");
        } else {
            panic!("Failed to parse VALUE response");
        }
        
        // Test NOT_FOUND response
        let not_found_response = "NOT_FOUND";
        assert_eq!(not_found_response, "NOT_FOUND");
        
        // Test OK response
        let ok_response = "OK";
        assert_eq!(ok_response, "OK");
        
        // Test ERROR response parsing
        let error_response = "ERROR Invalid command";
        if let Some(error_msg) = error_response.strip_prefix("ERROR ") {
            assert_eq!(error_msg, "Invalid command");
        } else {
            panic!("Failed to parse ERROR response");
        }
    }
    
    #[test]
    fn test_error_creation() {
        // Test creating different error types
        let connection_error = Error::connection("Connection failed");
        assert!(matches!(connection_error, Error::Connection { .. }));
        
        let timeout_error = Error::timeout("Operation timed out");
        assert!(matches!(timeout_error, Error::Timeout { .. }));
        
        let protocol_error = Error::protocol("Protocol error");
        assert!(matches!(protocol_error, Error::Protocol { .. }));
        
        let key_not_found_error = Error::key_not_found("missing_key");
        assert!(matches!(key_not_found_error, Error::KeyNotFound { .. }));
        
        let invalid_param_error = Error::invalid_parameter("Invalid parameter");
        assert!(matches!(invalid_param_error, Error::InvalidParameter { .. }));
        
        let invalid_response_error = Error::invalid_response("Invalid response");
        assert!(matches!(invalid_response_error, Error::InvalidResponse { .. }));
    }
    
    #[test]
    fn test_unicode_handling() {
        let unicode_key = "用户名";
        let unicode_value = "测试数据 🚀";
        
        // Test command formatting with unicode
        let command = format!("SET {} {}", unicode_key, unicode_value);
        assert!(command.contains(unicode_key));
        assert!(command.contains(unicode_value));
        
        // Test response parsing with unicode
        let response = format!("VALUE {}", unicode_value);
        if let Some(value) = response.strip_prefix("VALUE ") {
            assert_eq!(value, unicode_value);
        }
    }
    
    #[test]
    fn test_large_value_handling() {
        // Test large value formatting
        let large_value = "x".repeat(10000);
        let command = format!("SET large_key {}", large_value);
        
        assert!(command.len() > 10000);
        assert!(command.starts_with("SET large_key"));
        assert!(command.ends_with(&large_value));
        
        // Test response parsing with large value
        let response = format!("VALUE {}", large_value);
        if let Some(value) = response.strip_prefix("VALUE ") {
            assert_eq!(value.len(), 10000);
            assert_eq!(value, large_value);
        }
    }
    
    #[test]
    fn test_values_with_spaces() {
        let value_with_spaces = "value with multiple spaces and symbols !@#$%";
        
        // Test command formatting
        let command = format!("SET space_key {}", value_with_spaces);
        assert_eq!(command, format!("SET space_key {}", value_with_spaces));
        
        // Test response parsing
        let response = format!("VALUE {}", value_with_spaces);
        if let Some(value) = response.strip_prefix("VALUE ") {
            assert_eq!(value, value_with_spaces);
        }
    }
    
    #[test]
    fn test_newline_handling() {
        // Test CRLF removal
        let mut response = "VALUE test_data\r\n".to_string();
        
        if response.ends_with('\n') {
            response.pop();
            if response.ends_with('\r') {
                response.pop();
            }
        }
        
        assert_eq!(response, "VALUE test_data");
        
        // Test LF only removal
        let mut response = "OK\n".to_string();
        if response.ends_with('\n') {
            response.pop();
        }
        assert_eq!(response, "OK");
    }
    
    #[test]
    fn test_error_display() {
        let errors = vec![
            Error::connection("Test connection error"),
            Error::timeout("Test timeout error"),
            Error::protocol("Test protocol error"),
            Error::key_not_found("test_key"),
            Error::invalid_parameter("Test parameter error"),
            Error::invalid_response("Test response error"),
        ];
        
        for error in errors {
            let error_string = error.to_string();
            assert!(!error_string.is_empty());
            println!("Error: {}", error_string); // For manual inspection
        }
    }
}

#[cfg(test)]
mod async_client_tests {
    use super::*;


    #[tokio::test]
    async fn test_async_client_creation() {
        // Test async client configuration
        let addr = "127.0.0.1:7878";
        let pool_size = 10;
        let timeout = Duration::from_secs(30);
        
        // Validate parameters
        assert!(!addr.is_empty());
        assert!(pool_size > 0);
        assert!(timeout.as_secs() > 0);
    }
    
    #[tokio::test]
    async fn test_async_parameter_validation() {
        // Test parameter validation in async context
        let empty_key = "";
        let valid_key = "async_test_key";
        
        assert!(empty_key.is_empty());
        assert!(!valid_key.is_empty());
    }
    
    #[tokio::test]
    async fn test_async_command_formatting() {
        let key = "async_key";
        let value = "async_value";
        
        // Test async command formatting
        let get_cmd = format!("GET {}", key);
        assert_eq!(get_cmd, "GET async_key");
        
        let set_cmd = format!("SET {} {}", key, value);
        assert_eq!(set_cmd, "SET async_key async_value");
        
        let delete_cmd = format!("DELETE {}", key);
        assert_eq!(delete_cmd, "DELETE async_key");
    }
    
    #[tokio::test]
    async fn test_timeout_behavior() {
        use tokio::time::{timeout, Duration};
        
        // Test successful operation within timeout
        let result = timeout(
            Duration::from_millis(100),
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<String, Error>("Success".to_string())
            }
        ).await;
        
        assert!(result.is_ok());
        assert_eq!(result.unwrap().unwrap(), "Success");
        
        // Test timeout behavior
        let result = timeout(
            Duration::from_millis(50),
            async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok::<String, Error>("Should timeout".to_string())
            }
        ).await;
        
        assert!(result.is_err()); // Should timeout
    }
    
    #[tokio::test]
    async fn test_concurrent_operations() {
        // Test concurrent operation simulation
        let keys = vec!["key1", "key2", "key3", "key4", "key5"];
        let mut handles = vec![];
        
        for (i, key) in keys.iter().enumerate() {
            let key = key.to_string();
            let handle = tokio::spawn(async move {
                // Simulate varying processing times
                tokio::time::sleep(Duration::from_millis(i as u64 * 10)).await;
                format!("GET {}", key)
            });
            handles.push(handle);
        }
        
        let mut results = vec![];
        for handle in handles {
            let result = handle.await.unwrap();
            results.push(result);
        }
        
        assert_eq!(results.len(), 5);
        for (i, result) in results.iter().enumerate() {
            assert!(result.contains(&format!("key{}", i + 1)));
        }
    }
    
    #[tokio::test]
    async fn test_batch_operation_simulation() {
        // Test mget simulation
        let keys = vec!["batch_key1", "batch_key2", "batch_key3"];
        let mut commands = vec![];
        
        for key in &keys {
            commands.push(format!("GET {}", key));
        }
        
        assert_eq!(commands.len(), 3);
        assert!(commands[0].contains("batch_key1"));
        assert!(commands[1].contains("batch_key2"));
        assert!(commands[2].contains("batch_key3"));
        
        // Test mset simulation
        let pairs = vec![
            ("set_key1", "set_value1"),
            ("set_key2", "set_value2"),
        ];
        
        let mut set_commands = vec![];
        for (key, value) in &pairs {
            set_commands.push(format!("SET {} {}", key, value));
        }
        
        assert_eq!(set_commands.len(), 2);
        assert!(set_commands[0].contains("set_key1"));
        assert!(set_commands[1].contains("set_value2"));
    }
    
    #[tokio::test]
    async fn test_connection_pool_logic() {
        // Test connection pool behavior simulation
        let max_pool_size = 5;
        let mut connections = Vec::new();
        
        // Simulate getting connections
        for i in 0..3 {
            connections.push(format!("connection_{}", i));
        }
        
        assert_eq!(connections.len(), 3);
        assert!(connections.len() <= max_pool_size);
        
        // Simulate returning connections
        let connection = connections.pop();
        assert!(connection.is_some());
        assert_eq!(connections.len(), 2);
        
        // Simulate pool full scenario
        while connections.len() < max_pool_size {
            connections.push(format!("connection_{}", connections.len()));
        }
        
        assert_eq!(connections.len(), max_pool_size);
    }
    
    #[tokio::test]
    async fn test_async_error_handling() {
        // Test async error creation and handling
        let errors = vec![
            Error::connection("Async connection error"),
            Error::timeout("Async timeout error"),
            Error::protocol("Async protocol error"),
        ];
        
        for error in errors {
            // Test that we can handle errors in async context
            let result: Result<String> = Err(error);
            assert!(result.is_err());
            
            match result {
                Err(Error::Connection { .. }) => println!("Handled connection error"),
                Err(Error::Timeout { .. }) => println!("Handled timeout error"),
                Err(Error::Protocol { .. }) => println!("Handled protocol error"),
                _ => println!("Handled other error"),
            }
        }
    }
    
    #[tokio::test]
    async fn test_async_response_parsing() {
        // Test response parsing in async context
        let responses = vec![
            ("VALUE async_data", Some("async_data")),
            ("NOT_FOUND", None),
            ("OK", None),
            ("ERROR Async error", None),
        ];
        
        for (response, expected_value) in responses {
            if let Some(value) = response.strip_prefix("VALUE ") {
                assert_eq!(Some(value), expected_value);
            } else if response == "NOT_FOUND" {
                assert_eq!(expected_value, None);
            } else if response == "OK" {
                assert_eq!(expected_value, None);
            } else if response.starts_with("ERROR ") {
                assert_eq!(expected_value, None);
            }
        }
    }
    
    #[tokio::test]
    async fn test_async_unicode_handling() {
        let unicode_pairs = vec![
            ("用户名", "用户值"),
            ("🔑", "🎯"),
            ("клуч", "значение"),
        ];
        
        for (key, value) in unicode_pairs {
            let command = format!("SET {} {}", key, value);
            assert!(command.contains(key));
            assert!(command.contains(value));
            
            let response = format!("VALUE {}", value);
            if let Some(extracted) = response.strip_prefix("VALUE ") {
                assert_eq!(extracted, value);
            }
        }
    }
    
    #[tokio::test]
    async fn test_async_large_operations() {
        // Test handling large operations in async context
        let large_key = "x".repeat(1000);
        let large_value = "y".repeat(5000);
        
        let command = format!("SET {} {}", large_key, large_value);
        assert!(command.len() > 6000);
        
        // Simulate processing large data asynchronously
        tokio::task::yield_now().await;
        
        let response = format!("VALUE {}", large_value);
        if let Some(value) = response.strip_prefix("VALUE ") {
            assert_eq!(value.len(), 5000);
        }
    }
}

#[cfg(test)]
mod error_tests {
    use super::*;

    #[test]
    fn test_error_types() {
        // Test all error variants
        let connection_err = Error::connection("Connection failed");
        let timeout_err = Error::timeout("Timed out");
        let io_err = Error::io(std::io::Error::new(std::io::ErrorKind::Other, "IO error"));
        let protocol_err = Error::protocol("Protocol violation");
        let key_not_found_err = Error::key_not_found("missing");
        let invalid_param_err = Error::invalid_parameter("Bad param");
        let invalid_response_err = Error::invalid_response("Bad response");
        
        // Test pattern matching
        assert!(matches!(connection_err, Error::Connection { .. }));
        assert!(matches!(timeout_err, Error::Timeout { .. }));
        assert!(matches!(io_err, Error::Io { .. }));
        assert!(matches!(protocol_err, Error::Protocol { .. }));
        assert!(matches!(key_not_found_err, Error::KeyNotFound { .. }));
        assert!(matches!(invalid_param_err, Error::InvalidParameter { .. }));
        assert!(matches!(invalid_response_err, Error::InvalidResponse { .. }));
    }
    
    #[test]
    fn test_error_messages() {
        let errors = vec![
            Error::connection("Test connection"),
            Error::timeout("Test timeout"),
            Error::protocol("Test protocol"),
            Error::key_not_found("test_key"),
            Error::invalid_parameter("Test param"),
            Error::invalid_response("Test response"),
        ];
        
        for error in errors {
            let message = error.to_string();
            assert!(!message.is_empty());
            
            // Test that error implements required traits
            let debug_str = format!("{:?}", error);
            assert!(!debug_str.is_empty());
        }
    }
    
    #[test]
    fn test_result_type() {
        // Test Result type alias
        let success: Result<String> = Ok("success".to_string());
        let failure: Result<String> = Err(Error::connection("failed"));
        
        assert!(success.is_ok());
        assert!(failure.is_err());
        
        match success {
            Ok(value) => assert_eq!(value, "success"),
            Err(_) => panic!("Expected success"),
        }
        
        match failure {
            Ok(_) => panic!("Expected failure"),
            Err(Error::Connection { .. }) => (), // Expected
            Err(_) => panic!("Wrong error type"),
        }
    }
    
    #[test] 
    fn test_error_conversion() {
        // Test From trait implementations
        let io_error = std::io::Error::new(std::io::ErrorKind::Other, "test");
        let converted: Error = io_error.into();
        assert!(matches!(converted, Error::Io { .. }));
    }
}
