use merklekv_client::{Client, Result};
use std::env;

/// Synchronous MerkleKV client example
/// 
/// This example demonstrates basic operations using the synchronous client:
/// - Connecting to a MerkleKV server
/// - Setting key-value pairs
/// - Getting values by key
/// - Deleting keys
/// - Error handling

fn main() -> Result<()> {
    env_logger::init();
    
    // Get server address from environment or use default
    let server_addr = env::var("MERKLEKV_SERVER")
        .unwrap_or_else(|_| "127.0.0.1:7878".to_string());
    
    println!("MerkleKV Synchronous Client Example");
    println!("===================================");
    println!("Connecting to server: {}", server_addr);
    
    // Connect to the MerkleKV server
    let mut client = Client::connect(&server_addr)?;
    println!("✓ Connected successfully");
    
    // Example 1: Basic SET operation
    println!("\n1. Setting key-value pairs:");
    client.set("hello", "world")?;
    println!("   SET hello = world");
    
    client.set("rust", "programming language")?;
    println!("   SET rust = programming language");
    
    client.set("status", "operational")?;
    println!("   SET status = operational");
    
    // Example 2: Basic GET operations
    println!("\n2. Getting values:");
    match client.get("hello") {
        Ok(value) => println!("   GET hello -> {}", value),
        Err(e) => println!("   GET hello -> Error: {}", e),
    }
    
    match client.get("rust") {
        Ok(value) => println!("   GET rust -> {}", value),
        Err(e) => println!("   GET rust -> Error: {}", e),
    }
    
    // Example 3: Handling non-existent keys
    println!("\n3. Handling missing keys:");
    match client.get("nonexistent") {
        Ok(value) => println!("   GET nonexistent -> {}", value),
        Err(e) => println!("   GET nonexistent -> {}", e),
    }
    
    // Example 4: Unicode support
    println!("\n4. Unicode support:");
    client.set("用户名", "张三")?;
    println!("   SET 用户名 = 张三");
    
    match client.get("用户名") {
        Ok(value) => println!("   GET 用户名 -> {}", value),
        Err(e) => println!("   GET 用户名 -> Error: {}", e),
    }
    
    // Example 5: Values with spaces and special characters
    println!("\n5. Complex values:");
    let complex_value = "This is a value with spaces, symbols !@#$%, and numbers 12345";
    client.set("complex_key", complex_value)?;
    println!("   SET complex_key = {}", complex_value);
    
    match client.get("complex_key") {
        Ok(value) => println!("   GET complex_key -> {}", value),
        Err(e) => println!("   GET complex_key -> Error: {}", e),
    }
    
    // Example 6: Large value handling
    println!("\n6. Large value handling:");
    let large_value = "Large data: ".to_string() + &"x".repeat(1000);
    client.set("large_key", &large_value)?;
    println!("   SET large_key = <{} bytes>", large_value.len());
    
    match client.get("large_key") {
        Ok(value) => println!("   GET large_key -> <{} bytes> ({}...)", 
                              value.len(), 
                              &value[..std::cmp::min(50, value.len())]),
        Err(e) => println!("   GET large_key -> Error: {}", e),
    }
    
    // Example 7: DELETE operations
    println!("\n7. Delete operations:");
    match client.delete("hello") {
        Ok(true) => println!("   DELETE hello -> Key deleted"),
        Ok(false) => println!("   DELETE hello -> Key not found"),
        Err(e) => println!("   DELETE hello -> Error: {}", e),
    }
    
    // Try to delete the same key again
    match client.delete("hello") {
        Ok(true) => println!("   DELETE hello -> Key deleted"),
        Ok(false) => println!("   DELETE hello -> Key not found"),
        Err(e) => println!("   DELETE hello -> Error: {}", e),
    }
    
    // Verify deletion
    match client.get("hello") {
        Ok(value) => println!("   GET hello -> {} (unexpected!)", value),
        Err(e) => println!("   GET hello -> {} (expected)", e),
    }
    
    // Example 8: Batch operations using a loop
    println!("\n8. Batch operations:");
    let keys_values = vec![
        ("batch_key1", "batch_value1"),
        ("batch_key2", "batch_value2"),
        ("batch_key3", "batch_value3"),
        ("batch_key4", "batch_value4"),
        ("batch_key5", "batch_value5"),
    ];
    
    // Set multiple keys
    println!("   Setting multiple keys:");
    for (key, value) in &keys_values {
        match client.set(key, value) {
            Ok(()) => println!("     ✓ SET {} = {}", key, value),
            Err(e) => println!("     ✗ SET {} failed: {}", key, e),
        }
    }
    
    // Get multiple keys
    println!("   Getting multiple keys:");
    for (key, _) in &keys_values {
        match client.get(key) {
            Ok(value) => println!("     ✓ GET {} -> {}", key, value),
            Err(e) => println!("     ✗ GET {} failed: {}", key, e),
        }
    }
    
    // Example 9: Error handling demonstration
    println!("\n9. Error handling:");
    
    // Try operations with invalid parameters
    match client.get("") {
        Ok(_) => println!("   GET '' -> Unexpected success"),
        Err(e) => println!("   GET '' -> {} (expected error)", e),
    }
    
    match client.set("", "value") {
        Ok(()) => println!("   SET '' -> Unexpected success"),
        Err(e) => println!("   SET '' -> {} (expected error)", e),
    }
    
    match client.delete("") {
        Ok(_) => println!("   DELETE '' -> Unexpected success"),
        Err(e) => println!("   DELETE '' -> {} (expected error)", e),
    }
    
    // Clean up remaining keys
    println!("\n10. Cleanup:");
    let cleanup_keys = vec!["rust", "status", "用户名", "complex_key", "large_key"];
    for key in cleanup_keys {
        match client.delete(key) {
            Ok(deleted) => {
                if deleted {
                    println!("   ✓ Cleaned up key: {}", key);
                } else {
                    println!("   - Key already gone: {}", key);
                }
            },
            Err(e) => println!("   ✗ Failed to clean up {}: {}", key, e),
        }
    }
    
    // Clean up batch keys
    for (key, _) in &keys_values {
        match client.delete(key) {
            Ok(deleted) => {
                if deleted {
                    println!("   ✓ Cleaned up key: {}", key);
                } else {
                    println!("   - Key already gone: {}", key);
                }
            },
            Err(e) => println!("   ✗ Failed to clean up {}: {}", key, e),
        }
    }
    
    println!("\n✓ Synchronous client example completed successfully!");
    println!("Server address: {}", client.server_addr());
    
    Ok(())
}
