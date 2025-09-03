use merklekv_client::{AsyncClient, Result};
use std::env;
use std::time::Duration;
use tokio::time::Instant;

/// Asynchronous MerkleKV client example
/// 
/// This example demonstrates advanced operations using the asynchronous client:
/// - Connecting with custom options
/// - Concurrent operations
/// - Batch operations (mget/mset)
/// - Error handling in async context
/// - Performance measurement

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    
    // Get server address from environment or use default
    let server_addr = env::var("MERKLEKV_SERVER")
        .unwrap_or_else(|_| "127.0.0.1:7878".to_string());
    
    println!("MerkleKV Asynchronous Client Example");
    println!("====================================");
    println!("Connecting to server: {}", server_addr);
    
    // Connect with custom options
    let mut client = AsyncClient::connect_with_options(
        &server_addr,
        10,
        Duration::from_secs(10),
    ).await?;
    
    // Basic operations
    basic_operations(&mut client).await?;
    
    // Batch operations
    batch_operations(&mut client).await?;
    
    // Concurrent operations - this needs its own client connections
    concurrent_operations(&server_addr).await?;
    
    // Performance test
    performance_test(&mut client).await?;
    
    // Unicode handling
    unicode_operations(&mut client).await?;
    
    println!("\nAll async operations completed successfully!");
    Ok(())
}

async fn basic_operations(client: &mut AsyncClient) -> Result<()> {
    println!("\n--- Basic Operations ---");
    
    // Set a value
    client.set("hello", "world").await?;
    println!("Set hello = world");
    
    // Get the value
    if let Ok(value) = client.get("hello").await {
        println!("Got hello = {}", value);
    } else {
        println!("Key 'hello' not found");
    }
    
    // Update the value
    client.set("hello", "async world").await?;
    println!("Updated hello = async world");
    
    // Get the updated value
    if let Ok(value) = client.get("hello").await {
        println!("Got updated hello = {}", value);
    }
    
    // Delete the key
    client.delete("hello").await?;
    println!("Deleted hello");
    
    // Try to get deleted key
    if let Ok(value) = client.get("hello").await {
        println!("Unexpected: Got hello = {}", value);
    } else {
        println!("Confirmed: Key 'hello' not found after deletion");
    }
    
    Ok(())
}

async fn batch_operations(client: &mut AsyncClient) -> Result<()> {
    println!("\n--- Batch Operations ---");
    
    // Set up test data
    let keys = vec!["batch1", "batch2", "batch3"];
    let values = vec!["value1", "value2", "value3"];
    
    // Set multiple values
    for (key, value) in keys.iter().zip(values.iter()) {
        client.set(key, value).await?;
    }
    println!("Set {} key-value pairs", keys.len());
    
    // Get multiple values
    let results = client.mget(keys.clone()).await;
    println!("Retrieved {} values:", results.len());
    for (key, result) in keys.iter().zip(results.iter()) {
        match result {
            Ok(value) => println!("  {} = {}", key, value),
            Err(_) => println!("  {} = <not found>", key),
        }
    }
    
    // Clean up
    for key in &keys {
        client.delete(key).await?;
    }
    println!("Cleaned up batch keys");
    
    Ok(())
}

async fn concurrent_operations(server_addr: &str) -> Result<()> {
    println!("\n--- Concurrent Operations ---");
    
    let start = Instant::now();
    
    // Spawn multiple concurrent operations
    let tasks = (0..10).map(|i| {
        let server_addr = server_addr.to_string();
        tokio::spawn(async move {
            let mut client = AsyncClient::connect(&server_addr).await?;
            let key = format!("concurrent{}", i);
            let value = format!("value{}", i);
            
            // Set a value
            client.set(&key, &value).await?;
            
            // Get it back
            let result = client.get(&key).await.ok();
            
            // Delete it
            client.delete(&key).await?;
            
            Ok::<(String, Option<String>), merklekv_client::Error>((key, result))
        })
    }).collect::<Vec<_>>();
    
    // Wait for all tasks to complete
    let mut results = Vec::new();
    for task in tasks {
        match task.await {
            Ok(Ok((key, value))) => {
                results.push((key, value));
            }
            Ok(Err(e)) => {
                eprintln!("Task error: {}", e);
                return Err(e);
            }
            Err(e) => {
                eprintln!("Join error: {}", e);
                return Err(merklekv_client::Error::Connection { 
                    message: format!("Join error: {}", e) 
                });
            }
        }
    }
    
    let duration = start.elapsed();
    println!("Completed {} concurrent operations in {:?}", results.len(), duration);
    
    // Verify results
    for (key, value) in results {
        match value {
            Some(v) => println!("  {} had value: {}", key, v),
            None => println!("  {} was not found", key),
        }
    }
    
    Ok(())
}

async fn performance_test(client: &mut AsyncClient) -> Result<()> {
    println!("\n--- Performance Test ---");
    
    let num_operations = 100;
    let keys: Vec<String> = (0..num_operations).map(|i| format!("perf{}", i)).collect();
    
    // Test SET performance
    let start = Instant::now();
    for (i, key) in keys.iter().enumerate() {
        let value = format!("performance_value_{}", i);
        client.set(key, &value).await?;
    }
    let set_duration = start.elapsed();
    
    // Test GET performance
    let start = Instant::now();
    let mut found_count = 0;
    for key in &keys {
        if client.get(key).await.is_ok() {
            found_count += 1;
        }
    }
    let get_duration = start.elapsed();
    
    // Test DELETE performance
    let start = Instant::now();
    for key in &keys {
        client.delete(key).await?;
    }
    let delete_duration = start.elapsed();
    
    println!("Performance results for {} operations:", num_operations);
    println!("  SET: {:?} ({:.2} ops/sec)", set_duration, num_operations as f64 / set_duration.as_secs_f64());
    println!("  GET: {:?} ({:.2} ops/sec, {} found)", get_duration, num_operations as f64 / get_duration.as_secs_f64(), found_count);
    println!("  DELETE: {:?} ({:.2} ops/sec)", delete_duration, num_operations as f64 / delete_duration.as_secs_f64());
    
    Ok(())
}

async fn unicode_operations(client: &mut AsyncClient) -> Result<()> {
    println!("\n--- Unicode Operations ---");
    
    // Test various Unicode strings
    let unicode_tests = vec![
        ("emoji", "🚀🌟💫"),
        ("chinese", "你好世界"),
        ("arabic", "مرحبا بالعالم"),
        ("emoji_mix", "Hello 👋 World 🌍!"),
        ("special", "Special chars: àáâãäå æç èéêë"),
    ];
    
    for (key, value) in &unicode_tests {
        // Set Unicode value
        client.set(key, value).await?;
        
        // Get it back
        if let Ok(retrieved) = client.get(key).await {
            if retrieved == *value {
                println!("✓ Unicode test '{}': {}", key, value);
            } else {
                println!("✗ Unicode test '{}' failed: expected '{}', got '{}'", key, value, retrieved);
            }
        } else {
            println!("✗ Unicode test '{}' failed: value not found", key);
        }
        // Clean up
        client.delete(key).await?;
    }
    
    Ok(())
}
