# Java MerkleKV Client

A Java client library for interacting with MerkleKV, a distributed key-value store. This library provides both synchronous and asynchronous APIs for optimal performance and flexibility.

## Features

- **Synchronous API**: Simple, blocking operations for straightforward use cases
- **Asynchronous API**: Non-blocking operations using CompletableFuture for high-performance applications
- **Connection Pooling**: Automatic connection management for async operations
- **Unicode Support**: Full Unicode string support for keys and values
- **Error Handling**: Comprehensive exception hierarchy for different error conditions
- **Thread Safety**: Safe for concurrent use across multiple threads
- **Timeout Support**: Configurable timeouts for all operations

## Installation

### Maven

Add this dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>io.merklekv</groupId>
    <artifactId>merklekv-client-java</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Gradle

Add this dependency to your `build.gradle`:

```gradle
implementation 'io.merklekv:merklekv-client-java:1.0.0'
```

## Quick Start

### Synchronous Usage

```java
import io.merklekv.client.MerkleKVClient;
import io.merklekv.client.MerkleKVException;

try (MerkleKVClient client = new MerkleKVClient("localhost", 7878)) {
    // Set a key-value pair
    client.set("user:123", "Alice");
    
    // Get a value
    String value = client.get("user:123");
    System.out.println(value); // "Alice"
    
    // Delete a key
    boolean deleted = client.delete("user:123");
    System.out.println(deleted); // true
    
} catch (MerkleKVException e) {
    System.err.println("Error: " + e.getMessage());
}
```

### Asynchronous Usage

```java
import io.merklekv.client.AsyncMerkleKVClient;
import io.merklekv.client.MerkleKVException;

try (AsyncMerkleKVClient client = new AsyncMerkleKVClient("localhost", 7878)) {
    // Chain async operations
    client.setAsync("user:456", "Bob")
        .thenCompose(v -> client.getAsync("user:456"))
        .thenCompose(value -> {
            System.out.println("Retrieved: " + value);
            return client.deleteAsync("user:456");
        })
        .thenAccept(deleted -> System.out.println("Deleted: " + deleted))
        .get(); // Block for completion
        
} catch (Exception e) {
    System.err.println("Error: " + e.getMessage());
}
```

## API Reference

### MerkleKVClient (Synchronous)

#### Constructor
```java
MerkleKVClient(String host, int port)
MerkleKVClient(String host, int port, int timeoutMs)
```

#### Methods
- `String get(String key)` - Get value by key
- `void set(String key, String value)` - Set key-value pair
- `boolean delete(String key)` - Delete key, returns true if existed
- `boolean isConnected()` - Check connection status
- `void close()` - Close connection

### AsyncMerkleKVClient (Asynchronous)

#### Constructor
```java
AsyncMerkleKVClient(String host, int port)
AsyncMerkleKVClient(String host, int port, int timeoutMs, int maxConnections)
```

#### Methods
- `CompletableFuture<String> getAsync(String key)` - Get value by key
- `CompletableFuture<Void> setAsync(String key, String value)` - Set key-value pair
- `CompletableFuture<Boolean> deleteAsync(String key)` - Delete key
- `boolean isActive()` - Check if client is active
- `void close()` - Close client and all connections

## Exception Handling

The client uses a hierarchy of exceptions:

- `MerkleKVException` - Base exception for all client errors
- `ConnectionException` - Network connection errors
- `TimeoutException` - Operation timeout errors
- `ProtocolException` - Server protocol errors
- `KeyNotFoundException` - Key not found errors

```java
try {
    String value = client.get("nonexistent");
} catch (KeyNotFoundException e) {
    System.out.println("Key not found: " + e.getMessage());
} catch (ConnectionException e) {
    System.out.println("Connection error: " + e.getMessage());
} catch (MerkleKVException e) {
    System.out.println("Other error: " + e.getMessage());
}
```

## Advanced Usage

### Custom Timeouts

```java
// 10 second timeout
MerkleKVClient client = new MerkleKVClient("localhost", 7878, 10000);

// Async client with custom connection pool
AsyncMerkleKVClient asyncClient = new AsyncMerkleKVClient("localhost", 7878, 15000, 20);
```

### Parallel Operations

```java
try (AsyncMerkleKVClient client = new AsyncMerkleKVClient("localhost", 7878)) {
    // Run multiple operations in parallel
    CompletableFuture<Void> set1 = client.setAsync("key1", "value1");
    CompletableFuture<Void> set2 = client.setAsync("key2", "value2");
    CompletableFuture<Void> set3 = client.setAsync("key3", "value3");
    
    // Wait for all to complete
    CompletableFuture.allOf(set1, set2, set3).get();
}
```

### Error Handling with CompletableFuture

```java
client.getAsync("key")
    .handle((result, throwable) -> {
        if (throwable != null) {
            System.err.println("Operation failed: " + throwable.getMessage());
            return "default_value";
        }
        return result;
    })
    .thenAccept(value -> System.out.println("Value: " + value));
```

## Performance Considerations

- Use the async client for high-throughput applications
- The async client maintains a connection pool for optimal performance
- Both clients are thread-safe
- Consider batching operations when possible
- Monitor connection pool size in high-load scenarios

## Thread Safety

Both `MerkleKVClient` and `AsyncMerkleKVClient` are thread-safe and can be safely used from multiple threads concurrently. The async client uses an internal connection pool to handle concurrent operations efficiently.

## Unicode Support

The client fully supports Unicode strings for both keys and values:

```java
client.set("用户:123", "张三");
String name = client.get("用户:123"); // "张三"

client.set("emoji", "🚀🌟⭐");
String emoji = client.get("emoji"); // "🚀🌟⭐"
```

## Protocol Compliance

This client implements the MerkleKV TCP protocol:
- Commands are sent as UTF-8 text terminated with CRLF (`\r\n`)
- Responses follow the format: `OK`, `NOT_FOUND`, `VALUE <data>`, or `ERROR <message>`
- Default port is 7878

## Building from Source

```bash
# Clone the repository
git clone https://github.com/your-org/merkle-kv.git
cd merkle-kv/clients/java

# Build with Maven
mvn clean compile

# Run tests
mvn test

# Create JAR
mvn package
```

## Examples

See the `examples/` directory for complete usage examples:
- `SyncExample.java` - Synchronous client usage
- `AsyncExample.java` - Asynchronous client usage

## Requirements

- Java 8 or higher
- MerkleKV server running on accessible host/port

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Support

For questions, issues, or contributions, please visit the [GitHub repository](https://github.com/your-org/merkle-kv) or open an issue.
