# MerkleKV Go Client

[![Go Reference](https://pkg.go.dev/badge/github.com/AI-Decenter/MerkleKV/clients/go.svg)](https://pkg.go.dev/github.com/AI-Decenter/MerkleKV/clients/go)
[![Go Report Card](https://goreportcard.com/badge/github.com/AI-Decenter/MerkleKV/clients/go)](https://goreportcard.com/report/github.com/AI-Decenter/MerkleKV/clients/go)

A Go client library for [MerkleKV](https://github.com/AI-Decenter/MerkleKV), a high-performance distributed key-value store with self-healing replication.

## Features

- **Simple API**: Clean, idiomatic Go interface
- **Context Support**: Full context.Context integration for timeouts and cancellation
- **Connection Management**: Built-in connection handling with automatic error detection
- **Error Handling**: Comprehensive error types with proper error wrapping
- **Thread Safe**: Safe for concurrent use from multiple goroutines
- **Zero Dependencies**: No external dependencies except for testing

## Installation

```bash
go get github.com/AI-Decenter/MerkleKV/clients/go
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"
    
    merklekv "github.com/AI-Decenter/MerkleKV/clients/go"
)

func main() {
    // Create client
    client := merklekv.New("localhost", 7878)
    
    // Connect to server
    err := client.Connect()
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()
    
    // Set and get values
    err = client.Set("user:123", "john_doe")
    if err != nil {
        log.Fatal(err)
    }
    
    value, err := client.Get("user:123")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Value: %s\n", value) // Output: Value: john_doe
    
    // Delete keys
    err = client.Delete("user:123")
    if err != nil {
        log.Fatal(err)
    }
}
```

## API Reference

### Creating a Client

```go
// Create client with default 5-second timeout
client := merklekv.New("localhost", 7878)

// Create client with custom timeout
client := merklekv.NewWithTimeout("localhost", 7878, 10*time.Second)
```

### Connection Management

```go
// Connect to server
err := client.Connect()

// Connect with context (for timeout/cancellation)
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
err := client.ConnectWithContext(ctx)

// Check connection status
connected := client.IsConnected()

// Close connection
err := client.Close()
```

### Basic Operations

```go
// Set a key-value pair
err := client.Set("key", "value")

// Set with empty value (handled automatically)
err := client.Set("key", "")  // Empty values are represented as "" in the SET command. The client handles this automatically.

// Get a value
value, err := client.Get("key")
if err == merklekv.ErrNotFound {
    // Key doesn't exist
}

// Delete a key
err := client.Delete("key")
```

### Context-Aware Operations

```go
// Create context with timeout
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// All operations support context
err := client.SetWithContext(ctx, "key", "value")
value, err := client.GetWithContext(ctx, "key")
err := client.DeleteWithContext(ctx, "key")

// Ping for health checking
err := client.PingWithContext(ctx)
```

## Error Handling

The client provides comprehensive error handling with specific error types:

```go
value, err := client.Get("some_key")
if err != nil {
    switch {
    case errors.Is(err, merklekv.ErrNotFound):
        fmt.Println("Key not found")
    case errors.Is(err, merklekv.ErrNotConnected):
        fmt.Println("Not connected to server")
    case errors.Is(err, merklekv.ErrEmptyKey):
        fmt.Println("Key cannot be empty")
    default:
        var connErr *merklekv.ConnectionError
        var timeoutErr *merklekv.TimeoutError
        var protocolErr *merklekv.ProtocolError
        
        switch {
        case errors.As(err, &connErr):
            fmt.Printf("Connection error: %v\n", connErr)
        case errors.As(err, &timeoutErr):
            fmt.Printf("Timeout error: %v\n", timeoutErr)
        case errors.As(err, &protocolErr):
            fmt.Printf("Protocol error: %v\n", protocolErr)
        }
    }
}
```

### Error Types

- `ErrNotConnected`: Not connected to server
- `ErrEmptyKey`: Empty key provided
- `ErrNotFound`: Key not found in store
- `ConnectionError`: Connection-related errors
- `TimeoutError`: Operation timeout errors
- `ProtocolError`: Server-side protocol errors

## Advanced Usage

### Custom Timeouts

```go
// Create client with 10-second timeout
client := merklekv.NewWithTimeout("localhost", 7878, 10*time.Second)

// Or use context for per-operation timeouts
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
value, err := client.GetWithContext(ctx, "large_key")
```

### Health Checking

```go
// Simple ping
err := client.Ping()
if err != nil {
    log.Printf("Server not responding: %v", err)
}

// Ping with timeout
ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
defer cancel()
err := client.PingWithContext(ctx)
```

### Large Values

```go
// The client can handle large values (limited by available memory)
largeValue := strings.Repeat("x", 100000) // 100KB value
err := client.Set("large_key", largeValue)
if err != nil {
    log.Fatal(err)
}

retrieved, err := client.Get("large_key")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Retrieved %d bytes\n", len(retrieved))
```

### Unicode Support

```go
// Full Unicode support
err := client.Set("unicode_key", "Hello 世界 🚀")
if err != nil {
    log.Fatal(err)
}

value, err := client.Get("unicode_key")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Unicode value: %s\n", value) // Output: Unicode value: Hello 世界 🚀
```

## Requirements

- Go 1.19 or higher
- MerkleKV server running on accessible host/port

## Testing

```bash
# Run unit tests
go test

# Run tests with coverage
go test -cover

# Run integration tests (requires running MerkleKV server on localhost:7878)
go test -tags=integration

# Run example
cd examples/basic
go run main.go
```

## Benchmarks

```bash
# Run benchmarks
go test -bench=.

# Run benchmarks with memory profiling
go test -bench=. -memprofile=mem.prof
```

## Thread Safety

The client is safe for concurrent use from multiple goroutines. All operations are protected by internal synchronization.

```go
// Safe to use from multiple goroutines
var wg sync.WaitGroup
for i := 0; i < 10; i++ {
    wg.Add(1)
    go func(i int) {
        defer wg.Done()
        key := fmt.Sprintf("key-%d", i)
        value := fmt.Sprintf("value-%d", i)
        
        err := client.Set(key, value)
        if err != nil {
            log.Printf("Failed to set %s: %v", key, err)
        }
    }(i)
}
wg.Wait()
```

## Contributing

Please see the main [MerkleKV repository](https://github.com/AI-Decenter/MerkleKV) for contribution guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/AI-Decenter/MerkleKV/blob/main/LICENSE) file for details.

## Links

- [MerkleKV Main Repository](https://github.com/AI-Decenter/MerkleKV)
- [Go Package Documentation](https://pkg.go.dev/github.com/AI-Decenter/MerkleKV/clients/go)
- [Go Report Card](https://goreportcard.com/report/github.com/AI-Decenter/MerkleKV/clients/go)
- [Issues](https://github.com/AI-Decenter/MerkleKV/issues)
