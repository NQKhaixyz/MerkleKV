# MerkleKV Client Libraries

Official client libraries for [MerkleKV](https://github.com/AI-Decenter/MerkleKV), a high-performance distributed key-value store with self-healing replication.

## Available Clients

### Phase 1 (Implemented)

| Language | Package | Status | Documentation |
|----------|---------|--------|---------------|
| **Python** | `merklekv` | ✅ Ready | [README](python/README.md) |
| **Node.js** | `@merklekv/client` | ✅ Ready | [README](nodejs/README.md) |
| **Go** | `github.com/AI-Decenter/MerkleKV/clients/go` | ✅ Ready | [README](go/README.md) |

### Phase 2+ (Planned)

| Language | Package | Status | Priority |
|----------|---------|--------|----------|
| **Java** | `io.merklekv:client` | 📋 Planned | High |
| **Rust** | `merklekv` | 📋 Planned | High |
| **C#/.NET** | `MerkleKV.Client` | 📋 Planned | Medium |
| **C++** | `libmerklekv` | 📋 Planned | Medium |
| **Ruby** | `merklekv-rb` | 📋 Planned | Medium |
| **PHP** | `merklekv/client` | 📋 Planned | Medium |

## Protocol Overview

All clients implement the MerkleKV TCP text protocol:

- **Connection**: TCP socket to MerkleKV server (default port 7878)
- **Commands**: Text-based with CRLF (`\r\n`) termination
- **Core Operations**: `GET key`, `SET key value`, `DELETE key`
- **Responses**: `VALUE data`, `OK`, `NOT_FOUND`, `ERROR message`

## Installation & Usage

### Python

```bash
pip install merklekv
```

```python
from merklekv import MerkleKVClient

client = MerkleKVClient("localhost", 7878)
client.connect()

client.set("user:123", "john_doe")
value = client.get("user:123")  # Returns "john_doe"
client.delete("user:123")

client.close()
```

### Node.js

```bash
npm install @merklekv/client
```

```javascript
const { MerkleKVClient } = require('@merklekv/client');

const client = new MerkleKVClient('localhost', 7878);
await client.connect();

await client.set('user:123', 'john_doe');
const value = await client.get('user:123'); // Returns 'john_doe'
await client.delete('user:123');

await client.close();
```

### Go

```bash
go get github.com/AI-Decenter/MerkleKV/clients/go
```

```go
import merklekv "github.com/AI-Decenter/MerkleKV/clients/go"

client := merklekv.New("localhost", 7878)
err := client.Connect()
// Handle error...

err = client.Set("user:123", "john_doe")
value, err := client.Get("user:123") // Returns "john_doe"
err = client.Delete("user:123")

client.Close()
```

## Common Features

All client libraries provide:

### Core Operations
- **Connect/Disconnect**: Manage connections to MerkleKV server
- **GET**: Retrieve values by key
- **SET**: Store key-value pairs
- **DELETE**: Remove keys

### Error Handling
- **Connection Errors**: Network and connection failures
- **Timeout Errors**: Operation timeouts
- **Protocol Errors**: Server-side errors
- **Validation Errors**: Invalid inputs (empty keys, etc.)

### Advanced Features
- **Timeouts**: Configurable operation timeouts
- **Context Support**: Cancellation and deadline handling (where applicable)
- **Unicode Support**: Full UTF-8 string handling
- **Large Values**: Support for values up to available memory
- **Thread Safety**: Safe concurrent usage (language-dependent)

## API Design Consistency

All clients follow consistent naming and behavior patterns:

### Synchronous APIs
```
connect() -> void/error
get(key) -> value/null + error
set(key, value) -> success + error
delete(key) -> success + error
close() -> void/error
```

### Asynchronous APIs (Python asyncio, Node.js Promises, Go Context)
```
await/async versions of all operations
Context-aware operations with cancellation
Timeout handling via context or configuration
```

### Error Handling
```
ConnectionError: Network/connection issues
TimeoutError: Operation timeouts
ProtocolError: Server errors
ValidationError: Input validation failures
NotFoundError: Key not found (language-dependent)
```

## Testing

Each client library includes:

- **Unit Tests**: Mock socket/network operations
- **Integration Tests**: Against live MerkleKV server
- **Benchmarks**: Performance verification (<5ms local latency target)
- **Examples**: Complete usage demonstrations

### Running Integration Tests

1. **Start MerkleKV server**:
   ```bash
   cd /path/to/MerkleKV
   cargo run --release
   ```

2. **Run client tests**:
   ```bash
   # Python
   cd clients/python
   pip install -e ".[test]"
   pytest tests/

   # Node.js
   cd clients/nodejs
   npm install
   npm test

   # Go
   cd clients/go
   go test -tags=integration
   ```

## Performance Targets

All clients are designed to meet these performance benchmarks:

- **Latency**: <5ms for local connections (127.0.0.1)
- **Throughput**: >10,000 operations/second for simple operations
- **Memory**: Minimal overhead per connection
- **Concurrency**: Support multiple simultaneous operations

## Development Guidelines

When implementing new client libraries:

### Code Structure
```
clients/<language>/
├── src/                 # Main implementation
├── tests/              # Unit and integration tests
├── examples/           # Usage examples
├── docs/               # Documentation
├── README.md           # Language-specific documentation
└── <package-files>     # Package management files
```

### API Requirements
- Implement all core operations (connect, get, set, delete, close)
- Provide both sync and async APIs where language supports it
- Follow language-specific naming conventions
- Include comprehensive error handling
- Support configurable timeouts
- Handle Unicode strings properly
- Validate inputs (empty keys, etc.)

### Testing Requirements
- Unit tests with >90% code coverage
- Integration tests against live server
- Performance benchmarks
- Error condition testing
- Large value handling tests
- Unicode/encoding tests

### Documentation Requirements
- Complete API documentation
- Installation instructions
- Basic usage examples
- Advanced usage patterns
- Error handling examples
- Performance characteristics

## Contributing

1. Follow the established patterns from Phase 1 clients
2. Maintain API consistency across languages
3. Include comprehensive tests and documentation
4. Verify integration with MerkleKV server
5. Submit PR with examples and benchmarks

See the main [MerkleKV repository](https://github.com/AI-Decenter/MerkleKV) for detailed contribution guidelines.

## License

All client libraries are licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.

## Support

- **Documentation**: Language-specific READMEs in each client directory
- **Issues**: [GitHub Issues](https://github.com/AI-Decenter/MerkleKV/issues)
- **Discussions**: [GitHub Discussions](https://github.com/AI-Decenter/MerkleKV/discussions)
- **Examples**: Complete examples in each `examples/` directory
