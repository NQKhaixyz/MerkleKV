# MerkleKV Node.js Client

[![npm version](https://badge.fury.io/js/%40merklekv%2Fclient.svg)](https://badge.fury.io/js/%40merklekv%2Fclient)
[![Node.js Support](https://img.shields.io/node/v/@merklekv/client.svg)](https://www.npmjs.com/package/@merklekv/client)

A Node.js/TypeScript client library for [MerkleKV](https://github.com/AI-Decenter/MerkleKV), a high-performance distributed key-value store with self-healing replication.

## Features

- **Promise-based API**: Modern async/await interface
- **TypeScript Support**: Full type definitions included
- **Connection Management**: Built-in connection handling with timeouts
- **Error Handling**: Comprehensive exception types for different error conditions
- **Stream Processing**: Efficient response parsing using Node.js streams
- **Zero Dependencies**: No external runtime dependencies

## Installation

```bash
npm install @merklekv/client
```

## Quick Start

### TypeScript

```typescript
import { MerkleKVClient } from '@merklekv/client';

async function example() {
    // Connect to MerkleKV server
    const client = new MerkleKVClient('localhost', 7878);
    await client.connect();
    
    // Set and get values
    await client.set('user:123', 'john_doe');
    const value = await client.get('user:123'); // Returns 'john_doe'
    
    // Delete keys
    await client.delete('user:123');
    const deletedValue = await client.get('user:123'); // Returns null
    
    // Close connection
    await client.close();
}
```

### JavaScript

```javascript
const { MerkleKVClient } = require('@merklekv/client');

async function example() {
    // Connect to MerkleKV server
    const client = new MerkleKVClient('localhost', 7878);
    await client.connect();
    
    // Set and get values
    await client.set('user:456', 'jane_doe');
    const value = await client.get('user:456'); // Returns 'jane_doe'
    
    // Delete keys
    await client.delete('user:456');
    const deletedValue = await client.get('user:456'); // Returns null
    
    // Close connection
    await client.close();
}
```

## API Reference

### MerkleKVClient

#### Constructor

```typescript
new MerkleKVClient(host?: string, port?: number, timeout?: number)
```

- `host`: Server hostname (default: 'localhost')
- `port`: Server port (default: 7878)
- `timeout`: Connection and operation timeout in milliseconds (default: 5000)

#### Methods

- `connect(): Promise<void>` - Connect to the server
- `close(): Promise<void>` - Close the connection
- `isConnected(): boolean` - Check if connected
- `get(key: string): Promise<string | null>` - Get value for key (returns null if not found)
- `set(key: string, value: string): Promise<boolean>` - Set key-value pair (returns true on success)
- `delete(key: string): Promise<boolean>` - Delete key (returns true on success)

## Error Handling

```typescript
import { MerkleKVClient, ConnectionError, TimeoutError, ProtocolError } from '@merklekv/client';

try {
    const client = new MerkleKVClient('localhost', 7878, 10000);
    await client.connect();
    
    await client.set('test', 'value');
    const result = await client.get('test');
    
    await client.close();
} catch (error) {
    if (error instanceof ConnectionError) {
        console.log('Could not connect to server');
    } else if (error instanceof TimeoutError) {
        console.log('Operation timed out');
    } else if (error instanceof ProtocolError) {
        console.log(`Server error: ${error.message}`);
    }
}
```

## Exception Types

- `MerkleKVError`: Base exception class
- `ConnectionError`: Connection-related errors
- `TimeoutError`: Operation timeout errors
- `ProtocolError`: Server-side errors

## Advanced Usage

### Custom Timeout

```typescript
// Set custom timeout for slow networks
const client = new MerkleKVClient('remote-server', 7878, 15000); // 15 second timeout
```

### Large Values

```typescript
// The client can handle large values (limited by available memory)
const largeValue = 'x'.repeat(100000); // 100KB value
await client.set('large_key', largeValue);
const retrieved = await client.get('large_key');
```

### Unicode Support

```typescript
// Full Unicode support
await client.set('unicode_key', 'Hello 世界 🚀');
const value = await client.get('unicode_key'); // Returns 'Hello 世界 🚀'
```

## Requirements

- Node.js 16.0.0 or higher
- TypeScript 5.0+ (for TypeScript projects)

## Testing

```bash
# Install dependencies
npm install

# Build the project
npm run build

# Run tests
npm test

# Run with coverage
npm test -- --coverage

# Run example
npm run example
```

## Building

```bash
# Compile TypeScript to JavaScript
npm run build

# The compiled files will be in the dist/ directory
```

## Contributing

Please see the main [MerkleKV repository](https://github.com/AI-Decenter/MerkleKV) for contribution guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/AI-Decenter/MerkleKV/blob/main/LICENSE) file for details.

## Links

- [MerkleKV Main Repository](https://github.com/AI-Decenter/MerkleKV)
- [npm Package](https://www.npmjs.com/package/@merklekv/client)
- [Documentation](https://github.com/AI-Decenter/MerkleKV#readme)
- [Issues](https://github.com/AI-Decenter/MerkleKV/issues)
