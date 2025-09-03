# MerkleKV Python Client

[![PyPI version](https://badge.fury.io/py/merklekv.svg)](https://badge.fury.io/py/merklekv)
[![Python Support](https://img.shields.io/pypi/pyversions/merklekv.svg)](https://pypi.org/project/merklekv/)

A Python client library for [MerkleKV](https://github.com/AI-Decenter/MerkleKV), a high-performance distributed key-value store with self-healing replication.

## Features

- **Synchronous and Asynchronous APIs**: Use `MerkleKVClient` for sync operations or `AsyncMerkleKVClient` for async/await
- **Simple Protocol**: Clean, text-based TCP protocol
- **Connection Management**: Built-in connection handling with timeouts
- **Context Manager Support**: Use `with` statements for automatic connection management
- **Error Handling**: Comprehensive exception types for different error conditions
- **Type Hints**: Full typing support for better IDE integration

## Installation

```bash
pip install merklekv
```

## Quick Start

### Synchronous Client

```python
from merklekv import MerkleKVClient

# Connect to MerkleKV server
client = MerkleKVClient("localhost", 7878)
client.connect()

# Set and get values
client.set("user:123", "john_doe")
value = client.get("user:123")  # Returns "john_doe"

# Delete keys
client.delete("user:123")
value = client.get("user:123")  # Returns None

# Close connection
client.close()
```

### Asynchronous Client

```python
import asyncio
from merklekv import AsyncMerkleKVClient

async def main():
    # Connect to MerkleKV server
    client = AsyncMerkleKVClient("localhost", 7878)
    await client.connect()
    
    # Set and get values
    await client.set("user:456", "jane_doe")
    value = await client.get("user:456")  # Returns "jane_doe"
    
    # Delete keys
    await client.delete("user:456")
    value = await client.get("user:456")  # Returns None
    
    # Close connection
    await client.close()

asyncio.run(main())
```

### Context Manager

```python
from merklekv import MerkleKVClient

# Automatic connection management
with MerkleKVClient("localhost", 7878) as client:
    client.set("temp:key", "temp_value")
    value = client.get("temp:key")
    print(f"Value: {value}")
# Connection automatically closed
```

### Async Context Manager

```python
import asyncio
from merklekv import AsyncMerkleKVClient

async def main():
    async with AsyncMerkleKVClient("localhost", 7878) as client:
        await client.set("temp:async", "async_value")
        value = await client.get("temp:async")
        print(f"Async value: {value}")
    # Connection automatically closed

asyncio.run(main())
```

## API Reference

### MerkleKVClient

#### Constructor

```python
MerkleKVClient(host="localhost", port=7878, timeout=5.0)
```

- `host`: Server hostname (default: "localhost")
- `port`: Server port (default: 7878)  
- `timeout`: Socket timeout in seconds (default: 5.0)

#### Methods

- `connect()`: Connect to the server
- `close()`: Close the connection
- `is_connected()`: Check if connected
- `get(key: str) -> Optional[str]`: Get value for key (returns None if not found)
- `set(key: str, value: str) -> bool`: Set key-value pair (returns True on success)
- `delete(key: str) -> bool`: Delete key (returns True on success)

### AsyncMerkleKVClient

Same API as `MerkleKVClient` but with async/await:

- `await connect()`
- `await close()`
- `await get(key: str) -> Optional[str]`
- `await set(key: str, value: str) -> bool`
- `await delete(key: str) -> bool`

## Error Handling

```python
from merklekv import MerkleKVClient, ConnectionError, TimeoutError, ProtocolError

try:
    client = MerkleKVClient("localhost", 7878, timeout=10.0)
    client.connect()
    
    client.set("test", "value")
    result = client.get("test")
    
except ConnectionError:
    print("Could not connect to server")
except TimeoutError:
    print("Operation timed out")
except ProtocolError as e:
    print(f"Server error: {e}")
finally:
    client.close()
```

## Exception Types

- `MerkleKVError`: Base exception class
- `ConnectionError`: Connection-related errors
- `TimeoutError`: Operation timeout errors
- `ProtocolError`: Server-side errors

## Requirements

- Python 3.8+
- No external dependencies for the client
- `pytest` and `pytest-asyncio` for running tests

## Testing

```bash
# Install test dependencies
pip install -e ".[test]"

# Run tests
pytest tests/
```

## Contributing

Please see the main [MerkleKV repository](https://github.com/AI-Decenter/MerkleKV) for contribution guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/AI-Decenter/MerkleKV/blob/main/LICENSE) file for details.

## Links

- [MerkleKV Main Repository](https://github.com/AI-Decenter/MerkleKV)
- [PyPI Package](https://pypi.org/project/merklekv/)
- [Documentation](https://github.com/AI-Decenter/MerkleKV#readme)
- [Issues](https://github.com/AI-Decenter/MerkleKV/issues)
