"""
Basic usage example for MerkleKV Python client.
"""

import asyncio
from merklekv import MerkleKVClient, AsyncMerkleKVClient


def sync_example():
    """Example using the synchronous client."""
    print("=== Synchronous Client Example ===")
    
    # Create client
    client = MerkleKVClient("localhost", 7878)
    
    try:
        # Connect to server
        client.connect()
        print("Connected to MerkleKV server")
        
        # Set some key-value pairs
        client.set("user:123", "john_doe")
        client.set("user:456", "jane_smith")
        client.set("counter", "42")
        
        # Get values
        user_123 = client.get("user:123")
        user_456 = client.get("user:456")
        counter = client.get("counter")
        
        print(f"user:123 = {user_123}")
        print(f"user:456 = {user_456}")
        print(f"counter = {counter}")
        
        # Try to get non-existent key
        nonexistent = client.get("does_not_exist")
        print(f"non-existent key = {nonexistent}")
        
        # Delete a key
        client.delete("user:456")
        deleted_user = client.get("user:456")
        print(f"user:456 after delete = {deleted_user}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()
        print("Connection closed")


async def async_example():
    """Example using the asynchronous client."""
    print("\n=== Asynchronous Client Example ===")
    
    # Create async client
    client = AsyncMerkleKVClient("localhost", 7878)
    
    try:
        # Connect to server
        await client.connect()
        print("Connected to MerkleKV server (async)")
        
        # Set some key-value pairs
        await client.set("async:user:123", "alice")
        await client.set("async:user:456", "bob")
        await client.set("async:counter", "100")
        
        # Get values
        user_123 = await client.get("async:user:123")
        user_456 = await client.get("async:user:456")
        counter = await client.get("async:counter")
        
        print(f"async:user:123 = {user_123}")
        print(f"async:user:456 = {user_456}")
        print(f"async:counter = {counter}")
        
        # Try to get non-existent key
        nonexistent = await client.get("async:does_not_exist")
        print(f"non-existent async key = {nonexistent}")
        
        # Delete a key
        await client.delete("async:user:456")
        deleted_user = await client.get("async:user:456")
        print(f"async:user:456 after delete = {deleted_user}")
        
    except Exception as e:
        print(f"Async error: {e}")
    finally:
        await client.close()
        print("Async connection closed")


def context_manager_example():
    """Example using context manager."""
    print("\n=== Context Manager Example ===")
    
    try:
        # Using sync client as context manager
        with MerkleKVClient("localhost", 7878) as client:
            client.set("context:key", "context_value")
            value = client.get("context:key")
            print(f"context:key = {value}")
        
        print("Sync context manager completed")
        
    except Exception as e:
        print(f"Context manager error: {e}")


async def async_context_manager_example():
    """Example using async context manager."""
    print("\n=== Async Context Manager Example ===")
    
    try:
        # Using async client as context manager
        async with AsyncMerkleKVClient("localhost", 7878) as client:
            await client.set("async_context:key", "async_context_value")
            value = await client.get("async_context:key")
            print(f"async_context:key = {value}")
        
        print("Async context manager completed")
        
    except Exception as e:
        print(f"Async context manager error: {e}")


if __name__ == "__main__":
    # Run synchronous examples
    sync_example()
    context_manager_example()
    
    # Run asynchronous examples
    asyncio.run(async_example())
    asyncio.run(async_context_manager_example())
