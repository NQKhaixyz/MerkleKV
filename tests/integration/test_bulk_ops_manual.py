#!/usr/bin/env python3
"""
Manual test script for bulk operations in MerkleKV.

This script tests the bulk operations functionality:
- MGET - Get multiple keys in one command
- MSET - Set multiple key-value pairs
- TRUNCATE - Clear all keys/values in the store

The tests automatically start a MerkleKV server via the bulk_ops_server fixture.
"""

import pytest
import socket
import time

def connect_to_server():
    """Connect to the MerkleKV server."""
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(('127.0.0.1', 7379))
    return client

def test_mset(bulk_ops_server):
    """Test MSET command."""
    print("Testing MSET...")
    client = connect_to_server()
    
    # Set multiple key-value pairs using MSET
    client.send(b"MSET key1 value1 key2 value2 key3 value3\r\n")
    response = client.recv(1024)
    print(f"MSET response: {response}")
    
    # Verify the keys were set
    client.send(b"GET key1\r\n")
    response = client.recv(1024)
    print(f"GET key1 response: {response}")
    
    client.send(b"GET key2\r\n")
    response = client.recv(1024)
    print(f"GET key2 response: {response}")
    
    client.send(b"GET key3\r\n")
    response = client.recv(1024)
    print(f"GET key3 response: {response}")
    
    client.close()

def test_mget(bulk_ops_server):
    """Test MGET command."""
    print("\nTesting MGET...")
    client = connect_to_server()
    
    # Get multiple keys using MGET
    client.send(b"MGET key1 key2 key3\r\n")
    response = client.recv(1024)
    print(f"MGET response: {response}")
    
    client.close()

def test_truncate(bulk_ops_server):
    """Test TRUNCATE command."""
    print("\nTesting TRUNCATE...")
    client = connect_to_server()
    
    # Truncate the store
    client.send(b"TRUNCATE\r\n")
    response = client.recv(1024)
    print(f"TRUNCATE response: {response}")
    
    # Verify keys no longer exist
    client.send(b"GET key1\r\n")
    response = client.recv(1024)
    print(f"GET key1 after TRUNCATE: {response}")
    
    client.close()

def main():
    """Run all tests."""
    print("Starting manual bulk operations tests...")
    
    # Run tests
    test_mset(None)
    test_mget(None)
    test_truncate(None)
    
    print("\nAll tests completed!")

if __name__ == "__main__":
    main()
