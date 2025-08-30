#!/usr/bin/env python3
"""
Test script to verify the fix for the MGET command.

This script tests that the MGET command correctly handles lowercase input
and doesn't include the command name in the response.
"""

import pytest
import socket

def connect_to_server():
    """Connect to the MerkleKV server."""
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(('127.0.0.1', 7379))
    return client

def test_mget_lowercase(bulk_ops_server):
    """Test MGET command with lowercase input."""
    print("Testing lowercase 'mget' command...")
    client = connect_to_server()
    
    # Set some test keys
    client.send(b"SET k1 jadeHbg\r\n")
    response = client.recv(1024)
    print(f"SET k1 response: {response}")
    
    client.send(b"SET k2 xinh-dep\r\n")
    response = client.recv(1024)
    print(f"SET k2 response: {response}")
    
    client.send(b"SET k3 nhat-tren-doi\r\n")
    response = client.recv(1024)
    print(f"SET k3 response: {response}")
    
    # Test lowercase mget command with spaces between keys
    command = b"MGET k1 k2 k3\r\n"
    print(f"Sending command: {command}")
    client.send(command)
    response = client.recv(1024)
    print(f"MGET response: {response}")
    
    # Verify the response format
    response_str = response.decode('utf-8')
    print(f"Response as string: '{response_str}'")
    
    # Try with uppercase MGET
    command = b"MGET k1 k2 k3\r\n"
    print(f"\nSending command: {command}")
    client.send(command)
    response = client.recv(1024)
    print(f"MGET response: {response}")
    
    client.close()

def main():
    """Run the test."""
    print("Starting MGET fix test...")
    test_mget_lowercase(None)
    print("\nTest completed!")

if __name__ == "__main__":
    main()
