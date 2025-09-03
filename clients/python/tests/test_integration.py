"""
Integration tests for MerkleKV Python client.
"""

import pytest
import subprocess
import time
import tempfile
import os
from pathlib import Path

from merklekv import MerkleKVClient, AsyncMerkleKVClient, ConnectionError


class TestIntegration:
    """Integration tests against a real MerkleKV server."""
    
    @pytest.fixture
    def server_config(self):
        """Create a temporary config file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write("""
host = "127.0.0.1"
port = 7879
storage_path = "/tmp/merkle_test_data"
engine = "rwlock"

[replication]
enabled = false
""")
            return f.name
    
    @pytest.fixture
    def server_process(self, server_config):
        """Start a MerkleKV server for testing."""
        # Find the project root (where Cargo.toml is located)
        project_root = Path(__file__).parents[3]  # Go up from clients/python/tests/
        
        # Start server process
        process = subprocess.Popen(
            ["cargo", "run", "--release", "--", "--config", server_config],
            cwd=project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Wait for server to start
        time.sleep(2)
        
        # Check if server is running
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            pytest.fail(f"Server failed to start. stdout: {stdout.decode()}, stderr: {stderr.decode()}")
        
        yield process
        
        # Cleanup
        process.terminate()
        process.wait(timeout=5)
        os.unlink(server_config)
    
    def test_sync_basic_operations(self, server_process):
        """Test basic sync operations against real server."""
        client = MerkleKVClient("127.0.0.1", 7879, timeout=10.0)
        
        try:
            client.connect()
            
            # Test SET and GET
            assert client.set("test_key", "test_value") is True
            assert client.get("test_key") == "test_value"
            
            # Test GET non-existent key
            assert client.get("nonexistent") is None
            
            # Test DELETE
            assert client.delete("test_key") is True
            assert client.get("test_key") is None
            
            # Test empty value
            assert client.set("empty_key", "") is True
            assert client.get("empty_key") == '""'  # Server quotes empty values
            
        finally:
            client.close()
    
    @pytest.mark.asyncio
    async def test_async_basic_operations(self, server_process):
        """Test basic async operations against real server."""
        client = AsyncMerkleKVClient("127.0.0.1", 7879, timeout=10.0)
        
        try:
            await client.connect()
            
            # Test SET and GET
            assert await client.set("async_key", "async_value") is True
            assert await client.get("async_key") == "async_value"
            
            # Test GET non-existent key
            assert await client.get("nonexistent_async") is None
            
            # Test DELETE
            assert await client.delete("async_key") is True
            assert await client.get("async_key") is None
            
        finally:
            await client.close()
    
    def test_sync_context_manager(self, server_process):
        """Test sync client as context manager."""
        with MerkleKVClient("127.0.0.1", 7879, timeout=10.0) as client:
            client.set("context_key", "context_value")
            assert client.get("context_key") == "context_value"
    
    @pytest.mark.asyncio
    async def test_async_context_manager(self, server_process):
        """Test async client as context manager."""
        async with AsyncMerkleKVClient("127.0.0.1", 7879, timeout=10.0) as client:
            await client.set("async_context_key", "async_context_value")
            assert await client.get("async_context_key") == "async_context_value"
    
    def test_connection_error(self):
        """Test connection error handling."""
        client = MerkleKVClient("127.0.0.1", 9999)  # Wrong port
        
        with pytest.raises(ConnectionError):
            client.connect()
    
    @pytest.mark.asyncio
    async def test_async_connection_error(self):
        """Test async connection error handling."""
        client = AsyncMerkleKVClient("127.0.0.1", 9999)  # Wrong port
        
        with pytest.raises(ConnectionError):
            await client.connect()
    
    def test_large_value(self, server_process):
        """Test handling of large values."""
        client = MerkleKVClient("127.0.0.1", 7879, timeout=10.0)
        
        try:
            client.connect()
            
            # Create a large value (10KB)
            large_value = "x" * 10240
            
            assert client.set("large_key", large_value) is True
            retrieved = client.get("large_key")
            assert retrieved == large_value
            
        finally:
            client.close()
    
    def test_unicode_handling(self, server_process):
        """Test Unicode string handling."""
        client = MerkleKVClient("127.0.0.1", 7879, timeout=10.0)
        
        try:
            client.connect()
            
            # Test with Unicode characters
            unicode_key = "测试key"
            unicode_value = "测试value 🚀"
            
            assert client.set(unicode_key, unicode_value) is True
            assert client.get(unicode_key) == unicode_value
            
        finally:
            client.close()
