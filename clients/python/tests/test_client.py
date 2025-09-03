"""
Unit tests for MerkleKV Python client.
"""

import pytest
import socket
from unittest.mock import Mock, patch

from merklekv import MerkleKVClient, MerkleKVError, ConnectionError, TimeoutError, ProtocolError


class TestMerkleKVClient:
    """Test the synchronous MerkleKV client."""
    
    def test_init_default_values(self):
        """Test client initialization with default values."""
        client = MerkleKVClient()
        assert client.host == "localhost"
        assert client.port == 7878
        assert client.timeout == 5.0
        assert not client.is_connected()
    
    def test_init_custom_values(self):
        """Test client initialization with custom values."""
        client = MerkleKVClient("example.com", 9999, 10.0)
        assert client.host == "example.com"
        assert client.port == 9999
        assert client.timeout == 10.0
        assert not client.is_connected()
    
    @patch('socket.create_connection')
    def test_connect_success(self, mock_create_connection):
        """Test successful connection."""
        mock_socket = Mock()
        mock_create_connection.return_value = mock_socket
        
        client = MerkleKVClient()
        client.connect()
        
        assert client.is_connected()
        mock_create_connection.assert_called_once_with(("localhost", 7878), timeout=5.0)
    
    @patch('socket.create_connection')
    def test_connect_failure(self, mock_create_connection):
        """Test connection failure."""
        mock_create_connection.side_effect = socket.error("Connection refused")
        
        client = MerkleKVClient()
        
        with pytest.raises(ConnectionError, match="Failed to connect"):
            client.connect()
        
        assert not client.is_connected()
    
    def test_close(self):
        """Test closing connection."""
        client = MerkleKVClient()
        mock_socket = Mock()
        client._socket = mock_socket
        client._connected = True
        
        client.close()
        
        mock_socket.close.assert_called_once()
        assert not client.is_connected()
    
    def test_send_command_not_connected(self):
        """Test sending command when not connected."""
        client = MerkleKVClient()
        
        with pytest.raises(ConnectionError, match="Not connected"):
            client._send_command("GET test")
    
    @patch('socket.create_connection')
    def test_get_success(self, mock_create_connection):
        """Test successful GET operation."""
        mock_socket = Mock()
        mock_socket.recv.return_value = b"VALUE test_value\r\n"
        mock_create_connection.return_value = mock_socket
        
        client = MerkleKVClient()
        client.connect()
        
        result = client.get("test_key")
        
        assert result == "test_value"
        mock_socket.send.assert_called_with(b"GET test_key\r\n")
    
    @patch('socket.create_connection')
    def test_get_not_found(self, mock_create_connection):
        """Test GET operation for non-existent key."""
        mock_socket = Mock()
        mock_socket.recv.return_value = b"NOT_FOUND\r\n"
        mock_create_connection.return_value = mock_socket
        
        client = MerkleKVClient()
        client.connect()
        
        result = client.get("nonexistent_key")
        
        assert result is None
    
    @patch('socket.create_connection')
    def test_get_error_response(self, mock_create_connection):
        """Test GET operation with server error."""
        mock_socket = Mock()
        mock_socket.recv.return_value = b"ERROR Invalid key\r\n"
        mock_create_connection.return_value = mock_socket
        
        client = MerkleKVClient()
        client.connect()
        
        with pytest.raises(ProtocolError, match="Invalid key"):
            client.get("invalid_key")
    
    def test_get_empty_key(self):
        """Test GET with empty key."""
        client = MerkleKVClient()
        
        with pytest.raises(ValueError, match="Key cannot be empty"):
            client.get("")
    
    @patch('socket.create_connection')
    def test_set_success(self, mock_create_connection):
        """Test successful SET operation."""
        mock_socket = Mock()
        mock_socket.recv.return_value = b"OK\r\n"
        mock_create_connection.return_value = mock_socket
        
        client = MerkleKVClient()
        client.connect()
        
        result = client.set("test_key", "test_value")
        
        assert result is True
        mock_socket.send.assert_called_with(b"SET test_key test_value\r\n")
    
    @patch('socket.create_connection')
    def test_set_empty_value(self, mock_create_connection):
        """Test SET operation with empty value."""
        mock_socket = Mock()
        mock_socket.recv.return_value = b"OK\r\n"
        mock_create_connection.return_value = mock_socket
        
        client = MerkleKVClient()
        client.connect()
        
        result = client.set("test_key", "")
        
        assert result is True
        mock_socket.send.assert_called_with(b'SET test_key ""\r\n')
    
    def test_set_empty_key(self):
        """Test SET with empty key."""
        client = MerkleKVClient()
        
        with pytest.raises(ValueError, match="Key cannot be empty"):
            client.set("", "value")
    
    @patch('socket.create_connection')
    def test_delete_success(self, mock_create_connection):
        """Test successful DELETE operation."""
        mock_socket = Mock()
        mock_socket.recv.return_value = b"OK\r\n"
        mock_create_connection.return_value = mock_socket
        
        client = MerkleKVClient()
        client.connect()
        
        result = client.delete("test_key")
        
        assert result is True
        mock_socket.send.assert_called_with(b"DELETE test_key\r\n")
    
    def test_delete_empty_key(self):
        """Test DELETE with empty key."""
        client = MerkleKVClient()
        
        with pytest.raises(ValueError, match="Key cannot be empty"):
            client.delete("")
    
    @patch('socket.create_connection')
    def test_socket_timeout(self, mock_create_connection):
        """Test socket timeout handling."""
        mock_socket = Mock()
        mock_socket.recv.side_effect = socket.timeout()
        mock_create_connection.return_value = mock_socket
        
        client = MerkleKVClient()
        client.connect()
        
        with pytest.raises(TimeoutError, match="timed out"):
            client.get("test_key")
    
    @patch('socket.create_connection')
    def test_context_manager(self, mock_create_connection):
        """Test using client as context manager."""
        mock_socket = Mock()
        mock_create_connection.return_value = mock_socket
        
        with MerkleKVClient() as client:
            assert client.is_connected()
        
        mock_socket.close.assert_called_once()
    
    @patch('socket.create_connection')
    def test_unexpected_response(self, mock_create_connection):
        """Test handling of unexpected response."""
        mock_socket = Mock()
        mock_socket.recv.return_value = b"UNKNOWN_RESPONSE\r\n"
        mock_create_connection.return_value = mock_socket
        
        client = MerkleKVClient()
        client.connect()
        
        with pytest.raises(ProtocolError, match="Unexpected response"):
            client.get("test_key")
