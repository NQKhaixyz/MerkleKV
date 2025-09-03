"""
Synchronous MerkleKV client implementation.
"""

import socket
import time
from typing import Optional


class MerkleKVError(Exception):
    """Base exception for MerkleKV client errors."""
    pass


class ConnectionError(MerkleKVError):
    """Raised when connection to server fails."""
    pass


class TimeoutError(MerkleKVError):
    """Raised when operation times out."""
    pass


class ProtocolError(MerkleKVError):
    """Raised when server returns an error."""
    pass


class MerkleKVClient:
    """
    Synchronous client for MerkleKV server.
    
    This client provides a simple interface for connecting to a MerkleKV server
    and performing basic operations like GET, SET, and DELETE.
    
    Example:
        client = MerkleKVClient("localhost", 7878)
        client.connect()
        
        client.set("user:123", "john_doe")
        value = client.get("user:123")  # Returns "john_doe"
        client.delete("user:123")
        
        client.close()
    """
    
    def __init__(self, host: str = "localhost", port: int = 7878, timeout: float = 5.0):
        """
        Initialize the MerkleKV client.
        
        Args:
            host: Server hostname (default: "localhost")
            port: Server port (default: 7878)
            timeout: Socket timeout in seconds (default: 5.0)
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self._socket: Optional[socket.socket] = None
        self._connected = False
    
    def connect(self) -> None:
        """
        Connect to the MerkleKV server.
        
        Raises:
            ConnectionError: If connection fails
        """
        try:
            self._socket = socket.create_connection((self.host, self.port), timeout=self.timeout)
            self._connected = True
        except (socket.error, socket.timeout) as e:
            raise ConnectionError(f"Failed to connect to {self.host}:{self.port}: {e}")
    
    def close(self) -> None:
        """
        Close the connection to the server.
        """
        if self._socket:
            self._socket.close()
            self._socket = None
            self._connected = False
    
    def is_connected(self) -> bool:
        """
        Check if client is connected to server.
        
        Returns:
            True if connected, False otherwise
        """
        return self._connected and self._socket is not None
    
    def _send_command(self, command: str) -> str:
        """
        Send a command to the server and return the response.
        
        Args:
            command: Command string to send
            
        Returns:
            Server response string
            
        Raises:
            ConnectionError: If not connected or connection fails
            TimeoutError: If operation times out
            ProtocolError: If server returns an error
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to server. Call connect() first.")
        
        try:
            # Send command with CRLF termination
            message = f"{command}\r\n".encode('utf-8')
            self._socket.send(message)
            
            # Read response (up to 64KB for large values)
            response = self._socket.recv(65536).decode('utf-8').strip()
            
            # Check for protocol errors
            if response.startswith("ERROR "):
                error_msg = response[6:]  # Remove "ERROR " prefix
                raise ProtocolError(error_msg)
            
            return response
            
        except socket.timeout:
            raise TimeoutError(f"Operation timed out after {self.timeout} seconds")
        except socket.error as e:
            self._connected = False
            raise ConnectionError(f"Socket error: {e}")
    
    def get(self, key: str) -> Optional[str]:
        """
        Get the value for a key.
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value if key exists, None if key doesn't exist
            
        Raises:
            ConnectionError: If not connected or connection fails
            TimeoutError: If operation times out
            ProtocolError: If server returns an error
        """
        if not key:
            raise ValueError("Key cannot be empty")
        
        response = self._send_command(f"GET {key}")
        
        if response == "NOT_FOUND":
            return None
        elif response.startswith("VALUE "):
            return response[6:]  # Remove "VALUE " prefix
        else:
            raise ProtocolError(f"Unexpected response: {response}")
    
    def set(self, key: str, value: str) -> bool:
        """
        Set a key-value pair.
        
        Args:
            key: The key to set
            value: The value to associate with the key
            
        Returns:
            True if successful
            
        Raises:
            ConnectionError: If not connected or connection fails  
            TimeoutError: If operation times out
            ProtocolError: If server returns an error
        """
        if not key:
            raise ValueError("Key cannot be empty")
        
        # Handle empty values by quoting them
        if value == "":
            response = self._send_command(f'SET {key} ""')
        else:
            response = self._send_command(f"SET {key} {value}")
        
        if response == "OK":
            return True
        else:
            raise ProtocolError(f"Unexpected response: {response}")
    
    def delete(self, key: str) -> bool:
        """
        Delete a key.
        
        Args:
            key: The key to delete
            
        Returns:
            True if successful (whether key existed or not)
            
        Raises:
            ConnectionError: If not connected or connection fails
            TimeoutError: If operation times out  
            ProtocolError: If server returns an error
        """
        if not key:
            raise ValueError("Key cannot be empty")
        
        response = self._send_command(f"DELETE {key}")
        
        if response == "OK":
            return True
        else:
            raise ProtocolError(f"Unexpected response: {response}")
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
