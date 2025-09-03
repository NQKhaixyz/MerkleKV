"""
Asynchronous MerkleKV client implementation.
"""

import asyncio
from typing import Optional

from .client import MerkleKVError, ConnectionError, TimeoutError, ProtocolError


class AsyncMerkleKVClient:
    """
    Asynchronous client for MerkleKV server.
    
    This client provides an async/await interface for connecting to a MerkleKV server
    and performing basic operations like GET, SET, and DELETE.
    
    Example:
        client = AsyncMerkleKVClient("localhost", 7878)
        await client.connect()
        
        await client.set("user:123", "john_doe")
        value = await client.get("user:123")  # Returns "john_doe"
        await client.delete("user:123")
        
        await client.close()
    """
    
    def __init__(self, host: str = "localhost", port: int = 7878, timeout: float = 5.0):
        """
        Initialize the async MerkleKV client.
        
        Args:
            host: Server hostname (default: "localhost")
            port: Server port (default: 7878)
            timeout: Connection timeout in seconds (default: 5.0)
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected = False
    
    async def connect(self) -> None:
        """
        Connect to the MerkleKV server.
        
        Raises:
            ConnectionError: If connection fails
        """
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=self.timeout
            )
            self._connected = True
        except (OSError, asyncio.TimeoutError) as e:
            raise ConnectionError(f"Failed to connect to {self.host}:{self.port}: {e}")
    
    async def close(self) -> None:
        """
        Close the connection to the server.
        """
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
            self._writer = None
            self._reader = None
            self._connected = False
    
    def is_connected(self) -> bool:
        """
        Check if client is connected to server.
        
        Returns:
            True if connected, False otherwise
        """
        return self._connected and self._writer is not None and not self._writer.is_closing()
    
    async def _send_command(self, command: str) -> str:
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
            self._writer.write(message)
            await self._writer.drain()
            
            # Read response (up to 64KB for large values)
            response_data = await asyncio.wait_for(
                self._reader.read(65536),
                timeout=self.timeout
            )
            response = response_data.decode('utf-8').strip()
            
            # Check for protocol errors
            if response.startswith("ERROR "):
                error_msg = response[6:]  # Remove "ERROR " prefix
                raise ProtocolError(error_msg)
            
            return response
            
        except asyncio.TimeoutError:
            raise TimeoutError(f"Operation timed out after {self.timeout} seconds")
        except (OSError, ConnectionResetError) as e:
            self._connected = False
            raise ConnectionError(f"Connection error: {e}")
    
    async def get(self, key: str) -> Optional[str]:
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
        
        response = await self._send_command(f"GET {key}")
        
        if response == "NOT_FOUND":
            return None
        elif response.startswith("VALUE "):
            return response[6:]  # Remove "VALUE " prefix
        else:
            raise ProtocolError(f"Unexpected response: {response}")
    
    async def set(self, key: str, value: str) -> bool:
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
            response = await self._send_command(f'SET {key} ""')
        else:
            response = await self._send_command(f"SET {key} {value}")
        
        if response == "OK":
            return True
        else:
            raise ProtocolError(f"Unexpected response: {response}")
    
    async def delete(self, key: str) -> bool:
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
        
        response = await self._send_command(f"DELETE {key}")
        
        if response == "OK":
            return True
        else:
            raise ProtocolError(f"Unexpected response: {response}")
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
