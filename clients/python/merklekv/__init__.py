"""
MerkleKV Python Client Library

A Python client for connecting to and interacting with MerkleKV servers.
Provides both synchronous and asynchronous APIs.
"""

from .client import MerkleKVClient, MerkleKVError, ConnectionError, TimeoutError, ProtocolError
from .async_client import AsyncMerkleKVClient

__version__ = "1.0.0"
__all__ = ["MerkleKVClient", "AsyncMerkleKVClient", "MerkleKVError", "ConnectionError", "TimeoutError", "ProtocolError"]
