"""
Pytest configuration and fixtures for MerkleKV integration tests.

This module provides:
- Test configuration and setup
- Server process management
- Client connection utilities
- Test data generation
- Cleanup procedures
"""

import asyncio
import os
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import AsyncGenerator, Generator, Optional

import pytest
import pytest_asyncio
from rich.console import Console
from rich.panel import Panel

# Configure rich console for better test output
console = Console()

# Test configuration
TEST_HOST = "127.0.0.1"
TEST_PORT = 7379
TEST_STORAGE_PATH = "test_data"
SERVER_TIMEOUT = 30  # seconds to wait for server startup
CLIENT_TIMEOUT = 5   # seconds for client operations

class MerkleKVServer:
    """Manages a MerkleKV server process for testing."""
    
    def __init__(self, host: str = TEST_HOST, port: int = TEST_PORT, 
                 storage_path: str = TEST_STORAGE_PATH, config_path: Optional[str] = None):
        self.host = host
        self.port = port
        self.storage_path = storage_path
        self.custom_config_path = config_path
        self.process: Optional[subprocess.Popen] = None
        self.config_file: Optional[Path] = None
        
    def create_config(self, temp_dir: Path) -> Path:
        """Create a temporary config file for the server."""
        config_content = f"""
host = "{self.host}"
port = {self.port}
storage_path = "{temp_dir / self.storage_path}"
engine = "rwlock"
sync_interval_seconds = 60

[replication]
enabled = false
mqtt_broker = "localhost"
mqtt_port = 1883
topic_prefix = "merkle_kv"
client_id = "test_node"
"""
        config_file = temp_dir / "test_config.toml"
        config_file.write_text(config_content)
        return config_file
    
    def start(self, temp_dir: Optional[Path] = None) -> None:
        """Start the MerkleKV server process."""
        if self.custom_config_path:
            # Use custom config file
            self.config_file = Path(self.custom_config_path)
        else:
            # Create default config file
            if temp_dir is None:
                temp_dir = Path(tempfile.mkdtemp())
            self.config_file = self.create_config(temp_dir)
        
        # Create storage directory if using default config
        if not self.custom_config_path and temp_dir:
            storage_dir = temp_dir / self.storage_path
            storage_dir.mkdir(exist_ok=True)
        
        # Start the server process
        cmd = ["cargo", "run", "--", "--config", str(self.config_file)]
        console.print(f"[blue]Starting MerkleKV server: {' '.join(cmd)}[/blue]")
        
        # Get the project root directory (two levels up from tests/integration)
        project_root = Path.cwd()
        if "tests" in str(project_root):
            # We're running from tests directory, go up to project root
            project_root = project_root.parent.parent
        
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=project_root,  # Use project root
            env={**os.environ, "RUST_LOG": "info"}
        )
        
        # Wait for server to be ready
        self._wait_for_server()

    async def start_async(self, temp_dir: Optional[Path] = None) -> None:
        """Start the MerkleKV server process asynchronously."""
        if self.custom_config_path:
            # Use custom config file
            self.config_file = Path(self.custom_config_path)
        else:
            # Create default config file
            if temp_dir is None:
                temp_dir = Path(tempfile.mkdtemp())
            self.config_file = self.create_config(temp_dir)
        
        # Create storage directory if using default config
        if not self.custom_config_path and temp_dir:
            storage_dir = temp_dir / self.storage_path
            storage_dir.mkdir(exist_ok=True)
        
        # Start the server process
        cmd = ["cargo", "run", "--", "--config", str(self.config_file)]
        console.print(f"[blue]Starting MerkleKV server: {' '.join(cmd)}[/blue]")
        
        # Get the project root directory (two levels up from tests/integration)
        project_root = Path.cwd()
        if "tests" in str(project_root):
            # We're running from tests directory, go up to project root
            project_root = project_root.parent.parent
        
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=project_root,  # Use project root
            env={**os.environ, "RUST_LOG": "info"}
        )
        
        # Wait for server to be ready asynchronously
        await self._wait_for_server_async()

    async def execute_command(self, command: str) -> str:
        """Execute a command asynchronously and return the response."""
        reader, writer = await asyncio.open_connection(self.host, self.port)
        
        try:
            # Send command
            writer.write(f"{command}\r\n".encode())
            await writer.drain()
            
            # Read response
            data = await reader.read(1024)
            response = data.decode().strip()
            
            return response
        finally:
            writer.close()
            await writer.wait_closed()

    async def is_running(self) -> bool:
        """Check if the server is running and responsive."""
        try:
            await self.execute_command("GET test_health_check")
            return True
        except:
            return False

    async def stop(self) -> None:
        """Stop the server process asynchronously."""
        if self.process:
            console.print("[red]Stopping MerkleKV server...[/red]")
            self.process.terminate()
            
            try:
                await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, self.process.wait),
                    timeout=5
                )
            except asyncio.TimeoutError:
                console.print("[red]Force killing server process...[/red]")
                self.process.kill()
                await asyncio.get_event_loop().run_in_executor(None, self.process.wait)
            
            self.process = None
        
    def _wait_for_server(self) -> None:
        """Wait for the server to be ready to accept connections."""
        console.print("[yellow]Waiting for server to start...[/yellow]")
        
        start_time = time.time()
        while time.time() - start_time < SERVER_TIMEOUT:
            # Check if process is still running
            if self.process.poll() is not None:
                # Process has exited, check for errors
                stdout, stderr = self.process.communicate()
                error_msg = f"Server process exited with code {self.process.returncode}"
                if stderr:
                    error_msg += f"\nStderr: {stderr.decode()}"
                if stdout:
                    error_msg += f"\nStdout: {stdout.decode()}"
                raise RuntimeError(error_msg)
            
            try:
                with socket.create_connection((self.host, self.port), timeout=1):
                    console.print("[green]Server is ready![/green]")
                    return
            except (socket.timeout, ConnectionRefusedError):
                time.sleep(0.1)
                continue
        
        # If we get here, server didn't start in time
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        
        raise TimeoutError(f"Server failed to start within {SERVER_TIMEOUT} seconds")

    async def _wait_for_server_async(self) -> None:
        """Wait for the server to be ready to accept connections asynchronously."""
        console.print("[yellow]Waiting for server to start...[/yellow]")
        
        start_time = time.time()
        while time.time() - start_time < SERVER_TIMEOUT:
            # Check if process is still running
            if self.process.poll() is not None:
                # Process has exited, check for errors
                stdout, stderr = self.process.communicate()
                error_msg = f"Server process exited with code {self.process.returncode}"
                if stderr:
                    error_msg += f"\nStderr: {stderr.decode()}"
                if stdout:
                    error_msg += f"\nStdout: {stdout.decode()}"
                raise RuntimeError(error_msg)
            
            try:
                reader, writer = await asyncio.open_connection(self.host, self.port)
                writer.close()
                await writer.wait_closed()
                console.print("[green]Server is ready![/green]")
                return
            except (OSError, ConnectionRefusedError):
                await asyncio.sleep(0.1)
                continue
        
        # If we get here, server didn't start in time
        if self.process.poll() is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, self.process.wait),
                    timeout=5
                )
            except asyncio.TimeoutError:
                self.process.kill()
                await asyncio.get_event_loop().run_in_executor(None, self.process.wait)
        
        raise TimeoutError(f"Server failed to start within {SERVER_TIMEOUT} seconds")
    
    def stop(self) -> None:
        """Stop the server process."""
        if self.process:
            console.print("[red]Stopping MerkleKV server...[/red]")
            self.process.terminate()
            
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                console.print("[red]Force killing server process...[/red]")
                self.process.kill()
                self.process.wait()
            
            self.process = None

class MerkleKVClient:
    """Client for interacting with MerkleKV server."""
    
    def __init__(self, host: str = TEST_HOST, port: int = TEST_PORT):
        self.host = host
        self.port = port
        self.socket: Optional[socket.socket] = None
        
    def connect(self) -> None:
        """Connect to the server."""
        self.socket = socket.create_connection((self.host, self.port), timeout=CLIENT_TIMEOUT)
        
    def disconnect(self) -> None:
        """Disconnect from the server."""
        if self.socket:
            self.socket.close()
            self.socket = None
            
    def send_command(self, command: str) -> str:
        """Send a command to the server and return the response."""
        if not self.socket:
            raise RuntimeError("Not connected to server")
            
        # Send command
        self.socket.send(f"{command}\r\n".encode())
        
        # Receive response - use larger buffer for large values
        response = self.socket.recv(32768).decode().strip()
        return response
    
    def get(self, key: str) -> str:
        """Get a value by key."""
        return self.send_command(f"GET {key}")
    
    def set(self, key: str, value: str) -> str:
        """Set a key-value pair."""
        # Handle empty values properly by quoting them
        if value == "":
            return self.send_command(f'SET {key} ""')
        else:
            return self.send_command(f"SET {key} {value}")

    def scan(self, prefix: str) -> list[str]:
        resp = self.send_command(f"SCAN {prefix}")
        lines = resp.splitlines()

        if not lines:
            return []

        header = lines[0].strip()

        if header.startswith("ERROR"):
            raise RuntimeError(header)
        if header == "NOT_FOUND":
            return []

        if not header.startswith("KEYS "):
            return [l.strip() for l in lines if l and not l.startswith("VALUES")]
        try:
            n = int(header.split()[1])
        except (IndexError, ValueError):
            raise RuntimeError(f"Bad SCAN header: {header}")

        keys = [l.strip() for l in lines[1:1 + n]]
        return keys
    
    def delete(self, key: str) -> str:
        """Delete a key."""
        return self.send_command(f"DELETE {key}")
    
    def increment(self, key: str, amount: Optional[int] = None) -> str:
        """Increment a numeric value."""
        if amount is not None:
            return self.send_command(f"INC {key} {amount}")
        else:
            return self.send_command(f"INC {key}")
    
    def decrement(self, key: str, amount: Optional[int] = None) -> str:
        """Decrement a numeric value."""
        if amount is not None:
            return self.send_command(f"DEC {key} {amount}")
        else:
            return self.send_command(f"DEC {key}")
    
    def append(self, key: str, value: str) -> str:
        """Append a value to an existing string."""
        # Handle empty values properly by quoting them
        if value == "":
            return self.send_command(f'APPEND {key} ""')
        else:
            return self.send_command(f"APPEND {key} {value}")
    
    def prepend(self, key: str, value: str) -> str:
        """Prepend a value to an existing string."""
        # Handle empty values properly by quoting them
        if value == "":
            return self.send_command(f'PREPEND {key} ""')
        else:
            return self.send_command(f"PREPEND {key} {value}")
    

@pytest.fixture(scope="session")
def temp_test_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test data."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)

@pytest.fixture
def server(temp_test_dir: Path) -> Generator[MerkleKVServer, None, None]:
    """Provide a running MerkleKV server for tests."""
    server = MerkleKVServer()
    
    try:
        server.start(temp_test_dir)
        yield server
    finally:
        server.stop()

@pytest.fixture
def client() -> Generator[MerkleKVClient, None, None]:
    """Provide a connected client for tests."""
    client = MerkleKVClient()
    
    try:
        client.connect()
        yield client
    finally:
        client.disconnect()

@pytest.fixture
def connected_client(server: MerkleKVServer) -> Generator[MerkleKVClient, None, None]:
    """Provide a client connected to a running server."""
    client = MerkleKVClient()
    
    try:
        client.connect()
        yield client
    finally:
        client.disconnect()

def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "benchmark: marks tests as benchmark tests"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )

def pytest_collection_modifyitems(config, items):
    """Add markers to test items based on their names."""
    for item in items:
        if "benchmark" in item.name:
            item.add_marker(pytest.mark.benchmark)
        if "integration" in item.name:
            item.add_marker(pytest.mark.integration)

@pytest.fixture(autouse=True)
def setup_logging():
    """Setup logging for tests."""
    # Set environment variables for logging
    os.environ["RUST_LOG"] = "info"
    yield

def generate_test_data(size: int = 100) -> dict[str, str]:
    """Generate test key-value pairs."""
    import random
    import string
    
    data = {}
    for i in range(size):
        key = f"test_key_{i}"
        value = ''.join(random.choices(string.ascii_letters + string.digits, k=20))
        data[key] = value


def connect_to_server(host: str = TEST_HOST, port: int = TEST_PORT):
    """Connect to the MerkleKV server and return a socket."""
    sock = socket.create_connection((host, port), timeout=CLIENT_TIMEOUT)
    return sock


def send_command(client, command: str) -> str:
    """Send a command to the server and return the response.
    
    This is a helper function for tests that use raw sockets instead of the MerkleKVClient class.
    """
    # Send command
    client.send(f"{command}\r\n".encode())
    
    # Receive response
    response = client.recv(1024).decode().strip()
    return response


# Note: ReplicationTestSetup has been moved to use simple functions in test_replication.py
# The replication_setup fixture is no longer needed
