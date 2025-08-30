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
import toml
from rich.console import Console
from rich.panel import Panel

# Configure rich console for better test output
console = Console()

# Test configuration
TEST_HOST = "127.0.0.1"
TEST_PORT = 7379
TEST_STORAGE_PATH = "test_data"
SERVER_TIMEOUT = 30  #     return data


# End of conftest.pyr server startup
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
        
        # Receive response
        response = self.socket.recv(1024).decode().strip()
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


# NodeHandle class for replication testing
class NodeHandle:
    """Handle for managing a single MerkleKV node in replication tests."""
    
    def __init__(self, server: MerkleKVServer, host: str, port: int):
        self.server = server
        self.host = host
        self.port = port
    
    async def wait_ready(self, timeout: int = 30) -> None:
        """Wait for the node to be ready to accept connections."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                # Try to connect and execute a simple command
                await tcp_cmd(self.host, self.port, "GET health_check")
                return
            except (ConnectionRefusedError, OSError):
                await asyncio.sleep(0.1)
        
        raise TimeoutError(f"Node at {self.host}:{self.port} failed to become ready within {timeout} seconds")
    
    async def send(self, command: str) -> str:
        """Send a command to this node and return the response."""
        return await tcp_cmd(self.host, self.port, command)
    
    async def stop(self) -> None:
        """Stop this node."""
        if self.server:
            # Try graceful shutdown first
            try:
                await tcp_cmd(self.host, self.port, "SHUTDOWN")
                await asyncio.sleep(1)  # Give it time to shutdown gracefully
            except:
                pass  # Ignore errors during shutdown command
            
            await self.server.stop()


class ReplicationTestHarness:
    """Harness for managing multiple MerkleKV nodes in replication tests."""
    
    def __init__(self, topic_prefix: str):
        self.topic_prefix = topic_prefix
        self.nodes: List[NodeHandle] = []
        self.temp_configs: List[Path] = []
        
        # Get MQTT broker settings from environment
        self.mqtt_broker = os.environ.get("MQTT_BROKER_HOST", "test.mosquitto.org")
        self.mqtt_port = int(os.environ.get("MQTT_BROKER_PORT", "1883"))
    
    async def create_node(self, client_id: str, port: int) -> NodeHandle:
        """Create and start a new MerkleKV node with replication enabled.
        
        Args:
            client_id: Unique identifier for this node in the MQTT cluster
            port: TCP port for this node to listen on
            
        Returns:
            NodeHandle for the created node
        """
        # Create temporary storage directory for this node
        temp_dir = Path(tempfile.mkdtemp())
        storage_path = temp_dir / f"data_{client_id}"
        storage_path.mkdir(parents=True, exist_ok=True)
        
        # Create configuration for this node
        config = {
            "host": "127.0.0.1",
            "port": port,
            "storage_path": str(storage_path),
            "engine": "rwlock",
            "sync_interval_seconds": 60,
            "replication": {
                "enabled": True,
                "mqtt_broker": self.mqtt_broker,
                "mqtt_port": self.mqtt_port,
                "topic_prefix": self.topic_prefix,
                "client_id": client_id
            }
        }
        
        # Write config to temporary file
        config_file = temp_dir / f"config_{client_id}.toml"
        with open(config_file, 'w') as f:
            toml.dump(config, f)
        
        self.temp_configs.append(config_file)
        
        # Create and start the server
        server = MerkleKVServer(host="127.0.0.1", port=port, config_path=str(config_file))
        
        # Start the server asynchronously
        start_time = time.time()
        
        # Get project root directory (up from tests/integration)
        project_root = Path.cwd()
        if "tests" in str(project_root):
            project_root = project_root.parent.parent
        
        cmd = ["cargo", "run", "--quiet", "--", "--config", str(config_file)]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=project_root,
            env={**os.environ, "RUST_LOG": "info"}
        )
        
        server.process = process
        
        # Create node handle and wait for it to be ready
        node = NodeHandle(server, "127.0.0.1", port)
        await node.wait_ready()
        
        # Wait a bit for MQTT connection to establish
        await asyncio.sleep(2)
        
        self.nodes.append(node)
        return node
    
    async def shutdown(self) -> None:
        """Stop all nodes and clean up temporary files."""
        # Stop all nodes
        for node in self.nodes:
            try:
                await node.stop()
            except Exception as e:
                console.print(f"[red]Error stopping node: {e}[/red]")
        
        # Clean up temporary config files
        for config_file in self.temp_configs:
            try:
                if config_file.exists():
                    config_file.unlink()
                    # Also clean up the parent temp directory if it's empty
                    parent = config_file.parent
                    if parent.exists() and not any(parent.iterdir()):
                        parent.rmdir()
            except Exception as e:
                console.print(f"[yellow]Warning: Could not clean up {config_file}: {e}[/yellow]")
        
        self.nodes.clear()
        self.temp_configs.clear()


@pytest_asyncio.fixture
async def replication_setup():
    """Fixture providing a replication test harness.
    
    This fixture creates a ReplicationTestHarness that can be used to create
    multiple MerkleKV nodes with replication enabled for testing.
    
    Example usage:
        async def test_replication(replication_setup):
            harness = replication_setup
            node1 = await harness.create_node("node1", 7400)
            node2 = await harness.create_node("node2", 7401)
            
            # Set a value on node1
            await node1.send("SET key value")
            
            # Wait for replication and check on node2
            await asyncio.sleep(5)
            response = await node2.send("GET key")
            assert response == "VALUE value"
    """
    import uuid
    topic_prefix = f"test_merkle_kv_{uuid.uuid4().hex[:8]}"
    harness = ReplicationTestHarness(topic_prefix)
    
    try:
        yield harness
    finally:
        await harness.shutdown()


@pytest.fixture(scope="module")
def bulk_ops_server(request):
    """Module-scoped server fixture for bulk operations tests.
    
    This fixture starts a MerkleKV server on 127.0.0.1:7379
    for tests that need it. It's triggered by test modules that
    explicitly request this fixture.
    """
    # Create temporary directory and config
    temp_dir = Path(tempfile.mkdtemp())
    storage_path = temp_dir / "bulk_test_data"
    storage_path.mkdir(parents=True, exist_ok=True)
    
    config_content = f"""
host = "127.0.0.1"
port = 7379
storage_path = "{storage_path}"
engine = "rwlock"
sync_interval_seconds = 60

[replication]
enabled = false
mqtt_broker = "localhost"
mqtt_port = 1883
topic_prefix = "merkle_kv"
client_id = "bulk_test_node"
"""
    
    config_file = temp_dir / "bulk_test_config.toml"
    config_file.write_text(config_content)
    
    # Start the server
    project_root = Path.cwd()
    if "tests" in str(project_root):
        project_root = project_root.parent.parent
    
    cmd = ["cargo", "run", "--quiet", "--", "--config", str(config_file)]
    console.print(f"[blue]Starting MerkleKV server for bulk operations tests: {' '.join(cmd)}[/blue]")
    
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=project_root,
        env={**os.environ, "RUST_LOG": "info"}
    )
    
    # Wait for server to be ready
    start_time = time.time()
    server_ready = False
    
    while time.time() - start_time < SERVER_TIMEOUT:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(f"Server failed to start: {stderr.decode()}")
        
        try:
            with socket.create_connection(("127.0.0.1", 7379), timeout=1):
                server_ready = True
                break
        except (socket.timeout, ConnectionRefusedError):
            time.sleep(0.1)
    
    if not server_ready:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        raise TimeoutError("Server failed to start within timeout")
    
    console.print("[green]Bulk operations server is ready![/green]")
    
    try:
        yield process
    finally:
        # Stop server gracefully
        try:
            with socket.create_connection(("127.0.0.1", 7379), timeout=1) as sock:
                sock.send(b"SHUTDOWN\r\n")
                time.sleep(1)
        except:
            pass
        
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                console.print("[red]Force killing server...[/red]")
                process.kill()
                process.wait()
        
        # Clean up temporary files
        try:
            import shutil
            shutil.rmtree(temp_dir)
        except Exception as e:
            console.print(f"[yellow]Warning: Could not clean up temp dir: {e}[/yellow]")
        
        console.print("[red]Bulk operations server stopped[/red]")


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
    
    return data


async def tcp_cmd(host: str, port: int, line: str) -> str:
    """Execute a single command over TCP and return the response.
    
    This utility function connects to a MerkleKV server, sends a command,
    waits for the response, and then closes the connection.
    
    Args:
        host: Server hostname or IP address
        port: Server port number  
        line: Command to send (without CRLF - will be added automatically)
    
    Returns:
        Server response as a string (stripped of whitespace)
    
    Example:
        response = await tcp_cmd('127.0.0.1', 7379, 'SET key value')
    """
    reader, writer = await asyncio.open_connection(host, port)
    
    try:
        # Send command with CRLF terminator
        writer.write((line + "\r\n").encode())
        await writer.drain()
        
        # Read response (up to 1MB to handle large responses)
        data = await reader.read(1 << 20)
        return data.decode(errors="ignore").strip()
    finally:
        writer.close()
        await writer.wait_closed()


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


# Import ReplicationTestSetup
import sys
import os
sys.path.append(os.path.dirname(__file__))
from test_replication import ReplicationTestSetup


@pytest.fixture
async def replication_setup():
    """Fixture to provide a ReplicationTestSetup instance for replication tests."""
    setup = ReplicationTestSetup()
    try:
        yield setup
    finally:
        await setup.cleanup()
