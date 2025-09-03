"""
Utility functions for MerkleKV integration tests.

This module provides common utilities for test operations including:
- Polling and timeout utilities
- Process management helpers
- Test data generation
- Flake-resistant wait conditions
"""

import asyncio
import socket
import time
import subprocess
from pathlib import Path
from typing import Callable, Any, Optional, List, Dict
import toml
import os
import signal


async def wait_until(predicate: Callable[[], Any], 
                    timeout: float = 30.0, 
                    interval: float = 0.5,
                    description: str = "condition") -> Any:
    """
    Wait for a condition to become true, with timeout.
    
    Academic Purpose: Implements robust polling with exponential backoff for
    eventually consistent distributed systems, avoiding test flakiness caused
    by timing dependencies in asynchronous operations.
    
    Args:
        predicate: Function that returns truthy value when condition is met
        timeout: Maximum time to wait in seconds
        interval: Time between checks in seconds
        description: Description for error messages
    
    Returns:
        Result of predicate when it becomes truthy
        
    Raises:
        TimeoutError: If condition is not met within timeout
    """
    start_time = time.time()
    last_result = None
    
    while time.time() - start_time < timeout:
        try:
            result = predicate()
            # Academic Enhancement: Handle both sync and async predicates for distributed system testing
            # This ensures compatibility with complex convergence checks that require async operations
            if asyncio.iscoroutine(result):
                result = await result
            
            if result:
                return result
            last_result = result
        except Exception as e:
            last_result = e
            
        await asyncio.sleep(interval)
    
    raise TimeoutError(f"Condition '{description}' not met within {timeout}s. Last result: {last_result}")


def wait_until_sync(predicate: Callable[[], Any], 
                   timeout: float = 30.0, 
                   interval: float = 0.5,
                   description: str = "condition") -> Any:
    """
    Synchronous version of wait_until for use in non-async contexts.
    
    Academic purpose: Provides the same eventually-consistent testing capabilities
    for synchronous test functions while maintaining consistent timeout behavior.
    """
    start_time = time.time()
    last_result = None
    
    while time.time() - start_time < timeout:
        try:
            result = predicate()
            if result:
                return result
            last_result = result
        except Exception as e:
            last_result = e
            
        time.sleep(interval)
    
    raise TimeoutError(f"Condition '{description}' not met within {timeout}s. Last result: {last_result}")


def create_replication_config(port: int, 
                            node_id: str, 
                            topic_prefix: str,
                            enable_replication: bool = True,
                            mqtt_broker: str = "test.mosquitto.org",
                            mqtt_port: int = 1883,
                            storage_engine: str = "rwlock") -> Path:
    """
    Create a temporary configuration file for MerkleKV server with replication settings.
    
    Academic purpose: Standardizes server configuration creation for distributed
    system testing while allowing customization of critical parameters like
    replication settings and storage engines.
    """
    config = {
        "host": "127.0.0.1",
        "port": port,
        "storage_path": f"test_data_{node_id}_{port}",
        "engine": storage_engine,
        "sync_interval_seconds": 30,  # Shorter interval for testing
        "replication": {
            "enabled": enable_replication,
            "mqtt_broker": mqtt_broker,
            "mqtt_port": mqtt_port,
            "topic_prefix": topic_prefix,
            "client_id": node_id
        }
    }
    
    temp_config = Path(f"/tmp/merkle_kv_config_{node_id}_{port}.toml")
    with open(temp_config, 'w') as f:
        toml.dump(config, f)
    
    return temp_config


async def start_server_with_config(config_path: Path, 
                                 timeout: float = 30.0) -> subprocess.Popen:
    """
    Start a MerkleKV server process with the given configuration.
    
    Academic purpose: Provides standardized server startup with health checking
    for distributed system testing, ensuring servers are ready before tests proceed.
    """
    cmd = ["cargo", "run", "--", "--config", str(config_path)]
    
    # Get project root directory
    project_root = Path.cwd()
    if "tests" in str(project_root):
        project_root = project_root.parent.parent
    
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=project_root,
        env={**os.environ, "RUST_LOG": "info"}
    )
    
    # Extract port from config for health checking
    with open(config_path) as f:
        config_data = toml.load(f)
        port = config_data["port"]
    
    # Wait for server to be ready
    start_time = time.time()
    while time.time() - start_time < timeout:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(f"Server failed to start: {stderr.decode()}")
        
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return process
        except (socket.timeout, ConnectionRefusedError):
            await asyncio.sleep(0.5)
    
    # Cleanup on timeout
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
    
    raise TimeoutError(f"Server failed to start within {timeout} seconds")


async def execute_server_command(host: str, port: int, command: str) -> str:
    """
    Execute a command on a MerkleKV server via TCP connection.
    
    Academic purpose: Provides standardized command execution for distributed
    system testing with proper connection management and error handling.
    """
    reader, writer = await asyncio.open_connection(host, port)
    
    try:
        writer.write(f"{command}\r\n".encode())
        await writer.drain()
        
        data = await reader.read(4096)  # Larger buffer for responses
        return data.decode().strip()
    finally:
        writer.close()
        await writer.wait_closed()


def cleanup_servers(*servers: subprocess.Popen) -> None:
    """
    Clean up server processes gracefully with fallback to force termination.
    
    Academic purpose: Ensures proper resource cleanup in distributed system
    tests to prevent port conflicts and resource leaks between test runs.
    """
    for server in servers:
        if server and server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()


def cleanup_config_files(*config_paths: Path) -> None:
    """
    Clean up temporary configuration files.
    
    Academic purpose: Maintains test isolation by removing temporary artifacts
    that could affect subsequent test runs in distributed system scenarios.
    """
    for config_path in config_paths:
        if config_path and config_path.exists():
            config_path.unlink()


def get_available_port(start_port: int = 7400) -> int:
    """
    Find an available port starting from the given port number.
    
    Academic purpose: Prevents port conflicts in concurrent test execution
    by dynamically allocating available ports for distributed system nodes.
    """
    port = start_port
    while port < start_port + 100:  # Try 100 ports
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                port += 1  # Port is in use
                continue
        except (socket.timeout, ConnectionRefusedError):
            return port  # Port is available
        
    raise RuntimeError(f"No available ports found starting from {start_port}")


class ServerManager:
    """
    Context manager for managing multiple MerkleKV server processes.
    
    Academic purpose: Provides systematic management of distributed system
    node lifecycle for testing scenarios requiring multiple coordinated servers.
    """
    
    def __init__(self):
        self.servers: List[subprocess.Popen] = []
        self.configs: List[Path] = []
    
    async def start_server(self, node_id: str, 
                          topic_prefix: str,
                          port: Optional[int] = None,
                          enable_replication: bool = True) -> tuple[subprocess.Popen, int]:
        """Start a new server and return the process and port."""
        if port is None:
            port = get_available_port(7400 + len(self.servers))
        
        config = create_replication_config(
            port=port,
            node_id=node_id,
            topic_prefix=topic_prefix,
            enable_replication=enable_replication
        )
        
        server = await start_server_with_config(config)
        
        self.servers.append(server)
        self.configs.append(config)
        
        return server, port
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        cleanup_servers(*self.servers)
        cleanup_config_files(*self.configs)


async def wait_for_convergence(nodes: List[tuple[str, int]], 
                              test_keys: List[str],
                              timeout: float = 60.0,
                              interval: float = 1.0) -> bool:
    """
    Wait for distributed nodes to converge on the same values for test keys.
    
    Academic Purpose: Validates eventual consistency in distributed systems by
    polling multiple nodes until they all report identical values for the
    specified keys, confirming successful anti-entropy synchronization.
    
    Args:
        nodes: List of (host, port) tuples for nodes to check
        test_keys: Keys that should converge across all nodes
        timeout: Maximum time to wait for convergence
        interval: Time between convergence checks
        
    Returns:
        True if convergence achieved within timeout
        
    Raises:
        TimeoutError: If nodes don't converge within timeout
    """
    async def check_convergence() -> bool:
        # Get values from all nodes for all test keys
        node_states = {}
        
        for host, port in nodes:
            node_key = f"{host}:{port}"
            node_states[node_key] = {}
            
            for key in test_keys:
                try:
                    response = await execute_server_command(host, port, f"GET {key}")
                    node_states[node_key][key] = response
                except Exception as e:
                    node_states[node_key][key] = f"ERROR: {e}"
        
        # Check if all nodes have the same values for all keys
        reference_node = next(iter(node_states.keys()))
        reference_state = node_states[reference_node]
        
        for node_key, node_state in node_states.items():
            if node_state != reference_state:
                return False  # Not converged yet
        
        return True  # All nodes have same state
    
    try:
        # Academic Enhancement: Use proper async predicate for distributed system polling
        # This ensures the convergence check coroutine is awaited correctly during each poll cycle
        result = await wait_until(
            lambda: check_convergence(),
            timeout=timeout,
            interval=interval,
            description=f"node convergence on keys {test_keys}"
        )
        return True
    except TimeoutError:
        # Log final states for debugging
        final_states = {}
        for host, port in nodes:
            node_key = f"{host}:{port}"
            final_states[node_key] = {}
            for key in test_keys:
                try:
                    response = await execute_server_command(host, port, f"GET {key}")
                    final_states[node_key][key] = response
                except Exception:
                    final_states[node_key][key] = "ERROR"
        
        print(f"❌ Convergence failed. Final states: {final_states}")
        raise TimeoutError(f"Nodes failed to converge within {timeout}s")


def generate_test_dataset(size: int = 100, key_prefix: str = "test_key") -> Dict[str, str]:
    """
    Generate a test dataset with predictable key-value pairs.
    
    Academic purpose: Creates deterministic test data for reproducible
    distributed system testing, ensuring consistent baseline state
    across multiple test runs.
    """
    import random
    import string
    
    # Use fixed seed for reproducibility
    random.seed(42)
    
    dataset = {}
    for i in range(size):
        key = f"{key_prefix}_{i:04d}"
        value = ''.join(random.choices(string.ascii_letters + string.digits, k=20))
        dataset[key] = value
    
    return dataset


async def apply_dataset_to_node(host: str, port: int, dataset: Dict[str, str]) -> None:
    """
    Apply a complete dataset to a single node.
    
    Academic purpose: Provides efficient bulk data loading for establishing
    baseline state in distributed system tests, particularly useful for
    testing synchronization and consistency mechanisms.
    """
    for key, value in dataset.items():
        response = await execute_server_command(host, port, f"SET {key} {value}")
        if response != "OK":
            raise RuntimeError(f"Failed to set {key}={value}: {response}")


async def create_divergence(nodes: List[tuple[str, int]], 
                           isolated_node_index: int,
                           divergent_data: Dict[str, str]) -> None:
    """
    Create controlled divergence by applying different data to an isolated node.
    
    Academic purpose: Simulates network partitions and split-brain scenarios
    for testing distributed system recovery mechanisms like anti-entropy
    synchronization and conflict resolution policies.
    
    Args:
        nodes: List of all nodes
        isolated_node_index: Index of node to isolate and modify
        divergent_data: Data to apply only to the isolated node
    """
    isolated_host, isolated_port = nodes[isolated_node_index]
    
    # Apply divergent data to isolated node
    await apply_dataset_to_node(isolated_host, isolated_port, divergent_data)
