#!/usr/bin/env python3
"""
Test cases for MQTT-based replication functionality.

This module tests the real-time replication of write operations across
MerkleKV nodes using MQTT as the message transport.

Test Setup:
- Uses public MQTT broker: test.mosquitto.org:1883
- Creates multiple MerkleKV server instances
- Verifies that write operations on one node are replicated to others
- Tests various operations: SET, DELETE, INC, DEC, APPEND, PREPEND
"""

import asyncio
import json
import pytest
import socket
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import List, Dict, Any
import toml
import threading
import paho.mqtt.client as mqtt
import base64
import os

@pytest.fixture
def unique_topic_prefix():
    """Generate a unique topic prefix for each test to avoid interference."""
    return f"test_merkle_kv_{uuid.uuid4().hex[:8]}"

def create_simple_replication_config(port: int, node_id: str, topic_prefix: str) -> Path:
    """Create a temporary config file with replication enabled."""
    config = {
        "host": "127.0.0.1",
        "port": port,
        "storage_path": f"data_test_{node_id}",
        "engine": "rwlock",
        "sync_interval_seconds": 60,
        "replication": {
            "enabled": True,
            "mqtt_broker": "test.mosquitto.org",
            "mqtt_port": 1883,
            "topic_prefix": topic_prefix,
            "client_id": node_id
        }
    }
    
    # Create temporary config file
    temp_config = Path(f"/tmp/config_{node_id}_{port}.toml")
    with open(temp_config, 'w') as f:
        toml.dump(config, f)
    
    return temp_config

async def start_simple_server(config_path: Path, timeout: int = 30) -> subprocess.Popen:
    """Start a MerkleKV server with the given config."""
    cmd = ["cargo", "run", "--", "--config", str(config_path)]
    print(f"Starting server: {' '.join(cmd)}")
    
    # Get project root
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
    
    # Wait for server to start
    start_time = time.time()
    port = None
    
    # Extract port from config
    with open(config_path) as f:
        config_data = toml.load(f)
        port = config_data["port"]
    
    while time.time() - start_time < timeout:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(f"Server failed to start: {stderr.decode()}")
        
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                print(f"✅ Server started on port {port}")
                return process
        except (socket.timeout, ConnectionRefusedError):
            await asyncio.sleep(0.5)
    
    process.terminate()
    raise TimeoutError(f"Server failed to start within {timeout} seconds")

async def execute_simple_command(host: str, port: int, command: str) -> str:
    """Execute a command on the server."""
    reader, writer = await asyncio.open_connection(host, port)
    
    try:
        writer.write(f"{command}\r\n".encode())
        await writer.drain()
        
        data = await reader.read(1024)
        return data.decode().strip()
    finally:
        writer.close()
        await writer.wait_closed()

def cleanup_servers(*servers):
    """Clean up server processes."""
    for server in servers:
        if server:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()

async def create_replication_config(port: int, node_id: str, topic_prefix: str) -> Path:
    """Create a temporary config file with replication enabled (legacy function for compatibility)."""
    return create_simple_replication_config(port, node_id, topic_prefix)

class MQTTTestClient:
    """Test client to monitor MQTT messages."""
    
    def __init__(self, topic_prefix: str):
        self.topic_prefix = topic_prefix
        self.received_messages = []
        self.connected = threading.Event()
        
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected.set()
            topic = f"{self.topic_prefix}/events/#"
            client.subscribe(topic)
            
    def on_message(self, client, userdata, msg):
        try:
            # Try to decode as JSON first (legacy format)
            payload = json.loads(msg.payload.decode())
            self.received_messages.append({
                'topic': msg.topic,
                'payload': payload,
                'timestamp': time.time()
            })
        except json.JSONDecodeError:
            # Handle binary format (CBOR)
            self.received_messages.append({
                'topic': msg.topic,
                'payload': msg.payload,
                'timestamp': time.time(),
                'format': 'binary'
            })
        
    async def monitor_replication_messages(self, duration: float = 5.0):
        """Monitor MQTT messages for a specified duration."""
        try:
            client = mqtt.Client()
            client.on_connect = self.on_connect
            client.on_message = self.on_message
            
            client.connect("test.mosquitto.org", 1883, 60)
            client.loop_start()
            
            # Wait for connection
            if self.connected.wait(timeout=10):
                # Monitor for the specified duration
                await asyncio.sleep(duration)
            
            client.loop_stop()
            client.disconnect()
                        
        except Exception as e:
            print(f"MQTT monitoring error: {e}")

@pytest.mark.slow
@pytest.mark.asyncio
async def test_basic_replication_setup():
    """Test that replication nodes can be created and connected."""
    topic_prefix = f"test_merkle_kv_{int(time.time())}"
    
    # Create configs for two nodes
    config1 = create_simple_replication_config(7380, "node1", topic_prefix)
    config2 = create_simple_replication_config(7381, "node2", topic_prefix)
    
    server1 = None
    server2 = None
    
    try:
        # Start servers
        server1 = await start_simple_server(config1)
        server2 = await start_simple_server(config2)
        
        # Wait for MQTT connections
        await asyncio.sleep(5)
        
        # Basic connectivity test
        result = await execute_simple_command("127.0.0.1", 7380, "SET test_key test_value")
        assert result == "OK"
        
        response = await execute_simple_command("127.0.0.1", 7380, "GET test_key")
        assert response == "VALUE test_value"
        
        print("✅ Basic replication setup test passed")
        
    finally:
        cleanup_servers(server1, server2)
        # Clean up config files
        for config_file in [config1, config2]:
            if config_file.exists():
                config_file.unlink()

@pytest.mark.slow
@pytest.mark.asyncio
async def test_set_operation_replication(unique_topic_prefix):
    """Test that SET operations are replicated between nodes."""
    # Create configs for two nodes
    config1 = create_simple_replication_config(7382, "node1", unique_topic_prefix)
    config2 = create_simple_replication_config(7383, "node2", unique_topic_prefix)
    
    server1 = None
    server2 = None
    
    try:
        # Start servers
        server1 = await start_simple_server(config1)
        server2 = await start_simple_server(config2)
        
        # Wait for MQTT connections to stabilize
        await asyncio.sleep(5)
        
        # Perform SET operation on node1
        test_key = f"repl_test_{uuid.uuid4().hex[:8]}"
        test_value = "replicated_value"
        
        result = await execute_simple_command("127.0.0.1", 7382, f"SET {test_key} {test_value}")
        assert result == "OK"
        
        # Wait for replication to occur
        await asyncio.sleep(5)
        
        # Verify the value exists on node2
        result = await execute_simple_command("127.0.0.1", 7383, f"GET {test_key}")
        assert result == f"VALUE {test_value}", f"Expected VALUE {test_value}, got {result}"
        
        print(f"✅ SET replication test passed: {test_key} = {test_value}")
        
    finally:
        cleanup_servers(server1, server2)
        # Clean up config files
        for config_file in [config1, config2]:
            if config_file.exists():
                config_file.unlink()

@pytest.mark.slow
@pytest.mark.asyncio
async def test_delete_operation_replication(unique_topic_prefix):
    """Test that DELETE operations are replicated between nodes."""
    # Create configs for two nodes
    config1 = create_simple_replication_config(7384, "node1", unique_topic_prefix)
    config2 = create_simple_replication_config(7385, "node2", unique_topic_prefix)
    
    server1 = None
    server2 = None
    
    try:
        # Start servers
        server1 = await start_simple_server(config1)
        server2 = await start_simple_server(config2)
        
        # Wait for MQTT connections to stabilize
        await asyncio.sleep(5)
        
        test_key = f"delete_test_{uuid.uuid4().hex[:8]}"
        
        # Set initial value on node1
        result = await execute_simple_command("127.0.0.1", 7384, f"SET {test_key} initial_value")
        assert result == "OK"
        
        # Wait for replication
        await asyncio.sleep(5)
        
        # Verify both nodes have the value
        result1 = await execute_simple_command("127.0.0.1", 7384, f"GET {test_key}")
        result2 = await execute_simple_command("127.0.0.1", 7385, f"GET {test_key}")
        assert result1 == "VALUE initial_value"
        assert result2 == "VALUE initial_value"
        
        # Delete from node1
        result = await execute_simple_command("127.0.0.1", 7384, f"DEL {test_key}")
        assert result == "OK"
        
        # Wait for replication
        await asyncio.sleep(5)
        
        # Verify deletion on node2
        result2 = await execute_simple_command("127.0.0.1", 7385, f"GET {test_key}")
        assert result2 == "NOT_FOUND", f"Expected NOT_FOUND, got {result2}"
        
        print(f"✅ DELETE replication test passed: {test_key}")
        
    finally:
        cleanup_servers(server1, server2)
        # Clean up config files
        for config_file in [config1, config2]:
            if config_file.exists():
                config_file.unlink()

@pytest.mark.slow
@pytest.mark.asyncio
async def test_numeric_operations_replication():
    """Test that INC/DEC operations are replicated between nodes."""
    topic_prefix = f"test_merkle_kv_{int(time.time())}"
    
    # Create configs for two nodes
    config1 = create_simple_replication_config(7386, "node1", topic_prefix)
    config2 = create_simple_replication_config(7387, "node2", topic_prefix)
    
    server1 = None
    server2 = None
    
    try:
        # Start servers
        server1 = await start_simple_server(config1)
        server2 = await start_simple_server(config2)
        
        # Wait for MQTT connections to stabilize
        await asyncio.sleep(5)
        
        test_key = f"numeric_test_{uuid.uuid4().hex[:8]}"
        
        # Initialize with a numeric value
        result = await execute_simple_command("127.0.0.1", 7386, f"SET {test_key} 10")
        assert result == "OK"
        
        # Wait for replication
        await asyncio.sleep(5)
        
        # Verify initial value on both nodes
        result1 = await execute_simple_command("127.0.0.1", 7386, f"GET {test_key}")
        result2 = await execute_simple_command("127.0.0.1", 7387, f"GET {test_key}")
        assert result1 == "VALUE 10"
        assert result2 == "VALUE 10"
        
        # Increment on node1
        result = await execute_simple_command("127.0.0.1", 7386, f"INC {test_key}")
        assert result == "VALUE 11"
        
        # Wait for replication
        await asyncio.sleep(5)
        
        # Verify increment replicated to node2
        result2 = await execute_simple_command("127.0.0.1", 7387, f"GET {test_key}")
        assert result2 == "VALUE 11", f"Expected VALUE 11, got {result2}"
        
        print(f"✅ INC replication test passed: {test_key}")
        
    finally:
        cleanup_servers(server1, server2)
        # Clean up config files
        for config_file in [config1, config2]:
            if config_file.exists():
                config_file.unlink()

@pytest.mark.slow
@pytest.mark.asyncio
async def test_string_operations_replication():
    """Test that APPEND/PREPEND operations are replicated between nodes."""
    topic_prefix = f"test_merkle_kv_{int(time.time())}"
    
    # Create configs for two nodes
    config1 = create_simple_replication_config(7388, "node1", topic_prefix)
    config2 = create_simple_replication_config(7389, "node2", topic_prefix)
    
    server1 = None
    server2 = None
    
    try:
        # Start servers
        server1 = await start_simple_server(config1)
        server2 = await start_simple_server(config2)
        
        # Wait for MQTT connections to stabilize
        await asyncio.sleep(5)
        
        test_key = f"string_test_{uuid.uuid4().hex[:8]}"
        
        # Set initial value
        result = await execute_simple_command("127.0.0.1", 7388, f"SET {test_key} hello")
        assert result == "OK"
        
        # Wait for replication
        await asyncio.sleep(5)
        
        # Verify initial value on both nodes
        result1 = await execute_simple_command("127.0.0.1", 7388, f"GET {test_key}")
        result2 = await execute_simple_command("127.0.0.1", 7389, f"GET {test_key}")
        assert result1 == "VALUE hello"
        assert result2 == "VALUE hello"
        
        # Append on node1
        result = await execute_simple_command("127.0.0.1", 7388, f"APPEND {test_key} _world")
        assert "hello_world" in result
        
        # Wait for replication
        await asyncio.sleep(5)
        
        # Verify append replicated to node2
        result2 = await execute_simple_command("127.0.0.1", 7389, f"GET {test_key}")
        assert result2 == "VALUE hello_world", f"Expected VALUE hello_world, got {result2}"
        
        print(f"✅ APPEND replication test passed: {test_key}")
        
    finally:
        cleanup_servers(server1, server2)
        # Clean up config files
        for config_file in [config1, config2]:
            if config_file.exists():
                config_file.unlink()

@pytest.mark.slow
@pytest.mark.asyncio
async def test_concurrent_operations_replication():
    """Test replication behavior with concurrent operations on multiple nodes."""
    topic_prefix = f"test_merkle_kv_{int(time.time())}"
    
    # Create configs for three nodes
    config1 = create_simple_replication_config(7390, "node1", topic_prefix)
    config2 = create_simple_replication_config(7391, "node2", topic_prefix)
    config3 = create_simple_replication_config(7392, "node3", topic_prefix)
    
    server1 = None
    server2 = None
    server3 = None
    
    try:
        # Start servers
        server1 = await start_simple_server(config1)
        server2 = await start_simple_server(config2)
        server3 = await start_simple_server(config3)
        
        # Wait for MQTT connections to stabilize
        await asyncio.sleep(10)
        
        # Perform concurrent operations
        await asyncio.gather(
            execute_simple_command("127.0.0.1", 7390, "SET concurrent_test1 value1"),
            execute_simple_command("127.0.0.1", 7391, "SET concurrent_test2 value2"),
            execute_simple_command("127.0.0.1", 7392, "SET concurrent_test3 value3"),
        )
        
        # Wait for replication to settle
        await asyncio.sleep(15)
        
        # Verify all values are present on all nodes
        nodes_ports = [7390, 7391, 7392]
        for port in nodes_ports:
            result1 = await execute_simple_command("127.0.0.1", port, "GET concurrent_test1")
            result2 = await execute_simple_command("127.0.0.1", port, "GET concurrent_test2")
            result3 = await execute_simple_command("127.0.0.1", port, "GET concurrent_test3")
            
            assert result1 == "VALUE value1", f"Node {port} missing concurrent_test1: {result1}"
            assert result2 == "VALUE value2", f"Node {port} missing concurrent_test2: {result2}"
            assert result3 == "VALUE value3", f"Node {port} missing concurrent_test3: {result3}"
        
        print("✅ Concurrent operations replication test passed")
        
    finally:
        cleanup_servers(server1, server2, server3)
        # Clean up config files
        for config_file in [config1, config2, config3]:
            if config_file.exists():
                config_file.unlink()

@pytest.mark.slow
@pytest.mark.asyncio
async def test_replication_with_node_restart():
    """Test replication behavior when a node is restarted."""
    topic_prefix = f"test_merkle_kv_{int(time.time())}"
    
    # Create configs for two nodes
    config1 = create_simple_replication_config(7393, "node1", topic_prefix)
    config2 = create_simple_replication_config(7394, "node2", topic_prefix)
    
    server1 = None
    server2 = None
    server2_restarted = None
    
    try:
        # Start servers
        server1 = await start_simple_server(config1)
        server2 = await start_simple_server(config2)
        
        # Wait for MQTT connections to stabilize
        await asyncio.sleep(10)
        
        # Set some initial data
        result = await execute_simple_command("127.0.0.1", 7393, "SET restart_test1 before_restart")
        assert result == "OK"
        
        # Wait for replication
        await asyncio.sleep(8)
        
        # Verify replication
        result = await execute_simple_command("127.0.0.1", 7394, "GET restart_test1")
        assert result == "VALUE before_restart"
        
        # Stop node2
        cleanup_servers(server2)
        server2 = None
        
        # Add more data while node2 is down
        result = await execute_simple_command("127.0.0.1", 7393, "SET restart_test2 during_downtime")
        assert result == "OK"
        
        await asyncio.sleep(2)
        
        # Restart node2
        config2_restart = create_simple_replication_config(7395, "node2_restart", topic_prefix)
        server2_restarted = await start_simple_server(config2_restart)
        
        await asyncio.sleep(5)
        
        # Add data after restart
        result = await execute_simple_command("127.0.0.1", 7393, "SET restart_test3 after_restart")
        assert result == "OK"
        
        # Wait for replication
        await asyncio.sleep(5)
        
        # Verify new data is replicated to restarted node
        result = await execute_simple_command("127.0.0.1", 7395, "GET restart_test3")
        assert result == "VALUE after_restart"
        
        print("✅ Node restart replication test passed")
        
    finally:
        cleanup_servers(server1, server2, server2_restarted)
        # Clean up config files
        configs_to_clean = [config1, config2]
        if 'config2_restart' in locals():
            configs_to_clean.append(config2_restart)
        for config_file in configs_to_clean:
            if config_file.exists():
                config_file.unlink()

@pytest.mark.slow
@pytest.mark.asyncio
async def test_replication_loop_prevention(unique_topic_prefix):
    """Test that nodes don't create infinite loops by processing their own messages."""
    # Create a single node
    config1 = create_simple_replication_config(7396, "node1", unique_topic_prefix)
    
    server1 = None
    
    try:
        # Start server
        server1 = await start_simple_server(config1)
        
        # Start MQTT monitoring
        mqtt_client = MQTTTestClient(unique_topic_prefix)
        monitor_task = asyncio.create_task(
            mqtt_client.monitor_replication_messages(15.0)
        )
        
        # Wait for MQTT connections to stabilize
        await asyncio.sleep(5)
        
        # Perform multiple operations rapidly
        for i in range(5):
            result = await execute_simple_command("127.0.0.1", 7396, f"SET loop_test_{i} value_{i}")
            assert result == "OK"
            await asyncio.sleep(0.5)
        
        # Wait for all messages to be processed
        await asyncio.sleep(5)
        
        # Stop monitoring
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
        
        # Verify we don't have an excessive number of messages (indicating loops)
        # We should have roughly 5 messages, not 50+ from loops
        message_count = len(mqtt_client.received_messages)
        assert message_count <= 20, f"Too many messages detected ({message_count}), possible loop"
        
        print(f"✅ Loop prevention test passed: {message_count} messages for 5 operations")
        
    finally:
        cleanup_servers(server1)
        # Clean up config files
        if config1.exists():
            config1.unlink()

@pytest.mark.slow
@pytest.mark.asyncio
async def test_malformed_mqtt_message_handling(unique_topic_prefix):
    """Test that nodes handle malformed MQTT messages gracefully."""
    # Create a node
    config1 = create_simple_replication_config(7397, "node1", unique_topic_prefix)
    
    server1 = None
    
    try:
        # Start server
        server1 = await start_simple_server(config1)
        
        # Wait for MQTT connections to stabilize
        await asyncio.sleep(5)
        
        # Send a malformed message via MQTT
        try:
            def on_connect(client, userdata, flags, rc):
                if rc == 0:
                    topic = f"{unique_topic_prefix}/events"
                    
                    # Send invalid JSON
                    client.publish(topic, "invalid json message")
                    
                    # Send valid JSON but wrong format
                    client.publish(topic, json.dumps({"invalid": "format"}))
                    
            client = mqtt.Client()
            client.on_connect = on_connect
            client.connect("test.mosquitto.org", 1883, 60)
            client.loop_start()
            
            # Wait a bit
            await asyncio.sleep(5)
            
            client.loop_stop()
            client.disconnect()
            
            # Verify the node is still responsive
            result = await execute_simple_command("127.0.0.1", 7397, "SET test_after_malformed success")
            assert result == "OK"
            
            result = await execute_simple_command("127.0.0.1", 7397, "GET test_after_malformed")
            assert result == "VALUE success"
            
            print("✅ Malformed message handling test passed")
            
        except Exception as e:
            print(f"MQTT client error (expected in some cases): {e}")
        
    finally:
        cleanup_servers(server1)
        # Clean up config files
        if config1.exists():
            config1.unlink()

if __name__ == "__main__":
    # Run specific test
    import sys
    if len(sys.argv) > 1:
        test_name = sys.argv[1]
        pytest.main([f"-v", f"-k", test_name, __file__])
    else:
        pytest.main(["-v", __file__])
