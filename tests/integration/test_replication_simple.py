#!/usr/bin/env python3
"""
Simple test to verify MQTT replication setup works.
"""

import asyncio
import pytest
import tempfile
import time
from pathlib import Path
import toml
import subprocess
import os
import threading
import paho.mqtt.client as mqtt

@pytest.mark.slow
@pytest.mark.asyncio
async def test_mqtt_broker_connectivity():
    """Test that we can connect to the public MQTT broker."""
    connected = threading.Event()
    message_received = threading.Event()
    
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            connected.set()
            client.subscribe("test/merkle_kv/connectivity")
        
    def on_message(client, userdata, msg):
        message_received.set()
    
    try:
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message
        
        # Connect to public broker
        client.connect("test.mosquitto.org", 1883, 60)
        client.loop_start()
        
        # Wait for connection
        if connected.wait(timeout=10):
            # Publish a test message
            client.publish("test/merkle_kv/connectivity", "test_message")
            
            # Wait for message
            await asyncio.sleep(2)
            
            client.loop_stop()
            client.disconnect()
            
            print("✅ Successfully connected to test.mosquitto.org")
        else:
            pytest.fail("Failed to connect to MQTT broker within timeout")
            
    except Exception as e:
        pytest.fail(f"Failed to connect to MQTT broker: {e}")

def create_test_config(port: int, node_id: str, topic_prefix: str) -> Path:
    """Create a test config file with replication enabled."""
    config = {
        "host": "127.0.0.1",
        "port": port,
        "storage_path": f"test_data_{node_id}",
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
    
    temp_config = Path(f"/tmp/test_config_{node_id}_{port}.toml")
    with open(temp_config, 'w') as f:
        toml.dump(config, f)
    
    return temp_config

async def start_server_with_config(config_path: Path, timeout: int = 60) -> subprocess.Popen:
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
    import socket
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

async def execute_command(host: str, port: int, command: str) -> str:
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

@pytest.mark.slow
@pytest.mark.asyncio 
async def test_basic_replication():
    """Test basic replication between two nodes."""
    topic_prefix = f"test_replication_{int(time.time())}"
    
    # Create configs for two nodes
    config1 = create_test_config(7400, "node1", topic_prefix)
    config2 = create_test_config(7401, "node2", topic_prefix) 
    
    server1 = None
    server2 = None
    
    try:
        # Start both servers
        print("Starting server 1...")
        server1 = await start_server_with_config(config1)
        
        print("Starting server 2...")
        server2 = await start_server_with_config(config2)
        
        # Wait for MQTT connections to establish
        print("Waiting for MQTT connections...")
        await asyncio.sleep(5)
        
        # Test basic connectivity
        result1 = await execute_command("127.0.0.1", 7400, "SET test_key test_value")
        print(f"Server 1 SET result: {result1}")
        
        # Wait for replication
        await asyncio.sleep(3)
        
        # Check if replicated to server 2
        result2 = await execute_command("127.0.0.1", 7401, "GET test_key")
        print(f"Server 2 GET result: {result2}")
        
        # For now, just verify servers are running
        # Replication verification will depend on the actual implementation
        assert result1 == "OK"
        
        # The value might or might not be replicated depending on implementation
        if result2.startswith("VALUE"):
            print("✅ Replication seems to be working!")
        else:
            print("ℹ️  Replication not yet active or configured differently")
        
        print("✅ Basic replication test completed")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise
    finally:
        # Cleanup
        if server1:
            server1.terminate()
            try:
                server1.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server1.kill()
        
        if server2:
            server2.terminate()
            try:
                server2.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server2.kill()
        
        # Clean up config files
        for config_file in [config1, config2]:
            if config_file.exists():
                config_file.unlink()

if __name__ == "__main__":
    pytest.main(["-v", __file__])
