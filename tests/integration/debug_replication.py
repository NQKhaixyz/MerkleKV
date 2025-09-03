#!/usr/bin/env python3
"""
Debug MQTT replication by checking actual data on nodes
"""

import asyncio
import os
import subprocess
import tempfile
import time
from pathlib import Path
import toml

MQTT_BROKER = "test.mosquitto.org"
MQTT_PORT = 1883
BASE_PORT = 7450

class DebugHarness:
    def __init__(self):
        self.test_id = f"debug_{int(time.time())}"
        self.nodes = {}
        self.configs = {}
        self.topic_prefix = f"debug_test_{self.test_id}"
        self.temp_dirs = []
        
    def create_node_config(self, node_id: str, port: int) -> Path:
        temp_dir = Path(tempfile.mkdtemp(prefix=f"debug_{node_id}_"))
        self.temp_dirs.append(temp_dir)
        
        config = {
            "host": "127.0.0.1", 
            "port": port,
            "storage_path": str(temp_dir / "data"),
            "engine": "sled",
            "sync_interval_seconds": 60,
            "replication": {
                "enabled": True,
                "mqtt_broker": MQTT_BROKER,
                "mqtt_port": MQTT_PORT,
                "topic_prefix": self.topic_prefix,
                "client_id": node_id
            }
        }
        
        config_path = temp_dir / "config.toml"
        with open(config_path, 'w') as f:
            toml.dump(config, f)
            
        return config_path
        
    async def start_node(self, node_id: str, port: int) -> None:
        config_path = self.create_node_config(node_id, port)
        self.configs[node_id] = config_path
        
        cmd = ["cargo", "run", "--release", "--", "--config", str(config_path)]
        
        # Get project root
        project_root = Path.cwd()
        if "tests" in str(project_root):
            project_root = project_root.parent.parent
            
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, 
            cwd=project_root,
            env={**os.environ, "RUST_LOG": "debug"}
        )
        
        self.nodes[node_id] = process
        
        # Wait for node to be ready
        await self._wait_for_node(port)
        print(f"Node {node_id} started on port {port}")
        
    async def _wait_for_node(self, port: int, timeout: int = 30) -> None:
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                writer.close()
                await writer.wait_closed()
                return
            except:
                await asyncio.sleep(0.5)
        raise TimeoutError(f"Node on port {port} failed to start within {timeout}s")
        
    async def execute_command(self, node_id: str, command: str) -> str:
        config = toml.load(self.configs[node_id])
        port = config["port"]
        
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            writer.write(f"{command}\r\n".encode())
            await writer.drain()
            
            data = await reader.read(1024)
            return data.decode().strip()
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def get_all_keys_from_node(self, node_id: str) -> dict:
        """Get all key-value pairs from a node using KEYS command."""
        try:
            keys_response = await self.execute_command(node_id, "KEYS *")
            if keys_response == "(empty array)":
                return {}
                
            # Parse keys response
            keys = []
            if keys_response.startswith("1)"):
                # Multiple keys format: "1) key1\n2) key2\n..."
                lines = keys_response.split('\n')
                for line in lines:
                    if ') ' in line:
                        key = line.split(') ', 1)[1].strip().strip('"')
                        if key:
                            keys.append(key)
            else:
                # Single key or different format
                key = keys_response.strip().strip('"')
                if key and key != "(empty array)":
                    keys.append(key)
            
            # Get values for all keys
            result = {}
            for key in keys:
                value = await self.execute_command(node_id, f"GET {key}")
                result[key] = value.strip().strip('"')
            
            return result
        except Exception as e:
            print(f"Error getting keys from {node_id}: {e}")
            return {}
    
    def cleanup(self):
        for node_id, process in self.nodes.items():
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        
        # Clean up temp directories
        import shutil
        for temp_dir in self.temp_dirs:
            try:
                shutil.rmtree(temp_dir)
            except:
                pass


async def debug_replication():
    harness = DebugHarness()
    
    try:
        print("Starting 3 nodes...")
        ports = [BASE_PORT + i for i in range(3)]
        for i, port in enumerate(ports):
            await harness.start_node(f"node_{i}", port)
            
        print("Waiting for MQTT connections to establish...")
        await asyncio.sleep(5)  # Give nodes time to connect to MQTT
        
        print("\nExecuting commands on different nodes...")
        await harness.execute_command("node_0", "SET key1 value1_from_node0")
        print("node_0: SET key1 value1_from_node0")
        
        await harness.execute_command("node_1", "SET key2 value2_from_node1") 
        print("node_1: SET key2 value2_from_node1")
        
        await harness.execute_command("node_2", "SET key3 value3_from_node2")
        print("node_2: SET key3 value3_from_node2")
        
        print("\nWaiting for replication...")
        await asyncio.sleep(10)  # Wait for MQTT replication
        
        print("\nChecking data on all nodes:")
        for node_id in ["node_0", "node_1", "node_2"]:
            data = await harness.get_all_keys_from_node(node_id)
            print(f"{node_id}: {data}")
            
        print("\nChecking individual keys on all nodes:")
        for key in ["key1", "key2", "key3"]:
            print(f"\nKey '{key}':")
            for node_id in ["node_0", "node_1", "node_2"]:
                try:
                    value = await harness.execute_command(node_id, f"GET {key}")
                    print(f"  {node_id}: {value}")
                except Exception as e:
                    print(f"  {node_id}: ERROR - {e}")
        
    finally:
        harness.cleanup()


if __name__ == "__main__":
    asyncio.run(debug_replication())
