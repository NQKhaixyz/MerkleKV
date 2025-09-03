#!/usr/bin/env python3
"""
Debug MQTT replication using the same approach as the test
"""

import asyncio
import hashlib
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Dict, Set
import toml

MQTT_BROKER = "test.mosquitto.org"
MQTT_PORT = 1883
BASE_PORT = 7460

class DebugKeyRegistry:
    def __init__(self):
        self.keys: Set[str] = set()
    
    def register(self, key: str):
        self.keys.add(key)
    
    def get_all_keys(self):
        return sorted(list(self.keys))

class DebugHarness:
    def __init__(self):
        self.test_id = f"debug_{int(time.time())}"
        self.nodes = {}
        self.configs = {}
        self.topic_prefix = f"debug_test_{self.test_id}"
        self.temp_dirs = []
        self.key_registry = DebugKeyRegistry()
        
    def create_node_config(self, node_id: str, port: int) -> Path:
        temp_dir = Path(tempfile.mkdtemp(prefix=f"debug_{node_id}_"))
        self.temp_dirs.append(temp_dir)
        
        config = {
            "host": "127.0.0.1", 
            "port": port,
            "storage_path": str(temp_dir / "data"),
            "engine": "sled",
            "sync_interval_seconds": 5,
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
            env={**os.environ, "RUST_LOG": "info"}
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
    
    async def execute_write_command(self, node_id: str, command: str) -> str:
        # Extract key from command for registration  
        parts = command.strip().split()
        if len(parts) >= 2 and parts[0].upper() in ['SET', 'DELETE', 'INCR', 'DECR', 'APPEND', 'PREPEND']:
            key = parts[1]
            self.key_registry.register(key)
        
        return await self.execute_command(node_id, command)
    
    async def get_all_keys_from_node(self, node_id: str) -> Dict[str, str]:
        """Extract all key-value pairs from a node using the dynamic key registry."""
        result = {}
        
        # Use the key registry to enumerate all keys that were written during tests
        for key in self.key_registry.get_all_keys():
            try:
                response = await self.execute_command(node_id, f"GET {key}")
                if not response.startswith("ERROR") and response.startswith("VALUE "):
                    # Parse "VALUE <value>" response
                    value = response[6:]  # Remove "VALUE " prefix
                    result[key] = value
            except:
                continue  # Key doesn't exist on this node
                
        return result
        
    async def compute_node_hash(self, node_id: str) -> str:
        """Compute a hash of all data in a node for consistency checking.""" 
        data = await self.get_all_keys_from_node(node_id)
        # Create deterministic hash from sorted key-value pairs
        sorted_items = sorted(data.items())
        content = json.dumps(sorted_items, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()
    
    async def wait_for_convergence(self, timeout: int = 15) -> bool:
        """Wait for all nodes to converge to identical state."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            hashes = {}
            try:
                for node_id in self.nodes.keys():
                    hashes[node_id] = await self.compute_node_hash(node_id)
                    
                # Check if all hashes are identical
                unique_hashes = set(hashes.values())
                if len(unique_hashes) == 1:
                    print(f"Convergence achieved with hash: {list(unique_hashes)[0]}")
                    return True
                    
                print(f"Waiting for convergence... hashes: {hashes}")
                await asyncio.sleep(2)
            except Exception as e:
                print(f"Error during convergence check: {e}")
                await asyncio.sleep(2)
                
        print("Convergence failed after timeout")
        return False
    
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
        await asyncio.sleep(5)  # Wait for nodes to connect
        
        print("\nExecuting commands on different nodes (using test approach)...")
        await harness.execute_write_command("node_0", "SET replicated_key_0 value_from_node_0")
        await harness.execute_write_command("node_1", "SET replicated_key_1 value_from_node_1")
        await harness.execute_write_command("node_2", "SET replicated_key_2 value_from_node_2")
        
        print("Waiting for MQTT replication...")
        await asyncio.sleep(15)  # Same as test
        
        print("\nTesting convergence (same as test)...")
        converged = await harness.wait_for_convergence(timeout=20)
        
        if converged:
            print("SUCCESS: Replication converged!")
        else:
            print("FAILURE: Replication did not converge")
            
            print("\nDiagnosing data on each node:")
            for node_id in ["node_0", "node_1", "node_2"]:
                data = await harness.get_all_keys_from_node(node_id)
                print(f"{node_id}: {data}")
        
    finally:
        harness.cleanup()


if __name__ == "__main__":
    asyncio.run(debug_replication())
