#!/usr/bin/env python3
"""
Diagnostic test to investigate the partial replication failure.

This test provides detailed debugging information about why some nodes
fail to receive MQTT replication updates while others succeed.
"""

import asyncio
import subprocess
import tempfile
import time
from pathlib import Path
import toml
import os

async def diagnostic_replication_test():
    """
    Minimal test to debug the node_2 isolation issue discovered
    by the adversarial replication test suite.
    """
    print("🔍 Starting replication diagnostic test...")
    
    # Create configs for 3 nodes
    temp_dirs = []
    configs = []
    processes = []
    
    try:
        topic_prefix = f"diagnostic_test_{int(time.time())}"
        
        for i in range(3):
            # Create temp directory
            temp_dir = Path(tempfile.mkdtemp(prefix=f"diag_node_{i}_"))
            temp_dirs.append(temp_dir)
            
            # Create config
            config = {
                "host": "127.0.0.1",
                "port": 7500 + i,
                "storage_path": str(temp_dir / "data"),
                "engine": "sled",
                "sync_interval_seconds": 60,
                "replication": {
                    "enabled": True,
                    "mqtt_broker": "test.mosquitto.org",
                    "mqtt_port": 1883,
                    "topic_prefix": topic_prefix,
                    "client_id": f"diag_node_{i}"
                }
            }
            
            config_path = temp_dir / "config.toml"
            with open(config_path, 'w') as f:
                toml.dump(config, f)
            configs.append(config_path)
            
        # Start nodes one by one with detailed logging
        project_root = Path.cwd()
        if "tests" in str(project_root):
            project_root = project_root.parent.parent
            
        for i, config_path in enumerate(configs):
            print(f"🚀 Starting diagnostic node_{i} on port {7500 + i}...")
            
            cmd = ["cargo", "run", "--", "--config", str(config_path)]
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=project_root,
                env={**os.environ, "RUST_LOG": "debug"}  # Enable debug logging
            )
            processes.append(process)
            
            # Wait for each node to start before starting the next
            await wait_for_port(7500 + i)
            print(f"✅ Node_{i} started successfully")
            
            # Small delay between starts
            await asyncio.sleep(2)
            
        print("🔄 All nodes started, waiting for MQTT connections to stabilize...")
        await asyncio.sleep(10)
        
        # Perform writes from each node
        for i in range(3):
            port = 7500 + i
            print(f"📝 Writing test_key_{i} to node_{i} on port {port}...")
            
            try:
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                writer.write(f"SET test_key_{i} value_from_node_{i}\r\n".encode())
                await writer.drain()
                
                response = await reader.read(1024)
                print(f"   Response: {response.decode().strip()}")
                
                writer.close()
                await writer.wait_closed()
                
            except Exception as e:
                print(f"❌ Failed to write to node_{i}: {e}")
                
            await asyncio.sleep(2)  # Space out writes
            
        print("🔄 Waiting for replication to complete...")
        await asyncio.sleep(15)
        
        # Check what each node has
        print("\n📊 Checking replication results:")
        node_data = {}
        
        for i in range(3):
            port = 7500 + i
            print(f"\n🔍 Checking node_{i} (port {port}):")
            
            node_keys = {}
            for j in range(3):  # Check for all test keys
                try:
                    reader, writer = await asyncio.open_connection("127.0.0.1", port)
                    writer.write(f"GET test_key_{j}\r\n".encode())
                    await writer.drain()
                    
                    response = await reader.read(1024)
                    response_str = response.decode().strip()
                    
                    if response_str.startswith("VALUE "):
                        value = response_str[6:]
                        node_keys[f"test_key_{j}"] = value
                        print(f"   ✅ test_key_{j} = {value}")
                    else:
                        print(f"   ❌ test_key_{j} = {response_str}")
                    
                    writer.close()
                    await writer.wait_closed()
                    
                except Exception as e:
                    print(f"   ❌ Error checking test_key_{j}: {e}")
                    
            node_data[f"node_{i}"] = node_keys
            
        # Analyze replication patterns
        print("\n📈 Replication Analysis:")
        for i in range(3):
            node_name = f"node_{i}"
            keys = node_data[node_name]
            print(f"{node_name}: {len(keys)} keys - {list(keys.keys())}")
            
        # Check for convergence
        all_keys = set()
        for node_keys in node_data.values():
            all_keys.update(node_keys.keys())
            
        print(f"\n🎯 Expected keys: {sorted(all_keys)}")
        
        converged_nodes = []
        for node_name, node_keys in node_data.items():
            if set(node_keys.keys()) == all_keys:
                converged_nodes.append(node_name)
                print(f"✅ {node_name} has all keys")
            else:
                missing = all_keys - set(node_keys.keys())
                print(f"❌ {node_name} missing keys: {sorted(missing)}")
                
        if len(converged_nodes) == 3:
            print("🎉 Full convergence achieved!")
        else:
            print(f"⚠️  Partial convergence: {len(converged_nodes)}/3 nodes converged")
            print("This confirms the bug discovered by adversarial tests!")
            
    finally:
        # Cleanup
        print("\n🧹 Cleaning up...")
        for process in processes:
            if process:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    
        import shutil
        for temp_dir in temp_dirs:
            try:
                shutil.rmtree(temp_dir)
            except:
                pass


async def wait_for_port(port, timeout=30):
    """Wait for a port to become available."""
    import socket
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(("127.0.0.1", port))
            sock.close()
            
            if result == 0:
                return
        except:
            pass
        await asyncio.sleep(0.5)
    
    raise TimeoutError(f"Port {port} did not become available within {timeout}s")


if __name__ == "__main__":
    asyncio.run(diagnostic_replication_test())
