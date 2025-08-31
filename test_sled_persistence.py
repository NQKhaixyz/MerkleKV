#!/usr/bin/env python3
"""Test sled persistence in isolation."""

import subprocess
import tempfile
import toml
from pathlib import Path
import time
import asyncio

async def test_sled_persistence():
    """Test if sled persistence works for a single node."""
    temp_dir = Path(tempfile.mkdtemp(prefix="sled_test_"))
    storage_path = temp_dir / "storage"
    
    # Create config
    config = {
        "host": "127.0.0.1",
        "port": 7402,
        "storage_path": str(storage_path),
        "engine": "sled",
        "sync_interval_seconds": 60,
        "replication": {
            "enabled": False,  # Disable replication for pure sled test
            "mqtt_broker": "localhost",  # Required even when disabled
            "mqtt_port": 1883,
            "topic_prefix": "test",
            "client_id": "test_node"
        }
    }
    
    config_path = temp_dir / "config.toml"
    with open(config_path, 'w') as f:
        toml.dump(config, f)
    
    storage_path.mkdir(parents=True, exist_ok=True)
    
    print(f"Testing sled persistence with storage at: {storage_path}")
    
    # Start node 1
    cmd = ["cargo", "run", "--release", "--", "--config", str(config_path)]
    project_root = Path.cwd()
    if "tests" in str(project_root):
        project_root = project_root.parent.parent
    
    print("Starting node 1...")
    process1 = subprocess.Popen(
        cmd, 
        cwd=project_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )
    
    # Wait for node to start
    await asyncio.sleep(3)
    
    # Write some data
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", 7402)
        writer.write(b"SET test_key test_value\n")
        await writer.drain()
        
        response = await reader.readline()
        print(f"SET response: {response.decode().strip()}")
        
        writer.write(b"GET test_key\n") 
        await writer.drain()
        response = await reader.readline()
        print(f"GET response: {response.decode().strip()}")
        
        writer.close()
        await writer.wait_closed()
    except Exception as e:
        print(f"Error communicating with node: {e}")
        
    # Stop node 1
    process1.terminate()
    await asyncio.sleep(2)
    
    # Check if storage files were created
    print(f"Storage directory contents: {list(storage_path.iterdir()) if storage_path.exists() else 'Directory does not exist'}")
    
    # Start node 2 (same config)
    print("Starting node 2 with same storage...")
    process2 = subprocess.Popen(
        cmd,
        cwd=project_root, 
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )
    
    await asyncio.sleep(3)
    
    # Check if data persisted
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", 7402)
        
        writer.write(b"GET test_key\n")
        await writer.drain()
        response = await reader.readline()
        print(f"After restart GET response: {response.decode().strip()}")
        
        if b"test_value" in response:
            print("✅ Sled persistence WORKS!")
        else:
            print("❌ Sled persistence FAILED!")
        
        writer.close()
        await writer.wait_closed()
    except Exception as e:
        print(f"Error communicating with restarted node: {e}")
    
    # Cleanup
    process2.terminate()
    import shutil
    try:
        shutil.rmtree(temp_dir)
    except:
        pass

if __name__ == "__main__":
    asyncio.run(test_sled_persistence())
