#!/usr/bin/env python3
"""Debug script to understand why node startup fails in tests."""

import subprocess
import tempfile
import toml
from pathlib import Path
import time

def create_debug_config(node_id: str, port: int):
    """Create a test config similar to the harness."""
    temp_dir = Path(tempfile.mkdtemp(prefix=f"debug_merkle_{node_id}_"))
    
    config = {
        "host": "127.0.0.1", 
        "port": port,
        "storage_path": str(temp_dir / "storage"),
        "engine": "sled",
        "sync_interval_seconds": 30,
        "replication": {
            "enabled": True,
            "mqtt_broker": "test.mosquitto.org",
            "mqtt_port": 1883,
            "topic_prefix": f"debug_test_{int(time.time())}",
            "client_id": node_id
        }
    }
    
    config_path = temp_dir / "config.toml"
    with open(config_path, 'w') as f:
        toml.dump(config, f)
    
    print(f"Created config at: {config_path}")
    print("Config content:")
    print(toml.dumps(config))
    
    return config_path, temp_dir

def test_node_startup():
    """Try to start a node and see what happens."""
    config_path, temp_dir = create_debug_config("debug_node", 7401)
    
    # Try to start the node
    cmd = ["cargo", "run", "--release", "--", "--config", str(config_path)]
    project_root = Path.cwd()
    if "tests" in str(project_root):
        project_root = project_root.parent.parent
    
    print(f"Running command: {' '.join(cmd)}")
    print(f"Working directory: {project_root}")
    
    try:
        # Run with direct output so we can see errors
        process = subprocess.run(
            cmd,
            cwd=project_root,
            timeout=10,
            capture_output=True,
            text=True
        )
        
        print("STDOUT:")
        print(process.stdout)
        print("STDERR:")  
        print(process.stderr)
        print(f"Return code: {process.returncode}")
        
    except subprocess.TimeoutExpired:
        print("Process timed out after 10 seconds - might be running successfully")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Clean up
        import shutil
        try:
            shutil.rmtree(temp_dir)
        except:
            pass

if __name__ == "__main__":
    test_node_startup()
