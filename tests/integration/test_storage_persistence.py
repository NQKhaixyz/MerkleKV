#!/usr/bin/env python3
"""
Test that verifies server storage persistence across restarts using sled.
"""

import asyncio
import pytest
import subprocess
import time
from pathlib import Path
import toml
import os
import signal
import shutil

def create_persistence_config(port: int, node_id: str) -> Path:
    """Create a flat config file for sled persistence (no replication)."""
    # Use a path inside target directory which is git ignored
    storage_path = Path("target/test_data_persistence")
    storage_path.mkdir(parents=True, exist_ok=True)

    config = {
        "host": "127.0.0.1",
        "port": port,
        "storage_path": str(storage_path),
        "engine": "sled",
        "sync_interval_seconds": 60,
        "replication": {
            "enabled": False,
            "mqtt_broker": "localhost",
            "mqtt_port": 1883,
            "topic_prefix": "merkle_kv",
            "client_id": node_id,
        },
    }

    temp_config = Path(f"/tmp/test_config_persistence_{node_id}_{port}.toml")
    with open(temp_config, "w") as f:
        toml.dump(config, f)
        f.flush()
        os.fsync(f.fileno())

    return temp_config


async def start_server_with_config(config_path: Path, port: int, timeout: int = 30) -> subprocess.Popen:
    """Start the server with the given config and wait until ready."""
    cmd = ["cargo", "run", "--", "--config", str(config_path)]
    print(f"Starting server: {' '.join(cmd)}")

    project_root = Path.cwd()
    if "tests" in str(project_root):
        project_root = project_root.parent.parent

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=project_root,  # ensure relative paths match manual run
        env={**os.environ, "RUST_LOG": "info"}
    )

    import socket
    start_time = time.time()
    while time.time() - start_time < timeout:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(f"Server failed to start:\n{stdout.decode()}\n{stderr.decode()}")

        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                print(f"✅ Server started on port {port}")
                return process
        except (socket.timeout, ConnectionRefusedError):
            await asyncio.sleep(0.2)

    process.send_signal(signal.SIGINT)
    raise TimeoutError(f"Server failed to start within {timeout} seconds")


async def execute_command(host: str, port: int, command: str) -> str:
    """Send a command to the server."""
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(f"{command}\r\n".encode())
        await writer.drain()
        data = await reader.read(1024)
        return data.decode().strip()
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_storage_persistence_across_restart():
    """Verify that data persists after server restart."""
    port = 7379
    node_id = "persistence_node"
    config = create_persistence_config(port, node_id)
    server = None
    storage_path = Path("target/test_data_persistence")

    try:
        # --- First startup ---
        print("🔹 Starting server first time...")
        server = await start_server_with_config(config, port)
        await asyncio.sleep(1)

        # Write key
        result = await execute_command("127.0.0.1", port, "SET persist_key persist_value")
        print(f"SET result: {result}")
        assert result == "OK"

        # --- Wait a moment for sled to flush ---
        await asyncio.sleep(1)

        # Stop server gracefully
        print("🔹 Stopping server...")
        server.send_signal(signal.SIGINT)
        server.wait(timeout=10)
        server = None

        # Debug: check storage files exist
        print("Storage path exists:", storage_path.exists())
        print("Storage files after first run:", list(storage_path.glob("*")))

        # --- Restart ---
        print("🔹 Restarting server...")
        server = await start_server_with_config(config, port)
        await asyncio.sleep(1)

        # Check that key still exists
        result = await execute_command("127.0.0.1", port, "GET persist_key")
        print(f"GET after restart result: {result}")
        assert result == "VALUE persist_value"

        print("✅ Persistence test passed!")

    finally:
        if server:
            server.send_signal(signal.SIGINT)
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()

        # Clean up config and storage after test
        if config.exists():
            config.unlink()
        if storage_path.exists():
            shutil.rmtree(storage_path, ignore_errors=True)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
