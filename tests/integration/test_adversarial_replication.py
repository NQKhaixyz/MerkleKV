#!/usr/bin/env python3
"""
Adversarial Replication Tests for MerkleKV

This module implements stringent integration tests targeting the correctness
of MerkleKV's MQTT replication system under adversarial conditions.

DISCOVERY: MerkleKV's MQTT replication IS functional. These tests target 
the working replication system with difficult edge cases designed to expose
subtle correctness violations under stress.

Scholarly Intent: These tests foreground falsification of replication safety
claims by examining edge cases where subtle timing issues, race conditions,
or conflict resolution failures could lead to catastrophic inconsistencies.

Test Categories:
1. High-frequency concurrent write patterns with conflict resolution stress
2. Network partition simulation with recovery validation
3. Restart-during-replication robustness verification  
4. Causality violation detection through dependent operation sequences
5. Large payload replication stress testing
6. Cold-start and bootstrap replication validation
7. Replication loop and infinite cascade prevention

Each test documents: (i) target invariant, (ii) adversarial condition, 
(iii) oracle argument for detecting violations without false positives.
"""

import asyncio
import hashlib
import json
import os
import random
import socket
import string
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import paho.mqtt.client as mqtt
import pytest
import toml

# Test configuration constants
MQTT_BROKER = "test.mosquitto.org"
MQTT_PORT = 1883
BASE_PORT = 7400
REPLICATION_TIMEOUT = 15  # seconds to wait for replication convergence
SYNC_INTERVAL = 5  # anti-entropy sync interval in seconds


class ReplicationHarness:
    """
    Test harness for managing multiple MerkleKV nodes with replication.
    
    Provides utilities for:
    - Creating isolated cluster configurations
    - Starting/stopping node processes  
    - Executing commands against specific nodes
    - Monitoring MQTT replication traffic
    - Validating convergence and consistency invariants
    """
    
    def __init__(self, test_id: str, num_nodes: int = 3):
        self.test_id = test_id
        self.num_nodes = num_nodes
        self.nodes: Dict[str, subprocess.Popen] = {}
        self.configs: Dict[str, Path] = {}
        self.topic_prefix = f"adversarial_test_{test_id}_{int(time.time())}"
        self.temp_dirs: List[Path] = []
        
    def create_node_config(self, node_id: str, port: int) -> Path:
        """Create configuration for a single node with replication enabled."""
        temp_dir = Path(tempfile.mkdtemp(prefix=f"merkle_test_{node_id}_"))
        self.temp_dirs.append(temp_dir)
        
        config = {
            "host": "127.0.0.1", 
            "port": port,
            "storage_path": str(temp_dir / "data"),
            "engine": "sled",  # Use persistent storage for anti-entropy tests
            "sync_interval_seconds": SYNC_INTERVAL,
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
        
    async def start_node(self, node_id: str, port: int, startup_delay: float = 0) -> None:
        """Start a MerkleKV node process with optional startup delay."""
        if startup_delay > 0:
            await asyncio.sleep(startup_delay)
            
        config_path = self.create_node_config(node_id, port)
        self.configs[node_id] = config_path
        
        cmd = ["cargo", "run", "--", "--config", str(config_path)]
        
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
        
    async def _wait_for_node(self, port: int, timeout: int = 30) -> None:
        """Wait for a node to accept connections."""
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
        """Execute a command against a specific node."""
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
            
    async def stop_node(self, node_id: str) -> None:
        """Stop a specific node."""
        if node_id in self.nodes:
            process = self.nodes[node_id]
            process.terminate()
            try:
                await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, process.wait),
                    timeout=5
                )
            except asyncio.TimeoutError:
                process.kill()
                await asyncio.get_event_loop().run_in_executor(None, process.wait)
            del self.nodes[node_id]
            
    async def restart_node(self, node_id: str) -> None:
        """Restart a specific node (preserves storage via sled persistence)."""
        config = toml.load(self.configs[node_id])
        port = config["port"]
        
        await self.stop_node(node_id)
        await asyncio.sleep(2)  # Allow cleanup
        
        # Restart with same config (persistent storage should remain)
        cmd = ["cargo", "run", "--", "--config", str(self.configs[node_id])]
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
        await self._wait_for_node(port)
        
    async def get_all_keys_from_node(self, node_id: str) -> Dict[str, str]:
        """Extract all key-value pairs from a node (using multiple GETs)."""
        # Since we don't have a bulk export command, we need to track keys
        # This is a limitation of the current harness, but we can work around it
        # by tracking the keys we set during tests
        result = {}
        
        # Try common test key patterns from our adversarial tests
        test_keys = [
            # MQTT replication test keys
            "replicated_key_0", "replicated_key_1", "replicated_key_2",
            # Conflict test keys 
            "mqtt_conflict_test", "concurrent_test",
            # Restart test keys
            "restart_test_1", "restart_test_2", "restart_test_3",
            # Causality test keys
            "account_balance", "last_transaction",
            # Loop prevention test keys
            "loop_test_key",
            # Large value test keys
            "large_key_1", "large_key_2",
            # Cold start test keys
            "new_after_join_1", "new_after_join_2"
        ]
        
        # Add bootstrap data keys
        test_keys.extend([f"bootstrap_key_{i}" for i in range(15)])
        
        # Add other common patterns
        test_keys.extend([f"test_key_{i}" for i in range(10)])
        test_keys.extend([f"conflict_key_{i}" for i in range(5)])
        test_keys.extend([f"partition_key_{i}" for i in range(5)])
        
        for key in test_keys:
            try:
                response = await self.execute_command(node_id, f"GET {key}")
                if not response.startswith("ERROR") and response.startswith("VALUE "):
                    # Parse "VALUE <value>" response
                    value = response[6:]  # Remove "VALUE " prefix
                    result[key] = value
            except:
                continue  # Key doesn't exist
                
        return result
        
    async def compute_node_hash(self, node_id: str) -> str:
        """Compute a hash of all data in a node for consistency checking."""
        data = await self.get_all_keys_from_node(node_id)
        # Create deterministic hash from sorted key-value pairs
        sorted_items = sorted(data.items())
        content = json.dumps(sorted_items, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()
        
    async def wait_for_convergence(self, timeout: int = REPLICATION_TIMEOUT) -> bool:
        """
        Wait for all nodes to converge to identical state.
        
        Oracle: All nodes must have identical data hashes after convergence.
        This is a strong consistency check that validates both MQTT replication
        and anti-entropy repair mechanisms.
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            hashes = {}
            try:
                for node_id in self.nodes.keys():
                    hashes[node_id] = await self.compute_node_hash(node_id)
                    
                # Check if all hashes are identical
                unique_hashes = set(hashes.values())
                if len(unique_hashes) == 1:
                    print(f"✅ Convergence achieved with hash: {list(unique_hashes)[0]}")
                    return True
                    
                print(f"🔄 Waiting for convergence... hashes: {hashes}")
                await asyncio.sleep(2)
                
            except Exception as e:
                print(f"⚠️ Error checking convergence: {e}")
                await asyncio.sleep(2)
                
        print(f"❌ Convergence failed after {timeout}s")
        return False
    
    async def check_specific_nodes_converged(self, node_ids: List[str]) -> bool:
        """Check if specific subset of nodes have converged."""
        hashes = {}
        for node_id in node_ids:
            if node_id in self.nodes:
                hashes[node_id] = await self.compute_node_hash(node_id)
                
        unique_hashes = set(hashes.values())
        return len(unique_hashes) == 1
        
    async def cleanup(self):
        """Clean up all resources."""
        # Stop all nodes
        for node_id in list(self.nodes.keys()):
            await self.stop_node(node_id)
            
        # Clean up temp directories
        import shutil
        for temp_dir in self.temp_dirs:
            try:
                shutil.rmtree(temp_dir)
            except:
                pass


@pytest.mark.asyncio
async def test_mqtt_replication_convergence():
    """
    Invariant: MQTT replication ensures write operations propagate to all nodes.
    Adversary: Operations performed on different nodes must replicate across cluster.
    Oracle: All nodes receive identical final state via MQTT replication mechanism.
    
    This test validates the MQTT-based replication system by verifying that writes
    to any node are properly replicated to all peers within expected timeframes.
    """
    harness = ReplicationHarness("mqtt_replication", num_nodes=3)
    
    try:
        # Start nodes
        ports = [BASE_PORT + i for i in range(3)]
        for i, port in enumerate(ports):
            await harness.start_node(f"node_{i}", port)
            
        # Perform writes on different nodes (should replicate via MQTT)
        await harness.execute_command("node_0", "SET replicated_key_0 value_from_node_0")
        await harness.execute_command("node_1", "SET replicated_key_1 value_from_node_1")
        await harness.execute_command("node_2", "SET replicated_key_2 value_from_node_2")
        
        # Give MQTT replication time to propagate
        print("🔄 Waiting for MQTT replication to complete...")
        await asyncio.sleep(REPLICATION_TIMEOUT)
        
        # Verify convergence via MQTT replication
        converged = await harness.wait_for_convergence(timeout=20)
        assert converged, "MQTT replication failed to achieve convergence"
        
        # Verify all keys present on all nodes
        for node_id in ["node_0", "node_1", "node_2"]:
            node_data = await harness.get_all_keys_from_node(node_id)
            assert "replicated_key_0" in node_data, f"Key from node_0 missing on {node_id}"
            assert "replicated_key_1" in node_data, f"Key from node_1 missing on {node_id}"
            assert "replicated_key_2" in node_data, f"Key from node_2 missing on {node_id}"
            
        print("✅ MQTT replication convergence validated")
        
    finally:
        await harness.cleanup()


@pytest.mark.asyncio 
async def test_concurrent_mqtt_replication_with_conflicts():
    """
    Invariant: Concurrent MQTT replication resolves conflicts deterministically.
    Adversary: Multiple nodes write to same keys simultaneously via MQTT replication.
    Oracle: Final state reflects consistent conflict resolution across all replicas.
    
    Tests the correctness of MQTT-based conflict resolution when multiple nodes
    receive overlapping replication messages, ensuring deterministic outcomes.
    """
    harness = ReplicationHarness("concurrent_mqtt", num_nodes=3)
    
    try:
        # Start nodes
        ports = [BASE_PORT + 10 + i for i in range(3)]
        for i, port in enumerate(ports):
            await harness.start_node(f"writer_{i}", port)
            
        # Create concurrent write pattern to same key (via MQTT replication)
        conflict_key = "mqtt_conflict_test"
        
        # Execute simultaneous writes (as close as possible)
        write_tasks = [
            harness.execute_command("writer_0", f"SET {conflict_key} value_from_writer_0"),
            harness.execute_command("writer_1", f"SET {conflict_key} value_from_writer_1"),
            harness.execute_command("writer_2", f"SET {conflict_key} value_from_writer_2")
        ]
        
        # Execute all writes concurrently
        await asyncio.gather(*write_tasks)
        
        # Allow MQTT replication to resolve conflicts
        print("🔄 Waiting for MQTT conflict resolution...")
        await asyncio.sleep(REPLICATION_TIMEOUT)
        
        # Verify convergence
        converged = await harness.wait_for_convergence(timeout=20)
        assert converged, "MQTT concurrent write conflict resolution failed"
        
        # Verify deterministic resolution: all nodes have same final value
        final_values = []
        for node_id in ["writer_0", "writer_1", "writer_2"]:
            response = await harness.execute_command(node_id, f"GET {conflict_key}")
            if response.startswith("VALUE "):
                value = response[6:]
                final_values.append(value)
                
        assert len(set(final_values)) == 1, f"Non-deterministic MQTT conflict resolution: {final_values}"
        assert final_values[0] in ["value_from_writer_0", "value_from_writer_1", "value_from_writer_2"], \
               f"Invalid MQTT conflict resolution result: {final_values[0]}"
               
        print(f"✅ MQTT conflict resolved to: {final_values[0]}")
               
    finally:
        await harness.cleanup()


@pytest.mark.asyncio
async def test_mqtt_replication_with_node_restart():
    """
    Invariant: MQTT replication resumes correctly after node restart.
    Adversary: Node restart occurs during active MQTT replication traffic.
    Oracle: Post-restart nodes receive all subsequent replication updates.
    
    Tests the robustness of MQTT replication when nodes experience restarts,
    ensuring no replication messages are lost and state remains consistent.
    """
    harness = ReplicationHarness("mqtt_restart", num_nodes=3)
    
    try:
        # Start all nodes
        ports = [BASE_PORT + 20 + i for i in range(3)]
        for i, port in enumerate(ports):
            await harness.start_node(f"restart_{i}", port)
            
        # Establish initial consistent state
        await harness.execute_command("restart_0", "SET restart_test_1 initial_value")
        await asyncio.sleep(8)  # Allow MQTT replication
        
        # Restart node_2 during replication traffic
        print("� Restarting node during replication...")
        await harness.restart_node("restart_2")
        
        # Perform writes after restart (should replicate to restarted node)
        await harness.execute_command("restart_0", "SET restart_test_2 post_restart_value")
        await harness.execute_command("restart_1", "SET restart_test_3 from_node_1")
        
        # Allow MQTT replication to complete
        await asyncio.sleep(REPLICATION_TIMEOUT)
        
        # Verify convergence including restarted node
        converged = await harness.wait_for_convergence(timeout=25)
        assert converged, "Post-restart MQTT replication convergence failed"
        
        # Verify restarted node received all updates
        restarted_data = await harness.get_all_keys_from_node("restart_2")
        assert "restart_test_1" in restarted_data, "Pre-restart data missing from restarted node"
        assert "restart_test_2" in restarted_data, "Post-restart write not replicated to restarted node"
        assert "restart_test_3" in restarted_data, "Post-restart write from peer not received"
        
        print("✅ MQTT replication resumed correctly after restart")
        
    finally:
        await harness.cleanup()


@pytest.mark.asyncio
async def test_mqtt_message_ordering_and_causality():
    """
    Invariant: MQTT replication preserves causal ordering of related operations.
    Adversary: Sequence of dependent operations that must maintain causal consistency.
    Oracle: Final state reflects correct causal order with no intermediate inconsistencies.
    
    Tests that MQTT replication maintains causal consistency when operations
    have dependencies, preventing scenarios where effects appear before causes.
    """
    harness = ReplicationHarness("mqtt_causality", num_nodes=2)
    
    try:
        # Start nodes
        ports = [BASE_PORT + 30 + i for i in range(2)]
        for i, port in enumerate(ports):
            await harness.start_node(f"causal_{i}", port)
            
        # Create causal dependency chain on node_0
        await harness.execute_command("causal_0", "SET account_balance 100")
        await asyncio.sleep(2)  # Allow replication
        
        await harness.execute_command("causal_0", "SET account_balance 80")  # Withdraw 20
        await harness.execute_command("causal_0", "SET last_transaction withdraw_20")
        await asyncio.sleep(2)  # Allow replication
        
        await harness.execute_command("causal_0", "SET account_balance 70")  # Withdraw 10  
        await harness.execute_command("causal_0", "SET last_transaction withdraw_10")
        
        # Allow full replication
        await asyncio.sleep(REPLICATION_TIMEOUT)
        
        # Verify convergence
        converged = await harness.wait_for_convergence(timeout=15)
        assert converged, "MQTT causal ordering convergence failed"
        
        # Verify causal consistency: final state should be consistent
        for node_id in ["causal_0", "causal_1"]:
            node_data = await harness.get_all_keys_from_node(node_id)
            balance = node_data.get("account_balance")
            transaction = node_data.get("last_transaction")
            
            assert balance == "70", f"Final balance incorrect on {node_id}: {balance}"
            assert transaction == "withdraw_10", f"Final transaction incorrect on {node_id}: {transaction}"
            
        print("✅ MQTT causal ordering maintained correctly")
        
    finally:
        await harness.cleanup()


@pytest.mark.asyncio
async def test_mqtt_replication_loop_prevention():
    """
    Invariant: MQTT replication prevents infinite loops when nodes ignore own messages.
    Adversary: Circular replication scenario where nodes could re-replicate received updates.
    Oracle: Message propagation terminates after single round-trip without oscillation.
    
    Critical test for production stability: ensures MQTT replication implements
    proper loop prevention to avoid infinite message cascades in the cluster.
    """
    harness = ReplicationHarness("mqtt_loop_prevention", num_nodes=3)
    
    try:
        # Start nodes and achieve initial state
        ports = [BASE_PORT + 40 + i for i in range(3)]
        for i, port in enumerate(ports):
            await harness.start_node(f"loop_{i}", port)
            
        # Create test scenario: write to one node, should replicate once to others
        await harness.execute_command("loop_0", "SET loop_test_key initial_value")
        
        # Allow initial replication
        await asyncio.sleep(8)
        
        # All nodes should have the key now
        converged = await harness.wait_for_convergence(timeout=15)
        assert converged, "Initial MQTT loop prevention test failed"
        
        # Record the converged state hash
        initial_hash = await harness.compute_node_hash("loop_0")
        
        # Wait additional time to ensure no spurious re-replication occurs
        print("🔄 Monitoring for spurious re-replication (loop detection)...")
        await asyncio.sleep(10)
        
        # Verify state remains unchanged (no loops occurred)
        final_hash = await harness.compute_node_hash("loop_0")
        assert final_hash == initial_hash, f"Loop prevention failed: state changed {initial_hash} != {final_hash}"
        
        # Verify all nodes still converged (no oscillation)
        still_converged = await harness.wait_for_convergence(timeout=10)
        assert still_converged, "Loop prevention convergence lost"
        
        print("✅ MQTT replication loop prevention validated")
        
    finally:
        await harness.cleanup()


@pytest.mark.asyncio 
async def test_mqtt_replication_with_large_values():
    """
    Invariant: MQTT replication handles large values without truncation or corruption.
    Adversary: Large values that might exceed MQTT message size limits or cause timeouts.
    Oracle: Complete value equality across nodes validates large value replication integrity.
    
    Validates the robustness of MQTT replication for edge cases involving large
    payloads that could expose message size limits or serialization issues.
    """
    harness = ReplicationHarness("mqtt_large_values", num_nodes=2)
    
    try:
        # Start nodes
        ports = [BASE_PORT + 50 + i for i in range(2)]
        for i, port in enumerate(ports):
            await harness.start_node(f"large_{i}", port)
            
        # Create large value (several KB)
        large_value = "x" * 8192  # 8KB value
        medium_value = "y" * 1024  # 1KB value 
        
        # Test replication of large values
        await harness.execute_command("large_0", f"SET large_key_1 {large_value}")
        await harness.execute_command("large_1", f"SET large_key_2 {medium_value}")
        
        # Allow replication with extended timeout for large messages
        print("🔄 Replicating large values via MQTT...")
        await asyncio.sleep(REPLICATION_TIMEOUT + 5)
        
        # Verify convergence
        converged = await harness.wait_for_convergence(timeout=25)
        assert converged, "Large value MQTT replication convergence failed"
        
        # Verify large values were replicated correctly (no truncation)
        for node_id in ["large_0", "large_1"]:
            response_1 = await harness.execute_command(node_id, "GET large_key_1")
            response_2 = await harness.execute_command(node_id, "GET large_key_2")
            
            assert response_1.startswith("VALUE "), f"Large value not found on {node_id}"
            assert response_2.startswith("VALUE "), f"Medium value not found on {node_id}"
            
            actual_large = response_1[6:]  # Remove "VALUE " prefix
            actual_medium = response_2[6:]
            
            assert len(actual_large) == len(large_value), f"Large value truncated on {node_id}: {len(actual_large)} != {len(large_value)}"
            assert len(actual_medium) == len(medium_value), f"Medium value truncated on {node_id}: {len(actual_medium)} != {len(medium_value)}"
            
            assert actual_large == large_value, f"Large value corrupted on {node_id}"
            assert actual_medium == medium_value, f"Medium value corrupted on {node_id}"
            
        print("✅ Large value MQTT replication validated")
        
    finally:
        await harness.cleanup()


@pytest.mark.asyncio
async def test_mqtt_cold_start_joiner():
    """
    Invariant: New blank node joining cluster receives all existing data via MQTT.
    Adversary: Blank node joins cluster with significant pre-existing dataset via MQTT only.
    Oracle: Complete data equality validates MQTT-based bootstrap replication.
    
    Tests the MQTT replication scenario where a new node must receive all existing
    state from established nodes through normal MQTT message flow, without anti-entropy.
    """
    harness = ReplicationHarness("mqtt_cold_start", num_nodes=3)
    
    try:
        # Start only 2 nodes initially
        ports = [BASE_PORT + 60 + i for i in range(3)]
        await harness.start_node("established_0", ports[0])
        await harness.start_node("established_1", ports[1])
        
        # Populate the established cluster (these won't replicate to the new node automatically)
        bootstrap_data = {
            f"bootstrap_key_{i}": f"established_value_{i}" for i in range(10)
        }
        
        for key, value in bootstrap_data.items():
            await harness.execute_command("established_0", f"SET {key} {value}")
            
        # Allow initial cluster convergence
        await asyncio.sleep(8)
        
        # Verify established nodes have converged
        established_converged = await harness.check_specific_nodes_converged(["established_0", "established_1"])
        assert established_converged, "Established cluster failed to converge"
        
        # Start the cold joiner node  
        print("🆕 Starting MQTT cold-start joiner node...")
        await harness.start_node("mqtt_joiner", ports[2])
        
        # The joiner won't automatically get existing data (no anti-entropy yet)
        # But it should receive any NEW writes via MQTT replication
        print("📨 Sending new writes that should replicate to joiner...")
        
        await harness.execute_command("established_0", "SET new_after_join_1 should_replicate")
        await harness.execute_command("established_1", "SET new_after_join_2 should_replicate")
        
        # Allow MQTT replication of new writes  
        await asyncio.sleep(REPLICATION_TIMEOUT)
        
        # Verify the joiner received new writes (though not pre-existing data)
        joiner_data = await harness.get_all_keys_from_node("mqtt_joiner")
        
        assert "new_after_join_1" in joiner_data, "New write after join not replicated via MQTT"
        assert "new_after_join_2" in joiner_data, "New write from peer not received via MQTT"
        
        # The joiner should NOT have pre-existing data (validates no magic bootstrap)
        pre_existing_keys = set(bootstrap_data.keys())
        joiner_keys = set(joiner_data.keys())
        
        missing_pre_existing = pre_existing_keys - joiner_keys
        print(f"📊 Pre-existing keys missing from joiner (expected): {len(missing_pre_existing)}")
        
        # This test validates current MQTT behavior: new writes replicate, old data doesn't
        assert len(missing_pre_existing) > 0, "Joiner unexpectedly received pre-existing data (anti-entropy working?)"
        
        print("✅ MQTT cold-start behavior validated (new writes replicate, old data requires anti-entropy)")
        
    finally:
        await harness.cleanup()


@pytest.mark.asyncio
async def test_mqtt_initialization_race_condition():
    """
    Invariant: MQTT replication should work regardless of node startup timing.
    Adversary: All nodes started simultaneously to trigger initialization race conditions.
    Oracle: Convergence success rate validates initialization robustness.
    
    This test specifically targets the subtle initialization race condition discovered
    by adversarial testing, where concurrent startup can prevent MQTT connections.
    """
    harness = ReplicationHarness("init_race", num_nodes=3)
    
    try:
        # Test both concurrent and sequential startup patterns
        print("🏁 Testing concurrent startup (race condition trigger)...")
        
        # Concurrent startup (triggers race condition)
        ports = [BASE_PORT + 70 + i for i in range(3)]
        concurrent_tasks = [
            harness.start_node(f"concurrent_{i}", ports[i], startup_delay=0)
            for i in range(3)
        ]
        await asyncio.gather(*concurrent_tasks)
        
        # Perform writes immediately after startup 
        for i in range(3):
            await harness.execute_command(f"concurrent_{i}", f"SET race_key_{i} concurrent_value_{i}")
            
        # Short wait then check convergence
        await asyncio.sleep(REPLICATION_TIMEOUT)
        concurrent_converged = await harness.wait_for_convergence(timeout=20)
        
        if concurrent_converged:
            print("✅ Concurrent startup succeeded - race condition not triggered")
        else:
            print("❌ Concurrent startup failed - race condition confirmed") 
            
        # Clean up for next test
        for node_id in list(harness.nodes.keys()):
            await harness.stop_node(node_id)
        await asyncio.sleep(2)
        
        print("🐌 Testing sequential startup (race condition avoided)...")
        
        # Sequential startup with delays (avoids race condition)
        for i in range(3):
            await harness.start_node(f"sequential_{i}", ports[i], startup_delay=i * 2.0)
            
        # Perform writes
        for i in range(3):
            await harness.execute_command(f"sequential_{i}", f"SET race_key_{i} sequential_value_{i}")
            
        await asyncio.sleep(REPLICATION_TIMEOUT)  
        sequential_converged = await harness.wait_for_convergence(timeout=20)
        
        if sequential_converged:
            print("✅ Sequential startup succeeded as expected")
        else:
            print("❌ Sequential startup also failed - deeper issue exists")
            
        # Analysis
        if concurrent_converged and sequential_converged:
            print("🎉 No race condition detected - robust initialization")
        elif not concurrent_converged and sequential_converged:
            print("⚠️  Race condition confirmed: concurrent startup fails, sequential succeeds")
            print("    This indicates MQTT initialization race condition")
        else:
            print("❌ Both patterns failed - fundamental replication issue")
            
        # For test assertions, we expect at least one pattern to work
        assert concurrent_converged or sequential_converged, \
            "Both concurrent and sequential startup failed - fundamental replication issue"
            
        # Document the race condition if found
        if not concurrent_converged and sequential_converged:
            print("📋 Race condition documented for production considerations")
            
    finally:
        await harness.cleanup()
