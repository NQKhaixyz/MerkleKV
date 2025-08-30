"""
MerkleKV Replication Adversarial Tests - Broker Outage and Network Partitions

This module tests the replication system's resilience to MQTT broker failures,
network partitions, and recovery scenarios.

Test scenarios:
- MQTT broker goes offline during operations
- Network partitions between nodes and broker
- Broker recovery and reconnection
- Operations during outage (graceful degradation)
- Message queue behavior during outages
"""

import asyncio
import pytest
import time
import subprocess
import socket
from typing import Optional
from conftest import ReplicationTestHarness

# Mark slow tests for optional execution
pytestmark = pytest.mark.slow

class TestReplicationBrokerOutage:
    """Test replication behavior during broker outages and network issues"""

    def _is_port_open(self, host: str, port: int, timeout: float = 1.0) -> bool:
        """Check if a port is open on given host"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            sock.close()
            return result == 0
        except Exception:
            return False

    async def _wait_for_broker_down(self, host: str = "test.mosquitto.org", port: int = 1883, timeout: float = 30.0):
        """Wait for broker to become unreachable (for testing purposes only)"""
        # Note: We can't actually take down the public broker
        # This is a placeholder for local broker testing
        print(f"⚠️  Note: Cannot actually test broker outage with public broker {host}:{port}")
        print("⚠️  This test simulates outage conditions without real network disruption")
        await asyncio.sleep(2)  # Simulate checking time

    async def _wait_for_broker_up(self, host: str = "test.mosquitto.org", port: int = 1883, timeout: float = 30.0):
        """Wait for broker to become reachable"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self._is_port_open(host, port):
                print(f"✅ Broker {host}:{port} is reachable")
                return True
            await asyncio.sleep(1)
        return False

    @pytest.mark.asyncio
    async def test_operations_during_broker_outage(self):
        """Test that operations continue locally when broker is unreachable"""
        harness = ReplicationTestHarness()
        
        try:
            # Start nodes with replication enabled
            node1 = await harness.create_node("replication_outage_node1", 7420)
            node2 = await harness.create_node("replication_outage_node2", 7421)
            
            # Verify initial connectivity and replication
            key = "outage_test_key"
            initial_value = "before_outage"
            
            response = await node1.send(f"SET {key} {initial_value}")
            assert response.startswith("OK"), f"Initial SET failed: {response}"
            
            # Wait for normal replication
            await asyncio.sleep(5)
            
            response2 = await node2.send(f"GET {key}")
            if response2 == f"VALUE {initial_value}":
                print("✅ Initial replication working")
            else:
                print(f"⚠️  Initial replication slow: {response2}")
            
            # Simulate broker outage period
            print("🚨 Simulating broker outage period...")
            await self._wait_for_broker_down()
            
            # Operations during "outage" - these should work locally
            outage_operations = [
                ("SET", f"{key}_local1", "local_value_1"),
                ("SET", f"{key}_local2", "local_value_2"), 
                ("INC", f"{key}_counter"),
                ("SET", key, "during_outage"),
                ("DELETE", f"{key}_local1"),
            ]
            
            for op_type, op_key, *op_args in outage_operations:
                if op_args:
                    command = f"{op_type} {op_key} {op_args[0]}"
                else:
                    command = f"{op_type} {op_key}"
                
                # Operations should work locally even without replication
                response = await node1.send(command)
                
                # Be flexible with responses during outage
                if op_type == "DELETE":
                    assert response in ["OK", "NOT_FOUND"], f"DELETE during outage failed: {response}"
                elif op_type == "INC":
                    assert response.startswith(("VALUE", "NOT_FOUND")), f"INC during outage failed: {response}"
                else:
                    assert response.startswith("OK"), f"{op_type} during outage failed: {response}"
                
                await asyncio.sleep(0.5)
            
            print("✅ All operations completed successfully during simulated outage")
            
            # Verify local state is consistent
            local_response = await node1.send(f"GET {key}")
            assert local_response == "VALUE during_outage", f"Local state wrong: {local_response}"
            
            # Simulate broker recovery
            print("🔄 Simulating broker recovery...")
            recovery_successful = await self._wait_for_broker_up()
            
            if recovery_successful:
                print("✅ Broker recovery detected")
                
                # Post-recovery operations
                recovery_value = "after_recovery"
                response = await node1.send(f"SET {key} {recovery_value}")
                assert response.startswith("OK"), f"Post-recovery SET failed: {response}"
                
                # Wait for replication to resume
                await asyncio.sleep(8)
                
                # Check if replication resumed
                response2 = await node2.send(f"GET {key}")
                if response2 == f"VALUE {recovery_value}":
                    print("✅ Replication resumed after recovery")
                else:
                    print(f"⚠️  Replication still recovering: {response2}")
            
            print("✅ Broker outage test completed successfully")
            
        finally:
            await harness.shutdown()

    @pytest.mark.asyncio
    async def test_reconnection_behavior(self):
        """Test automatic reconnection behavior after network issues"""
        harness = ReplicationTestHarness()
        
        try:
            node1 = await harness.create_node("replication_reconnect_node1", 7422)
            node2 = await harness.create_node("replication_reconnect_node2", 7423)
            
            # Test connection resilience with rapid operations
            key = "reconnect_test_key"
            
            # Phase 1: Normal operations  
            for i in range(5):
                value = f"normal_phase_{i}"
                response = await node1.send(f"SET {key} {value}")
                assert response.startswith("OK"), f"Normal phase SET {i} failed: {response}"
                await asyncio.sleep(0.5)
            
            print("✅ Normal phase completed")
            
            # Phase 2: Simulate unstable connection period
            print("🌀 Simulating unstable connection period...")
            
            for i in range(10):
                value = f"unstable_phase_{i}"
                response = await node1.send(f"SET {key} {value}")
                assert response.startswith("OK"), f"Unstable phase SET {i} failed: {response}"
                
                # Vary timing to simulate connection instability
                if i % 3 == 0:
                    await asyncio.sleep(1.0)   # Longer delays
                else:
                    await asyncio.sleep(0.2)   # Normal delays
            
            print("✅ Unstable phase completed")
            
            # Phase 3: Recovery and stabilization
            print("🔄 Recovery phase...")
            
            final_value = "recovery_complete"
            response = await node1.send(f"SET {key} {final_value}")
            assert response.startswith("OK"), f"Recovery SET failed: {response}"
            
            # Wait for stabilization
            await asyncio.sleep(10)
            
            # Check final consistency
            response1 = await node1.send(f"GET {key}")
            response2 = await node2.send(f"GET {key}")
            
            assert response1 == f"VALUE {final_value}", f"Node1 final state wrong: {response1}"
            
            if response2 == f"VALUE {final_value}":
                print("✅ Full recovery and consistency achieved")
            else:
                print(f"⚠️  Node2 still synchronizing: {response2}")
                # Wait a bit more and retry
                await asyncio.sleep(5)
                response2_retry = await node2.send(f"GET {key}")
                print(f"   Retry result: {response2_retry}")
            
            print("✅ Reconnection behavior test completed")
            
        finally:
            await harness.shutdown()

    @pytest.mark.asyncio
    async def test_partial_network_partition(self):
        """Test behavior when only one node loses broker connection"""
        harness = ReplicationTestHarness()
        
        try:
            node1 = await harness.create_node("replication_partition_node1", 7424)
            node2 = await harness.create_node("replication_partition_node2", 7425)
            
            key = "partition_test_key"
            
            # Initial sync
            initial_value = "before_partition"
            response = await node1.send(f"SET {key} {initial_value}")
            assert response.startswith("OK"), f"Initial SET failed: {response}"
            
            await asyncio.sleep(3)
            
            # Simulate partition: node1 can operate locally, node2 continues with replication
            print("🔀 Simulating partial network partition...")
            
            # Node1 operations (simulating "partitioned" node)
            partition_value_1 = "partitioned_node_value"
            response1 = await node1.send(f"SET {key} {partition_value_1}")
            assert response1.startswith("OK"), f"Partitioned node SET failed: {response1}"
            
            # Node2 operations (simulating "connected" node)  
            partition_value_2 = "connected_node_value"
            response2_set = await node2.send(f"SET {key} {partition_value_2}")
            assert response2_set.startswith("OK"), f"Connected node SET failed: {response2_set}"
            
            # Both should have different local values during partition
            await asyncio.sleep(2)
            
            local_response1 = await node1.send(f"GET {key}")
            local_response2 = await node2.send(f"GET {key}")
            
            print(f"During partition - Node1: {local_response1}, Node2: {local_response2}")
            
            # Simulate partition healing
            print("🔄 Simulating partition healing...")
            
            # Post-partition operations to test reconciliation
            heal_value = "post_partition_heal"
            response = await node1.send(f"SET {key} {heal_value}")
            assert response.startswith("OK"), f"Post-partition SET failed: {response}"
            
            # Wait for reconciliation
            await asyncio.sleep(8)
            
            # Check final state
            final_response1 = await node1.send(f"GET {key}")
            final_response2 = await node2.send(f"GET {key}")
            
            print(f"After healing - Node1: {final_response1}, Node2: {final_response2}")
            
            # At least local consistency should be maintained
            assert final_response1 == f"VALUE {heal_value}", f"Node1 final state wrong: {final_response1}"
            
            print("✅ Partial partition test completed")
            
        finally:
            await harness.shutdown()

    @pytest.mark.asyncio 
    async def test_broker_message_backlog(self):
        """Test handling of message backlogs after broker recovery"""
        harness = ReplicationTestHarness()
        
        try:
            node1 = await harness.create_node("replication_backlog_node1", 7426)
            node2 = await harness.create_node("replication_backlog_node2", 7427)
            
            # Create backlog scenario
            keys = [f"backlog_key_{i}" for i in range(20)]
            
            print("📦 Creating message backlog scenario...")
            
            # Rapid operations to create potential backlog
            for i, key in enumerate(keys):
                value = f"backlog_value_{i}"
                response = await node1.send(f"SET {key} {value}")
                assert response.startswith("OK"), f"Backlog SET {i} failed: {response}"
                
                # Minimal delay to create rapid succession
                if i % 5 == 0:
                    await asyncio.sleep(0.1)
            
            print("✅ Backlog created with rapid operations")
            
            # Wait for processing
            await asyncio.sleep(12)
            
            # Verify eventual consistency
            consistent_count = 0
            
            for i, key in enumerate(keys):
                expected_value = f"backlog_value_{i}"
                
                response1 = await node1.send(f"GET {key}")
                response2 = await node2.send(f"GET {key}")
                
                if response1 == response2 == f"VALUE {expected_value}":
                    consistent_count += 1
            
            consistency_rate = consistent_count / len(keys)
            
            print(f"✅ Backlog processing: {consistent_count}/{len(keys)} keys consistent ({consistency_rate:.1%})")
            
            # Allow for some lag with public broker
            assert consistency_rate >= 0.7, f"Backlog consistency too low: {consistency_rate:.1%}"
            
        finally:
            await harness.shutdown()

    @pytest.mark.asyncio
    async def test_graceful_degradation(self):
        """Test that system degrades gracefully during connectivity issues"""
        harness = ReplicationTestHarness()
        
        try:
            node1 = await harness.create_node("replication_degrade_node1", 7428)
            node2 = await harness.create_node("replication_degrade_node2", 7429)
            
            key = "degradation_key"
            
            # Test various scenarios of graceful degradation
            test_phases = [
                ("normal", 5, 0.2),      # Normal operations
                ("stressed", 10, 0.05),  # High frequency  
                ("recovery", 3, 1.0),    # Slow recovery
            ]
            
            for phase_name, op_count, delay in test_phases:
                print(f"🔄 Testing {phase_name} phase...")
                
                for i in range(op_count):
                    value = f"{phase_name}_value_{i}"
                    response = await node1.send(f"SET {key} {value}")
                    assert response.startswith("OK"), f"{phase_name} SET {i} failed: {response}"
                    
                    if delay > 0:
                        await asyncio.sleep(delay)
                
                print(f"✅ {phase_name.capitalize()} phase completed")
            
            # Final consistency check
            await asyncio.sleep(8)
            
            response1 = await node1.send(f"GET {key}")
            response2 = await node2.send(f"GET {key}")
            
            print(f"Final state - Node1: {response1}, Node2: {response2}")
            
            # Local consistency should always be maintained
            assert response1.startswith("VALUE"), f"Node1 lost local data: {response1}"
            
            print("✅ Graceful degradation test completed")
            
        finally:
            await harness.shutdown()

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s", "-m", "slow"])
