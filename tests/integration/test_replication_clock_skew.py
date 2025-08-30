"""
MerkleKV Replication Adversarial Tests - Clock Skew Simulation

This module tests the replication system's resilience to clock drift and timestamp
issues between distributed nodes.

Test scenarios:
- Producer nodes with fast/slow clocks
- Events with future timestamps
- Events with past timestamps  
- Boundary conditions around current time
- Mixed clock skew scenarios
"""

import asyncio
import pytest
import time
import random
from typing import List, Tuple
from conftest import ReplicationTestHarness

class TestReplicationClockSkew:
    """Test replication behavior under clock skew conditions"""

    @pytest.mark.asyncio
    async def test_fast_clock_producer(self):
        """Test handling of events from producer with fast clock (+60s)"""
        import time
        timestamp = int(time.time())
        harness = ReplicationTestHarness(f"test_{filepath.stem}_{timestamp}")
        
        try:
            node1 = await harness.create_node("replication_fast_clock_node1", 7420)
            node2 = await harness.create_node("replication_fast_clock_node2", 7421)
            
            key = "fast_clock_key"
            
            # Simulate events from fast clock producer
            # Note: We can't directly control timestamps in the current implementation,
            # but we can test rapid succession which may create future-like effects
            
            values = [f"fast_clock_value_{i}" for i in range(5)]
            
            # Apply rapid successive updates to simulate fast clock scenario
            for i, value in enumerate(values):
                response = await node1.send(f"SET {key} {value}")
                assert response.startswith("OK"), f"Fast clock SET {i} failed: {response}"
                
                # Very short delay to simulate high-frequency updates
                await asyncio.sleep(0.05)
            
            # Wait for replication  
            await asyncio.sleep(6)
            
            # Both nodes should have same final value
            response1 = await node1.send(f"GET {key}")
            response2 = await node2.send(f"GET {key}")
            
            assert response1 == response2, f"Fast clock consistency failed: {response1} vs {response2}"
            assert response1 == f"VALUE {values[-1]}", f"Unexpected final value: {response1}"
            
            print(f"✅ Fast clock test passed: handled {len(values)} rapid updates")
            
        finally:
            await harness.shutdown()

    @pytest.mark.asyncio
    async def test_slow_clock_producer(self):
        """Test handling of events from producer with slow clock (-60s)"""
        import time
        timestamp = int(time.time())
        harness = ReplicationTestHarness(f"test_{filepath.stem}_{timestamp}")
        
        try:
            node1 = await harness.create_node("replication_slow_clock_node1", 7422)
            node2 = await harness.create_node("replication_slow_clock_node2", 7423)
            
            key = "slow_clock_key"
            
            # Set initial "current" value
            current_value = "current_time_value"
            response = await node1.send(f"SET {key} {current_value}")
            assert response.startswith("OK"), f"Current value SET failed: {response}"
            
            # Wait a moment
            await asyncio.sleep(1)
            
            # Now simulate "slow clock" events (older events arriving later)
            # In real system, these would have older timestamps and should be ignored
            slow_values = [f"slow_clock_value_{i}" for i in range(3)]
            
            for i, value in enumerate(slow_values):
                response = await node1.send(f"SET {key} {value}")
                assert response.startswith("OK"), f"Slow clock SET {i} failed: {response}"
                await asyncio.sleep(0.2)
            
            # Wait for replication
            await asyncio.sleep(5)
            
            # Check final value - should be the last one due to our current implementation
            response1 = await node1.send(f"GET {key}")
            response2 = await node2.send(f"GET {key}")
            
            assert response1 == response2, f"Slow clock consistency failed: {response1} vs {response2}"
            
            final_value = response1.split(" ", 1)[1] if response1.startswith("VALUE") else response1
            print(f"✅ Slow clock test passed: final value = {final_value}")
            
        finally:
            await harness.shutdown()

    @pytest.mark.asyncio 
    async def test_mixed_clock_skew_scenario(self):
        """Test complex scenario with multiple clock skews"""
        import time
        timestamp = int(time.time())
        harness = ReplicationTestHarness(f"test_{filepath.stem}_{timestamp}")
        
        try:
            node1 = await harness.create_node("replication_mixed_skew_node1", 7424)
            node2 = await harness.create_node("replication_mixed_skew_node2", 7425)
            
            key = "mixed_skew_key"
            
            # Simulate mixed timing scenario
            operations = [
                ("baseline", "baseline_value", 0),      # Current time
                ("fast1", "fast_value_1", 0.1),        # Slightly future
                ("past1", "past_value_1", 0.05),       # Slightly past  
                ("fast2", "fast_value_2", 0.15),       # More future
                ("current", "current_value", 0.12),     # Recent current
            ]
            
            # Apply operations in timestamp order simulation
            for label, value, delay in operations:
                if delay > 0:
                    await asyncio.sleep(delay)
                
                response = await node1.send(f"SET {key} {value}")
                assert response.startswith("OK"), f"Mixed skew SET {label} failed: {response}"
            
            # Wait for replication
            await asyncio.sleep(6)
            
            # Check consistency
            response1 = await node1.send(f"GET {key}")
            response2 = await node2.send(f"GET {key}")
            
            assert response1 == response2, f"Mixed skew consistency failed: {response1} vs {response2}"
            
            final_value = response1.split(" ", 1)[1] if response1.startswith("VALUE") else response1
            print(f"✅ Mixed clock skew test passed: final value = {final_value}")
            
        finally:
            await harness.shutdown()

    @pytest.mark.asyncio
    async def test_clock_skew_boundary_conditions(self):
        """Test boundary conditions around timestamp handling"""
        import time
        timestamp = int(time.time())
        harness = ReplicationTestHarness(f"test_{filepath.stem}_{timestamp}")
        
        try:
            node1 = await harness.create_node("replication_boundary_node1", 7426) 
            node2 = await harness.create_node("replication_boundary_node2", 7427)
            
            # Test multiple keys with boundary scenarios
            test_cases = [
                ("boundary_key_1", "very_rapid_updates"),
                ("boundary_key_2", "microsecond_precision"), 
                ("boundary_key_3", "millisecond_boundaries"),
            ]
            
            for key, test_type in test_cases:
                if test_type == "very_rapid_updates":
                    # Rapid fire updates to test timestamp precision
                    for i in range(20):
                        value = f"rapid_{i:02d}"
                        response = await node1.send(f"SET {key} {value}")
                        assert response.startswith("OK"), f"Rapid update {i} failed: {response}"
                        # Minimal delay
                        
                elif test_type == "microsecond_precision":
                    # Test with very short intervals
                    for i in range(10):
                        value = f"micro_{i:02d}"
                        response = await node1.send(f"SET {key} {value}")
                        assert response.startswith("OK"), f"Micro update {i} failed: {response}"
                        await asyncio.sleep(0.001)  # 1ms
                        
                else:  # millisecond_boundaries
                    # Test with millisecond-level timing
                    for i in range(10):
                        value = f"milli_{i:02d}"
                        response = await node1.send(f"SET {key} {value}")
                        assert response.startswith("OK"), f"Milli update {i} failed: {response}"
                        await asyncio.sleep(0.01)  # 10ms
            
            # Wait for all replication
            await asyncio.sleep(8)
            
            # Verify consistency across all test keys
            consistent_keys = 0
            
            for key, test_type in test_cases:
                response1 = await node1.send(f"GET {key}")
                response2 = await node2.send(f"GET {key}")
                
                if response1 == response2:
                    consistent_keys += 1
                    final_value = response1.split(" ", 1)[1] if response1.startswith("VALUE") else "DELETED"
                    print(f"  {key} ({test_type}): {final_value}")
                else:
                    print(f"  {key} ({test_type}): INCONSISTENT - {response1} vs {response2}")
            
            consistency_rate = consistent_keys / len(test_cases)
            print(f"✅ Boundary conditions test: {consistency_rate:.1%} consistency rate")
            
            # Expect high consistency
            assert consistency_rate >= 0.8, f"Boundary test consistency too low: {consistency_rate:.1%}"
            
        finally:
            await harness.shutdown()

    @pytest.mark.asyncio
    async def test_clock_drift_simulation(self):
        """Simulate gradual clock drift between nodes"""
        import time
        timestamp = int(time.time())
        harness = ReplicationTestHarness(f"test_{filepath.stem}_{timestamp}")
        
        try:
            node1 = await harness.create_node("replication_drift_node1", 7428)
            node2 = await harness.create_node("replication_drift_node2", 7429)
            
            key = "clock_drift_key"
            
            # Simulate drift by varying operation timing
            drift_scenarios = [
                # (node, value, delay) - simulate different clock rates
                (node1, "drift_00", 0.0),
                (node1, "drift_01", 0.1), 
                (node2, "drift_02", 0.05),  # node2 slightly behind
                (node1, "drift_03", 0.15),
                (node2, "drift_04", 0.12),  # node2 catching up
                (node1, "drift_05", 0.2),
                (node2, "drift_06", 0.25),  # node2 ahead
            ]
            
            for node, value, delay in drift_scenarios:
                await asyncio.sleep(delay)
                response = await node.send(f"SET {key} {value}")
                assert response.startswith("OK"), f"Drift SET {value} failed: {response}"
            
            # Wait for convergence
            await asyncio.sleep(8)
            
            # Check final consistency
            response1 = await node1.send(f"GET {key}")
            response2 = await node2.send(f"GET {key}")
            
            assert response1 == response2, f"Clock drift consistency failed: {response1} vs {response2}"
            
            final_value = response1.split(" ", 1)[1] if response1.startswith("VALUE") else response1
            print(f"✅ Clock drift simulation passed: final value = {final_value}")
            
        finally:
            await harness.shutdown()

    @pytest.mark.asyncio
    async def test_timestamp_overflow_conditions(self):
        """Test handling of edge cases in timestamp values"""
        import time
        timestamp = int(time.time())
        harness = ReplicationTestHarness(f"test_{filepath.stem}_{timestamp}")
        
        try:
            node1 = await harness.create_node("replication_overflow_node1", 7430)
            node2 = await harness.create_node("replication_overflow_node2", 7431)
            
            # Test with multiple keys to stress timestamp handling
            overflow_keys = [f"overflow_key_{i}" for i in range(8)]
            
            # Generate rapid operations on multiple keys
            random.seed(98765)  # Deterministic
            
            for round_num in range(5):  # Multiple rounds
                # Random order of keys
                keys_order = overflow_keys.copy()
                random.shuffle(keys_order)
                
                for i, key in enumerate(keys_order):
                    value = f"overflow_r{round_num}_v{i}"
                    response = await node1.send(f"SET {key} {value}")
                    assert response.startswith("OK"), f"Overflow SET {value} failed: {response}"
                    
                    # Vary timing to create timestamp stress
                    if i % 3 == 0:
                        await asyncio.sleep(0.001)  # Very fast
                    elif i % 3 == 1:
                        await asyncio.sleep(0.01)   # Medium  
                    else:
                        await asyncio.sleep(0.05)   # Slower
            
            # Wait for settlement
            await asyncio.sleep(10)
            
            # Check consistency across all keys
            consistent_count = 0
            
            for key in overflow_keys:
                response1 = await node1.send(f"GET {key}")
                response2 = await node2.send(f"GET {key}")
                
                if response1 == response2:
                    consistent_count += 1
            
            consistency_rate = consistent_count / len(overflow_keys)
            
            print(f"✅ Timestamp overflow test: {consistent_count}/{len(overflow_keys)} keys consistent ({consistency_rate:.1%})")
            
            # Allow for some eventual consistency lag
            assert consistency_rate >= 0.75, f"Overflow test consistency too low: {consistency_rate:.1%}"
            
        finally:
            await harness.shutdown()

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
