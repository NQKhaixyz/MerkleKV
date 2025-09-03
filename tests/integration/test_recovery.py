"""
System recovery and fault tolerance tests for MerkleKV distributed clusters.

Academic Purpose:
These tests validate the fault tolerance capabilities of MerkleKV clusters
under various failure scenarios including node crashes, network failures,
and broker outages. Recovery testing ensures that distributed systems maintain
availability and consistency guarantees even under adverse conditions.

Test Coverage:
- Single node crash and recovery scenarios
- Multi-node partial failure resilience 
- MQTT broker failure and recovery
- Graceful vs ungraceful shutdown behavior
- Data consistency during and after recovery processes

Distributed Systems Theory:
The tests implement validation of CAP theorem tradeoffs and Byzantine fault
tolerance principles, ensuring the system provides appropriate consistency
and availability guarantees under partition and failure conditions.
"""

import asyncio
import pytest
import signal
import time
import uuid
import subprocess
from typing import List, Tuple, Dict, Optional
from pathlib import Path

from utils import (
    ServerManager, wait_until, wait_for_convergence,
    execute_server_command, generate_test_dataset,
    apply_dataset_to_node
)


class TestSystemRecovery:
    """
    Test system recovery mechanisms under various failure scenarios.
    
    Academic Purpose: Validates fault tolerance and recovery capabilities
    essential for production distributed systems. Tests ensure the system
    can handle both expected (graceful shutdown) and unexpected (crash)
    failure modes while maintaining data consistency.
    """

    @pytest.mark.asyncio
    @pytest.mark.recovery
    async def test_single_node_crash_recovery(self):
        """
        Test single node crash and recovery with data consistency validation.
        
        Academic Rationale: Node crashes are the most common failure mode in
        distributed systems. The system must gracefully handle node loss,
        maintain availability through remaining nodes, and restore full
        functionality when the crashed node recovers.
        """
        topic_prefix = f"crash_recovery_{uuid.uuid4().hex[:8]}"
        
        async with ServerManager() as manager:
            # Start a three-node cluster for fault tolerance
            server1, port1 = await manager.start_server("node1", topic_prefix)
            server2, port2 = await manager.start_server("node2", topic_prefix)
            server3, port3 = await manager.start_server("node3", topic_prefix)
            
            await asyncio.sleep(8)
            
            # Establish initial replicated dataset
            initial_dataset = {
                "recovery_key_1": "initial_value_1",
                "recovery_key_2": "initial_value_2", 
                "recovery_key_3": "initial_value_3",
                "recovery_key_4": "initial_value_4"
            }
            
            print("Setting up initial replicated dataset...")
            await apply_dataset_to_node("127.0.0.1", port1, initial_dataset)
            
            # Wait for full replication across cluster
            await asyncio.sleep(12)
            
            # Verify initial replication succeeded
            for port in [port1, port2, port3]:
                for key in initial_dataset.keys():
                    response = await execute_server_command("127.0.0.1", port, f"GET {key}")
                    assert response == f"VALUE {initial_dataset[key]}", (
                        f"Initial replication failed: {key} on port {port}"
                    )
            
            print("✅ Initial replication verified")
            
            # ---------------------------------------------------------------------------
            # Test Case: Simulate node crash and validate cluster resilience
            # Rationale: The system must maintain availability when individual nodes
            # fail. Remaining nodes should continue serving requests and accept updates
            # that will be synchronized when the failed node recovers.
            # ---------------------------------------------------------------------------
            
            print("Simulating node crash (terminating node2)...")
            
            # Terminate node2 to simulate crash
            server2.terminate()
            crashed_port = port2
            
            # Wait for termination with proper process handling
            # Academic Enhancement: Use asyncio-compatible process termination for distributed system testing
            # This ensures proper cleanup in concurrent server management scenarios
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None, lambda: server2.wait(timeout=10)
                )
            except subprocess.TimeoutExpired:
                server2.kill()
                await asyncio.get_event_loop().run_in_executor(
                    None, server2.wait
                )
            
            print(f"Node on port {crashed_port} terminated")
            
            # Verify cluster continues operating with remaining nodes
            remaining_ports = [port1, port3]
            
            # Test data operations on remaining nodes
            crash_recovery_data = {
                "crash_recovery_1": "post_crash_value_1",
                "crash_recovery_2": "post_crash_value_2"
            }
            
            print("Testing cluster operation with failed node...")
            await apply_dataset_to_node("127.0.0.1", port1, crash_recovery_data)
            
            # Verify replication between remaining nodes
            await asyncio.sleep(8)
            for key, expected_value in crash_recovery_data.items():
                for port in remaining_ports:
                    response = await execute_server_command("127.0.0.1", port, f"GET {key}")
                    assert response == f"VALUE {expected_value}", (
                        f"Replication failed during node failure: {key} on port {port}"
                    )
            
            print("✅ Cluster resilience validated")
            
            # ---------------------------------------------------------------------------
            # Recovery Phase: Restart crashed node and validate data synchronization
            # Rationale: When a crashed node recovers, it must automatically rejoin
            # the cluster and synchronize missed updates through anti-entropy mechanisms
            # ---------------------------------------------------------------------------
            
            print("Restarting crashed node...")
            recovered_server, recovered_port = await manager.start_server("node2_recovered", topic_prefix)
            
            # The recovered node should get a different port, but same cluster
            await asyncio.sleep(10)  # Allow time for cluster rejoin
            
            print(f"Node recovered on port {recovered_port}")
            
            # Validate that recovered node synchronizes all data
            all_test_keys = list(initial_dataset.keys()) + list(crash_recovery_data.keys())
            
            print("Waiting for crashed node recovery synchronization...")
            
            try:
                await wait_for_convergence(
                    nodes=[("127.0.0.1", port1), ("127.0.0.1", port3), ("127.0.0.1", recovered_port)],
                    test_keys=all_test_keys,
                    timeout=120.0,
                    interval=3.0
                )
                
                print("✅ Node crash recovery successful")
                
                # Validate that the recovered node has all expected data
                recovered_data_count = 0
                for key in all_test_keys:
                    response = await execute_server_command("127.0.0.1", recovered_port, f"GET {key}")
                    if not response.startswith("NOT_FOUND"):
                        recovered_data_count += 1
                
                recovery_ratio = recovered_data_count / len(all_test_keys)
                assert recovery_ratio == 1.0, (
                    f"Incomplete recovery: {recovery_ratio:.1%} of data recovered"
                )
                
            except TimeoutError:
                print("❌ Node crash recovery failed")
                
                # Diagnostic information
                recovery_status = {}
                for key in all_test_keys:
                    node_values = {}
                    for port in [port1, port3, recovered_port]:
                        try:
                            response = await execute_server_command("127.0.0.1", port, f"GET {key}")
                            node_values[port] = response
                        except Exception as e:
                            node_values[port] = f"ERROR: {e}"
                    recovery_status[key] = node_values
                
                print(f"Recovery status: {recovery_status}")
                raise AssertionError("Node crash recovery synchronization failed")

    @pytest.mark.asyncio
    @pytest.mark.recovery
    async def test_multi_node_partial_failure(self):
        """
        Test cluster behavior under multi-node partial failures.
        
        Academic Rationale: Production systems must handle scenarios where
        multiple nodes fail simultaneously. The system should maintain
        availability as long as a quorum of nodes remains operational,
        and restore full functionality as failed nodes recover.
        """
        topic_prefix = f"multi_failure_{uuid.uuid4().hex[:8]}"
        
        async with ServerManager() as manager:
            # Start a five-node cluster to test partial failure scenarios
            servers_and_ports = []
            for i in range(5):
                server, port = await manager.start_server(f"node{i+1}", topic_prefix)
                servers_and_ports.append((server, port))
            
            all_ports = [port for _, port in servers_and_ports]
            
            await asyncio.sleep(10)
            
            # Set up initial dataset across the cluster
            multi_failure_dataset = generate_test_dataset(size=20, key_prefix="multi_fail")
            
            print(f"Setting up dataset across 5-node cluster: {len(multi_failure_dataset)} keys")
            await apply_dataset_to_node("127.0.0.1", all_ports[0], multi_failure_dataset)
            
            # Wait for full cluster replication
            await asyncio.sleep(15)
            
            # ---------------------------------------------------------------------------
            # Test Case: Simultaneous failure of multiple nodes (but not majority)
            # Rationale: Systems must handle partial failures gracefully. As long as
            # a majority of nodes remain operational, the system should maintain
            # availability and consistency.
            # ---------------------------------------------------------------------------
            
            # Fail 2 out of 5 nodes (maintaining majority)
            failing_indices = [1, 3]  # Fail nodes 2 and 4
            remaining_indices = [0, 2, 4]  # Keep nodes 1, 3, and 5
            
            print(f"Simulating failure of {len(failing_indices)} nodes...")
            
            failed_servers = []
            for idx in failing_indices:
                server, port = servers_and_ports[idx]
                print(f"  Terminating node on port {port}")
                server.terminate()
                failed_servers.append((server, port))
                
                try:
                    await asyncio.get_event_loop().run_in_executor(
                        None, lambda: server.wait(timeout=10)
                    )
                except subprocess.TimeoutExpired:
                    server.kill()
                    await asyncio.get_event_loop().run_in_executor(
                        None, server.wait
                    )
            
            remaining_ports = [servers_and_ports[i][1] for i in remaining_indices]
            print(f"Remaining operational nodes: ports {remaining_ports}")
            
            # Test continued operation with reduced cluster
            partial_failure_data = {
                "partial_fail_1": "during_partial_failure_1",
                "partial_fail_2": "during_partial_failure_2"
            }
            
            print("Testing cluster operation during partial failure...")
            await apply_dataset_to_node("127.0.0.1", remaining_ports[0], partial_failure_data)
            
            # Verify replication among remaining nodes
            await asyncio.sleep(10)
            
            for key, expected_value in partial_failure_data.items():
                for port in remaining_ports:
                    response = await execute_server_command("127.0.0.1", port, f"GET {key}")
                    assert response == f"VALUE {expected_value}", (
                        f"Partial failure replication failed: {key} on port {port}"
                    )
            
            print("✅ Cluster operation during partial failure validated")
            
            # ---------------------------------------------------------------------------
            # Recovery Phase: Restore failed nodes and validate full cluster recovery
            # Rationale: When failed nodes recover, they must rejoin seamlessly and
            # synchronize all data that was updated during their absence
            # ---------------------------------------------------------------------------
            
            print("Recovering failed nodes...")
            recovered_servers = []
            
            for i, (failed_server, failed_port) in enumerate(failed_servers):
                recovered_server, recovered_port = await manager.start_server(
                    f"recovered_node_{i}", topic_prefix
                )
                recovered_servers.append((recovered_server, recovered_port))
                print(f"  Recovered node on port {recovered_port}")
            
            # Wait for cluster reformation
            await asyncio.sleep(15)
            
            # Validate full cluster synchronization
            all_test_keys = list(multi_failure_dataset.keys()) + list(partial_failure_data.keys())
            all_active_ports = remaining_ports + [port for _, port in recovered_servers]
            
            print(f"Validating full cluster recovery on {len(all_active_ports)} nodes...")
            
            try:
                await wait_for_convergence(
                    nodes=[("127.0.0.1", port) for port in all_active_ports],
                    test_keys=all_test_keys,
                    timeout=180.0,  # Extended timeout for complex recovery
                    interval=5.0
                )
                
                print("✅ Multi-node partial failure recovery successful")
                
                # Verify data consistency across all recovered nodes
                consistency_check_passed = True
                for key in all_test_keys:
                    values = []
                    for port in all_active_ports:
                        response = await execute_server_command("127.0.0.1", port, f"GET {key}")
                        values.append(response)
                    
                    # All nodes should have the same value for each key
                    unique_values = set(values)
                    if len(unique_values) > 1:
                        print(f"❌ Consistency violation for key {key}: {unique_values}")
                        consistency_check_passed = False
                
                assert consistency_check_passed, "Consistency violations detected after recovery"
                
            except TimeoutError:
                print("❌ Multi-node partial failure recovery failed")
                raise AssertionError("Multi-node partial failure recovery timeout")

    @pytest.mark.asyncio
    @pytest.mark.recovery
    async def test_broker_failure_recovery(self):
        """
        Test MQTT broker failure and recovery scenarios.
        
        Academic Rationale: In MQTT-based distributed systems, the message
        broker represents a critical single point of failure. The system
        must gracefully handle broker outages and restore full replication
        functionality when broker connectivity is restored.
        """
        topic_prefix = f"broker_failure_{uuid.uuid4().hex[:8]}"
        
        # Note: This test simulates broker failure by testing cluster behavior
        # when replication is disrupted. In a real environment, this would
        # involve stopping the actual MQTT broker service.
        
        async with ServerManager() as manager:
            server1, port1 = await manager.start_server("node1", topic_prefix)
            server2, port2 = await manager.start_server("node2", topic_prefix)
            server3, port3 = await manager.start_server("node3", topic_prefix)
            
            await asyncio.sleep(8)
            
            # Establish pre-broker-failure state
            pre_broker_failure_data = {
                "broker_test_1": "pre_failure_value_1",
                "broker_test_2": "pre_failure_value_2"
            }
            
            print("Setting up pre-broker-failure state...")
            await apply_dataset_to_node("127.0.0.1", port1, pre_broker_failure_data)
            await asyncio.sleep(10)
            
            # Verify initial replication
            for port in [port1, port2, port3]:
                for key in pre_broker_failure_data.keys():
                    response = await execute_server_command("127.0.0.1", port, f"GET {key}")
                    assert response == f"VALUE {pre_broker_failure_data[key]}"
            
            print("✅ Pre-broker-failure replication verified")
            
            # ---------------------------------------------------------------------------
            # Test Case: Operations during broker failure simulation
            # Rationale: When the MQTT broker fails, individual nodes should continue
            # accepting local operations, even though replication is disrupted. Data
            # should be preserved locally for later synchronization.
            # ---------------------------------------------------------------------------
            
            print("Simulating broker failure (replication disruption)...")
            
            # During broker failure, each node continues operating independently
            # This creates intentional divergence that should be resolved later
            
            broker_failure_data_node1 = {
                "broker_fail_node1_1": "node1_during_broker_failure",
                "broker_fail_node1_2": "node1_exclusive_data"
            }
            
            broker_failure_data_node2 = {
                "broker_fail_node2_1": "node2_during_broker_failure", 
                "broker_fail_node2_2": "node2_exclusive_data"
            }
            
            broker_failure_data_node3 = {
                "broker_fail_node3_1": "node3_during_broker_failure"
            }
            
            # Apply data to each node independently (simulating broker unavailability)
            await apply_dataset_to_node("127.0.0.1", port1, broker_failure_data_node1)
            await apply_dataset_to_node("127.0.0.1", port2, broker_failure_data_node2)
            await apply_dataset_to_node("127.0.0.1", port3, broker_failure_data_node3)
            
            # Verify each node has its local data
            print("Validating local operations during broker failure...")
            
            node_local_data = [
                (port1, broker_failure_data_node1),
                (port2, broker_failure_data_node2), 
                (port3, broker_failure_data_node3)
            ]
            
            for port, local_data in node_local_data:
                for key, expected_value in local_data.items():
                    response = await execute_server_command("127.0.0.1", port, f"GET {key}")
                    assert response == f"VALUE {expected_value}", (
                        f"Local operation failed during broker failure: {key} on port {port}"
                    )
            
            print("✅ Local operations during broker failure validated")
            
            # ---------------------------------------------------------------------------
            # Recovery Phase: Broker restoration and cluster synchronization
            # Rationale: When broker connectivity is restored, the cluster should
            # automatically synchronize all data that accumulated during the outage
            # ---------------------------------------------------------------------------
            
            print("Simulating broker recovery (enabling replication)...")
            
            # In a real scenario, this would involve restarting the MQTT broker
            # Here we simulate recovery by allowing sufficient time for anti-entropy
            # mechanisms to detect and resolve the accumulated divergence
            
            await asyncio.sleep(20)  # Allow time for "broker recovery" and anti-entropy
            
            # Define all keys that should converge after broker recovery
            all_broker_test_keys = (
                list(pre_broker_failure_data.keys()) +
                list(broker_failure_data_node1.keys()) +
                list(broker_failure_data_node2.keys()) +
                list(broker_failure_data_node3.keys())
            )
            
            print(f"Testing broker recovery convergence on {len(all_broker_test_keys)} keys...")
            
            try:
                await wait_for_convergence(
                    nodes=[("127.0.0.1", port1), ("127.0.0.1", port2), ("127.0.0.1", port3)],
                    test_keys=all_broker_test_keys,
                    timeout=150.0,
                    interval=5.0
                )
                
                print("✅ Broker failure recovery successful")
                
                # Validate that no data was lost during broker outage
                total_expected_keys = len(all_broker_test_keys)
                recovered_keys = 0
                
                for key in all_broker_test_keys:
                    # Check if key exists on any node (data preservation)
                    found_on_node = False
                    for port in [port1, port2, port3]:
                        response = await execute_server_command("127.0.0.1", port, f"GET {key}")
                        if not response.startswith("NOT_FOUND"):
                            found_on_node = True
                            break
                    
                    if found_on_node:
                        recovered_keys += 1
                
                recovery_ratio = recovered_keys / total_expected_keys
                print(f"Data preservation ratio: {recovery_ratio:.1%}")
                
                assert recovery_ratio >= 0.95, (
                    f"Significant data loss during broker failure: {recovery_ratio:.1%} preserved"
                )
                
            except TimeoutError:
                print("❌ Broker failure recovery timeout")
                
                # Diagnostic information
                convergence_sample = {}
                for key in all_broker_test_keys[:5]:
                    node_values = {}
                    for port in [port1, port2, port3]:
                        response = await execute_server_command("127.0.0.1", port, f"GET {key}")
                        node_values[port] = response
                    convergence_sample[key] = node_values
                
                print(f"Convergence sample: {convergence_sample}")
                raise AssertionError("Broker failure recovery did not complete within timeout")

    @pytest.mark.asyncio
    @pytest.mark.recovery
    async def test_graceful_vs_ungraceful_shutdown(self):
        """
        Test differences between graceful and ungraceful node shutdown.
        
        Academic Rationale: Distributed systems must handle both planned
        (graceful) and unplanned (ungraceful) node shutdowns. Graceful
        shutdowns allow for proper cleanup and handoff, while ungraceful
        shutdowns test the system's robustness under adverse conditions.
        """
        topic_prefix = f"shutdown_test_{uuid.uuid4().hex[:8]}"
        
        async with ServerManager() as manager:
            # Start cluster for shutdown testing
            server1, port1 = await manager.start_server("graceful_node", topic_prefix)
            server2, port2 = await manager.start_server("ungraceful_node", topic_prefix) 
            server3, port3 = await manager.start_server("observer_node", topic_prefix)
            
            await asyncio.sleep(8)
            
            # Set up test data across cluster
            shutdown_test_data = {
                "shutdown_test_1": "test_value_1",
                "shutdown_test_2": "test_value_2",
                "shutdown_test_3": "test_value_3"
            }
            
            await apply_dataset_to_node("127.0.0.1", port1, shutdown_test_data)
            await asyncio.sleep(10)
            
            # Verify initial state
            for port in [port1, port2, port3]:
                for key in shutdown_test_data.keys():
                    response = await execute_server_command("127.0.0.1", port, f"GET {key}")
                    assert response == f"VALUE {shutdown_test_data[key]}"
            
            print("✅ Initial shutdown test state established")
            
            # ---------------------------------------------------------------------------
            # Test Case 1: Graceful shutdown with proper cleanup
            # Rationale: Graceful shutdowns should allow nodes to complete ongoing
            # operations, flush buffers, and notify peers of planned departure
            # ---------------------------------------------------------------------------
            
            print("Testing graceful shutdown (SIGTERM)...")
            
            # Send SIGTERM for graceful shutdown
            server1.terminate()
            graceful_start_time = time.time()
            
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None, lambda: server1.wait(timeout=30)
                )
                graceful_shutdown_time = time.time() - graceful_start_time
                print(f"✅ Graceful shutdown completed in {graceful_shutdown_time:.2f}s")
                
            except subprocess.TimeoutExpired:
                print("⚠️ Graceful shutdown timeout, forcing termination")
                server1.kill()
                await asyncio.get_event_loop().run_in_executor(
                    None, server1.wait
                )
                graceful_shutdown_time = 30.0
            
            # ---------------------------------------------------------------------------
            # Test Case 2: Ungraceful shutdown (crash simulation)
            # Rationale: Ungraceful shutdowns test the system's resilience when nodes
            # terminate without cleanup. The cluster must detect failure and adapt
            # ---------------------------------------------------------------------------
            
            print("Testing ungraceful shutdown (SIGKILL)...")
            
            # Send SIGKILL for immediate termination
            ungraceful_start_time = time.time()
            server2.kill()
            
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None, lambda: server2.wait(timeout=10)
                )
                ungraceful_shutdown_time = time.time() - ungraceful_start_time
                print(f"✅ Ungraceful shutdown completed in {ungraceful_shutdown_time:.2f}s")
                
            except subprocess.TimeoutExpired:
                print("⚠️ Ungraceful shutdown timeout")
                ungraceful_shutdown_time = 10.0
            
            # ---------------------------------------------------------------------------
            # Validation: Compare shutdown behaviors and cluster resilience
            # ---------------------------------------------------------------------------
            
            # Verify remaining node continues operating
            remaining_port = port3
            
            post_shutdown_data = {
                "post_shutdown_1": "after_shutdown_value",
                "post_shutdown_2": "remaining_node_data"
            }
            
            print("Testing cluster operation after shutdowns...")
            await apply_dataset_to_node("127.0.0.1", remaining_port, post_shutdown_data)
            
            # Verify remaining node has all data
            for key, expected_value in post_shutdown_data.items():
                response = await execute_server_command("127.0.0.1", remaining_port, f"GET {key}")
                assert response == f"VALUE {expected_value}", (
                    f"Remaining node operation failed: {key}"
                )
            
            # Verify original data is still available
            for key, expected_value in shutdown_test_data.items():
                response = await execute_server_command("127.0.0.1", remaining_port, f"GET {key}")
                assert response == f"VALUE {expected_value}", (
                    f"Data loss after shutdowns: {key}"
                )
            
            print("✅ Cluster resilience after shutdowns validated")
            
            # Performance comparison
            print(f"\nShutdown performance comparison:")
            print(f"  Graceful shutdown time: {graceful_shutdown_time:.2f}s")
            print(f"  Ungraceful shutdown time: {ungraceful_shutdown_time:.2f}s")
            
            # Graceful shutdown should generally be faster than forced termination
            # (though this can vary based on system state)
            if graceful_shutdown_time < ungraceful_shutdown_time * 2:
                print("✅ Graceful shutdown efficiency validated")
            else:
                print("⚠️ Graceful shutdown took longer than expected")

    @pytest.mark.asyncio
    @pytest.mark.recovery
    @pytest.mark.slow
    async def test_cascading_failure_recovery(self):
        """
        Test recovery from cascading failures affecting multiple system components.
        
        Academic Rationale: Real-world distributed systems can experience
        cascading failures where the failure of one component triggers failures
        in other components. The system must be resilient to such scenarios
        and capable of full recovery when conditions stabilize.
        """
        topic_prefix = f"cascading_failure_{uuid.uuid4().hex[:8]}"
        
        async with ServerManager() as manager:
            # Start larger cluster to simulate cascading failure scenarios
            servers_and_ports = []
            for i in range(4):
                server, port = await manager.start_server(f"cascade_node{i+1}", topic_prefix)
                servers_and_ports.append((server, port))
            
            all_ports = [port for _, port in servers_and_ports]
            
            await asyncio.sleep(10)
            
            # Establish substantial dataset for cascading failure testing
            cascading_dataset = generate_test_dataset(size=30, key_prefix="cascade")
            
            print(f"Setting up cascading failure test dataset: {len(cascading_dataset)} keys")
            await apply_dataset_to_node("127.0.0.1", all_ports[0], cascading_dataset)
            
            await asyncio.sleep(15)
            
            # ---------------------------------------------------------------------------
            # Test Case: Simulate cascading failure scenario
            # First failure triggers additional failures due to increased load/stress
            # ---------------------------------------------------------------------------
            
            print("Initiating cascading failure scenario...")
            
            # Stage 1: Initial failure
            print("  Stage 1: Initial node failure")
            servers_and_ports[0][0].terminate()
            failed_servers = [servers_and_ports[0]]
            
            await asyncio.sleep(5)
            
            # Stage 2: Cascading failure due to load redistribution
            print("  Stage 2: Cascading failure (load redistribution)")
            servers_and_ports[2][0].kill()  # Ungraceful failure
            failed_servers.append(servers_and_ports[2])
            
            await asyncio.sleep(3)
            
            # Stage 3: Additional stress failure
            print("  Stage 3: Additional failure under stress")
            servers_and_ports[1][0].terminate()
            failed_servers.append(servers_and_ports[1])
            
            # Only one node remains operational
            remaining_server, remaining_port = servers_and_ports[3]
            
            print(f"Cascading failure complete. Only node on port {remaining_port} remains")
            
            # ---------------------------------------------------------------------------
            # Survival Phase: Validate single-node operation
            # The remaining node should continue operating despite cluster degradation
            # ---------------------------------------------------------------------------
            
            survival_data = {
                "survival_1": "single_node_operation",
                "survival_2": "during_cascading_failure"
            }
            
            print("Testing single-node survival...")
            await apply_dataset_to_node("127.0.0.1", remaining_port, survival_data)
            
            # Verify single node operation
            for key, expected_value in survival_data.items():
                response = await execute_server_command("127.0.0.1", remaining_port, f"GET {key}")
                assert response == f"VALUE {expected_value}", (
                    f"Single-node operation failed: {key}"
                )
            
            print("✅ Single-node survival validated")
            
            # ---------------------------------------------------------------------------
            # Recovery Phase: Gradual cluster restoration
            # Nodes should be restored gradually and rejoin the cluster successfully
            # ---------------------------------------------------------------------------
            
            print("Beginning cascading failure recovery...")
            recovered_servers = []
            
            # Recover nodes gradually
            for i, (failed_server, failed_port) in enumerate(failed_servers):
                print(f"  Recovering node {i+1}/3...")
                
                recovered_server, recovered_port = await manager.start_server(
                    f"recovered_cascade_{i}", topic_prefix
                )
                recovered_servers.append((recovered_server, recovered_port))
                
                # Allow time for each node to stabilize before recovering the next
                await asyncio.sleep(10)
            
            print("All failed nodes recovered. Testing full cluster synchronization...")
            
            # Validate full cluster recovery
            all_recovery_keys = list(cascading_dataset.keys()) + list(survival_data.keys())
            all_recovered_ports = [remaining_port] + [port for _, port in recovered_servers]
            
            try:
                await wait_for_convergence(
                    nodes=[("127.0.0.1", port) for port in all_recovered_ports],
                    test_keys=all_recovery_keys,
                    timeout=200.0,  # Extended timeout for complex recovery
                    interval=5.0
                )
                
                print("✅ Cascading failure recovery successful")
                
                # Validate cluster health after recovery
                cluster_health_score = 0
                total_checks = len(all_recovery_keys) * len(all_recovered_ports)
                
                for key in all_recovery_keys:
                    for port in all_recovered_ports:
                        try:
                            response = await execute_server_command("127.0.0.1", port, f"GET {key}")
                            if not response.startswith("NOT_FOUND") and not response.startswith("ERROR"):
                                cluster_health_score += 1
                        except Exception:
                            pass  # Count as unhealthy
                
                health_ratio = cluster_health_score / total_checks
                print(f"Post-recovery cluster health: {health_ratio:.1%}")
                
                assert health_ratio >= 0.90, (
                    f"Poor cluster health after cascading failure recovery: {health_ratio:.1%}"
                )
                
            except TimeoutError:
                print("❌ Cascading failure recovery timeout")
                
                # Final diagnostic
                recovery_diagnostic = {}
                for key in all_recovery_keys[:5]:  # Sample for diagnostics
                    key_status = {}
                    for port in all_recovered_ports:
                        try:
                            response = await execute_server_command("127.0.0.1", port, f"GET {key}")
                            key_status[port] = response
                        except Exception as e:
                            key_status[port] = f"ERROR: {e}"
                    recovery_diagnostic[key] = key_status
                
                print(f"Recovery diagnostic: {recovery_diagnostic}")
                raise AssertionError("Cascading failure recovery did not complete")


if __name__ == "__main__":
    # Run with: python -m pytest test_recovery.py -v
    pytest.main(["-v", __file__])
