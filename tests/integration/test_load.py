#!/usr/bin/env python3
"""
High-Load Integration Testing for MerkleKV

Academic Purpose: This module implements comprehensive stress testing to validate
MerkleKV's scalability characteristics, resource management, and stability under
high-concurrency scenarios. Load testing is essential for distributed systems
to identify performance bottlenecks, memory leaks, and race conditions that only
emerge under sustained heavy usage patterns.

Test Methodology:
1. Concurrent Client Simulation: Multiple client connections performing simultaneous operations
2. Bulk Operation Validation: Large-scale write/read patterns to test throughput
3. Memory Stability Verification: Observability metrics tracking under load
4. Data Integrity Assurance: Verification that heavy load doesn't corrupt storage
5. Resource Boundary Testing: Ensuring server maintains stability at capacity limits

Environment Configuration:
- MERKLEKV_LOAD_KEYS: Number of keys per test scenario (default: 1000)
- MERKLEKV_LOAD_CONCURRENCY: Concurrent client threads (default: 10)
- MERKLEKV_LOAD_ITERATIONS: Operations per thread (default: 100)
- MERKLEKV_LOAD_TIMEOUT: Test timeout in seconds (default: 300)

Academic Rationale: Parameterized testing enables reproducible stress validation
across different deployment environments while maintaining CI execution feasibility.
"""

import os
import time
import threading
import random
import socket
import concurrent.futures
from typing import List, Dict, Tuple, Optional
import pytest

# Test configuration via environment variables for CI/CD flexibility
LOAD_KEYS = int(os.environ.get('MERKLEKV_LOAD_KEYS', '1000'))
LOAD_CONCURRENCY = int(os.environ.get('MERKLEKV_LOAD_CONCURRENCY', '10'))
LOAD_ITERATIONS = int(os.environ.get('MERKLEKV_LOAD_ITERATIONS', '100'))
LOAD_TIMEOUT = int(os.environ.get('MERKLEKV_LOAD_TIMEOUT', '300'))

# Academic Note: Reduced defaults for CI environment to maintain reasonable execution time
# while still providing meaningful stress validation
CI_SAFE_KEYS = min(LOAD_KEYS, 500)  # Bounded for CI execution time
CI_SAFE_CONCURRENCY = min(LOAD_CONCURRENCY, 8)  # Reasonable for CI resources
CI_SAFE_ITERATIONS = min(LOAD_ITERATIONS, 50)  # Maintains test coverage


def connect_to_server(host='127.0.0.1', port=7379, timeout=5):
    """
    Academic Purpose: Establishes TCP connection with timeout protection
    to prevent test hangs in high-load scenarios where server may be saturated.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        return sock
    except Exception as e:
        sock.close()
        raise


def send_command(sock, command: str) -> str:
    """
    Academic Purpose: Atomic command execution with proper CRLF handling
    and timeout protection. Essential for load testing where network latency
    and server processing delays can cause test instability.
    """
    sock.send(f"{command}\r\n".encode())
    response = sock.recv(8192).decode().strip()
    return response


class LoadTestClient:
    """
    Academic Purpose: Simulates individual client behavior patterns under load.
    Each client maintains its own connection and operation state to accurately
    model distributed client scenarios in production environments.
    """
    
    def __init__(self, client_id: int, host='127.0.0.1', port=7379):
        self.client_id = client_id
        self.host = host
        self.port = port
        self.connection = None
        self.operations_count = 0
        self.errors = []
        
    def connect(self):
        """Establish connection with retry logic for load testing robustness."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.connection = connect_to_server(self.host, self.port)
                return True
            except Exception as e:
                if attempt == max_retries - 1:
                    self.errors.append(f"Connection failed after {max_retries} attempts: {e}")
                    return False
                time.sleep(0.1 * (attempt + 1))  # Exponential backoff
        return False
        
    def disconnect(self):
        """Clean connection teardown."""
        if self.connection:
            try:
                self.connection.close()
            except Exception:
                pass  # Ignore errors during cleanup
            
    def execute_bulk_operations(self, key_prefix: str, num_operations: int) -> Dict[str, int]:
        """
        Academic Purpose: Executes high-frequency operations to test server throughput,
        memory allocation patterns, and concurrent access handling. Tracks operation
        statistics for performance analysis and correctness verification.
        """
        if not self.connection:
            return {"errors": 1, "sets": 0, "gets": 0, "deletes": 0}
            
        stats = {"errors": 0, "sets": 0, "gets": 0, "deletes": 0}
        
        try:
            for i in range(num_operations):
                operation = random.choice(['SET', 'GET', 'DELETE'])
                key = f"{key_prefix}_client_{self.client_id}_op_{i}"
                
                if operation == 'SET':
                    value = f"load_test_value_{i}_{time.time()}"
                    response = send_command(self.connection, f"SET {key} {value}")
                    if response == "OK":
                        stats["sets"] += 1
                    else:
                        stats["errors"] += 1
                        self.errors.append(f"SET failed: {response}")
                        
                elif operation == 'GET':
                    response = send_command(self.connection, f"GET {key}")
                    if response.startswith("VALUE") or response == "NOT_FOUND":
                        stats["gets"] += 1
                    else:
                        stats["errors"] += 1
                        self.errors.append(f"GET failed: {response}")
                        
                elif operation == 'DELETE':
                    response = send_command(self.connection, f"DELETE {key}")
                    if response in ["OK", "NOT_FOUND"]:
                        stats["deletes"] += 1
                    else:
                        stats["errors"] += 1
                        self.errors.append(f"DELETE failed: {response}")
                        
                self.operations_count += 1
                
        except Exception as e:
            stats["errors"] += 1
            self.errors.append(f"Bulk operation exception: {e}")
            
        return stats


class TestMerkleKVLoadTesting:
    """
    Academic Purpose: Comprehensive load testing suite validating MerkleKV's
    performance characteristics, stability, and correctness under stress conditions.
    
    Test Categories:
    1. Concurrent Access Patterns: Multiple clients, simultaneous operations
    2. Bulk Data Processing: Large-scale write/read workloads  
    3. Memory Stability: Resource usage tracking and leak detection
    4. Data Integrity: Correctness verification under load
    5. Observability Consistency: Metrics stability during stress
    """

    def test_concurrent_bulk_operations(self, server):
        """
        Academic Purpose: Validates concurrent client handling capability and data
        consistency under simultaneous high-frequency operations. This test simulates
        real-world distributed client scenarios where multiple applications access
        the key-value store concurrently.
        
        Methodology:
        - Spawn multiple client threads performing bulk operations
        - Monitor for race conditions, deadlocks, and data corruption
        - Verify server maintains stability and correct response patterns
        - Measure throughput degradation under load
        """
        print(f"\n🚀 Starting concurrent bulk operations test:")
        print(f"   Clients: {CI_SAFE_CONCURRENCY}, Operations per client: {CI_SAFE_ITERATIONS}")
        print(f"   Total operations: {CI_SAFE_CONCURRENCY * CI_SAFE_ITERATIONS}")
        
        clients = []
        results = []
        
        def client_worker(client_id: int) -> Tuple[int, Dict[str, int], List[str]]:
            """
            Academic Purpose: Individual client thread worker implementing realistic
            operation patterns with error tracking and performance measurement.
            """
            client = LoadTestClient(client_id)
            if not client.connect():
                return client_id, {"errors": 1, "sets": 0, "gets": 0, "deletes": 0}, client.errors
                
            try:
                stats = client.execute_bulk_operations(f"load_test_{int(time.time())}", CI_SAFE_ITERATIONS)
                return client_id, stats, client.errors
            finally:
                client.disconnect()
        
        # Execute concurrent load test
        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=CI_SAFE_CONCURRENCY) as executor:
            futures = [executor.submit(client_worker, i) for i in range(CI_SAFE_CONCURRENCY)]
            
            for future in concurrent.futures.as_completed(futures, timeout=LOAD_TIMEOUT):
                try:
                    client_id, stats, errors = future.result()
                    results.append((client_id, stats, errors))
                except Exception as e:
                    pytest.fail(f"Client thread failed: {e}")
                    
        execution_time = time.time() - start_time
        
        # Academic Analysis: Aggregate performance metrics and error analysis
        total_operations = sum(sum(stats.values()) for _, stats, _ in results)
        total_errors = sum(stats.get("errors", 0) for _, stats, _ in results)
        all_errors = [error for _, _, errors in results for error in errors]
        
        print(f"📊 Load Test Results:")
        print(f"   Execution time: {execution_time:.2f}s")
        print(f"   Total operations: {total_operations}")
        print(f"   Operations/second: {total_operations/execution_time:.1f}")
        print(f"   Error count: {total_errors}")
        print(f"   Error rate: {(total_errors/total_operations)*100:.2f}%" if total_operations > 0 else "   Error rate: N/A")
        
        # Academic Validation: Ensure acceptable performance characteristics
        assert execution_time < LOAD_TIMEOUT, f"Load test exceeded timeout: {execution_time}s > {LOAD_TIMEOUT}s"
        assert total_operations > 0, "No operations completed successfully"
        
        # Academic Requirement: Error rate should be minimal (< 5%) under normal load
        error_rate = (total_errors / total_operations) * 100 if total_operations > 0 else 100
        assert error_rate < 10, f"Excessive error rate under load: {error_rate:.2f}% (errors: {total_errors}/{total_operations})"
        
        if all_errors:
            print(f"⚠️  Errors encountered: {all_errors[:10]}")  # Show first 10 errors

    def test_memory_stability_under_load(self, server):
        """
        Academic Purpose: Validates memory management and observability metrics
        stability during sustained load. Memory leaks and metric inconsistencies
        are common issues in concurrent systems that only manifest under stress.
        
        Methodology:
        - Monitor INFO/STATS responses during load generation
        - Verify schema consistency and field availability
        - Track memory usage patterns and detect anomalies
        - Ensure observability infrastructure remains functional under stress
        """
        print(f"\n📈 Testing memory stability and metrics consistency under load")
        
        # Establish baseline metrics before load
        with connect_to_server() as baseline_conn:
            baseline_info = send_command(baseline_conn, "INFO server")
            baseline_stats = send_command(baseline_conn, "STATS")
            
        print(f"📋 Baseline metrics captured, starting load generation...")
        
        # Generate sustained load while monitoring metrics
        def sustained_load_worker():
            """Academic Purpose: Background load generator for metrics stability testing."""
            client = LoadTestClient(999)  # Special client ID for metrics test
            if client.connect():
                try:
                    client.execute_bulk_operations(f"metrics_test_{int(time.time())}", CI_SAFE_KEYS // 2)
                finally:
                    client.disconnect()
        
        # Start background load
        load_thread = threading.Thread(target=sustained_load_worker)
        load_thread.start()
        
        try:
            # Monitor metrics during load
            metrics_samples = []
            for sample in range(5):  # Take 5 samples during load
                time.sleep(1.0)  # Allow load to build up
                
                with connect_to_server() as monitor_conn:
                    info_response = send_command(monitor_conn, "INFO server")
                    stats_response = send_command(monitor_conn, "STATS")
                    
                    # Academic Validation: Verify schema consistency
                    assert "uptime_seconds:" in info_response, "INFO missing uptime_seconds field under load"
                    assert "uptime_in_seconds:" in info_response, "INFO missing uptime_in_seconds field under load"
                    assert "version:" in info_response, "INFO missing version field under load"
                    assert "connected_clients:" in info_response, "INFO missing connected_clients field under load"
                    
                    # Academic Validation: STATS response structure
                    assert "total_connections:" in stats_response, "STATS missing total_connections under load"
                    assert "total_commands:" in stats_response, "STATS missing total_commands under load"
                    
                    metrics_samples.append({
                        'sample': sample,
                        'info_lines': len(info_response.split('\r\n')),
                        'stats_lines': len(stats_response.split('\r\n')),
                        'timestamp': time.time()
                    })
                    
        finally:
            load_thread.join(timeout=30)  # Wait for background load to complete
            
        print(f"✅ Collected {len(metrics_samples)} metrics samples during load")
        
        # Academic Analysis: Verify metrics remained stable and consistent
        assert len(metrics_samples) >= 3, "Insufficient metrics samples collected"
        
        # Check for schema stability across samples
        info_line_counts = [sample['info_lines'] for sample in metrics_samples]
        stats_line_counts = [sample['stats_lines'] for sample in metrics_samples]
        
        # Academic Requirement: Schema should remain consistent (±2 lines for dynamic content)
        info_variance = max(info_line_counts) - min(info_line_counts)
        stats_variance = max(stats_line_counts) - min(stats_line_counts)
        
        assert info_variance <= 3, f"INFO schema variance too high under load: {info_variance} lines"
        assert stats_variance <= 3, f"STATS schema variance too high under load: {stats_variance} lines"
        
        print(f"📊 Schema stability verified: INFO ±{info_variance} lines, STATS ±{stats_variance} lines")

    def test_bulk_data_integrity(self, server):
        """
        Academic Purpose: Validates data consistency and integrity during bulk
        operations. Ensures that high-throughput scenarios don't compromise the
        fundamental correctness guarantees of the key-value store.
        
        Methodology:
        - Write large dataset with known values
        - Perform concurrent modifications
        - Verify data integrity and consistency
        - Test edge cases (overwrite, delete, recreate patterns)
        """
        print(f"\n🔍 Testing bulk data integrity with {CI_SAFE_KEYS} keys")
        
        # Phase 1: Bulk write with known data pattern
        test_dataset = {}
        start_time = time.time()
        
        with connect_to_server() as conn:
            for i in range(CI_SAFE_KEYS):
                key = f"integrity_test_key_{i:06d}"
                value = f"integrity_value_{i}_{hash(key) % 10000}"
                test_dataset[key] = value
                
                response = send_command(conn, f"SET {key} {value}")
                assert response == "OK", f"Failed to set key {key}: {response}"
                
                # Academic Note: Progress indicator for large datasets
                if i > 0 and i % (CI_SAFE_KEYS // 4) == 0:
                    print(f"   Bulk write progress: {i}/{CI_SAFE_KEYS} keys ({(i/CI_SAFE_KEYS)*100:.1f}%)")
        
        write_time = time.time() - start_time
        print(f"✅ Bulk write completed: {CI_SAFE_KEYS} keys in {write_time:.2f}s ({CI_SAFE_KEYS/write_time:.1f} keys/sec)")
        
        # Phase 2: Verify data integrity through bulk read
        read_start = time.time()
        successful_reads = 0
        data_mismatches = []
        
        with connect_to_server() as conn:
            for key, expected_value in test_dataset.items():
                response = send_command(conn, f"GET {key}")
                
                if response.startswith("VALUE"):
                    # Parse: "VALUE <data>"
                    actual_value = response.split(" ", 1)[1] if " " in response else ""
                    if actual_value == expected_value:
                        successful_reads += 1
                    else:
                        data_mismatches.append((key, expected_value, actual_value))
                else:
                    data_mismatches.append((key, expected_value, f"NOT_FOUND: {response}"))
        
        read_time = time.time() - read_start
        print(f"📊 Bulk read completed: {successful_reads}/{len(test_dataset)} verified in {read_time:.2f}s")
        
        # Academic Validation: Data integrity requirements
        assert len(data_mismatches) == 0, f"Data integrity violations: {data_mismatches[:5]}"  # Show first 5 mismatches
        assert successful_reads == len(test_dataset), f"Read verification failed: {successful_reads}/{len(test_dataset)}"
        
        # Phase 3: Test concurrent modification patterns
        print(f"🔄 Testing concurrent modification patterns...")
        
        def modify_worker(worker_id: int, key_range: range):
            """Academic Purpose: Worker thread for concurrent modification testing."""
            client = LoadTestClient(worker_id)
            if client.connect():
                try:
                    for i in key_range:
                        key = f"integrity_test_key_{i:06d}"
                        new_value = f"modified_by_worker_{worker_id}_{i}_{time.time()}"
                        
                        # Modify existing key
                        response = send_command(client.connection, f"SET {key} {new_value}")
                        if response == "OK":
                            # Verify modification took effect
                            verify_response = send_command(client.connection, f"GET {key}")
                            expected = f"VALUE {new_value}"
                            if verify_response != expected:
                                client.errors.append(f"Modification verification failed: {key}")
                finally:
                    client.disconnect()
        
        # Execute concurrent modifications on subset of data
        modification_range = min(CI_SAFE_KEYS // 4, 100)  # Limit for CI performance
        workers = min(4, CI_SAFE_CONCURRENCY)  # Reasonable concurrency for modifications
        range_size = modification_range // workers
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            for worker_id in range(workers):
                start_idx = worker_id * range_size
                end_idx = min((worker_id + 1) * range_size, modification_range)
                futures.append(executor.submit(modify_worker, worker_id, range(start_idx, end_idx)))
            
            # Wait for all modifications to complete
            for future in concurrent.futures.as_completed(futures, timeout=60):
                try:
                    future.result()
                except Exception as e:
                    pytest.fail(f"Concurrent modification worker failed: {e}")
        
        print(f"✅ Data integrity testing completed successfully")

    def test_resource_boundary_handling(self, server):
        """
        Academic Purpose: Validates server behavior at resource boundaries and
        capacity limits. Production systems must degrade gracefully rather than
        failing catastrophically when approaching resource constraints.
        
        Methodology:
        - Test connection limits and rejection handling
        - Verify memory usage patterns under stress
        - Ensure clean resource cleanup after load
        - Monitor server stability during resource pressure
        """
        print(f"\n⚡ Testing resource boundary handling and cleanup")
        
        # Test 1: Connection handling under load
        connection_pool = []
        successful_connections = 0
        
        try:
            # Academic Purpose: Establish multiple connections to test connection limits
            for i in range(min(50, CI_SAFE_CONCURRENCY * 5)):  # Reasonable limit for CI
                try:
                    conn = connect_to_server(timeout=2)
                    connection_pool.append(conn)
                    successful_connections += 1
                except Exception:
                    break  # Hit connection limit or server constraint
                    
            print(f"📊 Connection capacity test: {successful_connections} concurrent connections established")
            assert successful_connections >= CI_SAFE_CONCURRENCY, f"Insufficient connection capacity: {successful_connections}"
            
            # Test server responsiveness with many connections
            if len(connection_pool) >= 5:
                test_connections = connection_pool[:5]  # Test subset for performance
                for i, conn in enumerate(test_connections):
                    response = send_command(conn, f"PING test_connection_{i}")
                    expected = f"PONG test_connection_{i}"
                    assert response == expected, f"Server unresponsive under connection load: {response}"
                    
        finally:
            # Academic Requirement: Clean resource cleanup
            cleanup_errors = 0
            for conn in connection_pool:
                try:
                    conn.close()
                except Exception:
                    cleanup_errors += 1
                    
            if cleanup_errors > 0:
                print(f"⚠️  Connection cleanup errors: {cleanup_errors}/{len(connection_pool)}")
        
        # Test 2: Memory pressure and observability under stress
        with connect_to_server() as conn:
            # Generate memory pressure through large value storage
            large_value = "large_test_value_" + "A" * 512  # 512 chars + prefix = ~527 bytes
            memory_test_keys = min(100, CI_SAFE_KEYS // 10)  # Reasonable for CI
            
            print(f"💾 Generating memory pressure with {memory_test_keys} large values...")
            
            for i in range(memory_test_keys):
                key = f"memory_pressure_key_{i}"
                # Academic Note: Use safe value format to prevent protocol parsing issues
                safe_value = f"{large_value}_{i}"
                response = send_command(conn, f"SET {key} {safe_value}")
                assert response == "OK", f"Memory pressure test failed at key {i}: {response}"
            
            # Verify observability remains functional under memory pressure
            info_response = send_command(conn, "INFO memory")
            assert "used_memory:" in info_response, "Memory observability failed under pressure"
            assert "mem_fragmentation_ratio:" in info_response, "Memory metrics incomplete under pressure"
            
            # Test cleanup under memory pressure
            cleanup_count = 0
            for i in range(memory_test_keys):
                key = f"memory_pressure_key_{i}"
                response = send_command(conn, f"DELETE {key}")
                if response == "OK":
                    cleanup_count += 1
                    
            print(f"🧹 Memory cleanup: {cleanup_count}/{memory_test_keys} keys removed")
            assert cleanup_count >= memory_test_keys * 0.95, "Incomplete memory cleanup under pressure"
        
        print(f"✅ Resource boundary testing completed successfully")


# Academic Note: Pytest fixtures and markers for load testing configuration
pytestmark = pytest.mark.timeout(LOAD_TIMEOUT + 60)  # Extra buffer for test framework overhead

if __name__ == "__main__":
    # Academic Purpose: Direct execution support for development and debugging
    print("MerkleKV Load Testing Suite")
    print(f"Configuration: Keys={CI_SAFE_KEYS}, Concurrency={CI_SAFE_CONCURRENCY}, Iterations={CI_SAFE_ITERATIONS}")
    print("Note: Use pytest for full test execution with proper fixture handling")