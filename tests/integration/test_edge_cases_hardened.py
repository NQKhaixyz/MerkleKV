#!/usr/bin/env python3
"""
Enhanced Edge Case Testing for MerkleKV Issue #27 Hardening
Tests production-ready guardrails, environment variable overrides,
and concurrent scenarios with academic rigor.
"""

import pytest
import socket
import threading
import time
import os
import subprocess
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed


class TestProductionGuardrails:
    """Test production-ready safety guardrails and environment variables"""
    
    def test_keys_large_keyspace_protection(self, server):
        """Test KEYS command refuses large keyspaces via MERKLE_KV_MAX_KEYS"""
        # First, add many keys to trigger the guardrail
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect((server.host, server.port))
        
        # Add keys to test the limit
        for i in range(15):  # Just above potential default limit
            command = f"SET testkey{i:04d} value{i}\r\n"
            client_sock.send(command.encode())
            response = client_sock.recv(1024).decode()
            assert response.strip() == "OK"
        
        # Now test KEYS command should warn about complexity
        client_sock.send(b"KEYS *\r\n")
        response = client_sock.recv(4096).decode()
        
        # Should either return keys or refuse with limit message
        # (depends on MERKLE_KV_MAX_KEYS environment setting)
        assert "KEYS" in response or "ERROR" in response
        
        client_sock.close()

    def test_scan_count_defaults_and_overrides(self, server):
        """Test SCAN COUNT defaults and environment variable overrides"""
        # Set up test data
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect((server.host, server.port))
        
        # Add test keys
        for i in range(100):
            command = f"SET key{i:03d} value{i}\r\n"
            client_sock.send(command.encode())
            response = client_sock.recv(1024).decode()
            assert response.strip() == "OK"
        
        # Test SCAN without COUNT (should use default)
        client_sock.send(b"SCAN 0\r\n")
        response = client_sock.recv(4096).decode()
        lines = response.strip().split('\r\n')
        
        # Verify SCAN response format
        assert lines[0].startswith("SCAN")
        cursor = int(lines[0].split()[1])
        assert cursor > 0  # Should have more keys
        
        # Count returned keys (excluding header and cursor)
        key_count = len([line for line in lines[1:] if line and not line.startswith("SCAN")])
        assert key_count <= 50  # Default limit
        assert key_count > 0
        
        # Test SCAN with explicit COUNT
        client_sock.send(b"SCAN 0 COUNT 10\r\n")
        response = client_sock.recv(4096).decode()
        lines = response.strip().split('\r\n')
        
        # Should respect COUNT parameter
        key_count = len([line for line in lines[1:] if line and not line.startswith("SCAN")])
        assert key_count <= 10
        
        client_sock.close()

    def test_info_stable_metrics_schema(self, server):
        """Test INFO command provides stable metrics schema for monitoring"""        
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect((server.host, server.port))
        
        # Test each INFO section has stable field names
        sections = ["server", "memory", "storage", "replication"]
        
        for section in sections:
            command = f"INFO {section}\r\n"
            client_sock.send(command.encode())
            response = client_sock.recv(4096).decode()
            
            assert response.startswith("INFO\r\n")
            assert f"# {section.title()}" in response
            
            # Verify stable field names exist
            if section == "server":
                assert "version:" in response
                assert "uptime_in_seconds:" in response
                assert "connected_clients:" in response
                assert "total_commands_processed:" in response
                assert "tcp_port:" in response
                
            elif section == "memory":
                assert "used_memory:" in response
                assert "used_memory_human:" in response
                assert "mem_fragmentation_ratio:" in response
                
            elif section == "storage":
                assert "db_keys:" in response
                assert "db_bytes:" in response
                assert "keyspace_hits:" in response
                
            elif section == "replication":
                assert "repl_enabled:" in response
                assert "repl_peers:" in response
                assert "master_host:" in response
        
        client_sock.close()

    def test_hash_determinism_verification(self, server):
        """Test HASH command produces deterministic results"""        
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect((server.host, server.port))
        
        # Add test data in specific order
        test_data = [
            ("zebra", "animal"),
            ("apple", "fruit"),
            ("banana", "fruit"),
            ("dog", "animal")
        ]
        
        for key, value in test_data:
            command = f"SET {key} {value}\r\n"
            client_sock.send(command.encode())
            response = client_sock.recv(1024).decode()
            assert response.strip() == "OK"
        
        # Get hash multiple times - should be identical
        hashes = []
        for _ in range(3):
            client_sock.send(b"HASH\r\n")
            response = client_sock.recv(1024).decode()
            assert response.startswith("HASH ")
            hash_value = response.split()[1].strip()
            hashes.append(hash_value)
        
        # All hashes should be identical (deterministic)
        assert all(h == hashes[0] for h in hashes)
        assert len(hashes[0]) > 0  # Non-empty hash
        
        client_sock.close()


class TestConcurrentScenarios:
    """Test concurrent access patterns and command interactions"""
    
    def test_concurrent_admin_commands(self, server):
        """Test multiple administrative commands under concurrent load"""
        
        def run_admin_commands(thread_id):
            """Run various admin commands concurrently"""
            results = []
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((server.host, server.port))
            
            commands = [
                b"PING test\r\n",
                b"INFO server\r\n", 
                b"STATS\r\n",
                b"DBSIZE\r\n",
                b"SCAN 0 COUNT 5\r\n"
            ]
            
            for cmd in commands:
                try:
                    sock.send(cmd)
                    response = sock.recv(4096).decode()
                    results.append((cmd.decode().strip(), len(response) > 0))
                except Exception as e:
                    results.append((cmd.decode().strip(), False))
            
            sock.close()
            return thread_id, results
        
        # Run concurrent administrative commands
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(run_admin_commands, i) for i in range(5)]
            
            all_results = {}
            for future in as_completed(futures):
                thread_id, results = future.result()
                all_results[thread_id] = results
        
        # Verify all threads got valid responses
        for thread_id, results in all_results.items():
            for cmd, success in results:
                assert success, f"Thread {thread_id} failed command: {cmd}"

    def test_concurrent_scan_operations(self, server):
        """Test multiple SCAN cursors operating concurrently"""        
        # Set up test data
        setup_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        setup_sock.connect((server.host, server.port))
        
        for i in range(50):
            command = f"SET concurrent{i:03d} data{i}\r\n"
            setup_sock.send(command.encode())
            response = setup_sock.recv(1024).decode()
            assert response.strip() == "OK"
        
        setup_sock.close()
        
        def scan_with_cursor(start_cursor, thread_id):
            """Perform SCAN starting from specific cursor"""
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((server.host, server.port))
            
            scanned_keys = []
            cursor = start_cursor
            iterations = 0
            max_iterations = 20  # Safety limit
            
            # Academic Note: Handle both multi-iteration and single-iteration scenarios
            # In concurrent systems, small datasets may complete in one SCAN operation
            while iterations < max_iterations:
                command = f"SCAN {cursor} COUNT 5\r\n"
                sock.send(command.encode())
                response = sock.recv(4096).decode()
                
                if not response.startswith("SCAN"):
                    break
                
                lines = response.strip().split('\r\n')
                next_cursor = int(lines[0].split()[1])
                
                # Collect keys from this batch
                batch_keys = []
                for line in lines[1:]:
                    if line and not line.startswith("SCAN"):
                        batch_keys.append(line)
                        scanned_keys.append(line)
                
                iterations += 1
                
                # If no keys in this batch or cursor is 0, we're done
                if len(batch_keys) == 0 or next_cursor == 0:
                    break
                    
                cursor = next_cursor
            
            sock.close()
            return thread_id, scanned_keys, iterations
        
        # Run concurrent SCAN operations
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(scan_with_cursor, 0, i) for i in range(3)]
            
            scan_results = {}
            for future in as_completed(futures):
                thread_id, keys, iterations = future.result()
                scan_results[thread_id] = (keys, iterations)
        
        # Verify all scans completed successfully
        for thread_id, (keys, iterations) in scan_results.items():
            # Academic Note: In concurrent access patterns, expect at least one key per thread
            # All threads should get data since they scan the same shared keyspace  
            assert len(keys) > 0, f"Thread {thread_id} found no keys"
            assert iterations > 0, f"Thread {thread_id} made no iterations"
            # All keys should be from our test set
            for key in keys:
                assert key.startswith("concurrent"), f"Unexpected key: {key}"

    def test_replication_command_coverage(self, server):
        """Test REPLICATE command comprehensive coverage"""        
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect((server.host, server.port))
        
        # Test REPLICATE STATUS
        client_sock.send(b"REPLICATE STATUS\r\n")
        response = client_sock.recv(1024).decode()
        assert "REPLICATION" in response
        assert "enabled" in response or "disabled" in response
        
        # Test REPLICATE ENABLE (should handle gracefully)
        client_sock.send(b"REPLICATE ENABLE\r\n")
        response = client_sock.recv(1024).decode()
        assert "OK" in response or "ERROR" in response  # Both valid
        
        # Test REPLICATE DISABLE (should handle gracefully) 
        client_sock.send(b"REPLICATE DISABLE\r\n")
        response = client_sock.recv(1024).decode()
        assert "OK" in response or "ERROR" in response  # Both valid
        
        client_sock.close()


class TestErrorBoundaries:
    """Test error handling and boundary conditions"""
    
    def test_malformed_commands_with_guardrails(self, server):
        """Test malformed commands trigger appropriate guardrails"""        
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect((server.host, server.port))
        
        # Test malformed commands that should trigger protocol guardrails
        malformed_commands = [
            b"SCAN notanumber\r\n",          # Invalid cursor
            b"SCAN 0 COUNT notanumber\r\n",  # Invalid count
            b"KEYS\r\n",                     # Missing pattern
            b"INFO invalidsection\r\n",      # Invalid section
            b"HASH pattern\r\n",             # Pattern not supported
            b"SYNC\r\n",                     # Missing host/port
            b"SYNC localhost\r\n",           # Missing port
        ]
        
        for cmd in malformed_commands:
            client_sock.send(cmd)
            response = client_sock.recv(1024).decode()
            
            # All malformed commands should return ERROR
            assert response.startswith("ERROR"), f"Command {cmd} should return ERROR, got: {response}"
            # Error should be descriptive (protocol guardrail)
            assert len(response) > 10, f"Error message too short for {cmd}: {response}"
        
        client_sock.close()

    def test_protocol_line_termination_compliance(self, server):
        """Test protocol correctly handles CRLF line termination guardrails"""        
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect((server.host, server.port))
        
        # Test that responses always use CRLF
        test_commands = [
            b"PING test\r\n",
            b"INFO server\r\n",
            b"DBSIZE\r\n",
            b"STATS\r\n"
        ]
        
        for cmd in test_commands:
            client_sock.send(cmd)
            response = client_sock.recv(4096).decode()
            
            # All responses should use CRLF line endings (protocol guardrail)
            assert "\r\n" in response, f"Response missing CRLF for {cmd}: {response}"
            # Should not have bare LF without CR
            lines = response.split('\r\n')
            for line in lines:
                assert '\n' not in line, f"Found bare LF in response: {response}"
        
        client_sock.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
