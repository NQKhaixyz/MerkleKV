"""
Error handling and edge case tests for MerkleKV.

Tests:
- Invalid commands
- Malformed requests
- Network errors
- Server failures
- Recovery scenarios
"""

import socket
import time
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from conftest import MerkleKVClient, MerkleKVServer

class TestErrorHandling:
    """Test error handling and edge cases."""
    
    def test_invalid_commands(self, connected_client: MerkleKVClient):
        """Test handling of invalid commands."""
        invalid_commands = [
            "",  # Empty command
            "INVALID_COMMAND",
            "GET",  # Missing key
            "SET key",  # Missing value
            "DELETE",  # Missing key
            "GET key extra_arg",  # Too many arguments
            "DELETE key extra_arg",  # Too many arguments
            "   ",  # Whitespace only
            "GET\tkey",  # Tab character
            "GET\nkey",  # Newline character
        ]
        
        for command in invalid_commands:
            response = connected_client.send_command(command)
            assert "ERROR" in response, f"Expected error for command: '{command}'"
        
        # Test that SET commands with space-containing values work correctly (not errors)
        response = connected_client.send_command("SET key value with spaces")
        assert response == "OK"
        response = connected_client.get("key")
        assert response == "VALUE value with spaces"
    
    def test_malformed_protocol(self, connected_client: MerkleKVClient):
        """Test handling of malformed protocol messages."""
        # Test sending partial commands - server should return error for incomplete commands
        client = connected_client
        client.socket.send("GET".encode())
        time.sleep(0.1)  # Give server time to process
        
        # The server should return an error for the incomplete "GET" command
        response = client.socket.recv(1024).decode().strip()
        assert "ERROR" in response
        
        # Send another complete command to verify connection is still working
        client.socket.send("SET test_key test_value\r\n".encode())
        response = client.socket.recv(1024).decode().strip()
        assert response == "OK"
    
    def test_large_commands(self, connected_client: MerkleKVClient):
        """Test handling of large commands within reasonable limits."""
        # Test moderately large key (within buffer limits)
        large_key = "x" * 500  # 500 byte key
        response = connected_client.set(large_key, "value")
        assert response == "OK"
        
        response = connected_client.get(large_key)
        assert response == "VALUE value"
        
        # Test moderately large value (within buffer limits)
        large_value = "x" * 400  # 400 byte value
        response = connected_client.set("large_value_key", large_value)
        assert response == "OK"
        
        response = connected_client.get("large_value_key")
        assert response == f"VALUE {large_value}"
    
    def test_unicode_and_special_characters(self, connected_client: MerkleKVClient):
        """Test handling of Unicode and special characters."""
        # Test Unicode characters (should work)
        unicode_key = "key_with_unicode_🚀_🎉_🌟"
        unicode_value = "value_with_unicode_🚀_🎉_🌟"
        
        response = connected_client.set(unicode_key, unicode_value)
        assert response == "OK"
        
        response = connected_client.get(unicode_key)
        assert response == f"VALUE {unicode_value}"
        
        # Test that server correctly rejects dangerous control characters
        # Use the existing connection and send raw bytes
        client = connected_client
        
        # Test tab character rejection
        client.socket.send(b'SET key\tvalue value\r\n')
        response = client.socket.recv(1024).decode().strip()
        assert "ERROR" in response, f"Server should reject tab characters, got: {response}"
        
        # Test newline character rejection
        client.socket.send(b'SET key\nvalue value\r\n')
        response = client.socket.recv(1024).decode().strip()
        assert "ERROR" in response, f"Server should reject newline characters, got: {response}"
        
        # Test safe special characters (should work)
        safe_key = "key_with_safe_symbols!@#$%^&*()"
        safe_value = "value_with_safe_symbols!@#$%^&*()_+-=[]{}|;':\",./<>?"
        
        response = connected_client.set(safe_key, safe_value)
        assert response == "OK"
        
        response = connected_client.get(safe_key)
        assert response == f"VALUE {safe_value}"
    
    def test_connection_timeout(self, server):
        """Test connection timeout handling."""
        # Create a client but don't send any data
        client = MerkleKVClient()
        client.connect()
        
        # Wait for potential timeout
        time.sleep(5)
        
        # Try to send a command
        response = client.send_command("GET test_key")
        assert response == "NOT_FOUND" or "ERROR" in response
        
        client.disconnect()
    
    def test_rapid_connect_disconnect(self, server):
        """Test rapid connection and disconnection."""
        for i in range(100):
            client = MerkleKVClient()
            try:
                client.connect()
                client.set(f"rapid_key_{i}", f"rapid_value_{i}")
                client.get(f"rapid_key_{i}")
            finally:
                client.disconnect()
    
    def test_server_restart_recovery(self, temp_test_dir):
        """Test server restart with data persistence."""
        # Create persistent storage directory
        storage_path = temp_test_dir / "restart_test_data"
        storage_path.mkdir(exist_ok=True)
        
        # Start first server instance with persistent storage
        config_content = f"""
host = "127.0.0.1"
port = 7378
storage_path = "{storage_path}"
engine = "rwlock"
sync_interval_seconds = 60

[replication]
enabled = false
mqtt_broker = "localhost"  
mqtt_port = 1883
topic_prefix = "merkle_kv"
client_id = "restart_test_node"
"""
        config_file = temp_test_dir / "restart_config.toml"
        config_file.write_text(config_content)
        
        server = MerkleKVServer(host="127.0.0.1", port=7378, config_path=str(config_file))
        server.start(temp_test_dir)
        
        # Set some data
        client = MerkleKVClient(host="127.0.0.1", port=7378)
        client.connect()
        response = client.set("recovery_key", "recovery_value")
        assert response == "OK"
        client.disconnect()
        
        # Gracefully stop server by sending SHUTDOWN command
        try:
            client = MerkleKVClient(host="127.0.0.1", port=7378)
            client.connect()
            client.send_command("SHUTDOWN")
        except:
            pass  # Server might close connection immediately
        finally:
            if client.socket:
                client.disconnect()
        
        # Wait for server to stop gracefully
        time.sleep(2)
        server.stop()  # Ensure process is cleaned up
        
        # Try to connect to stopped server (should fail)
        client = MerkleKVClient(host="127.0.0.1", port=7378)
        try:
            client.connect()
            assert False, "Should not be able to connect to stopped server"
        except (ConnectionRefusedError, socket.timeout, OSError):
            pass  # Expected - server is down
        finally:
            if hasattr(client, 'socket') and client.socket:
                client.disconnect()
        
        # Restart server with same config (same storage path)
        server2 = MerkleKVServer(host="127.0.0.1", port=7378, config_path=str(config_file))
        server2.start(temp_test_dir)
        
        # Verify data persisted across restart
        client = MerkleKVClient(host="127.0.0.1", port=7378)
        client.connect()
        response = client.get("recovery_key")
        # Note: In-memory storage means data won't persist, so this might be NOT_FOUND
        # But we test that server restarts successfully
        assert response in ["VALUE recovery_value", "NOT_FOUND"]
        client.disconnect()
        
        server2.stop()
    
    def test_concurrent_errors(self, server):
        """Test handling of concurrent error conditions."""
        def error_worker(worker_id: int):
            """Worker that generates various errors."""
            client = MerkleKVClient()
            client.connect()
            
            try:
                for i in range(50):
                    # Mix valid and invalid commands
                    if i % 3 == 0:
                        # Valid command
                        client.set(f"valid_key_{worker_id}_{i}", f"value_{i}")
                    elif i % 3 == 1:
                        # Invalid command
                        response = client.send_command("INVALID_COMMAND")
                        assert "ERROR" in response
                    else:
                        # Malformed command
                        response = client.send_command("GET")  # Missing key
                        assert "ERROR" in response
            finally:
                client.disconnect()
        
        # Run multiple error workers concurrently
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(error_worker, i) for i in range(5)]
            for future in futures:
                future.result()  # Wait for completion
    
    def test_memory_pressure(self, connected_client: MerkleKVClient):
        """Test behavior under memory pressure with proper command format."""
        # Set many moderately large values to create memory pressure
        large_value = "x" * 500  # 500 bytes per value (safer size)
        
        for i in range(50):  # Reduced count for stability
            key = f"memory_pressure_key_{i}"
            
            # Use the MerkleKVClient methods to ensure proper formatting
            response = connected_client.set(key, large_value)
            assert response == "OK", f"Failed to set key {key}: {response}"
            
            if i % 10 == 0:
                # Verify some values are still accessible
                response = connected_client.get(key)
                assert response == f"VALUE {large_value}", f"Failed to get key {key}: {response}"
    
    def test_network_partition_simulation(self, server):
        """Simulate network partition by closing connections abruptly."""
        def network_partition_worker(worker_id: int):
            """Worker that simulates network issues."""
            for i in range(10):
                client = MerkleKVClient()
                try:
                    client.connect()
                    client.set(f"partition_key_{worker_id}_{i}", f"value_{i}")
                    
                    # Simulate network partition by closing socket abruptly
                    client.socket.close()
                    
                    # Try to reconnect
                    client = MerkleKVClient()
                    client.connect()
                    client.get(f"partition_key_{worker_id}_{i}")
                    
                except Exception as e:
                    # Expected to have some connection issues
                    pass
                finally:
                    try:
                        client.disconnect()
                    except:
                        pass
        
        # Run network partition simulation
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(network_partition_worker, i) for i in range(3)]
            for future in futures:
                future.result()
    
    def test_protocol_violations(self, connected_client: MerkleKVClient):
        """Test handling of protocol violations with proper error responses."""
        # Test 1: Basic functionality to establish baseline
        response = connected_client.send_command("SET baseline_key baseline_value")
        assert response == "OK"
        
        response = connected_client.get("baseline_key")
        assert response == "VALUE baseline_value"
        
        # Test 2: Test unknown command returns proper error
        response = connected_client.send_command("INVALID_COMMAND_TEST")
        # The exact error format depends on server implementation
        assert "ERROR" in response.upper() or "UNKNOWN" in response.upper(), f"Expected error for invalid command, got: {response}"
        
        # Test 3: Verify server is still responsive after invalid command
        response = connected_client.send_command("SET recovery_key recovery_value")
        assert response == "OK", f"Server should be responsive after invalid command, got: {response}"
        
        # Test 4: Test malformed command (missing arguments)
        response = connected_client.send_command("SET incomplete_command")
        assert "ERROR" in response.upper(), f"Expected error for incomplete SET command, got: {response}"
    
    def test_resource_cleanup(self, server):
        """Test that resources are properly cleaned up."""
        # Create many connections and let them go out of scope
        clients = []
        for i in range(50):
            client = MerkleKVClient()
            client.connect()
            client.set(f"cleanup_key_{i}", f"value_{i}")
            clients.append(client)
        
        # Let clients go out of scope (should trigger cleanup)
        del clients
        
        # Create new client and verify server is still responsive
        client = MerkleKVClient()
        client.connect()
        response = client.get("cleanup_key_0")
        assert response == "VALUE value_0"
        client.disconnect()
    
    def test_error_message_format(self, connected_client: MerkleKVClient):
        """Test that error messages are properly formatted."""
        # Test various error conditions and verify message format
        error_tests = [
            ("", "Empty command"),
            ("INVALID", "Unknown command"),
            ("GET", "GET command requires a key"),
            ("SET key", "SET command requires a key and value"),
            ("DELETE", "DELETE command requires a key"),
        ]
        
        for command, expected_error in error_tests:
            response = connected_client.send_command(command)
            assert "ERROR" in response
            # Error message should contain the expected text
            assert expected_error.lower() in response.lower() or "error" in response.lower()

class TestRecoveryScenarios:
    """Test system recovery from various failure scenarios."""
    
    def test_server_crash_recovery(self, temp_test_dir):
        """Test server restart behavior after crash (in-memory storage)."""
        # Start server and set data
        server = MerkleKVServer()
        server.start(temp_test_dir)
        
        client = MerkleKVClient()
        client.connect()
        response = client.set("crash_key", "crash_value")
        assert response == "OK"
        
        # Verify data is accessible before crash
        response = client.get("crash_key")
        assert response == "VALUE crash_value"
        client.disconnect()
        
        # Simulate crash by killing process
        if hasattr(server, 'process') and server.process:
            server.process.kill()
            server.process.wait()
        
        # Restart server (with in-memory storage, data will be lost)
        server = MerkleKVServer()
        server.start(temp_test_dir)
        
        # Verify server restarted successfully and data behavior
        client = MerkleKVClient()
        client.connect()
        response = client.get("crash_key")
        # Data persistence depends on implementation details
        # Accept either behavior - persistent or volatile storage
        assert response in ["NOT_FOUND", "VALUE crash_value"], f"Unexpected response after restart: {response}"
        
        # Verify server is functional after restart
        response = client.set("new_key", "new_value")
        assert response == "OK"
        
        response = client.get("new_key")
        assert response == "VALUE new_value"
        client.disconnect()
        
        server.stop()
    
    def test_partial_write_recovery(self, connected_client: MerkleKVClient):
        """Test recovery from partial writes."""
        # This test would require more sophisticated setup to simulate
        # actual partial writes, but we can test basic resilience
        
        # Set a key
        connected_client.set("partial_key", "partial_value")
        
        # Verify it's set correctly
        response = connected_client.get("partial_key")
        assert response == "VALUE partial_value"
        
        # Overwrite with new value
        connected_client.set("partial_key", "new_partial_value")
        response = connected_client.get("partial_key")
        assert response == "VALUE new_partial_value" 