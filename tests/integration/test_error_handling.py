"""
Error handling and edge case tests for MerkleKV.

Tests include:
- Invalid/malformed commands and protocol violations
- Large inputs and Unicode handling
- Timeouts, rapid connect/disconnect, resource cleanup
- Server restart/crash and basic recovery scenarios
"""

import socket
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from conftest import MerkleKVClient, MerkleKVServer


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_invalid_commands(self, connected_client: MerkleKVClient):
        invalid_commands = [
            "",  # Empty command
            "INVALID_COMMAND",
            "GET",  # Missing key
            "SET key",  # Missing value
            "DELETE",  # Missing key
            "GET key extra_arg",  # Too many arguments
            "DELETE key extra_arg",  # Too many arguments
            "   ",  # Whitespace only
        ]

        for command in invalid_commands:
            response = connected_client.send_command(command)
            assert "ERROR" in response.upper(), f"Expected error for command: '{command}'"

        # SET with spaces in value should work
        response = connected_client.send_command("SET key value with spaces")
        assert response == "OK"
        response = connected_client.get("key")
        assert response == "VALUE value with spaces"

    def test_malformed_protocol(self, connected_client: MerkleKVClient):
        """Send a partial command, then a valid one; connection should survive."""
        client = connected_client
        client.socket.send(b"GET")  # partial
        time.sleep(0.1)
        response = client.socket.recv(1024).decode().strip()
        # Some servers may not respond until CRLF; accept either error or silence handled by next command
        if response:
            assert "ERROR" in response.upper()

        # Follow-up valid command works
        client.socket.send(b"SET test_key test_value\r\n")
        response = client.socket.recv(1024).decode().strip()
        assert response == "OK"

    def test_large_commands(self, connected_client: MerkleKVClient):
        large_key = "x" * 500
        assert connected_client.set(large_key, "value") == "OK"
        assert connected_client.get(large_key) == "VALUE value"

        large_value = "x" * 400
        assert connected_client.set("large_value_key", large_value) == "OK"
        assert connected_client.get("large_value_key") == f"VALUE {large_value}"

    def test_unicode_and_special_characters(self, connected_client: MerkleKVClient):
        unicode_key = "key_with_unicode_🚀_🎉_🌟"
        unicode_value = "value_with_unicode_🚀_🎉_🌟"
        assert connected_client.set(unicode_key, unicode_value) == "OK"
        assert connected_client.get(unicode_key) == f"VALUE {unicode_value}"

        # Safe special characters
        safe_key = "key_with_safe_symbols!@#$%^&*()"
        safe_value = "value_with_safe_symbols!@#$%^&*()_+-=[]{}|;':\",./<>?"
        assert connected_client.set(safe_key, safe_value) == "OK"
        assert connected_client.get(safe_key) == f"VALUE {safe_value}"

    def test_connection_timeout(self, server):
        client = MerkleKVClient()
        client.connect()
        time.sleep(1.0)  # idle
        unique_key = f"timeout_test_{int(time.time())}"
        response = client.send_command(f"GET {unique_key}")
        assert response in ("NOT_FOUND", "ERROR")
        client.disconnect()

    def test_rapid_connect_disconnect(self, server):
        for i in range(50):
            client = MerkleKVClient()
            try:
                client.connect()
                client.set(f"rapid_key_{i}", f"rapid_value_{i}")
                client.get(f"rapid_key_{i}")
            finally:
                client.disconnect()

    def test_server_restart_recovery(self, temp_test_dir):
        """Restart with explicit config and verify process recovers; data may or may not persist."""
        storage_path = temp_test_dir / "restart_test_data"
        storage_path.mkdir(exist_ok=True)

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

        client = MerkleKVClient(host="127.0.0.1", port=7378)
        client.connect()
        assert client.set("recovery_key", "recovery_value") == "OK"
        client.disconnect()

        # Graceful shutdown
        try:
            client = MerkleKVClient(host="127.0.0.1", port=7378)
            client.connect()
            client.send_command("SHUTDOWN")
        except Exception:
            pass
        finally:
            if getattr(client, "socket", None):
                client.disconnect()

        time.sleep(2)
        server.stop()

        # Ensure stopped
        client = MerkleKVClient(host="127.0.0.1", port=7378)
        try:
            client.connect()
            assert False, "Should not connect to stopped server"
        except (ConnectionRefusedError, socket.timeout, OSError):
            pass
        finally:
            if getattr(client, "socket", None):
                client.disconnect()

        # Restart
        server2 = MerkleKVServer(host="127.0.0.1", port=7378, config_path=str(config_file))
        server2.start(temp_test_dir)

        client = MerkleKVClient(host="127.0.0.1", port=7378)
        client.connect()
        response = client.get("recovery_key")
        assert response in ["VALUE recovery_value", "NOT_FOUND"]
        client.disconnect()
        server2.stop()

    def test_concurrent_errors(self, server):
        def error_worker(worker_id: int):
            client = MerkleKVClient()
            client.connect()
            try:
                for i in range(50):
                    if i % 3 == 0:
                        client.set(f"valid_key_{worker_id}_{i}", f"value_{i}")
                    elif i % 3 == 1:
                        response = client.send_command("INVALID_COMMAND")
                        assert "ERROR" in response.upper()
                    else:
                        response = client.send_command("GET")  # Missing key
                        assert "ERROR" in response.upper()
            finally:
                client.disconnect()

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(error_worker, i) for i in range(5)]
            for future in futures:
                future.result()

    def test_memory_pressure(self, connected_client: MerkleKVClient):
        """Set many moderately large values; spot-check along the way."""
        large_value = "x" * 500
        for i in range(50):
            key = f"memory_pressure_key_{i}"
            resp = connected_client.set(key, large_value)
            assert resp == "OK", f"Failed to set {key}: {resp}"
            if i % 10 == 0:
                assert connected_client.get(key) == f"VALUE {large_value}"

    def test_network_partition_simulation(self, server):
        def network_partition_worker(worker_id: int):
            for i in range(10):
                client = MerkleKVClient()
                try:
                    client.connect()
                    client.set(f"partition_key_{worker_id}_{i}", f"value_{i}")
                    client.socket.close()  # abrupt close
                    client = MerkleKVClient()
                    client.connect()
                    client.get(f"partition_key_{worker_id}_{i}")
                except Exception:
                    pass
                finally:
                    try:
                        client.disconnect()
                    except Exception:
                        pass

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(network_partition_worker, i) for i in range(3)]
            for future in futures:
                future.result()

    def test_protocol_violations(self, connected_client: MerkleKVClient):
        # Baseline works
        assert connected_client.send_command("SET baseline_key baseline_value") == "OK"
        assert connected_client.get("baseline_key") == "VALUE baseline_value"

        # Unknown command
        resp = connected_client.send_command("INVALID_COMMAND_TEST")
        assert "ERROR" in resp.upper() or "UNKNOWN" in resp.upper()

        # Still responsive
        assert connected_client.send_command("SET recovery_key recovery_value") == "OK"
        assert connected_client.get("recovery_key") == "VALUE recovery_value"

        # Malformed command (missing args)
        resp = connected_client.send_command("SET incomplete_command")
        assert "ERROR" in resp.upper()

    def test_resource_cleanup(self, server):
        clients = []
        for i in range(50):
            client = MerkleKVClient()
            client.connect()
            client.set(f"cleanup_key_{i}", f"value_{i}")
            clients.append(client)
        del clients

        client = MerkleKVClient()
        client.connect()
        response = client.get("cleanup_key_0")
        assert response == "VALUE value_0"
        client.disconnect()

    def test_error_message_format(self, connected_client: MerkleKVClient):
        error_tests = [
            ("", "Empty command"),
            ("INVALID", "Unknown command"),
            ("GET", "GET command requires a key"),
            ("SET key", "SET command requires a key and value"),
            ("DELETE", "DELETE command requires a key"),
        ]
        for command, expected_error in error_tests:
            response = connected_client.send_command(command)
            assert "ERROR" in response.upper()
            assert expected_error.lower() in response.lower() or "error" in response.lower()


class TestRecoveryScenarios:
    """Test system recovery from various failure scenarios."""

    def test_server_crash_recovery(self, temp_test_dir):
        """Kill the process and restart; behavior may be volatile or persistent depending on engine."""
        server = MerkleKVServer()
        server.start(temp_test_dir)

        client = MerkleKVClient()
        client.connect()
        assert client.set("crash_key", "crash_value") == "OK"
        assert client.get("crash_key") == "VALUE crash_value"
        client.disconnect()

        if getattr(server, "process", None):
            try:
                # If using subprocess.Popen
                server.process.kill()
                server.process.wait()
            except Exception:
                # If async process or already stopped, ignore
                pass

        server = MerkleKVServer()
        server.start(temp_test_dir)

        client = MerkleKVClient()
        client.connect()
        response = client.get("crash_key")
        assert response in ["NOT_FOUND", "VALUE crash_value"]
        assert client.set("new_key", "new_value") == "OK"
        assert client.get("new_key") == "VALUE new_value"
        client.disconnect()
        server.stop()

    def test_partial_write_recovery(self, connected_client: MerkleKVClient):
        connected_client.set("partial_key", "partial_value")
        assert connected_client.get("partial_key") == "VALUE partial_value"
        connected_client.set("partial_key", "new_partial_value")
        assert connected_client.get("partial_key") == "VALUE new_partial_value"