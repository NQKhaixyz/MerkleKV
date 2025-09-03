"""
Integration tests for Administrative & Control Commands (Issue #27).

This module tests Phase 1 (Info) and Phase 2 (Key Management) commands:
- Phase 1: PING [message], ECHO <message>, INFO [section], STATS [reset], DBSIZE
- Phase 2: KEYS <pattern>, SCAN <cursor> [MATCH <pattern>] [COUNT <count>], EXISTS <key1> [key2] ...

These tests ensure full backward compatibility and verify the academic design
principles for distributed system observability and key management.
"""

import pytest
import time
from conftest import send_command, connect_to_server


class TestPhase1Commands:
    """Test Phase 1 (Info) administrative commands."""
    
    def test_ping_without_message(self, server):
        """Test PING command returns PONG."""
        with connect_to_server() as client:
            response = send_command(client, "PING")
            assert response == "PONG"
    
    def test_ping_with_message(self, server):
        """Test PING command with message echoes the message."""
        with connect_to_server() as client:
            response = send_command(client, "PING hello world")
            assert response == "PONG hello world"
    
    def test_ping_with_single_word(self, server):
        """Test PING command with single word message."""
        with connect_to_server() as client:
            response = send_command(client, "PING test")
            assert response == "PONG test"
    
    def test_echo_command(self, server):
        """Test ECHO command reflects the message."""
        with connect_to_server() as client:
            response = send_command(client, "ECHO test message")
            assert response == "ECHO test message"
    
    def test_echo_empty_message_error(self, server):
        """Test ECHO command requires a message."""
        with connect_to_server() as client:
            response = send_command(client, "ECHO")
            assert response.startswith("ERROR")
            assert "requires a message" in response
    
    def test_dbsize_empty_database(self, server):
        """Test DBSIZE returns 0 for empty database."""
        with connect_to_server() as client:
            response = send_command(client, "DBSIZE")
            assert response == "DBSIZE 0"
    
    def test_dbsize_with_keys(self, server):
        """Test DBSIZE returns correct count after adding keys."""
        with connect_to_server() as client:
            # Add some keys
            send_command(client, "SET key1 value1")
            send_command(client, "SET key2 value2")
            send_command(client, "SET key3 value3")
            
            response = send_command(client, "DBSIZE")
            assert response == "DBSIZE 3"
    
    def test_info_default_all_sections(self, server):
        """Test INFO without section returns all sections."""
        with connect_to_server() as client:
            # Add some data and activity
            send_command(client, "SET test_key test_value")
            send_command(client, "GET test_key")
            
            response = send_command(client, "INFO")
            
            # Verify response format and sections
            assert response.startswith("INFO")
            lines = response.strip().split("\r\n")
            
            # Check for section headers
            assert "# Server" in response
            assert "# Memory" in response  
            assert "# Storage" in response
            assert "# Replication" in response
            
            # Check for key server metrics
            assert "version:" in response
            assert "uptime_in_seconds:" in response
            assert "connected_clients:" in response
            assert "total_commands_processed:" in response
            assert "process_id:" in response
    
    def test_info_server_section(self, server):
        """Test INFO with server section only."""
        with connect_to_server() as client:
            response = send_command(client, "INFO server")
            
            assert response.startswith("INFO")
            assert "# Server" in response
            assert "version:" in response
            assert "uptime_in_seconds:" in response
            
            # Should not contain other sections
            assert "# Memory" not in response
            assert "# Storage" not in response
            assert "# Replication" not in response
    
    def test_info_memory_section(self, server):
        """Test INFO with memory section only."""
        with connect_to_server() as client:
            response = send_command(client, "INFO memory")
            
            assert response.startswith("INFO")
            assert "# Memory" in response
            assert "used_memory:" in response
            assert "used_memory_human:" in response
            
            # Should not contain other sections
            assert "# Server" not in response
            assert "# Storage" not in response
            assert "# Replication" not in response
    
    def test_info_storage_section(self, server):
        """Test INFO with storage section only."""
        with connect_to_server() as client:
            # Add some keys first
            send_command(client, "SET key1 value1")
            send_command(client, "SET key2 value2")
            
            response = send_command(client, "INFO storage")
            
            assert response.startswith("INFO")
            assert "# Storage" in response
            assert "db_keys:" in response
            assert "db_bytes:" in response
            
            # Should not contain other sections
            assert "# Server" not in response
            assert "# Memory" not in response
            assert "# Replication" not in response
    
    def test_info_replication_section(self, server):
        """Test INFO with replication section only."""
        with connect_to_server() as client:
            response = send_command(client, "INFO replication")
            
            assert response.startswith("INFO")
            assert "# Replication" in response
            assert "repl_enabled:" in response
            assert "repl_peers:" in response
            
            # Should not contain other sections
            assert "# Server" not in response
            assert "# Memory" not in response
            assert "# Storage" not in response
    
    def test_info_invalid_section(self, server):
        """Test INFO with multiple arguments fails gracefully."""
        with connect_to_server() as client:
            response = send_command(client, "INFO server memory")
            assert response.startswith("ERROR")
            assert "at most one section" in response
    
    def test_stats_without_reset(self, server):
        """Test STATS command returns statistics without reset."""
        with connect_to_server() as client:
            # Generate some activity
            send_command(client, "SET key1 value1")
            send_command(client, "GET key1")
            send_command(client, "INC counter")
            
            response = send_command(client, "STATS")
            
            # Verify response format
            assert response.startswith("STATS")
            
            # Parse stats into dictionary
            stats_lines = response.strip().split("\r\n")[1:]
            stats = {}
            for line in stats_lines:
                if ":" in line:
                    key, value = line.split(":", 1)
                    stats[key] = value
            
            # Check essential statistics are present
            required_stats = [
                "uptime_seconds", "total_connections", "active_connections",
                "total_commands", "get_commands", "set_commands", 
                "delete_commands", "numeric_commands", "string_commands",
                "bulk_commands", "stat_commands", "management_commands"
            ]
            
            for stat in required_stats:
                assert stat in stats, f"Missing statistic: {stat}"
            
            # Verify specific values reflect our activity
            assert int(stats["set_commands"]) >= 1
            assert int(stats["get_commands"]) >= 1
            assert int(stats["numeric_commands"]) >= 1
    
    def test_stats_with_reset(self, server):
        """Test STATS command with reset flag."""
        with connect_to_server() as client:
            # Generate some activity
            send_command(client, "SET key1 value1")
            send_command(client, "GET key1")
            send_command(client, "SET key2 value2")
            
            # Get stats before reset
            response1 = send_command(client, "STATS")
            stats1_lines = response1.strip().split("\r\n")[1:]
            stats1 = {}
            for line in stats1_lines:
                if ":" in line:
                    key, value = line.split(":", 1)
                    stats1[key] = value
            
            # Verify we have some activity
            assert int(stats1["set_commands"]) >= 2
            assert int(stats1["get_commands"]) >= 1
            
            # Reset stats
            reset_response = send_command(client, "STATS reset")
            assert reset_response.startswith("STATS")
            
            # Get stats after reset - they should be zeroed
            response2 = send_command(client, "STATS")
            stats2_lines = response2.strip().split("\r\n")[1:]
            stats2 = {}
            for line in stats2_lines:
                if ":" in line:
                    key, value = line.split(":", 1)
                    stats2[key] = value
            
            # Command counters should be reset to 0
            assert int(stats2["set_commands"]) == 0
            assert int(stats2["get_commands"]) == 0
            assert int(stats2["numeric_commands"]) == 0
            assert int(stats2["string_commands"]) == 0
            assert int(stats2["delete_commands"]) == 0
            assert int(stats2["bulk_commands"]) == 0
            
            # But connection counters should not be reset
            assert int(stats2["total_connections"]) > 0
    
    def test_stats_invalid_argument(self, server):
        """Test STATS with invalid argument returns error."""
        with connect_to_server() as client:
            response = send_command(client, "STATS invalid")
            assert response.startswith("ERROR")
            assert "invalid argument" in response


class TestPhase2Commands:
    """Test Phase 2 (Key Management) commands."""
    
    def test_keys_wildcard_pattern(self, server):
        """Test KEYS command with wildcard pattern."""
        with connect_to_server() as client:
            # Set up test data
            send_command(client, "SET key1 value1")
            send_command(client, "SET key2 value2")
            send_command(client, "SET test:item1 value")
            send_command(client, "SET test:item2 value")
            
            response = send_command(client, "KEYS *")
            
            # Verify response format
            assert response.startswith("KEYS")
            lines = response.strip().split("\r\n")
            
            # Should have all keys
            keys = [line for line in lines[1:] if line and line != ""]
            assert len(keys) >= 4
            assert "key1" in keys
            assert "key2" in keys
            assert "test:item1" in keys
            assert "test:item2" in keys
    
    def test_keys_prefix_pattern(self, server):
        """Test KEYS command with prefix pattern."""
        with connect_to_server() as client:
            # Set up test data
            send_command(client, "SET test:item1 value1")
            send_command(client, "SET test:item2 value2")
            send_command(client, "SET other:item value3")
            send_command(client, "SET item value4")
            
            response = send_command(client, "KEYS test:*")
            
            # Verify response format
            assert response.startswith("KEYS")
            lines = response.strip().split("\r\n")
            
            # Filter out empty lines and get actual keys
            keys = [line for line in lines[1:] if line and line != ""]
            assert len(keys) == 2
            assert "test:item1" in keys
            assert "test:item2" in keys
            assert "other:item" not in keys
            assert "item" not in keys
    
    def test_keys_no_matches(self, server):
        """Test KEYS command with pattern that matches nothing."""
        with connect_to_server() as client:
            send_command(client, "SET key1 value1")
            
            response = send_command(client, "KEYS nomatch:*")
            
            # Should return KEYS header followed by empty line
            assert response.startswith("KEYS")
            lines = response.strip().split("\r\n")
            assert len(lines) <= 2  # Just "KEYS" and possibly empty line
    
    def test_keys_missing_pattern(self, server):
        """Test KEYS command without pattern returns error."""
        with connect_to_server() as client:
            response = send_command(client, "KEYS")
            assert response.startswith("ERROR")
            assert "requires a pattern" in response
    
    def test_keys_multiple_arguments(self, server):
        """Test KEYS command with multiple arguments returns error."""
        with connect_to_server() as client:
            response = send_command(client, "KEYS pattern1 pattern2")
            assert response.startswith("ERROR")
            assert "only one pattern" in response
    
    def test_scan_cursor_basic(self, server):
        """Test basic SCAN cursor functionality."""
        with connect_to_server() as client:
            # Set up test data
            send_command(client, "SET key1 value1")
            send_command(client, "SET key2 value2")
            send_command(client, "SET key3 value3")
            
            response = send_command(client, "SCAN 0")
            
            # Verify response format
            assert response.startswith("SCAN")
            lines = response.strip().split("\r\n")
            
            # First line should be "SCAN <cursor>"
            scan_line = lines[0]
            assert scan_line.startswith("SCAN")
            cursor_parts = scan_line.split()
            assert len(cursor_parts) == 2
            next_cursor = cursor_parts[1]
            
            # Should have keys in remaining lines
            keys = lines[1:]
            assert len(keys) >= 3
    
    def test_scan_cursor_with_count(self, server):
        """Test SCAN cursor with COUNT parameter."""
        with connect_to_server() as client:
            # Set up test data
            for i in range(5):
                send_command(client, f"SET key{i} value{i}")
            
            # Get first page with COUNT 2
            response = send_command(client, "SCAN 0 COUNT 2")
            
            lines = response.strip().split("\r\n")
            scan_line = lines[0]
            assert scan_line.startswith("SCAN")
            
            # Extract cursor
            cursor_parts = scan_line.split()
            next_cursor = cursor_parts[1]
            
            # Should have exactly 2 keys (or less if fewer available)
            keys = lines[1:]
            assert len(keys) <= 2
            
            # If there are more keys, cursor should not be 0
            if len(keys) == 2:
                assert next_cursor != "0"
                
                # Get next page
                response2 = send_command(client, f"SCAN {next_cursor} COUNT 2")
                lines2 = response2.strip().split("\r\n")
                keys2 = lines2[1:]
                
                # Should get more keys or finish (cursor 0)
                scan_line2 = lines2[0]
                cursor_parts2 = scan_line2.split()
                final_cursor = cursor_parts2[1]
                
                # Either more keys or cursor is 0 (finished)
                assert len(keys2) > 0 or final_cursor == "0"
    
    def test_scan_cursor_with_match(self, server):
        """Test SCAN cursor with MATCH parameter."""
        with connect_to_server() as client:
            # Set up test data
            send_command(client, "SET test:item1 value1")
            send_command(client, "SET test:item2 value2")  
            send_command(client, "SET other:item value3")
            send_command(client, "SET item value4")
            
            response = send_command(client, "SCAN 0 MATCH test:*")
            
            lines = response.strip().split("\r\n")
            keys = lines[1:]
            
            # Should only get keys matching pattern
            for key in keys:
                if key:  # Skip empty lines
                    assert key.startswith("test:")
    
    def test_scan_cursor_with_match_and_count(self, server):
        """Test SCAN cursor with both MATCH and COUNT parameters."""
        with connect_to_server() as client:
            # Set up test data
            for i in range(5):
                send_command(client, f"SET test:item{i} value{i}")
                send_command(client, f"SET other:item{i} value{i}")
            
            response = send_command(client, "SCAN 0 MATCH test:* COUNT 3")
            
            lines = response.strip().split("\r\n")
            keys = lines[1:]
            
            # Should get at most 3 keys, all matching pattern
            assert len(keys) <= 3
            for key in keys:
                if key:  # Skip empty lines
                    assert key.startswith("test:")
    
    def test_scan_cursor_invalid_cursor(self, server):
        """Test SCAN cursor with invalid cursor value."""
        with connect_to_server() as client:
            response = send_command(client, "SCAN invalid")
            assert response.startswith("ERROR")
    
    def test_scan_cursor_invalid_count(self, server):
        """Test SCAN cursor with invalid COUNT value."""
        with connect_to_server() as client:
            response = send_command(client, "SCAN 0 COUNT invalid")
            assert response.startswith("ERROR")
            assert "must be a valid number" in response
    
    def test_scan_cursor_missing_match_value(self, server):
        """Test SCAN cursor with MATCH but no pattern."""
        with connect_to_server() as client:
            response = send_command(client, "SCAN 0 MATCH")
            assert response.startswith("ERROR")
            assert "requires a pattern" in response
    
    def test_scan_cursor_missing_count_value(self, server):
        """Test SCAN cursor with COUNT but no number."""
        with connect_to_server() as client:
            response = send_command(client, "SCAN 0 COUNT")
            assert response.startswith("ERROR")
            assert "requires a number" in response
    
    def test_exists_single_key(self, server):
        """Test EXISTS command with single key."""
        with connect_to_server() as client:
            send_command(client, "SET existing_key value")
            
            # Test existing key
            response1 = send_command(client, "EXISTS existing_key")
            assert response1 == "EXISTS 1"
            
            # Test non-existing key
            response2 = send_command(client, "EXISTS non_existing_key")
            assert response2 == "EXISTS 0"
    
    def test_exists_multiple_keys(self, server):
        """Test EXISTS command with multiple keys."""
        with connect_to_server() as client:
            send_command(client, "SET key1 value1")
            send_command(client, "SET key2 value2")
            # key3 doesn't exist
            
            response = send_command(client, "EXISTS key1 key2 key3")
            assert response == "EXISTS 2"
    
    def test_exists_no_keys_error(self, server):
        """Test EXISTS command without keys returns error."""
        with connect_to_server() as client:
            response = send_command(client, "EXISTS")
            assert response.startswith("ERROR")
            assert "requires at least one key" in response
    
    def test_exists_all_missing(self, server):
        """Test EXISTS command with all missing keys."""
        with connect_to_server() as client:
            response = send_command(client, "EXISTS missing1 missing2 missing3")
            assert response == "EXISTS 0"


class TestBackwardCompatibility:
    """Test that new commands don't break existing functionality."""
    
    def test_legacy_scan_still_works(self, server):
        """Test that legacy SCAN <prefix> command still works."""
        with connect_to_server() as client:
            # Set up test data
            send_command(client, "SET test:item1 value1")
            send_command(client, "SET test:item2 value2")
            send_command(client, "SET other:item value3")
            
            # Legacy SCAN should still work
            response = send_command(client, "SCAN test:")
            
            # Should get SCAN response with matching keys
            assert response.startswith("SCAN")
            assert "test:item1" in response
            assert "test:item2" in response
            assert "other:item" not in response
    
    def test_legacy_scan_wildcard(self, server):
        """Test that legacy SCAN * still works."""
        with connect_to_server() as client:
            send_command(client, "SET key1 value1")
            send_command(client, "SET key2 value2")
            
            response = send_command(client, "SCAN *")
            
            assert response.startswith("SCAN")
            assert "key1" in response
            assert "key2" in response
    
    def test_existing_commands_unaffected(self, server):
        """Test that existing commands still work as expected."""
        with connect_to_server() as client:
            # Test basic operations
            assert send_command(client, "SET test_key test_value") == "OK"
            assert send_command(client, "GET test_key") == "VALUE test_value"
            assert send_command(client, "DELETE test_key") == "OK"
            assert send_command(client, "GET test_key") == "NOT_FOUND"
            
            # Test numeric operations
            send_command(client, "SET counter 10")
            assert send_command(client, "INC counter") == "VALUE 11"
            assert send_command(client, "DEC counter 5") == "VALUE 6"


class TestErrorHandling:
    """Test error handling for new commands."""
    
    def test_command_arity_validation(self, server):
        """Test that commands validate argument count correctly."""
        with connect_to_server() as client:
            # Commands that require arguments
            assert send_command(client, "ECHO").startswith("ERROR")
            assert send_command(client, "KEYS").startswith("ERROR")  
            assert send_command(client, "EXISTS").startswith("ERROR")
            
            # Commands that don't allow extra arguments
            assert send_command(client, "DBSIZE extra").startswith("ERROR")
    
    def test_invalid_numeric_arguments(self, server):
        """Test handling of invalid numeric arguments."""
        with connect_to_server() as client:
            # Invalid cursor
            assert send_command(client, "SCAN abc").startswith("ERROR")
            
            # Invalid COUNT
            assert send_command(client, "SCAN 0 COUNT abc").startswith("ERROR")
    
    def test_malformed_scan_options(self, server):
        """Test handling of malformed SCAN options.""" 
        with connect_to_server() as client:
            # Unknown option
            assert send_command(client, "SCAN 0 UNKNOWN value").startswith("ERROR")
            
            # Missing values for options
            assert send_command(client, "SCAN 0 MATCH").startswith("ERROR")
            assert send_command(client, "SCAN 0 COUNT").startswith("ERROR")
