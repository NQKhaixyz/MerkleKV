#!/usr/bin/env python3
"""
MerkleKV Replication Adversarial Tests - Malformed Payload Handling

This module tests the replication system's resilience to malformed MQTT messages,
corrupted payloads, and invalid event structures.

Test scenarios:
- Invalid JSON messages
- Corrupted CBOR payloads  
- Messages with missing required fields
- Messages with invalid field types
- Oversized payloads
- Binary garbage data
"""

import asyncio
import json
import pytest
import time
import uuid
import paho.mqtt.client as mqtt
from conftest import ReplicationTestHarness


class TestReplicationMalformedPayloads:
    """Test malformed payload handling in replication"""

    @pytest.mark.asyncio
    async def test_invalid_json_messages(self):
        """Test handling of invalid JSON in MQTT messages"""
        timestamp = int(time.time())
        harness = ReplicationTestHarness(f"test_malformed_json_{timestamp}")
        
        try:
            # Create a node that should remain stable despite malformed messages
            node1 = await harness.create_node("malformed_json_node1", 7420)
            
            # Wait for MQTT connection to stabilize
            await asyncio.sleep(3)
            
            # Send various invalid JSON messages via MQTT
            malformed_messages = [
                b"invalid json",
                b'{"incomplete": ',
                b'{"invalid": "json"',  # missing closing brace
                b'{"key": value}',  # unquoted value
                b'{"duplicate": "key", "duplicate": "key2"}',
                b'null',
                b'[]',  # array instead of object
                b'"just a string"',
                b'123',  # just a number
                b'{"nested": {"incomplete":}',  # syntax error
            ]
            
            # Create MQTT client to send malformed messages
            def send_malformed_messages():
                try:
                    client = mqtt.Client()
                    client.connect("test.mosquitto.org", 1883, 60)
                    client.loop_start()
                    
                    topic = f"{harness.topic_prefix}/events"
                    
                    for i, bad_msg in enumerate(malformed_messages):
                        client.publish(topic, bad_msg)
                        time.sleep(0.1)  # Small delay between messages
                    
                    time.sleep(2)  # Let messages propagate
                    client.loop_stop()
                    client.disconnect()
                    
                except Exception as e:
                    print(f"MQTT client error: {e}")
            
            # Send malformed messages in background
            import threading
            mqtt_thread = threading.Thread(target=send_malformed_messages)
            mqtt_thread.start()
            
            # Wait for malformed messages to be processed
            await asyncio.sleep(5)
            
            # Verify node is still responsive after malformed messages
            response = await node1.send("SET test_after_malformed success")
            assert response == "OK", f"Node unresponsive after malformed JSON: {response}"
            
            response = await node1.send("GET test_after_malformed")
            assert response == "VALUE success", f"Node data corrupted: {response}"
            
            # Wait for thread to complete
            mqtt_thread.join(timeout=10)
            
            print(f"✅ Invalid JSON test passed: {len(malformed_messages)} malformed messages handled")
            
        finally:
            await harness.shutdown()

    @pytest.mark.asyncio
    async def test_corrupted_binary_payloads(self):
        """Test handling of corrupted binary (CBOR) payloads"""
        timestamp = int(time.time())
        harness = ReplicationTestHarness(f"test_corrupted_binary_{timestamp}")
        
        try:
            node1 = await harness.create_node("corrupted_binary_node1", 7421)
            
            await asyncio.sleep(3)
            
            # Generate various corrupted binary payloads
            corrupted_payloads = [
                b"\x00\x01\x02\x03",  # Random bytes
                b"\xff" * 100,  # All 0xFF bytes
                b"\x00" * 1000,  # Large null payload
                b"\x80\x81\x82\x83\x84",  # Invalid CBOR markers
                bytes(range(256)),  # Full byte range
                b"CBOR" + b"\x00" * 50,  # Fake CBOR header + nulls
                b"\xa1\x61\x61",  # Incomplete CBOR map
            ]
            
            def send_corrupted_payloads():
                try:
                    client = mqtt.Client()
                    client.connect("test.mosquitto.org", 1883, 60)
                    client.loop_start()
                    
                    topic = f"{harness.topic_prefix}/events"
                    
                    for payload in corrupted_payloads:
                        client.publish(topic, payload)
                        time.sleep(0.1)
                    
                    time.sleep(2)
                    client.loop_stop()
                    client.disconnect()
                    
                except Exception as e:
                    print(f"Binary MQTT error: {e}")
            
            # Send corrupted payloads
            import threading
            mqtt_thread = threading.Thread(target=send_corrupted_payloads)
            mqtt_thread.start()
            
            await asyncio.sleep(5)
            
            # Verify node stability
            response = await node1.send("SET test_after_binary success")
            assert response == "OK", f"Node failed after binary corruption: {response}"
            
            response = await node1.send("GET test_after_binary")
            assert response == "VALUE success", f"Node state corrupted: {response}"
            
            mqtt_thread.join(timeout=10)
            
            print(f"✅ Corrupted binary test passed: {len(corrupted_payloads)} corrupted payloads handled")
            
        finally:
            await harness.shutdown()

    @pytest.mark.asyncio
    async def test_missing_required_fields(self):
        """Test handling of messages with missing required fields"""
        timestamp = int(time.time())
        harness = ReplicationTestHarness(f"test_missing_fields_{timestamp}")
        
        try:
            node1 = await harness.create_node("missing_fields_node1", 7422)
            
            await asyncio.sleep(3)
            
            # Messages with various missing fields
            malformed_events = [
                {},  # Empty object
                {"operation": "SET"},  # Missing key
                {"key": "test"},  # Missing operation
                {"operation": "SET", "key": "test"},  # Missing value for SET
                {"operation": "DELETE", "value": "test"},  # Missing key for DELETE
                {"key": "test", "value": "data"},  # Missing operation
                {"operation": "INVALID", "key": "test", "value": "data"},  # Invalid operation
                {"operation": "SET", "key": "", "value": "data"},  # Empty key
                {"operation": "SET", "key": None, "value": "data"},  # Null key
            ]
            
            def send_incomplete_messages():
                try:
                    client = mqtt.Client()
                    client.connect("test.mosquitto.org", 1883, 60)
                    client.loop_start()
                    
                    topic = f"{harness.topic_prefix}/events"
                    
                    for event in malformed_events:
                        json_payload = json.dumps(event).encode()
                        client.publish(topic, json_payload)
                        time.sleep(0.1)
                    
                    time.sleep(2)
                    client.loop_stop()
                    client.disconnect()
                    
                except Exception as e:
                    print(f"Incomplete message MQTT error: {e}")
            
            import threading
            mqtt_thread = threading.Thread(target=send_incomplete_messages)
            mqtt_thread.start()
            
            await asyncio.sleep(5)
            
            # Verify node handles incomplete messages gracefully
            response = await node1.send("SET test_after_incomplete success")
            assert response == "OK", f"Node failed after incomplete messages: {response}"
            
            response = await node1.send("GET test_after_incomplete")
            assert response == "VALUE success", f"Node corrupted by incomplete messages: {response}"
            
            mqtt_thread.join(timeout=10)
            
            print(f"✅ Missing fields test passed: {len(malformed_events)} incomplete messages handled")
            
        finally:
            await harness.shutdown()

    @pytest.mark.asyncio
    async def test_invalid_field_types(self):
        """Test handling of messages with invalid field types"""
        timestamp = int(time.time())
        harness = ReplicationTestHarness(f"test_invalid_types_{timestamp}")
        
        try:
            node1 = await harness.create_node("invalid_types_node1", 7423)
            
            await asyncio.sleep(3)
            
            # Messages with wrong field types
            type_error_events = [
                {"operation": 123, "key": "test", "value": "data"},  # numeric operation
                {"operation": "SET", "key": 456, "value": "data"},  # numeric key
                {"operation": "SET", "key": "test", "value": 789},  # numeric value (could be valid)
                {"operation": ["SET"], "key": "test", "value": "data"},  # array operation
                {"operation": "SET", "key": ["test"], "value": "data"},  # array key
                {"operation": "SET", "key": "test", "value": {"nested": "object"}},  # object value
                {"operation": True, "key": "test", "value": "data"},  # boolean operation
                {"operation": "SET", "key": True, "value": "data"},  # boolean key
                {"operation": "SET", "key": "test", "value": [1, 2, 3]},  # array value
            ]
            
            def send_type_error_messages():
                try:
                    client = mqtt.Client()
                    client.connect("test.mosquitto.org", 1883, 60)
                    client.loop_start()
                    
                    topic = f"{harness.topic_prefix}/events"
                    
                    for event in type_error_events:
                        json_payload = json.dumps(event).encode()
                        client.publish(topic, json_payload)
                        time.sleep(0.1)
                    
                    time.sleep(2)
                    client.loop_stop()
                    client.disconnect()
                    
                except Exception as e:
                    print(f"Type error MQTT error: {e}")
            
            import threading
            mqtt_thread = threading.Thread(target=send_type_error_messages)
            mqtt_thread.start()
            
            await asyncio.sleep(5)
            
            # Verify node handles type errors gracefully
            response = await node1.send("SET test_after_type_errors success")
            assert response == "OK", f"Node failed after type errors: {response}"
            
            response = await node1.send("GET test_after_type_errors")
            assert response == "VALUE success", f"Node corrupted by type errors: {response}"
            
            mqtt_thread.join(timeout=10)
            
            print(f"✅ Invalid types test passed: {len(type_error_events)} type error messages handled")
            
        finally:
            await harness.shutdown()

    @pytest.mark.asyncio
    async def test_oversized_payloads(self):
        """Test handling of extremely large payloads"""
        timestamp = int(time.time())
        harness = ReplicationTestHarness(f"test_oversized_{timestamp}")
        
        try:
            node1 = await harness.create_node("oversized_node1", 7424)
            
            await asyncio.sleep(3)
            
            # Generate oversized payloads
            large_payloads = [
                json.dumps({"operation": "SET", "key": "test", "value": "x" * 10000}).encode(),
                json.dumps({"operation": "SET", "key": "y" * 1000, "value": "data"}).encode(),
                b"x" * 50000,  # 50KB of raw data
                json.dumps({"operation": "SET", "key": "test", "value": "z" * 100000}).encode(),
            ]
            
            def send_oversized_messages():
                try:
                    client = mqtt.Client()
                    client.connect("test.mosquitto.org", 1883, 60)
                    client.loop_start()
                    
                    topic = f"{harness.topic_prefix}/events"
                    
                    for payload in large_payloads:
                        try:
                            client.publish(topic, payload)
                            time.sleep(0.5)  # Longer delay for large messages
                        except Exception as e:
                            print(f"Failed to send oversized message: {e}")
                    
                    time.sleep(3)
                    client.loop_stop()
                    client.disconnect()
                    
                except Exception as e:
                    print(f"Oversized MQTT error: {e}")
            
            import threading
            mqtt_thread = threading.Thread(target=send_oversized_messages)
            mqtt_thread.start()
            
            await asyncio.sleep(8)
            
            # Verify node handles oversized messages gracefully
            response = await node1.send("SET test_after_oversized success")
            assert response == "OK", f"Node failed after oversized messages: {response}"
            
            response = await node1.send("GET test_after_oversized")
            assert response == "VALUE success", f"Node corrupted by oversized messages: {response}"
            
            mqtt_thread.join(timeout=15)
            
            print(f"✅ Oversized payloads test passed: {len(large_payloads)} large messages handled")
            
        finally:
            await harness.shutdown()

    @pytest.mark.asyncio
    async def test_mixed_malformed_payload_storm(self):
        """Test resilience to a mixed storm of various malformed payloads"""
        timestamp = int(time.time())
        harness = ReplicationTestHarness(f"test_malformed_storm_{timestamp}")
        
        try:
            node1 = await harness.create_node("malformed_storm_node1", 7425)
            node2 = await harness.create_node("malformed_storm_node2", 7426)
            
            await asyncio.sleep(3)
            
            # Set initial data to verify persistence through the storm
            await node1.send("SET storm_test_key initial_value")
            await asyncio.sleep(2)
            
            # Mixed storm of malformed messages
            malformed_storm = []
            
            # Add various types of malformed messages
            for i in range(20):
                malformed_storm.extend([
                    b"invalid json " + str(i).encode(),
                    json.dumps({"operation": "SET"}).encode(),  # missing fields
                    b"\xff" * (i + 1),  # binary garbage
                    json.dumps({"operation": i, "key": "test", "value": "data"}).encode(),  # type errors
                ])
            
            def send_malformed_storm():
                try:
                    client = mqtt.Client()
                    client.connect("test.mosquitto.org", 1883, 60)
                    client.loop_start()
                    
                    topic = f"{harness.topic_prefix}/events"
                    
                    for i, payload in enumerate(malformed_storm):
                        try:
                            client.publish(topic, payload)
                            if i % 10 == 0:
                                time.sleep(0.1)
                        except Exception as e:
                            print(f"Storm message {i} failed: {e}")
                    
                    time.sleep(3)
                    client.loop_stop()
                    client.disconnect()
                    
                except Exception as e:
                    print(f"Malformed storm MQTT error: {e}")
            
            import threading
            mqtt_thread = threading.Thread(target=send_malformed_storm)
            mqtt_thread.start()
            
            # Wait for storm to complete
            await asyncio.sleep(10)
            
            # Verify both nodes survived the storm and maintain data integrity
            response1 = await node1.send("GET storm_test_key")
            response2 = await node2.send("GET storm_test_key")
            
            assert response1 == "VALUE initial_value", f"Node1 data corrupted by storm: {response1}"
            assert response2 == "VALUE initial_value", f"Node2 data corrupted by storm: {response2}"
            
            # Verify both nodes can still process valid operations
            await node1.send("SET post_storm_test success")
            await asyncio.sleep(3)
            
            response2_post = await node2.send("GET post_storm_test")
            assert response2_post == "VALUE success", f"Replication broken after storm: {response2_post}"
            
            mqtt_thread.join(timeout=15)
            
            print(f"✅ Malformed storm test passed: {len(malformed_storm)} malformed messages survived")
            
        finally:
            await harness.shutdown()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
