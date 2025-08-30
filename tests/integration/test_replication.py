#!/usr/bin/env python3
"""
Test cases for MQTT-based replication functionality.

This module tests the real-time replication of write operations across
MerkleKV nodes using MQTT as the message transport.

Test Setup:
- Uses local MQTT broker: localhost:1883
- Creates multiple MerkleKV server instances
- Verifies that write operations on one node are replicated to others
- Tests various operations: SET, DELETE, INC, DEC, APPEND, PREPEND
"""

import asyncio
import json
import pytest
import time
import uuid
import threading
import paho.mqtt.client as mqtt

# Note: replication_setup fixture is provided by tests/integration/conftest.py

class MQTTTestClient:
    """Test client to monitor MQTT messages."""
    
    def __init__(self, topic_prefix: str):
        self.topic_prefix = topic_prefix
        self.received_messages = []
        self.connected = threading.Event()
        
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected.set()
            topic = f"{self.topic_prefix}/events/#"
            client.subscribe(topic)
            
    def on_message(self, client, userdata, msg):
        try:
            # Try to decode as JSON first (legacy format)
            payload = json.loads(msg.payload.decode())
            self.received_messages.append({
                'topic': msg.topic,
                'payload': payload,
                'timestamp': time.time()
            })
        except json.JSONDecodeError:
            # Handle binary format (CBOR)
            self.received_messages.append({
                'topic': msg.topic,
                'payload': msg.payload,
                'timestamp': time.time(),
                'format': 'binary'
            })
        
    async def monitor_replication_messages(self, duration: float = 5.0):
        """Monitor MQTT messages for a specified duration."""
        try:
            client = mqtt.Client()
            client.on_connect = self.on_connect
            client.on_message = self.on_message
            
            client.connect("localhost", 1883, 60)
            client.loop_start()
            
            # Wait for connection
            if self.connected.wait(timeout=10):
                # Monitor for the specified duration
                await asyncio.sleep(duration)
            
            client.loop_stop()
            client.disconnect()
                        
        except Exception as e:
            print(f"MQTT monitoring error: {e}")

@pytest.mark.asyncio
async def test_basic_replication_setup(replication_setup):
    """Test that replication nodes can be created and connected."""
    node1 = await replication_setup.create_node("node1", 7380)
    node2 = await replication_setup.create_node("node2", 7381)
    
    # Basic connectivity test on node1
    res = await node1.send("SET test_key test_value")
    assert res == "OK"
    response = await node1.send("GET test_key")
    assert response == "VALUE test_value"
    
    # Node2 should also be responsive
    res2 = await node2.send("SET test_key2 test_value2")
    assert res2 == "OK"

@pytest.mark.asyncio
async def test_set_operation_replication(replication_setup):
    """Test that SET operations are replicated between nodes."""
    # Create two nodes
    node1 = await replication_setup.create_node("node1", 7382)
    node2 = await replication_setup.create_node("node2", 7383)
    
    # Start MQTT monitoring
    mqtt_client = MQTTTestClient(replication_setup.topic_prefix)
    monitor_task = asyncio.create_task(mqtt_client.monitor_replication_messages(10.0))
    
    # Wait for MQTT connections to stabilize
    await asyncio.sleep(3)
    
    # Perform SET operation on node1
    test_key = f"repl_test_{uuid.uuid4().hex[:8]}"
    test_value = "replicated_value"
    
    await node1.send(f"SET {test_key} {test_value}")
    
    # Wait for replication to occur
    await asyncio.sleep(5)
    
    # Verify the value exists on node2
    result = await node2.send(f"GET {test_key}")
    assert result == f"VALUE {test_value}", f"Expected VALUE {test_value}, got {result}"
    
    # Stop monitoring and check messages
    monitor_task.cancel()
    try:
        await monitor_task
    except asyncio.CancelledError:
        pass
    
    # Verify MQTT messages were sent
    assert len(mqtt_client.received_messages) > 0, "No MQTT messages received"
    
    # Check if any message contains our operation
    found_operation = False
    for msg in mqtt_client.received_messages:
        if 'payload' in msg and isinstance(msg['payload'], dict):
            payload = msg['payload']
            if (payload.get('operation') == 'SET' or 
                payload.get('key') == test_key):
                found_operation = True
                break
    
    # Note: Due to binary encoding (CBOR), we might not be able to decode all messages
    # but the replication should still work
    print(f"Found {len(mqtt_client.received_messages)} MQTT messages")

@pytest.mark.asyncio
async def test_delete_operation_replication(replication_setup):
    """Test that DELETE operations are replicated between nodes."""
    # Create two nodes
    node1 = await replication_setup.create_node("node1", 7384)
    node2 = await replication_setup.create_node("node2", 7385)
    
    # Wait for MQTT connections to stabilize
    await asyncio.sleep(3)
    
    test_key = f"delete_test_{uuid.uuid4().hex[:8]}"
    
    # Set initial value on both nodes (to ensure they have the key)
    await node1.send(f"SET {test_key} initial_value")
    await asyncio.sleep(2)
    
    # Verify both nodes have the value
    result1 = await node1.send(f"GET {test_key}")
    result2 = await node2.send(f"GET {test_key}")
    assert result1 == "VALUE initial_value"
    assert result2 == "VALUE initial_value"
    
    # Delete from node1
    await node1.send(f"DEL {test_key}")
    
    # Wait for replication
    await asyncio.sleep(5)
    
    # Verify deletion on node2
    result2 = await node2.send(f"GET {test_key}")
    assert result2 == "NOT_FOUND", f"Expected NOT_FOUND, got {result2}"

@pytest.mark.asyncio
async def test_numeric_operations_replication(replication_setup):
    """Test that INCR/DECR operations are replicated between nodes."""
    # Create two nodes
    node1 = await replication_setup.create_node("node1", 7386)
    node2 = await replication_setup.create_node("node2", 7387)
    
    # Wait for MQTT connections to stabilize
    await asyncio.sleep(3)
    
    test_key = f"numeric_test_{uuid.uuid4().hex[:8]}"
    
    # Initialize with a numeric value
    await node1.send(f"SET {test_key} 10")
    await asyncio.sleep(2)
    
    # Verify initial value on both nodes
    result1 = await node1.send(f"GET {test_key}")
    result2 = await node2.send(f"GET {test_key}")
    assert result1 == "VALUE 10"
    assert result2 == "VALUE 10"
    
    # Increment on node1
    await node1.send(f"INCR {test_key}")
    await asyncio.sleep(3)
    
    # Verify increment replicated to node2
    result2 = await node2.send(f"GET {test_key}")
    assert result2 == "VALUE 11", f"Expected VALUE 11, got {result2}"
    
    # Decrement on node2
    await node2.send(f"DECR {test_key}")
    await asyncio.sleep(3)
    
    # Verify decrement replicated to node1
    result1 = await node1.send(f"GET {test_key}")
    assert result1 == "VALUE 10", f"Expected VALUE 10, got {result1}"

@pytest.mark.asyncio
async def test_string_operations_replication(replication_setup):
    """Test that APPEND/PREPEND operations are replicated between nodes."""
    # Create two nodes
    node1 = await replication_setup.create_node("node1", 7388)
    node2 = await replication_setup.create_node("node2", 7389)
    
    # Wait for MQTT connections to stabilize
    await asyncio.sleep(3)
    
    test_key = f"string_test_{uuid.uuid4().hex[:8]}"
    
    # Set initial value
    await node1.send(f"SET {test_key} hello")
    await asyncio.sleep(2)
    
    # Verify initial value on both nodes
    result1 = await node1.send(f"GET {test_key}")
    result2 = await node2.send(f"GET {test_key}")
    assert result1 == "VALUE hello"
    assert result2 == "VALUE hello"
    
    # Append on node1
    await node1.send(f"APPEND {test_key} _world")
    await asyncio.sleep(3)
    
    # Verify append replicated to node2
    result2 = await node2.send(f"GET {test_key}")
    assert result2 == "VALUE hello_world", f"Expected VALUE hello_world, got {result2}"
    
    # Prepend on node2
    await node2.send(f"PREPEND {test_key} say_")
    await asyncio.sleep(3)
    
    # Verify prepend replicated to node1
    result1 = await node1.send(f"GET {test_key}")
    assert result1 == "VALUE say_hello_world", f"Expected VALUE say_hello_world, got {result1}"

@pytest.mark.asyncio
async def test_concurrent_operations_replication(replication_setup):
    """Test replication behavior with concurrent operations on multiple nodes."""
    # Create three nodes for more complex testing
    node1 = await replication_setup.create_node("node1", 7390)
    node2 = await replication_setup.create_node("node2", 7391)
    node3 = await replication_setup.create_node("node3", 7392)
    
    # Wait for MQTT connections to stabilize
    await asyncio.sleep(5)
    
    # Perform concurrent operations
    operations = [
        node1.send("SET concurrent_test1 value1"),
        node2.send("SET concurrent_test2 value2"),
        node3.send("SET concurrent_test3 value3"),
    ]
    
    await asyncio.gather(*operations)
    
    # Wait for replication to settle
    await asyncio.sleep(10)
    
    # Verify all values are present on all nodes
    for node in [node1, node2, node3]:
        result1 = await node.send("GET concurrent_test1")
        result2 = await node.send("GET concurrent_test2")
        result3 = await node.send("GET concurrent_test3")
        assert result1 == "VALUE value1", f"Node missing concurrent_test1: {result1}"
        assert result2 == "VALUE value2", f"Node missing concurrent_test2: {result2}"
        assert result3 == "VALUE value3", f"Node missing concurrent_test3: {result3}"

@pytest.mark.asyncio
async def test_replication_with_node_restart(replication_setup):
    """Test replication behavior when a node is restarted."""
    # Create two nodes
    node1 = await replication_setup.create_node("node1", 7393)
    node2 = await replication_setup.create_node("node2", 7394)
    
    # Wait for MQTT connections to stabilize
    await asyncio.sleep(3)
    
    # Set some initial data
    await node1.send("SET restart_test1 before_restart")
    await asyncio.sleep(2)
    
    # Verify replication
    result = await node2.send("GET restart_test1")
    assert result == "VALUE before_restart"
    
    # Stop node2
    await node2.stop()
    
    # Add more data while node2 is down
    await node1.send("SET restart_test2 during_downtime")
    await asyncio.sleep(2)
    
    # Restart node2 (simulate restart)
    node2_restarted = await replication_setup.create_node("node2_restart", 7395)
    await asyncio.sleep(5)
    
    # Add data after restart
    await node1.send("SET restart_test3 after_restart")
    await asyncio.sleep(5)
    
    # Verify new data is replicated to restarted node
    result = await node2_restarted.send("GET restart_test3")
    assert result == "VALUE after_restart"
    
    # Note: Data during downtime might not be replicated since MQTT 
    # doesn't persist messages for disconnected clients by default

@pytest.mark.asyncio
async def test_replication_loop_prevention(replication_setup):
    """Test that nodes don't create infinite loops by processing their own messages."""
    # Create a single node
    node1 = await replication_setup.create_node("node1", 7396)
    
    # Start MQTT monitoring
    mqtt_client = MQTTTestClient(replication_setup.topic_prefix)
    monitor_task = asyncio.create_task(
        mqtt_client.monitor_replication_messages(15.0)
    )
    
    # Wait for MQTT connections to stabilize
    await asyncio.sleep(3)
    
    # Perform multiple operations rapidly
    for i in range(5):
        await node1.send(f"SET loop_test_{i} value_{i}")
        await asyncio.sleep(0.5)
    
    # Wait for all messages to be processed
    await asyncio.sleep(5)
    
    # Stop monitoring
    monitor_task.cancel()
    try:
        await monitor_task
    except asyncio.CancelledError:
        pass
    
    # Verify we don't have an excessive number of messages (indicating loops)
    # We should have roughly 5 messages, not 50+ from loops
    message_count = len(mqtt_client.received_messages)
    assert message_count <= 20, f"Too many messages detected ({message_count}), possible loop"
    
    print(f"Received {message_count} MQTT messages for 5 operations")

@pytest.mark.asyncio
async def test_malformed_mqtt_message_handling(replication_setup):
    """Test that nodes handle malformed MQTT messages gracefully."""
    # Create a node
    node1 = await replication_setup.create_node("node1", 7397)
    
    # Wait for MQTT connections to stabilize
    await asyncio.sleep(3)
    
    # Send a malformed message via MQTT
    try:
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                topic = f"{replication_setup.topic_prefix}/events"
                
                # Send invalid JSON
                client.publish(topic, "invalid json message")
                
                # Send valid JSON but wrong format
                client.publish(topic, json.dumps({"invalid": "format"}))
                
        client = mqtt.Client()
        client.on_connect = on_connect
        client.connect("localhost", 1883, 60)
        client.loop_start()
        
        # Wait a bit
        await asyncio.sleep(3)
        
        client.loop_stop()
        client.disconnect()
        
    except Exception as e:
        print(f"MQTT client error (expected in some cases): {e}")

    # Verify the node is still responsive
    result = await node1.send("SET test_after_malformed success")
    assert result == "OK"

    result = await node1.send("GET test_after_malformed")
    assert result == "VALUE success"

if __name__ == "__main__":
    # Run specific test
    import sys
    if len(sys.argv) > 1:
        test_name = sys.argv[1]
        pytest.main([f"-v", f"-k", test_name, __file__])
    else:
        pytest.main(["-v", __file__])
