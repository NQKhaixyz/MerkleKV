# Replication Testing Guide

This document describes how to test MerkleKV's replication functionality using MQTT brokers and provides comprehensive guidance for development and production scenarios.

## 🔍 Replication Compliance Audit (August 30, 2025)

**Status: ✅ FULLY COMPLIANT** - All documented replication requirements verified and tested.

### Requirements Verification

**Core Requirements Met:**
- ✅ **MQTT Integration**: rumqttc client with auto-reconnection (`src/replication.rs::Replicator`)
- ✅ **ChangeEvent Protocol**: Complete schema with CBOR encoding (`src/change_event.rs`) 
- ✅ **All Operations**: SET, DELETE, INCR, DECR, APPEND, PREPEND replication
- ✅ **LWW Resolution**: Timestamp-based with op_id tie-breaking
- ✅ **Loop Prevention**: Source node filtering (`ev.src == node_id`)
- ✅ **Idempotency**: UUID deduplication via HashSet
- ✅ **Best-effort**: Non-blocking client operations on MQTT errors

**Test Coverage:** 12 core scenarios + new adversarial test battery covering ordering, clock skew, broker outages, malformed payloads, and throughput spikes.

## Quick Start: Replication Test Modes

```bash
# Fast CI-friendly tests (<2m)
python tests/integration/run_tests.py --mode quick

# All replication tests except slow ones (<5m) 
python tests/integration/run_tests.py --mode replication

# Heavy/adversarial tests including @slow (<15m)
python tests/integration/run_tests.py --mode chaos
```

## 🚀 Quick Start Testing

### Environment Variables

Configure your test environment with these optional variables:

```bash
export MQTT_BROKER_HOST="test.mosquitto.org"    # Default: test.mosquitto.org
export MQTT_BROKER_PORT="1883"                 # Default: 1883  
export MQTT_TOPIC_PREFIX="test_merkle_kv"       # Default: test_merkle_kv_{timestamp}
```

### Two-Node Quickstart

1. **Create Node Configurations**

```toml
# node1.toml
host = "127.0.0.1"
port = 7600
storage_path = "data_node1"
engine = "rwlock"
sync_interval_seconds = 60

[replication]
enabled = true
mqtt_broker = "test.mosquitto.org"
mqtt_port = 1883
topic_prefix = "quickstart_demo" 
client_id = "node1"
```

```toml
# node2.toml  
host = "127.0.0.1"
port = 7601
storage_path = "data_node2"
engine = "rwlock"
sync_interval_seconds = 60

[replication]
enabled = true
mqtt_broker = "test.mosquitto.org"
mqtt_port = 1883
topic_prefix = "quickstart_demo"
client_id = "node2"
```

2. **Start Both Nodes**

```bash
# Terminal 1
cargo run -- --config node1.toml

# Terminal 2  
cargo run -- --config node2.toml
```

3. **Test Replication**

```bash
# Terminal 3: Write to Node 1
echo "SET user:alice Alice Johnson" | nc 127.0.0.1 7600
# Response: OK

# Terminal 4: Read from Node 2 (after 3-5 seconds)
echo "GET user:alice" | nc 127.0.0.1 7601  
# Response: VALUE Alice Johnson ✅
```

## 🧪 Test Modes & Commands

### Basic Testing

```bash
cd tests/integration

# Quick tests (fast, safe for CI)
python run_tests.py --mode quick

# All replication tests except slow ones
python run_tests.py --mode replication

# Heavy/adversarial tests (opt-in)
python run_tests.py --mode chaos

# Individual test categories
pytest -v -k "replication and not slow"
pytest -v -k "replication_ordering"
pytest -v -k "replication_clock_skew"
```

### Advanced Testing

```bash
# With custom broker
MQTT_BROKER_HOST=localhost pytest -v test_replication.py

# Throughput and stress tests
pytest -v -m "benchmark" test_replication_throughput_spike.py

# Malformed payload resilience
pytest -v test_replication_malformed_payloads.py
```

## 🏗️ Test Architecture

### Core Test Infrastructure

**ReplicationTestHarness** (`conftest.py`):
- Manages multiple MerkleKV server instances
- Handles configuration generation and cleanup
- Provides NodeHandle abstraction for test operations

**NodeHandle** (`conftest.py`):
- Encapsulates server process and TCP communication
- Provides `send(command)` and `get/set/delete` helpers  
- Automatic lifecycle management

### New Adversarial Test Battery

**Ordering & Consistency** (`test_replication_ordering.py`):
- Out-of-order delivery with shuffled timestamps
- Burst duplicate detection and idempotency
- LWW conflict resolution with op_id tie-breaking

**Clock Skew Simulation** (`test_replication_clock_skew.py`):
- Producer nodes with artificial time skew
- Boundary cases with equal timestamps
- Deterministic conflict resolution verification

**Broker Resilience** (`test_replication_broker_outage.py`):
- Simulated broker downtime and recovery
- Connection retry behavior validation
- State healing after network partition

**Malformed Data** (`test_replication_malformed_payloads.py`):
- Garbage bytes injection into MQTT topics
- Truncated/corrupted CBOR payloads
- Error handling without service disruption

**Performance** (`test_replication_throughput_spike.py`):
- Rapid-fire operations across multiple nodes
- Eventual consistency under load
- Latency measurements and SLA validation

## 🔧 Test Configuration

### MQTT Broker Options

**Option 1: Public Broker (Default)**
```bash
# Uses test.mosquitto.org:1883
# Good for: Quick testing, CI/CD
# Limitations: Network dependent, shared resource
```

**Option 2: Local Docker Broker**
```bash
# Start local mosquitto
docker run -d --name mqtt-test -p 1883:1883 eclipse-mosquitto

# Configure tests
export MQTT_BROKER_HOST=localhost
python run_tests.py --mode replication
```

**Option 3: CI-Embedded Broker**
```bash
# For GitHub Actions / CI pipelines
services:
  mqtt:
    image: eclipse-mosquitto:latest
    ports:
      - 1883:1883
```

## 🐛 Troubleshooting

| Error | Root Cause | Quick Fix |
|-------|------------|-----------|
| `Failed to connect to MQTT broker` | Network/firewall issues | Try `MQTT_BROKER_HOST=localhost` with Docker |
| `Server failed to start within timeout` | Port conflicts | Check `lsof -i :7600-7610` |
| `Expected replicated_value, got NOT_FOUND` | Timing/latency issues | Increase wait time in test config |
| `CBOR decode error` | Version mismatch | Rebuild with `cargo clean && cargo build` |
| `Loop prevention not working` | Same client_id | Ensure unique `client_id` per node |
| `Tests flaky/intermittent` | Race conditions | Use `pytest -v --tb=short -x` for debugging |
| `Throughput tests timeout` | Resource limits | Run with `--mode chaos` marker separately |

| Symptom | Likely Cause | Fix |
| --- | --- | --- |
| No replication across nodes | topic_prefix mismatch or same client_id | Make client_id unique; align topic_prefix |
| Flaky publish | broker down / network jitter | Re-run with local broker; check logs; chaos mode exercises outages |
| Decode errors | wrong codec / malformed payload | Ensure CBOR default; see malformed payload tests |

### Debug Mode

```bash
# Enable verbose Rust logging
RUST_LOG=debug pytest -v -s test_replication.py

# Capture all test output
pytest -v -s --tb=long --capture=no test_replication_ordering.py
```

## 📊 Performance Baselines

**Expected Latency (test.mosquitto.org):**
- SET replication: 3-5 seconds (P95)
- Throughput: ~100 ops/sec sustained
- Memory: <50MB per node (in-memory storage)

**Local Broker Performance:**
- SET replication: <500ms (P95)  
- Throughput: ~1000 ops/sec sustained
- Connection establishment: <100ms

## 🔄 Continuous Integration

**PR Checks (Fast):**
```bash
python run_tests.py --mode quick  # < 2 minutes
```

**Nightly/Main Branch:**
```bash  
python run_tests.py --mode replication  # < 5 minutes
```

**Weekly Stress Testing:**
```bash
python run_tests.py --mode chaos  # < 15 minutes  
```

## 📝 Adding New Replication Tests

When adding new replication scenarios:

1. **Use ReplicationTestHarness** for consistent setup
2. **Mark appropriately**: `@pytest.mark.slow` for heavy tests  
3. **Add clear docstrings** explaining the scenario
4. **Use deterministic seeds** for any randomness
5. **Implement proper timeouts** and retry logic
6. **Test both success and failure** paths

Example test skeleton:
```python
@pytest.mark.asyncio
async def test_my_replication_scenario(replication_setup):
    """Test description of what this validates."""
    harness = replication_setup
    node1, node2 = await harness.create_nodes(2)
    
    # Test implementation
    await node1.send("SET key value")
    await harness.wait_for_replication()
    result = await node2.send("GET key")
    assert "VALUE value" in result
```

# Test MQTT broker connectivity
pytest -v -k test_mqtt_broker_connectivity

# Simple replication test
pytest -v test_replication_simple.py

# Run full replication test suite
pytest -v test_replication.py

# Run specific test
pytest -v -k test_set_operation_replication
```

## Test structure

### test_replication_simple.py

Simple tests to verify:
- Connection to public MQTT broker
- Start 2 servers with replication enabled
- Basic connectivity testing

### test_replication.py

Full test suite including:

#### 1. Basic tests
- `test_basic_replication_setup`: Initialize multiple nodes
- `test_set_operation_replication`: SET operation replication
- `test_delete_operation_replication`: DELETE operation replication

#### 2. Numeric/string operation tests
- `test_numeric_operations_replication`: INCR/DECR
- `test_string_operations_replication`: APPEND/PREPEND

#### 3. Concurrent and edge case tests
- `test_concurrent_operations_replication`: Concurrent operations
- `test_replication_with_node_restart`: Node restart scenarios
- `test_replication_loop_prevention`: Infinite loop prevention
- `test_malformed_mqtt_message_handling`: Malformed message handling

### New Adversarial Test Battery

#### test_replication_ordering.py
- Out-of-order delivery with timestamp validation
- Burst duplicate handling and idempotency checks
- LWW resolution with op_id tie-breaking

#### test_replication_clock_skew.py  
- Simulated clock skew scenarios
- Equal timestamp boundary testing
- Deterministic conflict resolution

#### test_replication_broker_outage.py
- Broker downtime simulation (marked `slow`)
- Connection recovery validation
- State consistency after network healing

#### test_replication_malformed_payloads.py
- Invalid CBOR/JSON payload injection
- Truncated message handling
- Service resilience under corruption

#### test_replication_throughput_spike.py
- High-frequency operation bursts (marked `slow`, `benchmark`)
- Eventual consistency under load
- Performance regression detection

## MQTT Configuration

Tests use a public MQTT broker by default:
- **Broker**: test.mosquitto.org
- **Port**: 1883
- **Topic pattern**: `test_merkle_kv_{random_id}/events/#`

Each test uses a different topic prefix to avoid conflicts.

## Test Architecture

### ReplicationTestSetup
Helper class managing multiple MerkleKV instances:
- Create config files with replication enabled
- Start/stop server instances
- Automatic cleanup

### MQTTTestClient
Client to monitor MQTT messages:
- Subscribe to replication topics
- Decode messages (JSON or CBOR)
- Message tracking for verification

## Troubleshooting

### 1. MQTT connection error
```
Failed to connect to MQTT broker
```
**Solution**: Check internet connection and firewall. The test.mosquitto.org broker can sometimes be overloaded.

### 2. Server startup failure
```
Server failed to start within timeout
```
**Solution**: 
- Check for port conflicts
- Review server output logs for debugging
- Increase timeout in config

### 3. Replication not working
```
Expected replicated_value, got (nil)
```
**Solution**:
- Check replication configuration in server
- Verify MQTT topic names
- Increase replication wait time

### 4. Slow tests
**Cause**: MQTT network latency, server startup time
**Solution**: 
- Run tests sequentially instead of parallel
- Use local MQTT broker for faster testing

## Customizing tests

### Using different MQTT broker

Edit in test files:
```python
mqtt_config = {
    "mqtt_broker": "your-broker.com",
    # ...
}
```

### Adding new test cases

1. Create test function with `test_` prefix
2. Use `@pytest.mark.asyncio` for async tests
3. Use `replication_setup` fixture to manage servers
```

## Test structure

### test_replication_simple.py

Simple tests to verify:
- Connection to public MQTT broker
- Start 2 servers with replication enabled
- Basic connectivity testing

### test_replication.py

Full test suite including:

#### 1. Basic tests
- `test_basic_replication_setup`: Initialize multiple nodes
- `test_set_operation_replication`: SET operation replication
- `test_delete_operation_replication`: DELETE operation replication

#### 2. Numeric/string operation tests
- `test_numeric_operations_replication`: INCR/DECR
- `test_string_operations_replication`: APPEND/PREPEND

#### 3. Concurrent and edge case tests
- `test_concurrent_operations_replication`: Concurrent operations
- `test_replication_with_node_restart`: Node restart scenarios
- `test_replication_loop_prevention`: Infinite loop prevention
- `test_malformed_mqtt_message_handling`: Malformed message handling

## MQTT Configuration

Tests use a public MQTT broker:
- **Broker**: test.mosquitto.org
- **Port**: 1883
- **Topic pattern**: `test_merkle_kv_{random_id}/events/#`

Each test uses a different topic prefix to avoid conflicts.

## Test Architecture

### ReplicationTestSetup
Helper class managing multiple MerkleKV instances:
- Create config files with replication enabled
- Start/stop server instances
- Automatic cleanup

### MQTTTestClient
Client to monitor MQTT messages:
- Subscribe to replication topics
- Decode messages (JSON or CBOR)
- Message tracking for verification

## Troubleshooting

### 1. MQTT connection error
```
Failed to connect to MQTT broker
```
**Solution**: Check internet connection and firewall. The test.mosquitto.org broker can sometimes be overloaded.

### 2. Server startup failure
```
Server failed to start within timeout
```
**Solution**: 
- Check for port conflicts
- Review server output logs for debugging
- Increase timeout in config

### 3. Replication not working
```
Expected replicated_value, got (nil)
```
**Solution**:
- Check replication configuration in server
- Verify MQTT topic names
- Increase replication wait time

### 4. Slow tests
**Cause**: MQTT network latency, server startup time
**Solution**: 
- Run tests sequentially instead of parallel
- Use local MQTT broker for faster testing

## Customizing tests

### Using different MQTT broker

Edit in test files:
```python
mqtt_config = {
    "mqtt_broker": "your-broker.com",
    "mqtt_port": 1883,
    # ...
}
```

### Adding new test cases

1. Create test function with `test_` prefix
2. Use `@pytest.mark.asyncio` for async tests
3. Use `replication_setup` fixture to manage servers
4. Follow pattern: setup → action → verify → cleanup

### Debug tests

Run with verbose output:
```bash
pytest -v -s test_replication.py
```

Enable Rust logging:
```bash
RUST_LOG=debug pytest -v test_replication.py
```

## CI/CD Integration

To integrate into CI/CD pipeline:

```yaml
# Example GitHub Actions
- name: Run replication tests
  run: |
    cd tests/integration
    python run_replication_tests.py all
```

**Note**: Tests use external MQTT broker so may fail due to network issues. Consider setting up MQTT broker in CI environment.V's replication functionality using a public MQTT broker.

## Overview

MerkleKV's replication system uses MQTT to synchronize write operations between nodes in a cluster. The test cases are designed to:

- Test MQTT broker connectivity
- Verify replication of SET, DELETE, INCR, DECR, APPEND, PREPEND operations
- Test infinite loop prevention
- Test error handling for malformed messages
- Test replication in concurrent environments

## Setup

### 1. Install dependencies

```bash
cd tests/integration
pip install -r requirements.txt
```

### 2. Build MerkleKV server

```bash
# From project root directory
cargo build
```

## Running tests

### Utility script

Use the `run_replication_tests.py` script to run tests:

```bash
cd tests/integration

# Run all (install deps + build + tests)
python run_replication_tests.py all

# Test MQTT connectivity only
python run_replication_tests.py connectivity

# Simple replication test
python run_replication_tests.py simple

# Run full test suite
python run_replication_tests.py full

# Install dependencies
python run_replication_tests.py install-deps

# Build server
python run_replication_tests.py build
```

### Run tests directly with pytest

```bash
# Test kết nối MQTT broker
pytest -v -k test_mqtt_broker_connectivity

# Test replication đơn giản
pytest -v test_replication_simple.py

# Chạy toàn bộ test suite replication
pytest -v test_replication.py

# Chạy test cụ thể
pytest -v -k test_set_operation_replication
```

## Cấu trúc test cases

### test_replication_simple.py

Test đơn giản để kiểm tra:
- Kết nối đến MQTT broker công cộng
- Khởi tạo 2 server với replication enabled
- Kiểm tra connectivity cơ bản

### test_replication.py

Test suite đầy đủ bao gồm:

#### 1. Test cơ bản
- `test_basic_replication_setup`: Khởi tạo nhiều node
- `test_set_operation_replication`: Nhân bản thao tác SET
- `test_delete_operation_replication`: Nhân bản thao tác DELETE

#### 2. Test các thao tác numeric/string
- `test_numeric_operations_replication`: INCR/DECR
- `test_string_operations_replication`: APPEND/PREPEND

#### 3. Test concurrent và edge cases
- `test_concurrent_operations_replication`: Thao tác đồng thời
- `test_replication_with_node_restart`: Khởi động lại node
- `test_replication_loop_prevention`: Chống loop vô hạn
- `test_malformed_mqtt_message_handling`: Xử lý message lỗi

## Cấu hình MQTT

Tests sử dụng MQTT broker công cộng:
- **Broker**: test.mosquitto.org
- **Port**: 1883
- **Topic pattern**: `test_merkle_kv_{random_id}/events/#`

Mỗi test sử dụng topic prefix khác nhau để tránh xung đột.

## Kiến trúc test

### ReplicationTestSetup
Helper class quản lý nhiều MerkleKV instances:
- Tạo config file với replication enabled
- Khởi động/dừng các server instances
- Cleanup tự động

### MQTTTestClient
Client để monitor MQTT messages:
- Subscribe đến replication topics
- Decode messages (JSON hoặc CBOR)
- Tracking messages cho verification

## Troubleshooting

### 1. Lỗi kết nối MQTT
```
Failed to connect to MQTT broker
```
**Giải pháp**: Kiểm tra kết nối internet và firewall. Broker test.mosquitto.org đôi khi có thể bị quá tải.

### 2. Server không khởi động
```
Server failed to start within timeout
```
**Giải pháp**: 
- Kiểm tra port có bị conflict không
- Xem log server output để debug
- Tăng timeout trong config

### 3. Replication không hoạt động
```
Expected replicated_value, got (nil)
```
**Giải pháp**:
- Kiểm tra cấu hình replication trong server
- Verify MQTT topic names
- Tăng thời gian chờ replication

### 4. Tests chạy chậm
**Nguyên nhân**: MQTT network latency, server startup time
**Giải pháp**: 
- Chạy tests tuần tự thay vì parallel
- Sử dụng local MQTT broker cho testing nhanh hơn

## Tùy chỉnh tests

### Sử dụng MQTT broker khác

Chỉnh sửa trong test files:
```python
mqtt_config = {
    "mqtt_broker": "your-broker.com",
    "mqtt_port": 1883,
    # ...
}
```

### Thêm test cases mới

1. Tạo function test với prefix `test_`
2. Sử dụng `@pytest.mark.asyncio` cho async tests
3. Sử dụng `replication_setup` fixture để quản lý servers
4. Follow pattern: setup → action → verify → cleanup

### Debug tests

Chạy với verbose output:
```bash
pytest -v -s test_replication.py
```

Enable Rust logging:
```bash
RUST_LOG=debug pytest -v test_replication.py
```

## Tích hợp CI/CD

Để tích hợp vào CI/CD pipeline:

```yaml
# Example GitHub Actions
- name: Run replication tests
  run: |
    cd tests/integration
    python run_replication_tests.py all
```

**Lưu ý**: Test sử dụng external MQTT broker nên có thể bị fail do network issues. Cân nhắc setup MQTT broker trong CI environment.
