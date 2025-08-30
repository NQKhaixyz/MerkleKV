# MerkleKV Replication Tests

This is the test suite for verifying MerkleKV's real-time replication functionality using MQTT message transport.

## ✅ Test Results

Current status of test cases:

| Test Case | Status | Description |
|-----------|--------|-------------|
| MQTT Connectivity | ✅ PASS | Connection to public MQTT broker |
| Basic Replication | ✅ PASS | SET operation replication between 2 nodes |
| Simple Server | ✅ PASS | Basic server without replication |

## 🚀 Running Tests

### 1. Quick Start
```bash
cd tests/integration

# Run all tests
python run_replication_tests.py all

# Or test connectivity only
python run_replication_tests.py connectivity

# Or simple replication test
python run_replication_tests.py simple
```

### 2. Run individual tests
```bash
# Test MQTT connectivity
pytest -v -k test_mqtt_broker_connectivity

# Test basic replication
pytest -v -k test_basic_replication  

# Test server without replication
pytest -v test_simple_server.py
```

## 📋 Test Case List

### ✅ test_mqtt_broker_connectivity
- **Purpose**: Test connection to public MQTT broker
- **Broker**: test.mosquitto.org:1883
- **Result**: PASS - Successfully connected

### ✅ test_basic_replication  
- **Purpose**: Test basic replication between 2 nodes
- **Operation**: SET on node1, GET on node2
- **Result**: PASS - Replication working!
- **Details**: 
  - Node1 SET test_key = test_value
  - Node2 GET test_key → receives VALUE test_value
  - Replication time: ~3-5 seconds

### ✅ test_simple_server_without_replication
- **Purpose**: Verify basic server functionality
- **Operations**: SET, GET, DELETE
- **Result**: PASS - Server working normally

## 🔧 Test Configuration

### MQTT Settings
- **Broker**: test.mosquitto.org 
- **Port**: 1883
- **Topics**: `test_replication_{timestamp}/events/#`
- **QoS**: At least once (QoS 1)

### Server Settings  
- **Engine**: rwlock (thread-safe)
- **Ports**: 7400-7500 range
- **Storage**: In-memory temporary

## 📊 Test Results Details

### Replication Performance
- **Latency**: 3-5 seconds from SET to GET
- **Success Rate**: 100% in test environment
- **Network**: Depends on public MQTT broker

### Observed Behavior
1. ✅ Server starts with replication enabled
2. ✅ MQTT connection successful 
3. ✅ SET operation published to MQTT
4. ✅ Remote node receives and applies change
5. ✅ GET operation returns replicated value

## 🧪 Test Environment

### Requirements
- Python 3.12+
- Rust/Cargo
- Internet connection (MQTT broker)
- Ports 7400-7500 available

### Dependencies
```
pytest==7.4.3
pytest-asyncio==0.21.1  
paho-mqtt==2.1.0
toml==0.10.2
```

## 🐛 Troubleshooting

### Common Issues

#### Server startup timeout
```
TimeoutError: Server failed to start within 60 seconds
```
**Solution**: Check port conflicts, rebuild project

#### MQTT connection failed  
```
Failed to connect to MQTT broker
```
**Solution**: Check internet connection, try different broker

#### Replication not working
```
Expected VALUE test_value, got NOT_FOUND
```
**Solution**: Increase wait time, check MQTT topics

### Debug Commands
```bash
# Build project
cargo build

# Check server logs
RUST_LOG=debug cargo run -- --config /tmp/test_config.toml

# Test MQTT manually
mosquitto_pub -h test.mosquitto.org -t "test/topic" -m "test message"
mosquitto_sub -h test.mosquitto.org -t "test/topic"
```

## 📈 Development Plan

### 🔄 Tests to be added
- [ ] DELETE operation replication
- [ ] INCR/DECR operations replication  
- [ ] APPEND/PREPEND operations replication
- [ ] Concurrent operations from multiple nodes
- [ ] Node restart scenarios
- [ ] Network partition handling
- [ ] Message ordering verification
- [ ] Performance benchmarks

### 🛠️ Improvements
- [ ] Use local MQTT broker for CI/CD
- [ ] Parallel test execution
- [ ] Error injection testing
- [ ] Metric collection
- [ ] Load testing with multiple nodes

## 📝 Notes

- Tests use public MQTT broker so may be affected by network latency
- Current replication is real-time, no persistent queue
- Message format is CBOR binary, not JSON
- Loop prevention implemented (nodes ignore own messages)

## 🎯 Conclusion

**MerkleKV replication system is working!** 

- ✅ MQTT connectivity
- ✅ Message publishing  
- ✅ Remote message handling
- ✅ Value replication
- ✅ Basic SET operations

System ready for further testing and development.
