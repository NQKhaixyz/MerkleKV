# MerkleKV Integration Test Suite

This directory contains comprehensive integration tests for the MerkleKV distributed key-value store. The tests validate the entire system end-to-end, ensuring all components work together correctly under various conditions.

## 🚀 Quick Start

### Prerequisites

1. **Python 3.8+** installed
2. **Rust toolchain** (for building MerkleKV server)
3. **Cargo** available in PATH

### Installation

```bash
# Install Python dependencies
cd tests/integration
pip install -r requirements.txt

# Build the MerkleKV server
cd ../..  # Go back to project root
cargo build
```

### Running Tests

```bash
# Run basic tests
python run_tests.py --mode basic

# Run all tests (except benchmarks)
python run_tests.py --mode all

# Run benchmarks only
python run_tests.py --benchmark-only

# Run with verbose output
python run_tests.py --mode concurrency --verbose

# Run CI/CD tests with coverage
python run_tests.py --mode ci --report

# NEW: Replication test modes
python run_tests.py --mode quick        # Fast CI tests (< 2 minutes)
python run_tests.py --mode replication  # All replication tests (< 5 minutes)
python run_tests.py --mode chaos        # Adversarial test battery (< 15 minutes)
```

## 📋 Test Categories

### 1. Basic Operations (`test_basic_operations.py`)

Tests the core functionality of the key-value store:

- ✅ GET, SET, DELETE commands
- ✅ Error handling for invalid commands
- ✅ Data persistence across server restarts
- ✅ Special characters and Unicode support
- ✅ Large value handling
- ✅ Command case insensitivity

### 2. Concurrency (`test_concurrency.py`)

Tests thread safety and concurrent access:

- ✅ Multiple clients accessing same keys
- ✅ Concurrent reads and writes
- ✅ Connection stress testing
- ✅ Thread safety validation
- ✅ Race condition prevention

### 3. Performance Benchmarks (`test_benchmark.py`)

Measures system performance under load:

- ✅ Throughput testing (ops/sec)
- ✅ Latency measurements (P50, P95, P99)
- ✅ Memory and CPU usage monitoring
- ✅ Scalability testing with multiple clients
- ✅ Connection handling capacity

### 4. Error Handling (`test_error_handling.py`)

Tests system resilience and error recovery:

- ✅ Invalid command handling
- ✅ Malformed protocol messages
- ✅ Network partition simulation
- ✅ Server crash recovery
- ✅ Resource cleanup validation

### 5. Replication Testing (`test_replication_*.py`)

The replication testing suite validates the distributed replication functionality of MerkleKV, ensuring data consistency and system reliability across multiple nodes connected via MQTT message transport. These tests are essential for verifying the correctness of the distributed system under various real-world scenarios.

#### 5.1 Basic Replication Testing
**Files:** `test_replication.py`, `test_replication_simple.py`

**Purpose:** Ensures fundamental replication functionality operates correctly across distributed nodes using MQTT as the message transport mechanism.

**Test Scenarios:**
- ✅ **MQTT Connectivity Validation**: Verifies successful connection to MQTT brokers (both public `test.mosquitto.org` and local brokers)
- ✅ **Core Operation Replication**: Tests replication of all write operations:
  - `SET` operations: Key-value pair insertion and updates
  - `DELETE` operations: Key removal propagation
  - `INCR`/`DECR` operations: Atomic numeric increment/decrement operations
  - `APPEND`/`PREPEND` operations: String concatenation operations
- ✅ **Bi-directional Synchronization**: Validates that changes made on any node are correctly propagated to all other nodes in the cluster
- ✅ **Message Format Validation**: Ensures CBOR binary encoding is correctly used for efficient message serialization

**Educational Value:** These tests demonstrate the foundational concepts of distributed database replication, including eventual consistency and message-based synchronization patterns.

#### 5.2 Adversarial Scenarios Testing
**File:** `test_replication_ordering.py`

**Purpose:** Validates the system's robustness against edge cases and challenging scenarios that commonly occur in distributed systems.

**Test Scenarios:**
- ✅ **Out-of-Order Message Delivery**: Simulates network conditions where messages arrive in different sequences than they were sent, testing the Last-Write-Wins (LWW) conflict resolution mechanism
- ✅ **Duplicate Event Handling**: Verifies the system's idempotency by ensuring duplicate events with identical operation IDs are properly deduplicated
- ✅ **Concurrent Write Conflict Resolution**: Tests LWW resolution for concurrent writes with different timestamps and operation IDs, ensuring deterministic conflict resolution
- ✅ **Timestamp Tie-Breaking**: Validates the secondary ordering mechanism (operation ID comparison) when multiple operations have identical timestamps

**Educational Value:** These tests illustrate critical distributed systems concepts including conflict resolution strategies, idempotency, and the challenges of maintaining consistency in asynchronous, message-driven architectures.

#### 5.3 Clock Skew Testing
**File:** `test_replication_clock_skew.py`

**Purpose:** Simulates and validates system behavior under clock synchronization issues commonly found in distributed environments.

**Test Scenarios:**
- ✅ **Producer Time Drift Simulation**: Tests scenarios where different nodes have slightly different system clocks (±60 seconds drift)
- ✅ **Timestamp Boundary Case Analysis**: Examines system behavior when events have very similar or identical timestamps
- ✅ **Temporal Ordering Validation**: Ensures that despite clock drift, the logical ordering of operations is maintained through proper timestamp comparison and tie-breaking mechanisms
- ✅ **Clock Synchronization Edge Cases**: Tests extreme scenarios such as significant time differences between nodes

**Educational Value:** These tests demonstrate the complexity of time-based ordering in distributed systems and the importance of logical clocks and proper timestamp handling mechanisms.

#### 5.4 Network Resilience Testing
**File:** `test_replication_broker_outage.py` (marked as `slow` tests)

**Purpose:** Evaluates system resilience and recovery capabilities under network partition and broker failure scenarios.

**Test Scenarios:**
- ✅ **MQTT Broker Outage Simulation**: Tests system behavior when the central MQTT broker becomes unavailable
- ✅ **Network Partition Recovery**: Validates automatic reconnection and message backlog processing when network connectivity is restored
- ✅ **Graceful Degradation**: Ensures that local operations continue to function even when replication is temporarily unavailable
- ✅ **Connection Retry Logic**: Tests the automatic retry mechanisms and exponential backoff strategies for broker reconnection

**Educational Value:** These tests showcase fault tolerance principles in distributed systems, including partition tolerance (from CAP theorem), graceful degradation strategies, and recovery mechanisms.

#### 5.5 Data Corruption Handling
**File:** `test_replication_malformed_payloads.py`

**Purpose:** Verifies system robustness against data corruption and malformed message scenarios that may occur due to network issues or implementation bugs.

**Test Scenarios:**
- ✅ **CBOR Corruption Testing**: Injects corrupted CBOR binary data to test deserialization error handling
- ✅ **Invalid Payload Structure Detection**: Tests system response to structurally invalid messages that don't conform to the expected ChangeEvent format
- ✅ **Message Validation Mechanisms**: Validates that the system properly detects and rejects malformed replication messages without affecting system stability
- ✅ **Error Recovery**: Ensures that encountering corrupted messages doesn't prevent processing of subsequent valid messages

**Educational Value:** These tests emphasize the importance of robust input validation and error handling in distributed systems, particularly in the face of unreliable networks and potential security threats.

#### 5.6 Performance Testing
**File:** `test_replication_throughput_spike.py`

**Purpose:** Validates system performance characteristics under high-load conditions and measures key performance indicators for capacity planning.

**Test Scenarios:**
- ✅ **Throughput Measurement**: Measures operations per second under sustained load conditions
- ✅ **Latency Validation**: Records and analyzes replication latency from operation initiation to propagation completion
- ✅ **Load Spike Handling**: Tests system behavior during sudden increases in operation volume
- ✅ **Performance Regression Detection**: Establishes baseline performance metrics for detecting future performance degradations

**Educational Value:** These tests demonstrate performance engineering principles in distributed systems, including load testing methodologies, latency measurement techniques, and the trade-offs between consistency and performance.

## 📚 Theoretical Foundations of Replication Testing

### Distributed Systems Concepts Validated

The replication test suite validates several fundamental concepts from distributed systems theory:

#### **Eventual Consistency Model**
MerkleKV implements an eventually consistent system where all nodes will eventually converge to the same state, even in the presence of network partitions or temporary failures. The replication tests validate this property by:
- Verifying that updates propagate across all nodes
- Testing convergence after network partitions heal
- Ensuring consistency is maintained despite message reordering

#### **CAP Theorem Implications** 
The system prioritizes **Availability** and **Partition Tolerance** while providing **Eventual Consistency**:
- **Consistency**: Validated through LWW conflict resolution tests
- **Availability**: Confirmed by testing continued operation during broker outages
- **Partition Tolerance**: Demonstrated through network resilience testing

#### **Conflict Resolution Strategies**
The test suite validates the **Last-Write-Wins (LWW)** conflict resolution strategy:
- Primary ordering by timestamp (wall-clock time)
- Secondary ordering by operation ID for tie-breaking
- Deterministic conflict resolution ensuring all nodes converge to the same state

#### **Message-Driven Architecture Patterns**
Tests validate the implementation of message-driven architecture principles:
- **Asynchronous Communication**: MQTT-based non-blocking message passing
- **Event Sourcing**: All state changes are represented as discrete events
- **Idempotency**: Duplicate message handling ensures operations can be safely retried

### Performance and Scalability Considerations

#### **Latency Characteristics**
- **Public MQTT Broker**: 3-5 seconds average replication latency
- **Local MQTT Broker**: <500ms average replication latency
- **Throughput**: 100+ operations/second sustained with public broker

#### **Scalability Patterns**
The system demonstrates horizontal scalability through:
- Peer-to-peer architecture with no single point of failure
- MQTT fan-out for efficient message distribution
- Independent node operation with eventual synchronization

### Testing Methodologies

#### **Chaos Engineering Principles**
The adversarial tests implement chaos engineering concepts:
- **Fault Injection**: Introducing network partitions and broker outages
- **Stress Testing**: High-load scenarios and resource exhaustion
- **Recovery Validation**: Ensuring graceful recovery from failure states

#### **Property-Based Testing Concepts**
While not explicitly using property-based testing frameworks, the tests validate system properties:
- **Safety Properties**: No data corruption or inconsistent states
- **Liveness Properties**: Eventually consistent convergence
- **Invariant Preservation**: System constraints maintained under all conditions

## 🎯 Test Modes

### Basic Mode

```bash
python run_tests.py --mode basic
```

- Fast execution (~30 seconds)
- Core functionality validation
- Suitable for development and CI

### Concurrency Mode

```bash
python run_tests.py --mode concurrency
```

- Tests thread safety
- Multiple client scenarios
- Connection handling

### Benchmark Mode

```bash
python run_tests.py --mode benchmark
```

- Performance measurements
- Load testing
- Resource usage analysis

### Error Mode

```bash
python run_tests.py --mode error
```

- Error handling validation
- Recovery scenario testing
- Edge case coverage

### CI Mode

```bash
python run_tests.py --mode ci
```

- Full test suite (except benchmarks)
- Coverage reporting
- JUnit XML output
- HTML coverage reports

### All Mode

```bash
python run_tests.py --mode all
```

- Complete test suite
- All categories except benchmarks
- Comprehensive validation

### Replication Modes (NEW)

#### Quick Mode
```bash
python run_tests.py --mode quick
```

- Fast tests suitable for CI/CD (< 2 minutes)
- Basic operations and MQTT connectivity
- Core replication functionality validation

#### Replication Mode
```bash
python run_tests.py --mode replication
```

- All replication tests except slow ones (< 5 minutes)
- Comprehensive replication validation
- MQTT broker testing and multi-node scenarios

#### Chaos Mode
```bash
python run_tests.py --mode chaos
```

- Full adversarial test battery (< 15 minutes)
- Clock skew, broker outages, malformed payloads
- Stress testing and edge case validation
- Includes tests marked as `slow` and `benchmark`

## 🔧 Configuration

### Environment Variables

```bash
# Server configuration
export MERKLEKV_TEST_HOST="127.0.0.1"
export MERKLEKV_TEST_PORT="7379"

# Test configuration
export RUST_LOG="info"
export PYTHONPATH="tests/integration"
```

### Command Line Options

```bash
python run_tests.py [options]

Options:
  --mode MODE           Test mode: quick, basic, replication, chaos, concurrency, benchmark, error, ci, all
  --host HOST           Server host (default: 127.0.0.1)
  --port PORT           Server port (default: 7379)
  --workers N           Number of parallel workers (default: auto)
  --verbose             Verbose output
  --report              Generate detailed report
  --benchmark-only      Run only benchmark tests
  --help                Show help message
```

### Replication Environment Variables

```bash
# MQTT broker configuration for replication tests
export MQTT_BROKER_HOST="test.mosquitto.org"    # Default: test.mosquitto.org
export MQTT_BROKER_PORT="1883"                 # Default: 1883
export MQTT_TOPIC_PREFIX="test_merkle_kv"       # Default: test_merkle_kv_{timestamp}
```

## 📊 Performance Benchmarks

### Expected Performance Metrics

| Operation   | Throughput     | Latency (P95) | Memory Usage | Notes |
| ----------- | -------------- | ------------- | ------------ | ----- |
| SET         | >1,000 ops/sec | <100ms        | <100MB       | Local operations |
| GET         | >2,000 ops/sec | <50ms         | <100MB       | Local operations |
| Mixed       | >800 ops/sec   | <80ms         | <100MB       | Local operations |
| Connections | >50 conn/sec   | <1s           | <200MB       | Connection handling |
| **Replication** | **100+ ops/sec** | **3-5s public broker** | **<100MB** | **MQTT replication** |
| **Replication** | **1000+ ops/sec** | **<500ms local broker** | **<100MB** | **Local MQTT** |

### Running Benchmarks

```bash
# Quick benchmark
python run_tests.py --mode benchmark

# Detailed benchmark with verbose output
python run_tests.py --benchmark-only --verbose

# Scalability test
python run_tests.py --mode benchmark --workers 4
```

## 🏗️ Architecture

### Test Structure

```
tests/integration/
├── conftest.py              # Pytest configuration and fixtures
├── test_basic_operations.py # Basic functionality tests
├── test_concurrency.py      # Concurrency and thread safety
├── test_benchmark.py        # Performance benchmarks
├── test_error_handling.py   # Error handling and recovery
├── test_replication.py      # Full replication test suite
├── test_replication_simple.py # Basic MQTT connectivity and replication
├── test_replication_ordering.py # Out-of-order delivery and LWW resolution
├── test_replication_clock_skew.py # Clock drift simulation and boundaries
├── test_replication_broker_outage.py # Network partition and recovery (slow)
├── test_replication_malformed_payloads.py # Corrupt data resilience
├── run_tests.py            # Main test runner with replication modes
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

### Key Components

1. **MerkleKVServer**: Manages server process lifecycle
2. **MerkleKVClient**: TCP client for server communication
3. **PerformanceMonitor**: System resource monitoring
4. **BenchmarkResult**: Performance measurement data
5. **TestRunner**: Orchestrates test execution
6. **ReplicationTestHarness**: Multi-node test orchestration for replication scenarios
7. **NodeHandle**: Abstraction for individual server instances in replication tests

## 🐛 Troubleshooting

### Common Issues

1. **Server won't start**

   ```bash
   # Check if port is available
   netstat -tuln | grep 7379

   # Check Rust installation
   cargo --version
   ```

2. **Tests fail with connection errors**

   ```bash
   # Check server logs
   RUST_LOG=debug cargo run

   # Verify server is running
   telnet 127.0.0.1 7379
   ```

3. **Performance tests are slow**

   ```bash
   # Reduce test load
   python run_tests.py --mode benchmark --workers 1

   # Check system resources
   htop
   ```

4. **Replication tests fail**

   ```bash
   # Check MQTT broker connectivity
   python -c "import paho.mqtt.client as mqtt; c=mqtt.Client(); print('OK' if c.connect('test.mosquitto.org', 1883, 60) == 0 else 'FAIL')"

   # Test with local broker
   docker run -d -p 1883:1883 eclipse-mosquitto
   MQTT_BROKER_HOST=localhost python run_tests.py --mode replication

   # Check replication logs
   RUST_LOG=debug python run_tests.py --mode replication --verbose
   ```

### Debug Mode

```bash
# Run with debug logging
RUST_LOG=debug python run_tests.py --mode basic --verbose

# Run single test
python -m pytest tests/integration/test_basic_operations.py::TestBasicOperations::test_set_and_get_single_key -v -s

# Run replication tests with debug
RUST_LOG=debug python -m pytest test_replication_simple.py -v -s

# Run adversarial tests
python -m pytest test_replication_ordering.py -v
python -m pytest test_replication_clock_skew.py -v
python -m pytest -m "slow" test_replication_broker_outage.py -v
```

## 📈 CI/CD Integration

### GitHub Actions Example

```yaml
name: Integration Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: "3.9"
      - uses: actions/setup-rust@v1
        with:
          toolchain: stable

      - name: Install dependencies
        run: |
          pip install -r tests/integration/requirements.txt
          cargo build

      - name: Run integration tests
        run: |
          cd tests/integration
          python run_tests.py --mode ci --report

      # NEW: Add replication testing
      - name: Run replication tests
        run: |
          cd tests/integration
          python run_tests.py --mode replication --report

      - name: Upload coverage
        uses: codecov/codecov-action@v3
        with:
          file: tests/integration/coverage.xml
```

### Local CI Simulation

```bash
# Run CI tests locally
python run_tests.py --mode ci --report --workers 4

# Check coverage
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

## 📝 Test Development

### Adding New Tests

1. **Create test file**: `test_new_feature.py`
2. **Add test class**: Inherit from existing patterns
3. **Use fixtures**: Leverage `connected_client`, `server` fixtures
4. **Add markers**: Use `@pytest.mark.benchmark` for performance tests

### Example Test Structure

```python
import pytest
from conftest import MerkleKVClient

class TestNewFeature:
    """Test new feature functionality."""

    def test_new_feature_basic(self, connected_client: MerkleKVClient):
        """Test basic new feature functionality."""
        # Test implementation
        response = connected_client.send_command("NEW_COMMAND arg")
        assert response == "OK"

    @pytest.mark.benchmark
    def test_new_feature_performance(self, server):
        """Test new feature performance."""
        # Performance test implementation
        pass
```

### Best Practices

1. **Use descriptive test names**: Clear, specific test names
2. **Test one thing per test**: Single responsibility principle
3. **Use appropriate fixtures**: Reuse existing infrastructure
4. **Add proper assertions**: Clear failure messages
5. **Handle cleanup**: Ensure tests don't leave state

## 🤝 Contributing

### Running Tests Before Committing

```bash
# Run all tests
python run_tests.py --mode all

# Run benchmarks
python run_tests.py --benchmark-only

# Check for regressions
python run_tests.py --mode ci --report
```

### Test Guidelines

1. **Maintain test isolation**: Tests should not depend on each other
2. **Use realistic data**: Test with real-world scenarios
3. **Measure performance**: Include benchmarks for new features
4. **Document edge cases**: Test error conditions thoroughly
5. **Keep tests fast**: Optimize for quick feedback

## 🎓 Learning Guide for Students and Developers

### Understanding Distributed Systems Through Testing

This test suite serves as an excellent educational resource for understanding distributed systems concepts. Here's a recommended learning path:

#### **Beginner Level: Start with Basic Concepts**
1. **Begin with Basic Operations Tests** (`test_basic_operations.py`):
   - Understand client-server communication patterns
   - Learn about protocol design and validation
   - Observe error handling and edge case management

2. **Explore Replication Basics** (`test_replication_simple.py`):
   - Understand MQTT publish-subscribe patterns
   - Learn about eventual consistency concepts
   - Observe bi-directional data synchronization

#### **Intermediate Level: Distributed System Challenges**
3. **Study Adversarial Scenarios** (`test_replication_ordering.py`):
   - Learn about the challenges of message ordering in distributed systems
   - Understand conflict resolution strategies (LWW)
   - Observe idempotency implementation and importance

4. **Analyze Clock Synchronization Issues** (`test_replication_clock_skew.py`):
   - Understand the role of time in distributed systems
   - Learn about logical vs. physical clocks
   - Study timestamp-based ordering mechanisms

#### **Advanced Level: System Resilience and Performance**
5. **Examine Network Partition Tolerance** (`test_replication_broker_outage.py`):
   - Understand the CAP theorem in practice
   - Learn about graceful degradation strategies
   - Study automatic recovery mechanisms

6. **Performance Analysis** (`test_replication_throughput_spike.py`):
   - Learn performance testing methodologies
   - Understand scalability measurement techniques
   - Study the trade-offs between consistency and performance

### Practical Exercises for Students

#### **Exercise 1: Understanding Eventual Consistency**
Run the basic replication tests and observe:
```bash
# Terminal 1: Start observing logs
python run_tests.py --mode replication --verbose

# Terminal 2: Monitor MQTT messages (if using local broker)
mosquitto_sub -h localhost -t "test_merkle_kv/+/events"
```
**Learning Objective**: Observe how changes propagate through the system

#### **Exercise 2: Conflict Resolution Analysis**
Modify `test_replication_ordering.py` to create custom conflict scenarios:
- Add your own timestamp manipulation tests
- Observe how LWW resolution works with different scenarios
**Learning Objective**: Understand deterministic conflict resolution

#### **Exercise 3: Failure Mode Analysis**
Run network resilience tests and analyze the logs:
```bash
python -m pytest test_replication_broker_outage.py -v -s
```
**Learning Objective**: Understand how distributed systems handle failures

### Key Concepts to Focus On

#### **For Computer Science Students:**
- **Distributed Systems Theory**: CAP theorem, eventual consistency, conflict resolution
- **Network Programming**: MQTT protocol, message serialization (CBOR), publish-subscribe patterns
- **Concurrency**: Thread safety, race conditions, atomic operations
- **Testing Methodologies**: Integration testing, chaos engineering, performance testing

#### **For Software Engineers:**
- **Practical Implementation**: How theoretical concepts translate to real code
- **System Design**: Trade-offs between consistency, availability, and partition tolerance
- **Operational Concerns**: Monitoring, debugging, performance optimization
- **Quality Assurance**: Comprehensive testing strategies for distributed systems

### Research and Extension Opportunities

#### **Potential Research Projects:**
1. **Alternative Conflict Resolution Strategies**: Implement and test vector clocks or CRDTs
2. **Performance Optimization**: Investigate batching strategies or alternative message formats
3. **Security Analysis**: Add authentication and authorization to the replication protocol
4. **Consistency Models**: Implement and compare different consistency levels (strong, causal, eventual)

#### **Industry Applications:**
- **Database Systems**: Understanding how real databases like Cassandra, MongoDB handle replication
- **Microservices**: Event-driven architecture and service-to-service communication
- **Cloud Computing**: Distributed caching, content delivery networks, global data distribution

## 📚 Additional Resources

### Project Documentation
- [MerkleKV Project Documentation](../README.md)
- [Replication Testing Guide](REPLICATION_TESTING.md)
- [Replication Test Results](REPLICATION_TEST_RESULTS.md)
- [Replication Summary](REPLICATION_SUMMARY.md)

### Technical References
- [Rust Testing Guide](https://doc.rust-lang.org/book/ch11-00-testing.html)
- [Pytest Documentation](https://docs.pytest.org/)
- [Python asyncio](https://docs.python.org/3/library/asyncio.html)
- [MQTT Protocol Specification](https://mqtt.org/mqtt-specification/)

### Academic and Theoretical Resources

#### **Distributed Systems Fundamentals:**
- *Distributed Systems: Concepts and Design* by Coulouris, Dollimore, and Kindberg
- *Designing Data-Intensive Applications* by Martin Kleppmann
- [CAP Theorem Explained](https://en.wikipedia.org/wiki/CAP_theorem)
- [Eventual Consistency](https://en.wikipedia.org/wiki/Eventual_consistency)

#### **Research Papers:**
- "Time, Clocks, and the Ordering of Events in a Distributed System" by Leslie Lamport
- "Conflict-free Replicated Data Types" by Shapiro, Preguiça, Baquero, and Zawirski
- "Dynamo: Amazon's Highly Available Key-value Store" by DeCandia et al.
- "The Part-Time Parliament" by Leslie Lamport (Paxos Algorithm)

#### **Online Courses and Tutorials:**
- MIT 6.824: Distributed Systems Course Materials
- [Raft Consensus Algorithm Visualization](https://raft.github.io/)
- [Distributed Systems for Fun and Profit](http://book.mixu.net/distsys/)

#### **Testing and Quality Assurance:**
- "Chaos Engineering" by Netflix Engineering Team
- "Building Microservices" by Sam Newman
- "Site Reliability Engineering" by Google SRE Team

### Industry Examples and Case Studies
- **Apache Cassandra**: Wide-column distributed database with tunable consistency
- **Amazon DynamoDB**: Managed NoSQL database with eventual consistency
- **Apache Kafka**: Distributed streaming platform with replication
- **Consul by HashiCorp**: Distributed service discovery with consensus

## 📞 Support

For issues with the integration tests:

1. Check the troubleshooting section above
2. Review server logs with `RUST_LOG=debug`
3. Run tests with `--verbose` flag
4. Create an issue with test output and error details
