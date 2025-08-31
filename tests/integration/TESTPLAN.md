# Adversarial Anti-Entropy Test Plan

## Overview

This test plan documents the adversarial integration tests designed to validate MerkleKV's anti-entropy synchronization under stringent conditions. The tests target subtle edge cases where replication safety claims could be violated.

## Test Invariants and Adversarial Conditions

### 1. `test_divergence_then_repair`
- **Invariant**: After anti-entropy sync, all replicas converge to identical state
- **Adversary**: Nodes start with intentionally divergent datasets (disjoint keysets)
- **Oracle**: Final data hash equality across all nodes proves convergence without false positives
- **Rationale**: Validates core anti-entropy mechanism under maximum initial divergence

### 2. `test_concurrent_writers_with_conflict_resolution`  
- **Invariant**: Concurrent writes resolve deterministically per conflict resolution policy
- **Adversary**: Multiple nodes write to same keys simultaneously before replication
- **Oracle**: Final state reflects consistent last-writer-wins semantics across replicas
- **Rationale**: Exposes race conditions in MQTT replication vs local writes

### 3. `test_partition_heal_scenario`
- **Invariant**: After partition heal, all nodes converge with correct conflict outcomes
- **Adversary**: Network partition simulated by node restart, writes on both sides
- **Oracle**: Post-heal convergence with deterministic conflict resolution
- **Rationale**: Tests split-brain resilience and partition tolerance

### 4. `test_restart_during_anti_entropy`
- **Invariant**: Anti-entropy completes correctly despite node restarts mid-sync
- **Adversary**: Node restart occurs during active synchronization process  
- **Oracle**: Post-restart convergence validates resumption without corruption
- **Rationale**: Ensures robustness against unexpected failures during repair

### 5. `test_idempotent_multi_round_repair`
- **Invariant**: Repeated anti-entropy rounds preserve convergence without oscillation
- **Adversary**: Multiple sync cycles on already-converged cluster
- **Oracle**: State hash stability across sync rounds proves idempotency
- **Rationale**: Critical for production stability - prevents repair loops

### 6. `test_minimal_difference_deep_subtree`
- **Invariant**: Anti-entropy efficiently detects single-key differences in large datasets
- **Adversary**: Large datasets differing by exactly one key-value pair
- **Oracle**: Convergence with minimal transfer proves efficient Merkle descent
- **Rationale**: Validates efficiency claims of Merkle tree synchronization

### 7. `test_cold_start_joiner`
- **Invariant**: New blank replica achieves exact parity with existing populated cluster
- **Adversary**: Blank node joins cluster with significant existing dataset
- **Oracle**: Complete data equality validates bootstrap replication correctness
- **Rationale**: Tests bootstrap scenario for cluster expansion

## Running the Tests

### Prerequisites
```bash
cd /workspaces/MerkleKV
cargo build
cd tests/integration  
pip install -r requirements.txt
```

### Execute Adversarial Tests
```bash
# Run all adversarial replication tests
python -m pytest -v test_adversarial_replication.py

# Run specific test
python -m pytest -v test_adversarial_replication.py::test_divergence_then_repair

# Run with full output
python -m pytest -v -s test_adversarial_replication.py
```

### Integration with Existing Test Suite
```bash
# Run all replication tests (existing + adversarial)
python -m pytest -v -k "replication"

# Run via the project's test runner  
python run_tests.py --mode all
```

## Oracle Soundness

Each test employs strong consistency oracles:

1. **Data Hash Convergence**: All nodes must have identical SHA256 hashes of their sorted key-value mappings. This eliminates false positives from ordering differences.

2. **Explicit Key Verification**: Critical keys are individually verified for expected values, catching corruption that might produce identical but wrong hashes.

3. **No Data Loss**: All keys written during tests must be present in final state, ensuring anti-entropy doesn't "repair" by deletion.

4. **Deterministic Conflict Resolution**: Conflicting writes must resolve consistently across all replicas according to the documented policy.

## Limitations and Constraints

1. **Key Enumeration**: The harness works around the lack of a bulk export command by tracking specific test keys. This is acceptable for validation but limits comprehensive state inspection.

2. **Network Simulation**: True network partitions are simulated via process restart rather than network-level isolation, which is sufficient for the available infrastructure.

3. **Timing Dependencies**: Tests use conservative timeouts to account for MQTT propagation delays and anti-entropy intervals, ensuring stability across different CI environments.

4. **MQTT Broker Dependency**: Tests rely on test.mosquitto.org availability, which is acceptable given existing project patterns.

## Expected Outcomes

All tests should pass consistently, demonstrating:
- Robust convergence under adversarial conditions
- Correct conflict resolution semantics  
- Efficient synchronization mechanisms
- Production-ready stability guarantees

Any test failure indicates a genuine safety violation requiring investigation.
