#!/usr/bin/env python3
"""
Script to fix the adversarial test files with proper harness initialization
and port assignments.
"""

import re
from pathlib import Path

def fix_test_file(filepath: Path):
    """Fix a single test file"""
    print(f"Fixing {filepath.name}...")
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Fix harness initialization pattern
    content = re.sub(
        r'import time; timestamp = int\(time\.time\(\)\); harness = ReplicationTestHarness\(f"test_clock_\{timestamp\}"\)',
        'import time\n        timestamp = int(time.time())\n        harness = ReplicationTestHarness(f"test_{filepath.stem}_{timestamp}")',
        content
    )
    
    # Add missing port numbers to create_node calls
    port_counter = 7420  # Start from high port to avoid conflicts
    
    def replace_create_node(match):
        nonlocal port_counter
        node_name = match.group(1)
        result = f'await harness.create_node("{node_name}", {port_counter})'
        port_counter += 1
        return result
    
    content = re.sub(
        r'await harness\.create_node\("([^"]+)"\)',
        replace_create_node,
        content
    )
    
    with open(filepath, 'w') as f:
        f.write(content)
    
    print(f"  Fixed {filepath.name}")

def main():
    test_dir = Path(".")
    
    # Fix all replication test files
    test_files = [
        "test_replication_clock_skew.py",
        "test_replication_broker_outage.py", 
        "test_replication_malformed_payloads.py"
    ]
    
    for filename in test_files:
        filepath = test_dir / filename
        if filepath.exists():
            fix_test_file(filepath)
        else:
            print(f"Warning: {filename} not found")
    
    print("All files fixed!")

if __name__ == "__main__":
    main()
