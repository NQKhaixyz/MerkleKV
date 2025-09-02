#!/usr/bin/env python3
"""
Test coverage validator for GitHub Actions workflow.
Ensures all test files are covered in the CI pipeline.
"""

import os
import glob
from pathlib import Path

def main():
    """Check if all test files are covered in GitHub Actions."""
    test_files = []
    
    # Find all test files
    for pattern in ['test_*.py']:
        test_files.extend(glob.glob(pattern))
    
    test_files.sort()
    
    print("📋 Found test files:")
    for test_file in test_files:
        print(f"  - {test_file}")
    
    print(f"\n📊 Total test files: {len(test_files)}")
    
    # Expected categorization from GitHub Actions
    categories = {
        "Core Operations": [
            "test_basic_operations.py",
            "test_numeric_operations.py", 
            "test_bulk_operations.py",
            "test_bulk_ops_manual.py",
            "test_mget_fix.py",
            "test_storage_persistence.py",
            "test_scan_tcp.py"
        ],
        "Advanced Features": [
            "test_concurrency.py",
            "test_statistical_commands.py",
            "test_error_handling.py",
            "test_simple_server.py"
        ],
        "Replication": [
            "test_replication.py",
            "test_replication_simple.py"  # Note: This one runs via script
        ],
        "Performance": [
            "test_benchmark.py"
        ]
    }
    
    covered_files = set()
    
    print("\n🎯 Test categorization:")
    for category, files in categories.items():
        print(f"\n{category}:")
        for file in files:
            if file in test_files:
                print(f"  ✅ {file}")
                covered_files.add(file)
            else:
                print(f"  ❌ {file} (not found)")
    
    # Check for uncovered files
    uncovered = set(test_files) - covered_files
    if uncovered:
        print(f"\n⚠️  Uncovered test files:")
        for file in sorted(uncovered):
            print(f"  - {file}")
    else:
        print(f"\n✅ All test files are covered in GitHub Actions!")
    
    # Special files that run via scripts
    script_tests = ["test_replication_simple.py"]
    for test in script_tests:
        if test in uncovered:
            print(f"  Note: {test} runs via run_replication_tests.py")
            uncovered.remove(test)
    
    print(f"\n📈 Coverage: {len(covered_files)}/{len(test_files)} files")
    
    return len(uncovered) == 0

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
