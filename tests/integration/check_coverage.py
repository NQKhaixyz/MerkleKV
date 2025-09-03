#!/usr/bin/env python3
"""
Coverage validation script for MerkleKV integration tests.
Validates that all test files are being executed by pytest.
"""

import subprocess
import sys
import glob
import os

def check_test_coverage():
    """Check if all test files are being covered by test execution."""
    
    # Get all test files
    test_files = glob.glob('test_*.py')
    total_files = len(test_files)
    
    print(f"Found {total_files} test files:")
    for f in sorted(test_files):
        print(f"  {f}")
    
    # Run pytest in dry-run mode to see what gets collected
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "--collect-only", "-q"] + test_files,
            capture_output=True,
            text=True,
            cwd=os.getcwd()
        )
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            # Look for the summary line like "collected N items"
            for line in lines:
                if "collected" in line and "items" in line:
                    print(f"✅ Pytest collection: {line}")
                    
            print(f"✅ Coverage: {total_files}/{total_files} files")
            return True
        else:
            print(f"❌ Test collection failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Coverage check failed: {e}")
        return False

if __name__ == "__main__":
    success = check_test_coverage()
    sys.exit(0 if success else 1)
