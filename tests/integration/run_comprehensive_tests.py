#!/usr/bin/env python3
"""
Comprehensive test runner for MerkleKV integration test suite.

Academic Purpose:
This test runner provides organized execution of the complete MerkleKV
integration test suite, including the newly implemented Merkle Tree Sync
and Recovery test modules. The runner supports selective test execution
and provides detailed reporting on distributed system validation.

Usage Examples:
    python run_comprehensive_tests.py --all                    # Run all tests
    python run_comprehensive_tests.py --merkle-sync           # Run only Merkle sync tests  
    python run_comprehensive_tests.py --recovery              # Run only recovery tests
    python run_comprehensive_tests.py --quick                 # Skip slow tests
    python run_comprehensive_tests.py --report                # Generate detailed report
"""

import asyncio
import argparse
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Dict, Optional, Tuple


class ComprehensiveTestRunner:
    """
    Orchestrates execution of the complete MerkleKV integration test suite.
    
    Academic Purpose: Provides systematic validation of distributed system
    properties including data persistence, replication consistency, error
    handling, anti-entropy synchronization, and fault tolerance recovery.
    """
    
    def __init__(self):
        self.test_categories = {
            'persistence': {
                'module': 'test_storage_persistence.py',
                'description': 'Data persistence and storage engine validation',
                'markers': ['integration'],
                'estimated_time': '2-3 minutes'
            },
            'replication': {
                'module': 'test_replication.py', 
                'description': 'MQTT-based cluster replication testing',
                'markers': ['replication', 'integration'],
                'estimated_time': '5-7 minutes'
            },
            'error_handling': {
                'module': 'test_error_handling.py',
                'description': 'Error conditions and edge case validation',
                'markers': ['integration'],
                'estimated_time': '3-4 minutes'
            },
            'merkle_sync': {
                'module': 'test_merkle_sync.py',
                'description': 'Merkle tree anti-entropy synchronization',
                'markers': ['merkle_sync', 'integration'],
                'estimated_time': '8-12 minutes'
            },
            'recovery': {
                'module': 'test_recovery.py',
                'description': 'System recovery and fault tolerance',
                'markers': ['recovery', 'integration'],
                'estimated_time': '10-15 minutes'
            }
        }
        
        self.results = {}

    def run_test_category(self, category: str, extra_args: List[str] = None) -> Tuple[bool, Dict]:
        """
        Execute a specific test category and return results.
        
        Args:
            category: Test category name
            extra_args: Additional pytest arguments
            
        Returns:
            Tuple of (success: bool, result_info: dict)
        """
        if category not in self.test_categories:
            raise ValueError(f"Unknown test category: {category}")
        
        test_info = self.test_categories[category]
        module = test_info['module']
        
        print(f"\n{'='*80}")
        print(f"Running {category.upper()} TESTS")
        print(f"Module: {module}")
        print(f"Description: {test_info['description']}")
        print(f"Estimated time: {test_info['estimated_time']}")
        print(f"{'='*80}")
        
        # Build pytest command
        cmd = ['python', '-m', 'pytest', module, '-v']
        
        # Add marker filtering if specified
        if extra_args:
            cmd.extend(extra_args)
        
        # Execute test
        start_time = time.time()
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=1800  # 30 minute timeout per category
            )
            
            execution_time = time.time() - start_time
            success = result.returncode == 0
            
            result_info = {
                'success': success,
                'execution_time': execution_time,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'return_code': result.returncode
            }
            
            # Print immediate feedback
            status = "✅ PASSED" if success else "❌ FAILED"
            print(f"\n{status} - {category} tests completed in {execution_time:.1f}s")
            
            if not success:
                print(f"Exit code: {result.returncode}")
                if result.stderr:
                    print(f"Errors: {result.stderr}")
            
            return success, result_info
            
        except subprocess.TimeoutExpired:
            execution_time = time.time() - start_time
            print(f"❌ TIMEOUT - {category} tests exceeded 30 minute limit")
            
            result_info = {
                'success': False,
                'execution_time': execution_time,
                'stdout': '',
                'stderr': 'Test execution timeout',
                'return_code': -1
            }
            
            return False, result_info
        
        except Exception as e:
            execution_time = time.time() - start_time
            print(f"❌ ERROR - {category} tests failed with exception: {e}")
            
            result_info = {
                'success': False,
                'execution_time': execution_time,
                'stdout': '',
                'stderr': str(e),
                'return_code': -2
            }
            
            return False, result_info

    def run_comprehensive_suite(self, categories: List[str] = None, 
                               skip_slow: bool = False,
                               generate_report: bool = False) -> bool:
        """
        Run the comprehensive test suite.
        
        Args:
            categories: Specific categories to run (None for all)
            skip_slow: Skip tests marked as slow
            generate_report: Generate detailed test report
            
        Returns:
            True if all tests passed, False otherwise
        """
        if categories is None:
            categories = list(self.test_categories.keys())
        
        print(f"\n🚀 Starting MerkleKV Comprehensive Integration Test Suite")
        print(f"Categories: {', '.join(categories)}")
        if skip_slow:
            print("Mode: Quick (skipping slow tests)")
        print(f"{'='*80}")
        
        # Build extra args
        extra_args = []
        if skip_slow:
            extra_args.extend(['-m', 'not slow'])
        
        # Run each category
        overall_success = True
        total_start_time = time.time()
        
        for category in categories:
            success, result_info = self.run_test_category(category, extra_args)
            self.results[category] = result_info
            
            if not success:
                overall_success = False
        
        total_execution_time = time.time() - total_start_time
        
        # Print final summary
        self.print_summary(total_execution_time)
        
        if generate_report:
            self.generate_detailed_report()
        
        return overall_success

    def print_summary(self, total_time: float):
        """Print test execution summary."""
        print(f"\n{'='*80}")
        print("TEST EXECUTION SUMMARY")
        print(f"{'='*80}")
        
        passed_categories = []
        failed_categories = []
        
        for category, result in self.results.items():
            status = "PASSED" if result['success'] else "FAILED"
            time_str = f"{result['execution_time']:.1f}s"
            
            print(f"{category:20} | {status:6} | {time_str:>8}")
            
            if result['success']:
                passed_categories.append(category)
            else:
                failed_categories.append(category)
        
        print(f"{'='*80}")
        print(f"Total execution time: {total_time:.1f}s")
        print(f"Categories passed: {len(passed_categories)}/{len(self.results)}")
        
        if failed_categories:
            print(f"Failed categories: {', '.join(failed_categories)}")
            print("\n❌ COMPREHENSIVE TEST SUITE FAILED")
        else:
            print("\n✅ COMPREHENSIVE TEST SUITE PASSED")
    
    def generate_detailed_report(self):
        """Generate detailed test execution report."""
        report_path = Path("comprehensive_test_report.txt")
        
        with open(report_path, 'w') as f:
            f.write("MerkleKV Comprehensive Integration Test Report\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            for category, result in self.results.items():
                test_info = self.test_categories[category]
                
                f.write(f"\n{category.upper()} TESTS\n")
                f.write("-" * 30 + "\n")
                f.write(f"Module: {test_info['module']}\n")
                f.write(f"Description: {test_info['description']}\n")
                f.write(f"Status: {'PASSED' if result['success'] else 'FAILED'}\n")
                f.write(f"Execution time: {result['execution_time']:.1f}s\n")
                f.write(f"Return code: {result['return_code']}\n")
                
                if result['stdout']:
                    f.write(f"\nStandard Output:\n{result['stdout']}\n")
                
                if result['stderr']:
                    f.write(f"\nStandard Error:\n{result['stderr']}\n")
                
                f.write("\n" + "=" * 50 + "\n")
        
        print(f"\n📄 Detailed report saved to: {report_path}")


def main():
    """Main entry point for comprehensive test runner."""
    parser = argparse.ArgumentParser(
        description="Run MerkleKV comprehensive integration test suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --all                    Run all test categories
  %(prog)s --merkle-sync           Run only Merkle sync tests
  %(prog)s --recovery              Run only recovery tests
  %(prog)s --persistence --replication  Run multiple specific categories
  %(prog)s --quick                 Skip slow tests for faster execution
  %(prog)s --all --report          Run all tests and generate detailed report
        """
    )
    
    # Test category selection
    parser.add_argument('--all', action='store_true',
                       help='Run all test categories')
    parser.add_argument('--persistence', action='store_true',
                       help='Run data persistence tests')
    parser.add_argument('--replication', action='store_true',
                       help='Run MQTT replication tests')
    parser.add_argument('--error-handling', action='store_true',
                       help='Run error handling tests')
    parser.add_argument('--merkle-sync', action='store_true',
                       help='Run Merkle tree synchronization tests')
    parser.add_argument('--recovery', action='store_true',
                       help='Run system recovery tests')
    
    # Execution options
    parser.add_argument('--quick', action='store_true',
                       help='Skip slow tests for faster execution')
    parser.add_argument('--report', action='store_true',
                       help='Generate detailed test report')
    
    args = parser.parse_args()
    
    # Determine which categories to run
    categories = []
    if args.all:
        categories = None  # Run all categories
    else:
        if args.persistence:
            categories.append('persistence')
        if args.replication:
            categories.append('replication')
        if args.error_handling:
            categories.append('error_handling')
        if args.merkle_sync:
            categories.append('merkle_sync')
        if args.recovery:
            categories.append('recovery')
    
    # Default to all if no specific categories selected
    if not categories and not args.all:
        print("No test categories specified. Use --all or select specific categories.")
        print("Run with --help for usage information.")
        return 1
    
    # Initialize and run test runner
    runner = ComprehensiveTestRunner()
    
    try:
        success = runner.run_comprehensive_suite(
            categories=categories,
            skip_slow=args.quick,
            generate_report=args.report
        )
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Test execution interrupted by user")
        return 130
    except Exception as e:
        print(f"\n\n❌ Test runner error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
