#!/usr/bin/env python3
"""
MerkleKV Integration Test Runner

This script provides a comprehensive test runner for the MerkleKV integration tests
with different modes for development, CI/CD, and performance testing.

Usage:
    python run_tests.py [options]

Options:
    --mode MODE           Test mode: basic, concurrency, benchmark, all, ci
    --host HOST           Server host (default: 127.0.0.1)
    --port PORT           Server port (default: 7379)
    --workers N           Number of parallel workers (default: auto)
    --verbose             Verbose output
    --report              Generate detailed report
    --benchmark-only      Run only benchmark tests
    --help                Show this help message
"""

import argparse
import os
import sys
import subprocess
import time
from pathlib import Path
from typing import List, Dict, Any

import pytest
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()

class TestRunner:
    """Main test runner for MerkleKV integration tests."""
    
    def __init__(self, args):
        self.args = args
        self.test_dir = Path(__file__).parent
        self.results = {}
        
    def run_tests(self) -> bool:
        """Run the integration tests based on the selected mode."""
        console.print(Panel.fit("🚀 MerkleKV Integration Test Suite", style="bold blue"))
        
        # Determine test mode and arguments
        pytest_args = self._build_pytest_args()
        
        # Run the tests
        start_time = time.time()
        success = self._execute_pytest(pytest_args)
        end_time = time.time()
        
        # Generate report if requested
        if self.args.report:
            self._generate_report(start_time, end_time)
        
        return success
    
    def _build_pytest_args(self) -> List[str]:
        """Build pytest command line arguments."""
        args = [
            str(self.test_dir),
            "-v",
            "--tb=short",
        ]
        
        # Add mode-specific arguments
        if self.args.mode == "basic":
            args.extend([
                "-k", "TestBasicOperations or TestDataPersistence or TestScanOverTcp",
                "-m", "not benchmark and not slow"
            ])
        elif self.args.mode == "concurrency":
            args.extend([
                "-k", "TestConcurrency",
                "-m", "not benchmark"
            ])
        elif self.args.mode == "benchmark":
            args.extend([
                "-k", "TestBenchmarks or TestPerformanceBenchmarks",
                "-m", "benchmark"
            ])
        elif self.args.mode == "error":
            args.extend([
                "-k", "TestErrorHandling or TestRecoveryScenarios",
                "-m", "not benchmark"
            ])
        elif self.args.mode == "ci":
            args.extend([
                "-m", "not benchmark and not slow",
                "--junitxml=test-results.xml",
                "--cov=src",
                "--cov-report=xml",
                "--cov-report=html"
            ])
        elif self.args.mode == "all":
            # Run all tests except benchmarks
            args.extend([
                "-m", "not benchmark"
            ])
        
        # Add benchmark-only mode
        if self.args.benchmark_only:
            args = [
                str(self.test_dir),
                "-v",
                "--tb=short",
                "-k", "TestBenchmarks or TestPerformanceBenchmarks",
                "-m", "benchmark"
            ]
        
        # Add parallel execution
        if self.args.workers:
            args.extend(["-n", str(self.args.workers)])
        
        # Add verbose output
        if self.args.verbose:
            args.append("-s")
        
        return args
    
    def _execute_pytest(self, pytest_args: List[str]) -> bool:
        """Execute pytest with the given arguments."""
        console.print(f"\n[green]Running tests with args: {' '.join(pytest_args)}[/green]")
        
        # Set environment variables
        env = os.environ.copy()
        env["RUST_LOG"] = "info"
        env["PYTHONPATH"] = str(self.test_dir)
        
        # Change to test directory for running pytest
        original_cwd = os.getcwd()
        os.chdir(self.test_dir)
        
        # Run pytest
        try:
            result = subprocess.run(
                [sys.executable, "-m", "pytest"] + pytest_args,
                env=env,
                capture_output=not self.args.verbose,
                text=True
            )
            
            if result.returncode == 0:
                console.print("[green]✅ All tests passed![/green]")
                return True
            else:
                console.print(f"[red]❌ Tests failed with exit code {result.returncode}[/red]")
                if not self.args.verbose and result.stdout:
                    console.print(result.stdout)
                if result.stderr:
                    console.print(f"[red]Errors:[/red]\n{result.stderr}")
                return False
                
        except Exception as e:
            console.print(f"[red]❌ Failed to run tests: {e}[/red]")
            return False
        finally:
            # Restore original working directory
            os.chdir(original_cwd)
    
    def _generate_report(self, start_time: float, end_time: float):
        """Generate a detailed test report."""
        duration = end_time - start_time
        
        # Create report table
        table = Table(title="MerkleKV Integration Test Report")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="magenta")
        
        table.add_row("Test Mode", self.args.mode)
        table.add_row("Duration", f"{duration:.2f}s")
        table.add_row("Python Version", sys.version.split()[0])
        table.add_row("Test Directory", str(self.test_dir))
        
        console.print(table)
        
        # Save report to file
        report_file = self.test_dir / "test_report.txt"
        with open(report_file, "w") as f:
            f.write(f"MerkleKV Integration Test Report\n")
            f.write(f"================================\n")
            f.write(f"Test Mode: {self.args.mode}\n")
            f.write(f"Duration: {duration:.2f}s\n")
            f.write(f"Python Version: {sys.version}\n")
            f.write(f"Test Directory: {self.test_dir}\n")
        
        console.print(f"[green]Report saved to: {report_file}[/green]")

def main():
    """Main entry point for the test runner."""
    parser = argparse.ArgumentParser(
        description="MerkleKV Integration Test Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_tests.py --mode basic                    # Run basic tests only
  python run_tests.py --mode concurrency             # Run concurrency tests
  python run_tests.py --mode benchmark --verbose     # Run benchmarks with verbose output
  python run_tests.py --mode ci                      # Run CI/CD tests
  python run_tests.py --mode all --workers 4         # Run all tests with 4 workers
  python run_tests.py --benchmark-only               # Run only benchmark tests
        """
    )
    
    parser.add_argument(
        "--mode",
        choices=["basic", "concurrency", "benchmark", "error", "ci", "all"],
        default="basic",
        help="Test mode to run (default: basic)"
    )
    
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Server host (default: 127.0.0.1)"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=7379,
        help="Server port (default: 7379)"
    )
    
    parser.add_argument(
        "--workers",
        type=int,
        help="Number of parallel workers (default: auto)"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose output"
    )
    
    parser.add_argument(
        "--report",
        action="store_true",
        help="Generate detailed report"
    )
    
    parser.add_argument(
        "--benchmark-only",
        action="store_true",
        help="Run only benchmark tests"
    )
    
    args = parser.parse_args()
    
    # Set environment variables for test configuration
    os.environ["MERKLEKV_TEST_HOST"] = args.host
    os.environ["MERKLEKV_TEST_PORT"] = str(args.port)
    
    # Run tests
    runner = TestRunner(args)
    success = runner.run_tests()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 