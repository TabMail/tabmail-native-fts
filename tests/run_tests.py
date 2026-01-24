#!/usr/bin/env python3
"""
Test runner for native FTS helper tests.

Usage:
    python tests/run_tests.py           # Run all tests
    python tests/run_tests.py rust      # Run Rust helper tests
    python tests/run_tests.py update    # Run update mechanism tests
    python tests/run_tests.py -v        # Verbose output

Requires:
    - Built Rust binary: cargo build --release
    - Set TABMAIL_RUST_FTS_HELPER env var to binary path (optional, auto-detected)
"""

import argparse
import sys
import unittest
from pathlib import Path

# Ensure parent directory is in path
sys.path.insert(0, str(Path(__file__).parent.parent))


def run_tests(test_type=None, verbosity=2):
    """Run tests based on type."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    tests_dir = Path(__file__).parent

    if test_type is None or test_type == 'all':
        # Run all tests
        suite.addTests(loader.discover(str(tests_dir), pattern='test_*.py'))
    elif test_type == 'rust':
        # Run Rust process parity tests
        from tests import test_rust_process_parity
        suite.addTests(loader.loadTestsFromModule(test_rust_process_parity))
    elif test_type == 'update':
        # Run update mechanism tests
        from tests import test_rust_update_request
        suite.addTests(loader.loadTestsFromModule(test_rust_update_request))
    else:
        print(f"Unknown test type: {test_type}")
        print("Valid types: all, rust, update")
        sys.exit(1)

    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(suite)

    # Return exit code based on success
    return 0 if result.wasSuccessful() else 1


def main():
    parser = argparse.ArgumentParser(description='Run native FTS helper tests')
    parser.add_argument(
        'type',
        nargs='?',
        default='all',
        choices=['all', 'rust', 'update'],
        help='Type of tests to run (default: all)'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbose output'
    )
    parser.add_argument(
        '-q', '--quiet',
        action='store_true',
        help='Minimal output'
    )

    args = parser.parse_args()

    verbosity = 2
    if args.verbose:
        verbosity = 3
    elif args.quiet:
        verbosity = 1

    sys.exit(run_tests(args.type, verbosity))


if __name__ == '__main__':
    main()
