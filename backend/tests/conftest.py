"""
Pytest configuration for Smart Trader test suite.

Excludes standalone integration scripts from pytest collection — those files
use a non-pytest `client: APIClient` pattern and must be run directly.
"""
import sys
import os

# Ensure backend root is on the import path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Tell pytest not to collect standalone integration scripts
collect_ignore = [
    "test_broker_apis.py",
    "test_integration_e2e.py",
]
