#!/usr/bin/env python3
from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / "scripts" / "tests"
GATE_PATTERN = "test_postgres_backup_gates.py"


class TrackingResult(unittest.TextTestResult):
    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.started_test_ids: set[str] = set()

    def startTest(self, test: unittest.TestCase) -> None:
        self.started_test_ids.add(test.id())
        super().startTest(test)


def test_ids(suite: unittest.TestSuite) -> set[str]:
    ids: set[str] = set()
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            ids.update(test_ids(test))
        else:
            ids.add(test.id())
    return ids


def main() -> int:
    loader = unittest.TestLoader()
    gate_suite = loader.discover(str(TESTS), pattern=GATE_PATTERN)
    gate_count = gate_suite.countTestCases()
    if gate_count < 1:
        print(f"required PostgreSQL backup gate tests are missing: {GATE_PATTERN}", file=sys.stderr)
        return 1
    gate_ids = test_ids(gate_suite)

    suite = loader.discover(str(TESTS), pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=1, resultclass=TrackingResult).run(suite)
    if not gate_ids.issubset(result.started_test_ids):
        print("Python test discovery did not execute the required gate suite", file=sys.stderr)
        return 1
    skipped_gate_ids = {test.id() for test, _ in result.skipped if test.id() in gate_ids}
    if skipped_gate_ids:
        print("required PostgreSQL backup gate tests must not be skipped", file=sys.stderr)
        return 1
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
