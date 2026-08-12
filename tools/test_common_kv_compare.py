import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common_kv_compare import compare_manifest


def process_manifest(*, different: bool = False) -> dict:
    cases = []
    for engine in ("fjall", "seerdb"):
        for case in ("old-state", "complete-new-state"):
            recovery = case
            if different and engine == "seerdb" and case == "complete-new-state":
                recovery = "old-state"
            cases.append(
                {
                    "engine": engine,
                    "case": case,
                    "requested_prefix": 64 if case == "old-state" else 80,
                    "child_exit_code": 137,
                    "execution_outcome": "terminated-by-sigkill",
                    "recovery_outcome": recovery,
                    "reopen_outcome": "stable-two-reopen",
                    "resource_outcome": "not-collected",
                    "accepted": True,
                    "reopen_passes": 2,
                }
            )
    return {
        "format": "seerdb-common-kv-process-crash-manifest-v1",
        "status": "accepted",
        "accepted": True,
        "durability": "durable",
        "workload": "batch-put",
        "keys": 256,
        "operations": 128,
        "batch_size": 16,
        "value_bytes": 64,
        "seed": 7,
        "trace_digest_fnv1a64": "0123456789abcdef",
        "cases": cases,
    }


def syscall_manifest() -> dict:
    cases = []
    for engine in ("fjall", "seerdb"):
        for mode in ("before", "after"):
            for syscall in ("fsync", "rename"):
                cases.append(
                    {
                        "engine": engine,
                        "mode": mode,
                        "syscall": syscall,
                        "failed_call": 1 if engine == "fjall" else 4,
                        "observed_calls": 4 if engine == "fjall" else 7,
                        "child_exit_status": 1,
                        "accepted_prefix": 48,
                        "execution_outcome": "refused",
                        "recovery_outcome": "complete-prefix",
                        "reopen_outcome": "stable-two-reopen",
                        "resource_outcome": "not-collected",
                        "accepted": True,
                        "reopen_passes": 2,
                    }
                )
    return {
        "format": "seerdb-common-kv-syscall-fault-manifest-v1",
        "status": "accepted",
        "accepted": True,
        "durability": "durable",
        "workload": "batch-put seeded mutation",
        "keys": 256,
        "base_operations": 64,
        "operations": 64,
        "batch_size": 16,
        "value_bytes": 64,
        "seed": 7,
        "trace_digest_fnv1a64": "0123456789abcdef",
        "cases": cases,
    }


class CommonKvCompareTests(unittest.TestCase):
    def test_process_outcomes_are_equivalent_without_matching_signal_details(self):
        report = compare_manifest(process_manifest(), ["fjall", "seerdb"])
        self.assertEqual(report["status"], "accepted")
        self.assertTrue(report["accepted"])
        self.assertFalse(report["resource_qualified"])

    def test_resource_reporting_is_separate_from_logical_equivalence(self):
        manifest = process_manifest()
        manifest["cases"][0]["resource_outcome"] = "collected"
        report = compare_manifest(manifest, ["fjall", "seerdb"])
        self.assertEqual(report["status"], "accepted")
        self.assertTrue(report["accepted"])
        self.assertFalse(report["resource_qualified"])

    def test_process_logical_difference_is_not_accepted(self):
        report = compare_manifest(process_manifest(different=True), ["fjall", "seerdb"])
        self.assertEqual(report["status"], "different-outcomes")
        self.assertFalse(report["accepted"])
        self.assertTrue(report["comparisons"][0]["differences"])

    def test_syscall_failed_call_indexes_are_not_logical_differences(self):
        report = compare_manifest(syscall_manifest(), ["fjall", "seerdb"])
        self.assertEqual(report["status"], "accepted")
        self.assertTrue(report["accepted"])

    def test_missing_required_engine_is_incomplete(self):
        report = compare_manifest(process_manifest(), ["fjall", "rocksdb", "seerdb"])
        self.assertEqual(report["status"], "incomplete")
        self.assertFalse(report["accepted"])
        self.assertEqual(report["missing_engines"], ["rocksdb"])

    def test_unsupported_source_never_passes(self):
        manifest = process_manifest()
        manifest.update(
            {
                "status": "unsupported",
                "accepted": False,
                "reason": "Linux-only gate was not run",
                "cases": [],
            }
        )
        report = compare_manifest(manifest, ["fjall", "seerdb"])
        self.assertEqual(report["status"], "unsupported")
        self.assertFalse(report["accepted"])


if __name__ == "__main__":
    unittest.main()
