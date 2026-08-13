"""Compare the logical outcomes in a common-KV fault manifest.

The fault harnesses intentionally record one manifest containing cases for
SeerDB and its ordered-KV peers.  This tool turns that record into a
fail-closed differential result without treating different syscall counts or
failed-call indexes as logical differences.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

OUTPUT_FORMAT = "seerdb-common-kv-fault-comparison-v1"
PROCESS_FORMAT = "seerdb-common-kv-process-crash-manifest-v1"
SYSCALL_FORMAT = "seerdb-common-kv-syscall-fault-manifest-v1"
SUPPORTED_FORMATS = {PROCESS_FORMAT, SYSCALL_FORMAT}


class ManifestError(ValueError):
    """The source artifact cannot be compared under its declared contract."""


def _require(mapping: dict[str, Any], key: str, context: str) -> Any:
    if key not in mapping:
        raise ManifestError(f"{context} is missing required field {key!r}")
    return mapping[key]


def _as_bool(value: Any, context: str) -> bool:
    if not isinstance(value, bool):
        raise ManifestError(f"{context} must be a boolean")
    return value


def _as_int(value: Any, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ManifestError(f"{context} must be an integer")
    return value


def _as_string(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value:
        raise ManifestError(f"{context} must be a non-empty string")
    return value


def _source_status(manifest: dict[str, Any]) -> str:
    status = _as_string(_require(manifest, "status", "manifest"), "manifest.status")
    if status not in {"accepted", "unsupported"}:
        raise ManifestError(f"manifest.status has unsupported value {status!r}")
    return status


def _shared_identity(manifest: dict[str, Any]) -> dict[str, Any]:
    fields = [
        "durability",
        "workload",
        "keys",
        "operations",
        "batch_size",
        "value_bytes",
        "seed",
    ]
    if manifest["format"] == SYSCALL_FORMAT:
        fields.insert(2, "base_operations")
    identity: dict[str, Any] = {}
    for field in fields:
        value = _require(manifest, field, "manifest")
        if field in {
            "keys",
            "operations",
            "base_operations",
            "batch_size",
            "value_bytes",
            "seed",
        }:
            _as_int(value, f"manifest.{field}")
        else:
            _as_string(value, f"manifest.{field}")
        identity[field] = value
    trace_digest = _require(manifest, "trace_digest_fnv1a64", "manifest")
    identity["trace_digest_fnv1a64"] = _as_string(
        trace_digest, "manifest.trace_digest_fnv1a64"
    )
    return identity


def _validate_process_cases(cases: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not isinstance(cases, list):
        raise ManifestError("manifest.cases must be a list")
    expected_names = {"old-state", "complete-new-state"}
    by_engine: dict[str, set[str]] = defaultdict(set)
    normalized: list[dict[str, Any]] = []
    for index, case in enumerate(cases):
        if not isinstance(case, dict):
            raise ManifestError(f"manifest.cases[{index}] must be an object")
        engine = _as_string(
            _require(case, "engine", f"manifest.cases[{index}]"),
            f"case[{index}].engine",
        )
        name = _as_string(
            _require(case, "case", f"manifest.cases[{index}]"), f"case[{index}].case"
        )
        if name not in expected_names:
            raise ManifestError(f"case[{index}].case has unsupported value {name!r}")
        accepted = _as_bool(
            _require(case, "accepted", f"manifest.cases[{index}]"),
            f"case[{index}].accepted",
        )
        reopen_passes = _as_int(
            _require(case, "reopen_passes", f"manifest.cases[{index}]"),
            f"case[{index}].reopen_passes",
        )
        normalized.append(
            {
                "engine": engine,
                "key": name,
                "requested_prefix": _as_int(
                    _require(case, "requested_prefix", f"manifest.cases[{index}]"),
                    f"case[{index}].requested_prefix",
                ),
                "child_exit_code": _as_int(
                    _require(case, "child_exit_code", f"manifest.cases[{index}]"),
                    f"case[{index}].child_exit_code",
                ),
                "execution_outcome": _as_string(
                    case.get("execution_outcome", ""),
                    f"case[{index}].execution_outcome",
                ),
                "recovery_outcome": _as_string(
                    case.get("recovery_outcome", ""), f"case[{index}].recovery_outcome"
                ),
                "reopen_outcome": _as_string(
                    case.get("reopen_outcome", ""), f"case[{index}].reopen_outcome"
                ),
                "accepted": accepted,
                "reopen_passes": reopen_passes,
                "resource_outcome": _as_string(
                    case.get("resource_outcome", ""), f"case[{index}].resource_outcome"
                ),
            }
        )
        if name in by_engine[engine]:
            raise ManifestError(f"duplicate process case for {engine!r}: {name!r}")
        by_engine[engine].add(name)
    if not normalized:
        raise ManifestError("manifest.cases must contain at least one case")
    return normalized, {
        engine: {
            "case_count": len(names),
            "complete_case_coverage": names == expected_names,
        }
        for engine, names in by_engine.items()
    }


def _validate_syscall_cases(cases: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not isinstance(cases, list):
        raise ManifestError("manifest.cases must be a list")
    by_engine: dict[str, dict[tuple[str, str], list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    normalized: list[dict[str, Any]] = []
    for index, case in enumerate(cases):
        if not isinstance(case, dict):
            raise ManifestError(f"manifest.cases[{index}] must be an object")
        context = f"manifest.cases[{index}]"
        engine = _as_string(_require(case, "engine", context), f"case[{index}].engine")
        mode = _as_string(_require(case, "mode", context), f"case[{index}].mode")
        syscall = _as_string(
            _require(case, "syscall", context), f"case[{index}].syscall"
        )
        if mode not in {"before", "after"}:
            raise ManifestError(f"case[{index}].mode has unsupported value {mode!r}")
        accepted = _as_bool(
            _require(case, "accepted", context), f"case[{index}].accepted"
        )
        reopen_passes = _as_int(
            _require(case, "reopen_passes", context), f"case[{index}].reopen_passes"
        )
        key = (mode, syscall)
        record = {
            "engine": engine,
            "key": f"{mode}:{syscall}",
            "execution_outcome": _as_string(
                _require(case, "execution_outcome", context),
                f"case[{index}].execution_outcome",
            ),
            "recovery_outcome": _as_string(
                _require(case, "recovery_outcome", context),
                f"case[{index}].recovery_outcome",
            ),
            "reopen_outcome": _as_string(
                _require(case, "reopen_outcome", context),
                f"case[{index}].reopen_outcome",
            ),
            "accepted": accepted,
            "reopen_passes": reopen_passes,
            "resource_outcome": _as_string(
                case.get("resource_outcome", ""), f"case[{index}].resource_outcome"
            ),
            "accepted_prefix": _as_int(
                _require(case, "accepted_prefix", context),
                f"case[{index}].accepted_prefix",
            ),
        }
        by_engine[engine][key].append(record)
        normalized.append(record)
    if not normalized:
        raise ManifestError("manifest.cases must contain at least one case")
    coverage = {}
    for engine, buckets in by_engine.items():
        modes = {mode for mode, _ in buckets}
        coverage[engine] = {
            "case_count": sum(len(records) for records in buckets.values()),
            "schedule": sorted(f"{mode}:{syscall}" for mode, syscall in buckets),
            "complete_case_coverage": bool(buckets) and {"before", "after"} <= modes,
        }
    return normalized, coverage


def _validate_syscall_outcomes(
    cases: list[dict[str, Any]], identity: dict[str, Any]
) -> None:
    """Validate outcome fields that define a complete mutation prefix.

    The source harness records only complete batch prefixes. Without this
    check, two engines could report the same malformed prefix and the pairwise
    comparator would incorrectly call the result equivalent.
    """

    batch_size = identity["batch_size"]
    operations = identity["operations"]
    if batch_size <= 0:
        raise ManifestError("manifest.batch_size must be positive")
    if operations <= 0:
        raise ManifestError("manifest.operations must be positive")
    if operations % batch_size != 0:
        raise ManifestError(
            "manifest.operations must be divisible by manifest.batch_size"
        )

    for index, case in enumerate(cases):
        prefix = case["accepted_prefix"]
        if prefix < 0 or prefix > operations:
            raise ManifestError(
                f"case[{index}].accepted_prefix must be between 0 and operations"
            )
        if prefix % batch_size != 0:
            raise ManifestError(
                f"case[{index}].accepted_prefix must be a complete batch prefix"
            )
        expected_recovery = (
            "complete-new-state" if prefix == operations else "complete-prefix"
        )
        if case["recovery_outcome"] != expected_recovery:
            raise ManifestError(
                f"case[{index}].recovery_outcome does not match accepted_prefix"
            )


def _validate_manifest(
    manifest: Any,
) -> tuple[str, dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    if not isinstance(manifest, dict):
        raise ManifestError("manifest root must be an object")
    manifest_format = _as_string(
        _require(manifest, "format", "manifest"), "manifest.format"
    )
    if manifest_format not in SUPPORTED_FORMATS:
        raise ManifestError(f"unsupported manifest format {manifest_format!r}")
    status = _source_status(manifest)
    manifest_accepted = _as_bool(
        _require(manifest, "accepted", "manifest"), "manifest.accepted"
    )
    if manifest_accepted != (status == "accepted"):
        raise ManifestError("manifest.status and manifest.accepted disagree")
    if status == "unsupported":
        return manifest_format, {}, [], {}
    identity = _shared_identity(manifest)
    if manifest_format == PROCESS_FORMAT:
        cases, coverage = _validate_process_cases(manifest.get("cases"))
    else:
        cases, coverage = _validate_syscall_cases(manifest.get("cases"))
        _validate_syscall_outcomes(cases, identity)
    return manifest_format, identity, cases, coverage


def _engine_records(
    manifest_format: str, cases: list[dict[str, Any]], engine: str
) -> dict[str, Any]:
    records = [case for case in cases if case["engine"] == engine]
    if manifest_format == PROCESS_FORMAT:
        return {
            record["key"]: {
                key: value for key, value in record.items() if key != "engine"
            }
            for record in records
        }
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[record["key"]].append(record)
    return {
        key: {
            "execution_outcomes": sorted(
                {item["execution_outcome"] for item in values}
            ),
            "recovery_outcomes": sorted({item["recovery_outcome"] for item in values}),
            "reopen_outcomes": sorted({item["reopen_outcome"] for item in values}),
            "accepted_case_count": sum(item["accepted"] for item in values),
            "case_count": len(values),
            "reopen_passes": sorted({item["reopen_passes"] for item in values}),
            "resource_outcomes": sorted({item["resource_outcome"] for item in values}),
            "accepted_prefixes": sorted(
                {
                    item.get("accepted_prefix")
                    for item in values
                    if item.get("accepted_prefix") is not None
                }
            ),
        }
        for key, values in grouped.items()
    }


def _engine_contract_status(
    manifest_format: str, records: dict[str, Any], coverage: dict[str, Any]
) -> dict[str, Any]:
    if manifest_format == PROCESS_FORMAT:
        expected = {"old-state", "complete-new-state"}
        complete = set(records) == expected
        valid = all(
            record["accepted"]
            and record["reopen_passes"] == 2
            and record["reopen_outcome"] == "stable-two-reopen"
            and record["requested_prefix"]
            == {"old-state": 64, "complete-new-state": 80}[record["key"]]
            and record["child_exit_code"] == 137
            and record["execution_outcome"] == "terminated-by-sigkill"
            and record["recovery_outcome"] == record["key"]
            for record in records.values()
        )
    else:
        complete = bool(records) and coverage["complete_case_coverage"]
        valid = all(
            record["accepted_case_count"] == record["case_count"]
            and record["reopen_passes"] == [2]
            and record["reopen_outcomes"] == ["stable-two-reopen"]
            and set(record["execution_outcomes"]) <= {"refused", "completed"}
            and set(record["recovery_outcomes"])
            <= {"complete-new-state", "complete-prefix"}
            for record in records.values()
        )
    return {
        "accepted": complete and valid,
        "complete_case_coverage": complete,
        "all_cases_accepted": valid,
        "resource_qualified": all(
            "not-collected"
            not in (record.get("resource_outcomes", [record.get("resource_outcome")]))
            for record in records.values()
        ),
    }


def _logical_comparison_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _logical_comparison_value(item)
            for key, item in value.items()
            if key not in {"resource_outcome", "resource_outcomes"}
        }
    return value


def _compare_pair(
    manifest_format: str,
    left: str,
    right: str,
    cases: list[dict[str, Any]],
    coverage: dict[str, Any],
) -> dict[str, Any]:
    left_records = _engine_records(manifest_format, cases, left)
    right_records = _engine_records(manifest_format, cases, right)
    left_status = _engine_contract_status(manifest_format, left_records, coverage[left])
    right_status = _engine_contract_status(
        manifest_format, right_records, coverage[right]
    )
    keys = sorted(set(left_records) | set(right_records))
    differences = []
    for key in keys:
        left_value = _logical_comparison_value(left_records.get(key, "missing"))
        right_value = _logical_comparison_value(right_records.get(key, "missing"))
        if left_value != right_value:
            differences.append({"case": key, left: left_value, right: right_value})
    equivalent = (
        left_status["accepted"] and right_status["accepted"] and not differences
    )
    if (
        not left_status["complete_case_coverage"]
        or not right_status["complete_case_coverage"]
        or set(left_records) != set(right_records)
    ):
        comparison_status = "incomplete"
    elif equivalent:
        comparison_status = "equivalent"
    else:
        comparison_status = "different-outcomes"
    return {
        "left_engine": left,
        "right_engine": right,
        "status": comparison_status,
        "equivalent": equivalent,
        "individual_contract": {left: left_status, right: right_status},
        "resource_qualified": left_status["resource_qualified"]
        and right_status["resource_qualified"],
        "differences": differences,
    }


def compare_manifest(
    manifest: Any, required_engines: list[str] | None = None
) -> dict[str, Any]:
    """Return a versioned comparison report for one fault manifest."""

    manifest_format, identity, cases, coverage = _validate_manifest(manifest)
    observed_engines = sorted({case["engine"] for case in cases})
    required = sorted(set(required_engines or observed_engines))
    missing = sorted(set(required) - set(observed_engines))
    if manifest["status"] == "unsupported":
        return {
            "format": OUTPUT_FORMAT,
            "source_format": manifest_format,
            "status": "unsupported",
            "accepted": False,
            "resource_qualified": False,
            "required_engines": required,
            "observed_engines": observed_engines,
            "missing_engines": missing,
            "comparisons": [],
            "reason": manifest.get("reason", "source manifest is unsupported"),
        }
    if not identity:
        raise ManifestError(
            "accepted manifest did not produce shared workload identity"
        )
    engine_reports = {
        engine: _engine_contract_status(
            manifest_format,
            _engine_records(manifest_format, cases, engine),
            coverage[engine],
        )
        for engine in observed_engines
    }
    comparisons = [
        _compare_pair(manifest_format, left, right, cases, coverage)
        for index, left in enumerate(required)
        for right in required[index + 1 :]
        if left in engine_reports and right in engine_reports
    ]
    has_pair = (
        len(required) >= 2
        and not missing
        and len(comparisons) == len(required) * (len(required) - 1) // 2
    )
    accepted = (
        not missing
        and has_pair
        and all(report["accepted"] for report in engine_reports.values())
        and all(comparison["equivalent"] for comparison in comparisons)
    )
    return {
        "format": OUTPUT_FORMAT,
        "source_format": manifest_format,
        "status": "accepted"
        if accepted
        else ("incomplete" if missing or not has_pair else "different-outcomes"),
        "accepted": accepted,
        "resource_qualified": all(
            report["resource_qualified"] for report in engine_reports.values()
        ),
        "identity": identity,
        "required_engines": required,
        "observed_engines": observed_engines,
        "missing_engines": missing,
        "engines": engine_reports,
        "comparisons": comparisons,
        "notes": [
            "accepted means logical fault-outcome equivalence only",
            "resource_qualified is false when the source harness recorded resource_outcome=not-collected",
            "failed syscall indexes and observed syscall counts are intentionally excluded from logical equivalence",
        ],
    }


def _parse_required_engines(value: str | None) -> list[str] | None:
    if value is None:
        return None
    engines = [engine.strip() for engine in value.split(",") if engine.strip()]
    if len(set(engines)) != len(engines):
        raise argparse.ArgumentTypeError(
            "--require-engines must not contain duplicates"
        )
    if len(engines) < 2:
        raise argparse.ArgumentTypeError("--require-engines needs at least two engines")
    return engines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument(
        "--output", type=Path, help="write the comparison report to this path"
    )
    parser.add_argument(
        "--require-engines",
        type=_parse_required_engines,
        help="comma-separated engine set required for a complete comparison",
    )
    parser.add_argument(
        "--require-equivalent",
        action="store_true",
        help="exit 1 unless the report is accepted",
    )
    args = parser.parse_args(argv)
    try:
        manifest = json.loads(args.manifest.read_text())
        report = compare_manifest(manifest, args.require_engines)
    except (OSError, json.JSONDecodeError, ManifestError) as error:
        print(f"common_kv_compare: invalid manifest: {error}", file=sys.stderr)
        return 2
    encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        try:
            args.output.write_text(encoded)
        except OSError as error:
            print(f"common_kv_compare: cannot write output: {error}", file=sys.stderr)
            return 2
    else:
        print(encoded, end="")
    if args.require_equivalent and not report["accepted"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
