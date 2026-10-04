#!/usr/bin/env python3
"""
F1 Pipeline Validator
Checks data consistency between local extraction cache, Kafka replay logs,
and final Eventhouse tables.

Read-only: this script never writes, deletes, or modifies Eventhouse data.

Usage:
    python validate_pipeline.py --session-keys 9877 9159
    python validate_pipeline.py --session-keys 9523
    python validate_pipeline.py                          # all sessions in cache
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

# Reuse existing modules — no logic duplication
from extractor import ENDPOINTS
from replayer import dedupe, load_cache

# Kusto SDK
try:
    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
    KUSTO_AVAILABLE = True
except ImportError:
    KUSTO_AVAILABLE = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# OpenF1 endpoint name → Eventhouse table name
# NOTE: "weather" endpoint maps to table literally named "wheather" (typo)
ENDPOINT_TO_TABLE = {
    "laps": "laps",
    "position": "position",
    "intervals": "intervals",
    "session_result": "session_result",
    "pit": "pit",
    "stints": "stints",
    "race_control": "race_control",
    "weather": "wheather",
    "drivers": "drivers",
}

# Envelope fields to verify in sample records
ENVELOPE_FIELDS = [
    "schema_version",
    "source_endpoint",
    "meeting_key",
    "session_key",
    "event_time",
    "ingest_time",
    "replay_run_id",
]

# driver_number can be null only for these endpoints
NULLABLE_DRIVER_NUMBER = {"race_control", "weather"}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("validate")

# ---------------------------------------------------------------------------
# Kusto connection and query helpers
# ---------------------------------------------------------------------------


def connect_eventhouse() -> tuple[KustoClient, str]:
    """
    Create a Kusto client from environment variables.

    Auth methods (in order of preference):
    1. AAD app registration (AZURE_CLIENT_ID + AZURE_CLIENT_SECRET + AZURE_TENANT_ID)
    2. Device code auth (interactive, if app vars not set)
    """
    cluster_uri = os.environ.get("EVENTHOUSE_CLUSTER_URI")
    database = os.environ.get("EVENTHOUSE_DATABASE")

    if not cluster_uri:
        raise EnvironmentError(
            "EVENTHOUSE_CLUSTER_URI not set. "
            "Example: https://trd-xxxxx.z16.kusto.fabric.microsoft.com"
        )
    if not database:
        raise EnvironmentError("EVENTHOUSE_DATABASE not set")

    # Try app registration auth first
    client_id = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")
    tenant_id = os.environ.get("AZURE_TENANT_ID")

    if client_id and client_secret and tenant_id:
        kcsb = KustoConnectionStringBuilder.with_aad_app_key(
            cluster_uri, client_id, client_secret, tenant_id
        )
        log.info("Connecting to Eventhouse via AAD app registration")
    else:
        kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(cluster_uri)
        log.info("Connecting to Eventhouse via device code auth (interactive)")

    client = KustoClient(kcsb)
    return client, database


def run_query(client: KustoClient, database: str, query: str) -> list[dict]:
    """Execute a KQL query and return rows as list of dicts."""
    try:
        response = client.execute(database, query)
        table = response.primary_results[0]
        column_names = [col.column_name for col in table.columns]
        rows = []
        for row in table:
            rows.append({name: row[name] for name in column_names})
        return rows
    except Exception as e:
        raise RuntimeError(f"KQL query failed: {e}\nQuery was:\n{query}") from e


# ---------------------------------------------------------------------------
# Eventhouse query functions
# ---------------------------------------------------------------------------


def get_replay_run_ids(
    client: KustoClient,
    database: str,
    table: str,
    session_key: int,
) -> list[str]:
    """Get all distinct replay_run_ids for a session from a table."""
    query = f"""
{table}
| where session_key == {session_key}
| distinct replay_run_id
| order by replay_run_id desc
"""
    try:
        rows = run_query(client, database, query)
        return [r["replay_run_id"] for r in rows]
    except RuntimeError as e:
        if "cannot be found" in str(e).lower() or "table" in str(e).lower():
            log.warning("  Table '%s' not found or not accessible", table)
            return []
        raise


def get_table_count(
    client: KustoClient,
    database: str,
    table: str,
    session_key: int,
    replay_run_id: str,
) -> int:
    """Count rows in a table for a given session and replay_run_id."""
    query = f"""
{table}
| where session_key == {session_key}
| where replay_run_id == "{replay_run_id}"
| count
"""
    rows = run_query(client, database, query)
    if not rows:
        return 0
    # The count column is typically named "Count"
    for key in ("Count", "count"):
        if key in rows[0]:
            return int(rows[0][key])
    # Fallback: get the first value
    return int(list(rows[0].values())[0])


def get_sample_records(
    client: KustoClient,
    database: str,
    table: str,
    session_key: int,
    replay_run_id: str,
    total_count: int,
) -> list[dict]:
    """
    Get 3 sample records (first, middle, last by event_time).

    Returns fewer if table has < 3 records.
    """
    if total_count == 0:
        return []

    # Determine which positions to sample
    if total_count == 1:
        positions = [1]
    elif total_count == 2:
        positions = [1, 2]
    else:
        mid = total_count // 2
        positions = [1, mid, total_count]

    pos_str = ", ".join(str(p) for p in positions)

    query = f"""
{table}
| where session_key == {session_key}
| where replay_run_id == "{replay_run_id}"
| order by event_time asc
| serialize
| extend _rn = row_number()
| where _rn in ({pos_str})
| project-away _rn
"""
    return run_query(client, database, query)


# ---------------------------------------------------------------------------
# Envelope verification
# ---------------------------------------------------------------------------


def verify_envelope(record: dict, endpoint: str) -> list[str]:
    """
    Verify all envelope fields are present and non-null where expected.

    Returns a list of issue descriptions (empty = no issues).
    """
    issues: list[str] = []

    # Check envelope fields
    for field in ENVELOPE_FIELDS:
        if field not in record:
            issues.append(f"missing field '{field}'")
        elif record[field] is None:
            if field == "driver_number" and endpoint in NULLABLE_DRIVER_NUMBER:
                pass  # driver_number can be null for these endpoints
            else:
                issues.append(f"field '{field}' is null")

    # Check driver_number separately (it can be null for certain endpoints)
    if "driver_number" in record:
        dn = record["driver_number"]
        if dn is None and endpoint not in NULLABLE_DRIVER_NUMBER:
            issues.append(
                f"driver_number is null but endpoint '{endpoint}' expects non-null"
            )

    # Check payload
    payload = record.get("payload")
    if payload is None:
        issues.append("payload is null")
    elif isinstance(payload, str):
        # Payload stored as JSON string — try to parse
        try:
            parsed = json.loads(payload)
            if isinstance(parsed, dict) and len(parsed) == 0:
                issues.append("payload is an empty object after parsing")
        except json.JSONDecodeError:
            issues.append("payload is a string but not valid JSON")
    elif isinstance(payload, dict):
        if len(payload) == 0:
            issues.append("payload is an empty object")

    return issues


# ---------------------------------------------------------------------------
# Session validation
# ---------------------------------------------------------------------------


def validate_session(
    client: KustoClient,
    database: str,
    session_key: int,
    cache_dir: Path,
) -> dict:
    """
    Validate one session across all 9 tables.

    Returns a summary dict with per-table results.
    """
    session_dir = cache_dir / str(session_key)

    if not session_dir.exists():
        log.error("  Cache directory not found: %s", session_dir)
        return {"error": "cache_dir_not_found", "tables": {}, "duplicates": False}

    # --- Load and dedupe cache (reuse replayer logic) ---
    log.info("  Loading cache and deduplicating ...")
    session_info, raw_cache = load_cache(session_dir)
    deduped = dedupe(raw_cache)

    # Expected counts: 0 for endpoints with no cache data
    expected_counts = {ep: 0 for ep in ENDPOINTS}
    for ep, records in deduped.items():
        expected_counts[ep] = len(records)

    # --- Validate each table ---
    table_results: dict[str, dict] = {}
    session_duplicates: list[str] = []  # replay_run_ids if >1 found

    for endpoint in ENDPOINTS:
        table_name = ENDPOINT_TO_TABLE[endpoint]
        expected = expected_counts[endpoint]

        log.debug("  Checking %s (table: %s) ...", endpoint, table_name)

        # --- Get replay_run_ids from Eventhouse ---
        try:
            replay_run_ids = get_replay_run_ids(client, database, table_name, session_key)
        except RuntimeError as e:
            log.error("  ✗ %s: Failed to query table — %s", endpoint, e)
            table_results[endpoint] = {
                "table": table_name,
                "expected": expected,
                "actual": -1,  # sentinel for query failure
                "match": False,
                "difference": "QUERY_FAILED",
                "envelope_issues": ["query failed"],
            }
            continue

        if not replay_run_ids:
            # No data in Eventhouse for this table
            actual = 0
            table_results[endpoint] = {
                "table": table_name,
                "expected": expected,
                "actual": actual,
                "match": expected == actual,
                "difference": actual - expected,
                "envelope_issues": [],
            }
            if expected > 0:
                log.warning(
                    "  ✗ %s: expected=%d actual=0 (no data in Eventhouse)",
                    endpoint, expected,
                )
            continue

        # --- Check for duplicate replay_run_ids ---
        if len(replay_run_ids) > 1:
            session_duplicates.extend(replay_run_ids)
            log.warning(
                "  DUPLICATE replay_run_ids in %s: %s",
                table_name, replay_run_ids,
            )

        # Use the most recent replay_run_id (first in desc order)
        replay_run_id = replay_run_ids[0]

        # --- Get actual count ---
        try:
            actual = get_table_count(client, database, table_name, session_key, replay_run_id)
        except RuntimeError as e:
            log.error("  ✗ %s: Failed to count — %s", endpoint, e)
            table_results[endpoint] = {
                "table": table_name,
                "expected": expected,
                "actual": -1,
                "match": False,
                "difference": "COUNT_FAILED",
                "envelope_issues": ["count query failed"],
            }
            continue

        match = expected == actual
        difference = actual - expected

        # --- Sample and verify envelope ---
        envelope_issues: list[str] = []
        if actual > 0:
            try:
                samples = get_sample_records(
                    client, database, table_name,
                    session_key, replay_run_id, actual,
                )
                for i, sample in enumerate(samples):
                    issues = verify_envelope(sample, endpoint)
                    if issues:
                        envelope_issues.extend(
                            f"sample[{i}]: {issue}" for issue in issues
                        )
            except RuntimeError as e:
                envelope_issues.append(f"sample query failed: {e}")

        table_results[endpoint] = {
            "table": table_name,
            "expected": expected,
            "actual": actual,
            "match": match,
            "difference": difference,
            "envelope_issues": envelope_issues,
        }

        # Log per-table result
        symbol = "✓" if match else "✗"
        if match and not envelope_issues:
            log.info(
                "  %s %s: expected=%-6d actual=%-6d",
                symbol, endpoint, expected, actual,
            )
        else:
            if not match:
                log.warning(
                    "  ✗ %s: expected=%-6d actual=%-6d diff=%+d",
                    endpoint, expected, actual, difference,
                )
            if envelope_issues:
                for issue in envelope_issues:
                    log.warning("    ENVELOPE: %s", issue)

    return {
        "session_key": session_key,
        "tables": table_results,
        "duplicates": list(set(session_duplicates)),  # dedupe the list
        "error": None,
    }


# ---------------------------------------------------------------------------
# Summary reporting
# ---------------------------------------------------------------------------


def print_session_summary(result: dict) -> None:
    """Print a formatted table for one session's validation results."""
    if result.get("error"):
        log.error("  SESSION ERROR: %s", result["error"])
        return

    session_key = result["session_key"]
    tables = result["tables"]

    log.info("")
    log.info("  Session %d Results:", session_key)
    log.info("  %-18s %-8s %-8s %-6s %-8s %s",
             "Table", "Expected", "Actual", "Match", "Diff", "Envelope")
    log.info("  %-18s %-8s %-8s %-6s %-8s %s",
             "─" * 18, "─" * 8, "─" * 8, "─" * 6, "─" * 8, "─" * 20)

    for endpoint in ENDPOINTS:
        if endpoint not in tables:
            continue
        t = tables[endpoint]
        match_str = "✓" if t["match"] else "✗"
        env_str = "OK" if not t["envelope_issues"] else "ISSUES"
        log.info(
            "  %-18s %-8d %-8d %-6s %-8s %s",
            endpoint, t["expected"], t["actual"],
            match_str, t["difference"], env_str,
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate F1 pipeline data consistency (cache → Kafka → Eventhouse)."
    )
    parser.add_argument(
        "--session-keys",
        type=int,
        nargs="+",
        default=None,
        help="Session keys to validate (default: all in cache dir)",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("cache"),
        help="Base cache directory (default: ./cache)",
    )
    args = parser.parse_args()

    # --- Check Kusto SDK ---
    if not KUSTO_AVAILABLE:
        log.error(
            "azure-kusto-data not installed. "
            "Add it with: uv add azure-kusto-data"
        )
        sys.exit(1)

    # --- Connect to Eventhouse ---
    try:
        client, database = connect_eventhouse()
    except EnvironmentError as e:
        log.error("%s", e)
        sys.exit(1)
    except Exception as e:
        log.error("Failed to connect to Eventhouse: %s", e)
        sys.exit(1)

    # --- Discover sessions to validate ---
    if args.session_keys:
        session_keys = args.session_keys
    else:
        # Find all session directories in cache
        if not args.cache_dir.exists():
            log.error("Cache directory not found: %s", args.cache_dir)
            sys.exit(1)
        session_keys = sorted(
            int(d.name) for d in args.cache_dir.iterdir()
            if d.is_dir() and d.name.isdigit()
        )

    if not session_keys:
        log.error("No sessions to validate")
        sys.exit(1)

    log.info("=" * 70)
    log.info("F1 PIPELINE VALIDATOR")
    log.info("  Sessions to validate: %d", len(session_keys))
    log.info("  Cache directory: %s", args.cache_dir)
    log.info("  Eventhouse database: %s", database)
    log.info("=" * 70)

    # --- Validate each session ---
    all_results: list[dict] = []
    total_mismatches: list[dict] = []
    total_duplicate_sessions: list[int] = []

    for session_key in session_keys:
        log.info("")
        log.info("Validating session %d ...", session_key)

        result = validate_session(client, database, session_key, args.cache_dir)
        all_results.append(result)

        print_session_summary(result)

        # Collect mismatches
        if result.get("error"):
            total_mismatches.append({
                "session_key": session_key,
                "table": "ALL",
                "expected": "?",
                "actual": "?",
                "issue": result["error"],
            })
        else:
            for endpoint, t in result.get("tables", {}).items():
                if not t["match"]:
                    total_mismatches.append({
                        "session_key": session_key,
                        "table": t["table"],
                        "expected": t["expected"],
                        "actual": t["actual"],
                        "issue": f"diff={t['difference']}",
                    })
                if t["envelope_issues"]:
                    total_mismatches.append({
                        "session_key": session_key,
                        "table": t["table"],
                        "expected": t["expected"],
                        "actual": t["actual"],
                        "issue": "; ".join(t["envelope_issues"][:3]),
                    })

            # Collect duplicate sessions
            if result.get("duplicates"):
                total_duplicate_sessions.append(session_key)

    # --- Final summary ---
    total_tables_checked = sum(
        len(r.get("tables", {})) for r in all_results
    )

    log.info("")
    log.info("=" * 70)
    log.info("VALIDATION SUMMARY")
    log.info("=" * 70)
    log.info("  Sessions validated:    %d", len(all_results))
    log.info("  Tables checked:        %d", total_tables_checked)
    log.info("  Mismatches found:      %d", len(total_mismatches))
    log.info("  Duplicate sessions:    %d", len(total_duplicate_sessions))
    log.info("=" * 70)

    if total_mismatches:
        log.warning("")
        log.warning("MISMATCHES:")
        for m in total_mismatches:
            log.warning(
                "  session=%d table=%s expected=%s actual=%s — %s",
                m["session_key"], m["table"],
                m["expected"], m["actual"], m["issue"],
            )

    if total_duplicate_sessions:
        log.warning("")
        log.warning("SESSIONS WITH DUPLICATE replay_run_ids:")
        for sk in total_duplicate_sessions:
            log.warning("  session_key=%d", sk)

    # --- Final verdict ---
    if total_mismatches or total_duplicate_sessions:
        log.error("")
        log.error("RESULT: FAIL")
        sys.exit(1)
    else:
        log.info("")
        log.info("RESULT: PASS — all counts match, no duplicates, envelopes intact")
        sys.exit(0)


if __name__ == "__main__":
    main()