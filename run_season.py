#!/usr/bin/env python3
"""
F1 Season Batch Orchestrator
Discovers all sessions for given years, extracts, and replays them sequentially.

Usage:
    python run_season.py --years 2023 2024 2025
    python run_season.py --years 2025 --session-types Race Sprint
    python run_season.py --years 2023 2024 --speed 50 --skip-existing
    python run_season.py --years 2025 --skip-existing          # resume interrupted batch
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from urllib.parse import urlencode

# Reuse existing modules — no logic duplication
from extractor import (
    ENDPOINTS,
    OPENF1_BASE,
    RateLimiter,
    MAX_REQ_PER_SEC,
    MAX_REQ_PER_MIN,
    extract_session,
    fetch_json,
)
from replayer import replay

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("run_season")

# ---------------------------------------------------------------------------
# Session discovery
# ---------------------------------------------------------------------------


def discover_sessions(
    years: list[int],
    session_types: set[str],
    limiter: RateLimiter,
) -> list[dict]:
    """
    Fetch all sessions for the given years, filter by session type,
    and return a flat sorted list.

    Each entry: {session_key, meeting_key, session_type, date_start, country_name, year}
    """
    all_sessions: list[dict] = []

    for year in years:
        params = urlencode({"year": year})
        url = f"{OPENF1_BASE}/sessions?{params}"

        log.info("Discovering sessions for %d ...", year)
        try:
            sessions = fetch_json(url, limiter)
        except RuntimeError as e:
            log.error("Failed to discover sessions for %d: %s", year, e)
            log.error("Skipping year %d — run individually later if needed", year)
            continue

        # Filter by requested session types
        matching = [s for s in sessions if s.get("session_type") in session_types]

        log.info(
            "  %d: %d total sessions, %d matching %s",
            year, len(sessions), len(matching), sorted(session_types),
        )

        # Attach year for downstream logging
        for s in matching:
            s["year"] = year

        all_sessions.extend(matching)

    # Sort chronologically across all years
    all_sessions.sort(key=lambda s: s.get("date_start", ""))

    return all_sessions


# ---------------------------------------------------------------------------
# Cache completeness check
# ---------------------------------------------------------------------------


def cache_is_complete(session_dir: Path) -> bool:
    """Check if all 9 endpoint files exist and are non-empty JSON arrays."""
    if not session_dir.exists():
        return False

    for endpoint in ENDPOINTS:
        path = session_dir / f"{endpoint}.json"
        if not path.exists():
            return False

        try:
            data = json.loads(path.read_text())
            if not isinstance(data, list):
                return False
            # Zero-record endpoints are acceptable (e.g. no pit in a short race)
        except (json.JSONDecodeError, OSError):
            return False

    # Also check session_info.json exists
    return bool((session_dir / "session_info.json").exists())


# ---------------------------------------------------------------------------
# Batch orchestration
# ---------------------------------------------------------------------------


def run_batch(
    years: list[int],
    session_types: set[str],
    speed: float,
    cache_dir: Path,
    skip_existing: bool,
) -> dict:
    """
    Main batch loop. Returns a summary dict.
    """
    # Shared rate limiter for discovery (extraction creates its own internally,
    # but we want a single limiter per process for discovery calls)
    limiter = RateLimiter(MAX_REQ_PER_SEC, MAX_REQ_PER_MIN)

    # --- Discover sessions ---
    sessions = discover_sessions(years, session_types, limiter)

    if not sessions:
        log.error("No sessions found matching criteria. Nothing to do.")
        return {"attempted": 0, "succeeded": [], "failed": [], "skipped": []}

    # --- Sanity check on count ---
    expected_races = len(years) * 22  # ~22 races per season
    if len(sessions) < expected_races * 0.5:
        log.warning(
            "SANITY: Found only %d sessions across %d years "
            "(expected roughly %d). Calendar may be incomplete or "
            "session types too restrictive.",
            len(sessions), len(years), expected_races,
        )

    log.info("=" * 70)
    log.info("BATCH PLAN")
    log.info("  Years: %s", years)
    log.info("  Session types: %s", sorted(session_types))
    log.info("  Sessions to process: %d", len(sessions))
    log.info("  Speed: %s", f"{speed}x" if speed > 0 else "instant")
    log.info("  Skip existing: %s", skip_existing)
    log.info("=" * 70)

    # --- Process each session ---
    succeeded: list[dict] = []
    failed: list[dict] = []
    skipped: list[dict] = []
    attempted: int = 0

    for i, session in enumerate(sessions, 1):
        session_key = session["session_key"]
        meeting_key = session["meeting_key"]
        country = session.get("country_name", "Unknown")
        year = session.get("year", "?")
        date_start = session.get("date_start", "?")

        session_dir = cache_dir / str(session_key)

        log.info("")
        log.info("─" * 70)
        log.info(
            "[%d/%d] session_key=%d (%s %d, %s)",
            i, len(sessions), session_key, country, year, date_start,
        )
        log.info("─" * 70)

        # --- Skip if cache exists and flag is set ---
        if skip_existing and cache_is_complete(session_dir):
            log.info("  Cache already complete — skipping extraction")
            skipped.append({"session_key": session_key, "country": country, "year": year})
            # Still replay? No — if we're skipping, assume replay was done too.
            # User can re-run individually if needed.
            continue

        # --- Extract ---
        attempted += 1
        try:
            extract_summary = extract_session(
                session_key=session_key,
                meeting_key=meeting_key,
                cache_dir=cache_dir,
            )
        except SystemExit:
            # extract_session calls sys.exit(1) on failure
            log.error("  EXTRACTION FAILED — skipping replay for this session")
            failed.append({"session_key": session_key, "country": country, "year": year})
            continue
        except Exception as e:
            log.error("  EXTRACTION FAILED: %s — skipping replay", e)
            failed.append({"session_key": session_key, "country": country, "year": year})
            continue

        # --- Replay ---
        replay_run_id = f"run-{session_key}-batch"
        try:
            replay_summary = replay(
                session_dir=session_dir,
                session_key=session_key,
                replay_run_id=replay_run_id,
                speed=speed,
                dry_run=False,
            )

            # Check for failures in replay
            if replay_summary.get("failed", 0) > 0:
                log.warning(
                    "  REPLAY completed with %d failed messages",
                    replay_summary["failed"],
                )

            succeeded.append({
                "session_key": session_key,
                "country": country,
                "year": year,
                "total_published": replay_summary.get("total_published", 0),
                "failed_messages": replay_summary.get("failed", 0),
            })

            # One-line summary per session
            log.info(
                "  ✓ session_key=%d (%s %d): %d published, %d failed msgs",
                session_key, country, year,
                replay_summary.get("total_published", 0),
                replay_summary.get("failed", 0),
            )

        except Exception as e:
            log.error("  REPLAY FAILED: %s", e)
            failed.append({"session_key": session_key, "country": country, "year": year})

    # --- Final summary ---
    log.info("")
    log.info("=" * 70)
    log.info("BATCH COMPLETE")
    log.info("=" * 70)
    log.info("  Total sessions discovered: %d", len(sessions))
    log.info("  Attempted:                 %d", attempted)
    log.info("  Succeeded:                 %d", len(succeeded))
    log.info("  Skipped (cache exists):    %d", len(skipped))
    log.info("  Failed:                    %d", len(failed))
    log.info("=" * 70)

    if succeeded:
        log.info("")
        log.info("Successful sessions:")
        for s in succeeded:
            log.info(
                "  ✓ %6d  %s %d  %5d msgs  %d failed",
                s["session_key"], s["country"], s["year"],
                s["total_published"], s["failed_messages"],
            )

    if skipped:
        log.info("")
        log.info("Skipped (cache existed):")
        for s in skipped:
            log.info("  → %6d  %s %d", s["session_key"], s["country"], s["year"])

    log.info("")
    if failed:
        _extracted_from_run_batch_158(failed, speed)
    else:
        log.info("No failures — all sessions processed successfully.")

    return {
        "attempted": attempted,
        "succeeded": succeeded,
        "failed": failed,
        "skipped": skipped,
        "total_discovered": len(sessions),
    }


# TODO Rename this here and in `run_batch`
def _extracted_from_run_batch_158(failed, speed):
    log.warning("FAILED sessions (re-run individually):")
    for s in failed:
        log.warning(
            "  ✗ %6d  %s %d",
            s["session_key"], s["country"], s["year"],
        )
    log.warning("")
    log.warning("To re-run a failed session individually:")
    log.warning("  uv run python extractor.py --session-key %s", failed[0]["session_key"])
    log.warning("  uv run python replayer.py --session-key %s --speed %s", failed[0]["session_key"], speed)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Batch process multiple F1 sessions across seasons."
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        required=True,
        help="Years to process (e.g., 2023 2024 2025)",
    )
    parser.add_argument(
        "--session-types",
        type=str,
        nargs="+",
        default=["Race"],
        help="Session types to include (default: Race only). "
             "Options: Race, Sprint, Qualifying, Practice",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=50.0,
        help="Replay speed multiplier (default: 50)",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("cache"),
        help="Base cache directory (default: ./cache)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip extraction if cache is already complete for a session",
    )
    args = parser.parse_args()

    # Validate years
    current_year = 2026
    for y in args.years:
        if y < 2023 or y > current_year:
            log.error("Year %d is out of range (2023–%d)", y, current_year)
            sys.exit(1)

    session_types = set(args.session_types)
    valid_types = {"Race", "Sprint", "Qualifying", "Practice"}
    if invalid := session_types - valid_types:
        log.error("Invalid session types: %s. Valid: %s", invalid, valid_types)
        sys.exit(1)

    log.info("=" * 70)
    log.info("F1 SEASON BATCH ORCHESTRATOR")
    log.info("  Years:         %s", args.years)
    log.info("  Session types: %s", sorted(session_types))
    log.info("  Speed:         %sx", args.speed)
    log.info("  Cache dir:     %s", args.cache_dir)
    log.info("  Skip existing: %s", args.skip_existing)
    log.info("=" * 70)

    summary = run_batch(
        years=args.years,
        session_types=session_types,
        speed=args.speed,
        cache_dir=args.cache_dir,
        skip_existing=args.skip_existing,
    )

    # Exit with non-zero if any session failed
    if summary["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()