#!/usr/bin/env python3
"""
F1 OpenF1 API Extractor
Pulls session data from OpenF1 and caches locally as raw JSON arrays.

Usage:
    python extractor.py --session-key 9877
    python extractor.py --session-key 9877 --meeting-key 1272
    python extractor.py --session-key 9877 --cache-dir ./cache
"""
from __future__ import annotations
import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OPENF1_BASE = "https://api.openf1.org/v1"

ENDPOINTS = [
    "laps",
    "position",
    "intervals",
    "session_result",
    "pit",
    "stints",
    "race_control",
    "weather",
    "drivers",
]

# Rate limits (free tier)
MAX_REQ_PER_SEC = 3
MAX_REQ_PER_MIN = 30

# Retry policy
MAX_RETRIES = 5
BASE_BACKOFF_SEC = 2.0

# Timeout per request
REQUEST_TIMEOUT_SEC = 30

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("extractor")

# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------


class RateLimiter:
    """Sliding-window rate limiter for requests/sec and requests/min."""

    def __init__(self, per_sec: int, per_min: int):
        self._per_sec = per_sec
        self._per_min = per_min
        self._sec_window: list[float] = []
        self._min_window: list[float] = []

    def acquire(self) -> None:
        """Block until a request slot is available."""
        while True:
            now = time.monotonic()

            # Purge expired entries
            self._sec_window = [t for t in self._sec_window if now - t < 1.0]
            self._min_window = [t for t in self._min_window if now - t < 60.0]

            # Check if we can proceed
            if len(self._sec_window) < self._per_sec and len(self._min_window) < self._per_min:
                self._sec_window.append(now)
                self._min_window.append(now)
                return

            # Calculate wait time
            if len(self._sec_window) >= self._per_sec:
                wait = 1.0 - (now - self._sec_window[0])
            else:
                wait = 60.0 - (now - self._min_window[0])

            wait = max(wait, 0.05)  # minimum 50ms between checks
            log.debug("Rate limit: waiting %.2fs", wait)
            time.sleep(wait)


# ---------------------------------------------------------------------------
# HTTP client with retry
# ---------------------------------------------------------------------------


def fetch_json(
    url: str,
    limiter: RateLimiter,
    max_retries: int = MAX_RETRIES,
) -> list:
    """
    GET a JSON array from OpenF1 with rate limiting and retry on transient errors.

    Raises RuntimeError on repeated failure.
    """
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        limiter.acquire()

        try:
            req = Request(url, headers={"Accept": "application/json"})
            with urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
                if resp.status != 200:
                    raise HTTPError(url, resp.status, "Non-200 response", {}, None)
                data = json.loads(resp.read().decode("utf-8"))
                if not isinstance(data, list):
                    raise ValueError(f"Expected JSON array, got {type(data).__name__}")
                return data

        except HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                backoff = BASE_BACKOFF_SEC * (2 ** (attempt - 1))
                log.warning(
                    "Attempt %d/%d failed (HTTP %d) — retrying in %.1fs",
                    attempt, max_retries, e.code, backoff,
                )
                last_error = e
                time.sleep(backoff)
                continue
            # Non-transient HTTP error — don't retry
            raise RuntimeError(f"HTTP {e.code} fetching {url}: {e.reason}") from e

        except (URLError, TimeoutError, json.JSONDecodeError, ValueError) as e:
            backoff = BASE_BACKOFF_SEC * (2 ** (attempt - 1))
            log.warning(
                "Attempt %d/%d failed (%s) — retrying in %.1fs",
                attempt, max_retries, type(e).__name__, backoff,
            )
            last_error = e
            time.sleep(backoff)
            continue

    raise RuntimeError(
        f"Failed to fetch {url} after {max_retries} attempts. Last error: {last_error}"
    )


# ---------------------------------------------------------------------------
# Extraction logic
# ---------------------------------------------------------------------------


def fetch_session_info(session_key: int, limiter: RateLimiter) -> dict:
    """Fetch session metadata (meeting_key, date_start, etc.)."""
    params = urlencode({"session_key": session_key})
    url = f"{OPENF1_BASE}/sessions?{params}"
    results = fetch_json(url, limiter)

    if not results:
        raise RuntimeError(f"No session found with session_key={session_key}")

    info = results[0]
    required = {"session_key", "meeting_key", "date_start"}
    if missing := required - set(info.keys()):
        raise RuntimeError(f"Session info missing required fields: {missing}")

    return info


def extract_session(
    session_key: int,
    meeting_key: int | None,
    cache_dir: Path,
) -> dict:
    """
    Extract all 9 endpoints for a session and cache them.

    Returns a summary dict: {endpoint: record_count}
    """
    limiter = RateLimiter(MAX_REQ_PER_SEC, MAX_REQ_PER_MIN)

    # Create cache directory
    session_dir = cache_dir / str(session_key)
    session_dir.mkdir(parents=True, exist_ok=True)

    # --- Fetch session metadata (also confirms session exists) ---
    log.info("Fetching session metadata for session_key=%d ...", session_key)
    session_info = fetch_session_info(session_key, limiter)

    resolved_meeting_key = meeting_key or session_info["meeting_key"]
    if meeting_key and meeting_key != session_info["meeting_key"]:
        log.warning(
            "Provided meeting_key=%d does not match API's meeting_key=%d — using API value",
            meeting_key, session_info["meeting_key"],
        )

    # Cache session info
    info_path = session_dir / "session_info.json"
    info_path.write_text(json.dumps(session_info, indent=2) + "\n")
    log.info(
        "Session: key=%d, meeting_key=%d, type=%s, start=%s",
        session_key, resolved_meeting_key,
        session_info.get("session_type", "?"),
        session_info.get("date_start", "?"),
    )

    # --- Fetch all 9 endpoints ---
    summary: dict[str, int] = {}
    failures: list[str] = []

    for endpoint in ENDPOINTS:
        params = urlencode({"session_key": session_key})
        url = f"{OPENF1_BASE}/{endpoint}?{params}"

        log.info("Fetching /%s ...", endpoint)
        try:
            records = fetch_json(url, limiter)
        except RuntimeError as e:
            log.error("FAILED to fetch /%s: %s", endpoint, e)
            failures.append(endpoint)
            summary[endpoint] = -1
            continue

        out_path = session_dir / f"{endpoint}.json"
        out_path.write_text(json.dumps(records, indent=2) + "\n")
        summary[endpoint] = len(records)
        log.info("  /%s: %d records → %s", endpoint, len(records), out_path)

    # --- Fail loudly if any endpoint failed ---
    if failures:
        log.error(
            "EXTRACTION INCOMPLETE — failed endpoints: %s. "
            "Cache may be partial. Fix the issue and re-run.",
            ", ".join(failures),
        )
        sys.exit(1)

    # --- Sanity check ---
    _sanity_check(session_key, summary)

    return summary


def _sanity_check(session_key: int, summary: dict) -> None:
    """Basic sanity checks on record counts."""
    issues: list[str] = []

    laps = summary.get("laps", 0)
    drivers = summary.get("drivers", 0)
    if laps == 0 and drivers > 0:
        issues.append(f"laps=0 but drivers={drivers} — expected laps > 0 for a race session")

    if drivers == 0:
        issues.append("drivers=0 — expected at least 1 driver record")

    if summary.get("session_result", 0) == 0:
        issues.append("session_result=0 — expected results for a completed session")

    if issues:
        for issue in issues:
            log.warning("SANITY: %s", issue)
        log.warning(
            "Sanity check failed for session_key=%d. Data may be incomplete or "
            "this may not be a race session. Review before replaying.",
            session_key,
        )
    else:
        log.info("Sanity check passed for session_key=%d", session_key)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract OpenF1 session data to local cache."
    )
    parser.add_argument(
        "--session-key",
        type=int,
        required=True,
        help="OpenF1 session_key (e.g., 9877 for a 2025 race)",
    )
    parser.add_argument(
        "--meeting-key",
        type=int,
        default=None,
        help="Optional meeting_key (fetched from /sessions if not provided)",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("cache"),
        help="Base cache directory (default: ./cache)",
    )
    args = parser.parse_args()

    log.info("Starting extraction for session_key=%d", args.session_key)

    summary = extract_session(
        session_key=args.session_key,
        meeting_key=args.meeting_key,
        cache_dir=args.cache_dir,
    )

    log.info("=" * 60)
    log.info("EXTRACTION COMPLETE — session_key=%d", args.session_key)
    log.info("Cache location: %s", args.cache_dir / str(args.session_key))
    log.info("=" * 60)
    log.info("Record counts:")
    for endpoint in ENDPOINTS:
        count = summary.get(endpoint, "?")
        log.info("  %-16s %s", endpoint, count)
    log.info("=" * 60)


if __name__ == "__main__":
    main()