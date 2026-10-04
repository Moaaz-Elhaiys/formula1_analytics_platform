#!/usr/bin/env python3
"""
F1 Kafka Replayer
Reads cached OpenF1 session data and publishes to Confluent Cloud Kafka
at a controllable speed.

Usage:
    python replayer.py --session-key 9877 --speed 1.0
    python replayer.py --session-key 9877 --speed 10.0
    python replayer.py --session-key 9877 --speed 0          # instant
    python replayer.py --session-key 9877 --dry-run         # no Kafka, just log
"""

from __future__ import annotations
import argparse
import contextlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

try:
    from confluent_kafka import Producer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

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

TOPIC_MAP = {
    "laps":          "f1.timing.laps",
    "position":      "f1.timing.position",
    "intervals":     "f1.timing.intervals",
    "session_result": "f1.timing.session_result",
    "pit":           "f1.strategy.pit",
    "stints":        "f1.strategy.stints",
    "race_control":  "f1.race.control",
    "weather":       "f1.env.weather",
    "drivers":       "f1.ref.drivers",
}

# Which field holds the event timestamp (None = use session start time)
TIMESTAMP_FIELD = {
    "laps":          "date_start",
    "position":      "date",
    "intervals":     "date",
    "pit":           "date",
    "race_control":  "date",
    "weather":       "date",
    "session_result": None,
    "stints":        None,
    "drivers":       None,
}

# Topics where messages are keyed by session_key instead of driver_number
SESSION_WIDE_TOPICS = {"race_control", "weather"}

# Natural key extractors for dedup
# Each returns a tuple that uniquely identifies a record within its endpoint
NATURAL_KEYS = {
    "laps":          lambda r: (r.get("driver_number"), r.get("lap_number")),
    "position":      lambda r: (r.get("date"), r.get("driver_number")),
    "intervals":     lambda r: (r.get("date"), r.get("driver_number")),
    "session_result": lambda r: (r.get("driver_number"),),
    "pit":           lambda r: (r.get("driver_number"), r.get("lap_number")),
    "stints":        lambda r: (r.get("driver_number"), r.get("stint_number")),
    "race_control":  lambda r: (r.get("date"), r.get("category"), r.get("flag")),
    "weather":       lambda r: (r.get("date"),),
    "drivers":       lambda r: (r.get("driver_number"),),
}

SCHEMA_VERSION = 1

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("replayer")

# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------


def parse_timestamp(ts: str) -> datetime | None:
    """
    Parse an OpenF1 ISO 8601 timestamp string.

    Returns a timezone-aware datetime, or None if parsing fails.
    """
    if not ts or not isinstance(ts, str):
        return None

    # Python 3.11+ handles all ISO 8601 variants
    with contextlib.suppress(ValueError, TypeError):
        return _extracted_from_parse_timestamp_(ts)
    # Fallback: handle 'Z' suffix (Python < 3.11)
    with contextlib.suppress(ValueError, TypeError):
        cleaned = ts.rstrip("Z") + "+00:00" if ts.endswith("Z") else ts
        return _extracted_from_parse_timestamp_(cleaned)
    # Fallback: strptime
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            dt = datetime.strptime(ts, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            continue

    return None


# TODO Rename this here and in `parse_timestamp`
def _extracted_from_parse_timestamp_(arg0):
    dt = datetime.fromisoformat(arg0)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def iso_now() -> str:
    """Current UTC time as ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Cache loading
# ---------------------------------------------------------------------------


def load_cache(session_dir: Path) -> tuple[dict, dict]:
    """
    Load session info and all endpoint caches.

    Returns (session_info, {endpoint: [records]}) — skips missing/empty files.
    """
    # Load session metadata
    info_path = session_dir / "session_info.json"
    if not info_path.exists():
        raise FileNotFoundError(
            f"session_info.json not found in {session_dir}. "
            "Run extractor.py first."
        )
    session_info = json.loads(info_path.read_text())

    # Load endpoint data
    cache: dict[str, list] = {}
    for endpoint in ENDPOINTS:
        path = session_dir / f"{endpoint}.json"
        if not path.exists():
            log.warning("Cache file missing for /%s — skipping", endpoint)
            continue

        try:
            records = json.loads(path.read_text())
        except json.JSONDecodeError as e:
            log.error("Cache file for /%s is corrupt: %s — skipping", endpoint, e)
            continue

        if not isinstance(records, list):
            log.error("Cache file for /%s is not a JSON array — skipping", endpoint)
            continue

        if len(records) == 0:
            log.info("Cache file for /%s is empty (0 records) — will skip", endpoint)
            continue

        cache[endpoint] = records
        log.info("Loaded /%s: %d raw records", endpoint, len(records))

    return session_info, cache


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------


def dedupe(cache: dict[str, list]) -> dict[str, list]:
    """Remove duplicate records by natural key within each endpoint."""
    deduped: dict[str, list] = {}

    for endpoint, records in cache.items():
        key_fn = NATURAL_KEYS[endpoint]
        seen: set = set()
        unique: list = []
        dup_count = 0

        for record in records:
            key = key_fn(record)
            if key in seen:
                dup_count += 1
                continue
            seen.add(key)
            unique.append(record)

        if dup_count > 0:
            log.info("  /%s: removed %d duplicates (%d → %d)",
                     endpoint, dup_count, len(records), len(unique))

        deduped[endpoint] = unique

    return deduped


# ---------------------------------------------------------------------------
# Timeline building
# ---------------------------------------------------------------------------


def build_timeline(
    session_info: dict,
    cache: dict[str, list],
) -> list[dict]:
    """
    Merge all records into a single timeline sorted by event timestamp.

    Each timeline entry: {"endpoint": str, "record": dict, "event_time": datetime}
    """
    session_start = parse_timestamp(session_info.get("date_start", ""))

    if session_start is None:
        # Fall back to first available timestamp in the cache
        for endpoint in ENDPOINTS:
            if endpoint not in cache:
                continue
            ts_field = TIMESTAMP_FIELD[endpoint]
            if ts_field is None:
                continue
            for record in cache[endpoint]:
                dt = parse_timestamp(record.get(ts_field, ""))
                if dt is not None:
                    session_start = dt
                    log.warning(
                        "Could not parse session date_start — "
                        "using first record timestamp: %s", dt.isoformat()
                    )
                    break
            if session_start is not None:
                break

    if session_start is None:
        session_start = datetime.now(timezone.utc)
        log.warning(
            "No usable timestamps found — defaulting session start to now: %s",
            session_start.isoformat(),
        )

    timeline: list[dict] = []
    skipped_no_ts = 0

    for endpoint, records in cache.items():
        ts_field = TIMESTAMP_FIELD[endpoint]

        for record in records:
            if ts_field is not None:
                raw_ts = record.get(ts_field)
                if raw_ts is None or raw_ts == "":
                    log.debug(
                        "  /%s: record missing timestamp field '%s' — skipping",
                        endpoint, ts_field,
                    )
                    skipped_no_ts += 1
                    continue

                event_time = parse_timestamp(raw_ts)
                if event_time is None:
                    log.warning(
                        "  /%s: unparseable timestamp '%s' — skipping record",
                        endpoint, raw_ts,
                    )
                    skipped_no_ts += 1
                    continue
            else:
                # Endpoint lacks a timestamp field — use session start
                event_time = session_start

            timeline.append({
                "endpoint": endpoint,
                "record": record,
                "event_time": event_time,
            })

    if skipped_no_ts > 0:
        log.warning("Skipped %d records with null/missing timestamps", skipped_no_ts)

    # Sort by event_time (stable sort preserves original order for ties)
    timeline.sort(key=lambda entry: entry["event_time"])

    log.info(
        "Timeline built: %d events spanning %s to %s",
        len(timeline),
        timeline[0]["event_time"].isoformat() if timeline else "N/A",
        timeline[-1]["event_time"].isoformat() if timeline else "N/A",
    )

    return timeline


# ---------------------------------------------------------------------------
# Envelope construction
# ---------------------------------------------------------------------------


def build_envelope(
    entry: dict,
    meeting_key: int,
    session_key: int,
    replay_run_id: str,
) -> dict:
    """Wrap a timeline entry in the shared envelope structure."""
    endpoint = entry["endpoint"]
    record = entry["record"]
    event_time = entry["event_time"]

    driver_number = record.get("driver_number")  # may be present in race_control
    # Ensure it's either an int or None (not a float or string)
    if driver_number is not None:
        try:
            driver_number = int(driver_number)
        except (ValueError, TypeError):
            driver_number = None

    return {
        "schema_version": SCHEMA_VERSION,
        "source_endpoint": f"/{endpoint}",
        "meeting_key": meeting_key,
        "session_key": session_key,
        "driver_number": driver_number,
        "event_time": event_time.isoformat(),
        "ingest_time": iso_now(),
        "replay_run_id": replay_run_id,
        "payload": record,
    }


# ---------------------------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------------------------


def create_producer() -> Producer:
    """Create and configure a Confluent Cloud producer from environment variables."""
    required_env = [
        "CONFLUENT_BOOTSTRAP_SERVERS",
        "CONFLUENT_API_KEY",
        "CONFLUENT_API_SECRET",
    ]
    if missing := [v for v in required_env if not os.getenv(v)]:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}. "
            "Set them before running the replayer."
        )

    conf = {
        "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["CONFLUENT_API_KEY"],
        "sasl.password": os.environ["CONFLUENT_API_SECRET"],
        # Reliability settings
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "delivery.timeout.ms": 120000,
        # Performance — FIX 1: sized for instant replay bursts
        "linger.ms": 5,
        "batch.size": 32768,                        # was 16384
        "queue.buffering.max.messages": 500000,      # was default 100000
        "queue.buffering.max.kbytes": 2097152,       # 2 GB (was default 1048589)
        "compression.type": "snappy",
    }

    return Producer(conf)


def publish_message(
    producer: Producer,
    topic: str,
    key: str,
    value: dict,
    delivery_callbacks: list,
) -> None:
    """Publish a single message with a delivery-report callback."""
    payload = json.dumps(value, separators=(",", ":")).encode("utf-8")

    def _on_delivery(err, msg):
        if err is not None:
            delivery_callbacks.append(("error", topic, key, str(err)))
        else:
            delivery_callbacks.append(("ok", topic, key, msg.topic()))

    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=payload,
        on_delivery=_on_delivery,
    )
    producer.poll(0)  # Non-blocking delivery check


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------


def replay(
    session_dir: Path,
    session_key: int,
    replay_run_id: str,
    speed: float,
    dry_run: bool = False,
) -> dict:    # sourcery skip: extract-method, for-index-underscore, low-code-quality
    """
    Execute the replay. Returns a summary dict.
    """
    # --- Load and prepare data ---
    session_info, raw_cache = load_cache(session_dir)
    deduped = dedupe(raw_cache)
    timeline = build_timeline(session_info, deduped)

    meeting_key = session_info["meeting_key"]

    # --- Validate counts ---
    log.info("=" * 60)
    log.info("REPLAY PREPARATION")
    log.info("  Session: %d | Meeting: %d", session_key, meeting_key)
    log.info("  Replay run ID: %s", replay_run_id)
    log.info("  Speed: %s", f"{speed}x" if speed > 0 else "instant")
    log.info("  Messages to publish: %d", len(timeline))
    log.info("=" * 60)

    expected_counts = {ep: len(records) for ep, records in deduped.items()}
    for ep in ENDPOINTS:
        if ep not in deduped:
            log.info("  /%s: SKIPPED (empty or missing)", ep)
        else:
            log.info("  /%s: %d messages", ep, expected_counts[ep])

    # --- Setup producer ---
    producer = None
    delivery_callbacks: list = []

    if not dry_run:
        producer = create_producer()
        log.info("Kafka producer connected to Confluent Cloud")

        # Warm up connection — force metadata fetch for all topics
        log.info("Warming up Kafka connection...")
        for topic_name in TOPIC_MAP.values():
            producer.poll(0)
        producer.flush(30)  # Wait for metadata to propagate
        log.info("Kafka connection warm — ready to produce")

    else:
        log.info("DRY RUN — no messages will be published to Kafka")

    # --- Walk timeline ---
    published: dict[str, int] = {ep: 0 for ep in ENDPOINTS}
    failed: int = 0
    total_published: int = 0
    wall_start = time.monotonic()
    last_log_time = time.monotonic()
    last_event_time: datetime | None = None

    log.info("Starting replay...")

    for i, entry in enumerate(timeline):
        endpoint = entry["endpoint"]
        record = entry["record"]
        event_time = entry["event_time"]

        # --- Pacing ---
        if speed > 0 and last_event_time is not None:
            delta = (event_time - last_event_time).total_seconds()
            delay = delta / speed
            if delay > 0:
                if delay > 300:
                    log.warning(
                        "Large gap detected: %.1fs (sim time jump to %s)",
                        delay, event_time.isoformat(),
                    )
                time.sleep(delay)

        last_event_time = event_time

        # --- Build envelope ---
        envelope = build_envelope(entry, meeting_key, session_key, replay_run_id)

        # --- Determine topic and key ---
        topic = TOPIC_MAP[endpoint]
        if endpoint in SESSION_WIDE_TOPICS:
            key = str(session_key)
        else:
            dn = record.get("driver_number")
            key = str(dn) if dn is not None else str(session_key)

        # --- Publish ---
        if dry_run:
            if i < 3 or i % 500 == 0:
                log.info(
                    "  [DRY] #%d → %s (key=%s, event_time=%s)",
                    i + 1, topic, key, event_time.isoformat(),
                )
            published[endpoint] += 1
            total_published += 1
        else:
            # FIX 1: Retry loop with poll-drain instead of single retry
            max_publish_attempts = 10
            max_poll_wait_sec = 30

            for attempt in range(1, max_publish_attempts + 1):
                try:
                    publish_message(producer, topic, key, envelope, delivery_callbacks)
                    published[endpoint] += 1
                    total_published += 1
                    break

                except BufferError:
                    # Queue full — drain and retry
                    log.warning(
                        "  Buffer overflow on %s (attempt %d/%d) — draining queue",
                        topic, attempt, max_publish_attempts,
                    )
                    producer.poll(max_poll_wait_sec)

                    if attempt == max_publish_attempts:
                        log.error(
                            "  FAILED to publish #%d to %s after %d attempts",
                            i + 1, topic, max_publish_attempts,
                        )
                        failed += 1

                except Exception as e:
                    log.error("Failed to publish message #%d to %s: %s", i + 1, topic, e)
                    failed += 1
                    break

        # --- Periodic progress log ---
        now = time.monotonic()
        if now - last_log_time >= 30 or (i + 1) % 500 == 0:
            elapsed = now - wall_start
            sim_elapsed = (event_time - timeline[0]["event_time"]).total_seconds()
            log.info(
                "Progress: %d/%d messages | sim time: %s | wall: %.1fs | %s",
                i + 1, len(timeline),
                event_time.isoformat(),
                elapsed,
                " | ".join(f"{ep}:{published[ep]}" for ep in ENDPOINTS if published[ep] > 0),
            )
            last_log_time = now

    # --- Flush remaining messages ---
    if producer is not None:
        log.info("Flushing remaining messages...")
        remaining = producer.flush(30)
        if remaining > 0:
            log.warning("%d messages still in queue after flush timeout", remaining)

    # --- Collect delivery results ---
    if delivery_callbacks:
        if errors := [cb for cb in delivery_callbacks if cb[0] == "error"]:
            failed += len(errors)
            for _, topic, key, err in errors[:10]:
                log.error("Delivery error on %s (key=%s): %s", topic, key, err)

    wall_elapsed = time.monotonic() - wall_start

    # --- Summary ---
    log.info("=" * 60)
    log.info("REPLAY COMPLETE")
    log.info("  Replay run ID: %s", replay_run_id)
    log.info("  Session: %d | Meeting: %d", session_key, meeting_key)
    log.info("  Speed: %s", f"{speed}x" if speed > 0 else "instant")
    log.info("  Wall-clock time: %.2f seconds", wall_elapsed)
    log.info("  Total published: %d", total_published)
    log.info("  Failed: %d", failed)
    log.info("=" * 60)
    log.info("Messages per topic:")
    for ep in ENDPOINTS:
        expected = expected_counts.get(ep, 0)
        actual = published[ep]
        match = "✓" if expected == actual else "✗ MISMATCH"
        log.info("  %-28s expected=%-5d published=%-5d %s",
                 TOPIC_MAP[ep], expected, actual, match)
    log.info("=" * 60)

    if failed > 0:
        log.warning("Replay completed with %d failures", failed)

    return {
        "replay_run_id": replay_run_id,
        "session_key": session_key,
        "meeting_key": meeting_key,
        "speed": speed,
        "total_published": total_published,
        "failed": failed,
        "per_topic": {TOPIC_MAP[ep]: published[ep] for ep in ENDPOINTS},
        "wall_time_sec": wall_elapsed,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:  # sourcery skip: for-index-underscore
    parser = argparse.ArgumentParser(
        description="Replay cached OpenF1 session data to Confluent Cloud Kafka."
    )
    parser.add_argument(
        "--session-key",
        type=int,
        required=True,
        help="OpenF1 session_key (must have been extracted)",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("cache"),
        help="Base cache directory (default: ./cache)",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="Replay speed multiplier (1.0=real-time, 10.0=10x, 0=instant). Default: 1.0",
    )
    parser.add_argument(
        "--replay-run-id",
        type=str,
        default=None,
        help="Replay run ID (auto-generated if not provided)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log messages without publishing to Kafka",
    )
    args = parser.parse_args()

    session_dir = args.cache_dir / str(args.session_key)

    if not session_dir.exists():
        log.error(
            "Cache directory not found: %s\nRun extractor.py --session-key %d first.",
            session_dir, args.session_key,
        )
        sys.exit(1)

    # Generate replay_run_id if not provided
    if args.replay_run_id:
        replay_run_id = args.replay_run_id
    else:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        replay_run_id = f"run-{args.session_key}-{ts}"

    # Validate speed
    if args.speed < 0:
        log.error("Speed must be >= 0 (got %s)", args.speed)
        sys.exit(1)

    replay(
        session_dir=session_dir,
        session_key=args.session_key,
        replay_run_id=replay_run_id,
        speed=args.speed,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()