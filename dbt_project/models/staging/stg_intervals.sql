SELECT
  schema_version,
  source_endpoint,
  meeting_key,
  session_key,
  driver_number,
  event_time,
  ingest_time,
  replay_run_id,
  payload,
  -- Nested payload columns
  payload['date'] AS date,
  payload['gap_to_leader'] AS gap_to_leader,
  payload['interval'] AS interval
FROM
  {{ source('f1_lakehouse', 'intervals') }}
WHERE
  driver_number IS NOT NULL
