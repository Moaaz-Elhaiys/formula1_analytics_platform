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
  payload['position'] AS position
FROM
  {{ source('f1_lakehouse', 'position') }}
WHERE
  driver_number IS NOT NULL
