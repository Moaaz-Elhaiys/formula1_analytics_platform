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
  payload['stint_number'] AS stint_number,
  payload['compound'] AS compound,
  payload['lap_start'] AS lap_start,
  payload['lap_end'] AS lap_end,
  payload['tyre_age_at_start'] AS tyre_age_at_start
FROM
  {{ source('f1_lakehouse', 'stints') }}
WHERE
  driver_number IS NOT NULL
