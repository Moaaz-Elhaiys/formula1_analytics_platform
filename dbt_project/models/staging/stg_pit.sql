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
  payload['lap_number'] AS lap_number,
  payload['pit_duration'] AS pit_duration,
  payload['date'] AS date
FROM
  {{ source('f1_lakehouse', 'pit') }}
WHERE
  driver_number IS NOT NULL
